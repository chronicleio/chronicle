use crate::unit::admin_service::{AdminService, STATE_WRITABLE, STATE_READONLY};
use crate::actor::read_handle_group::ReadHandleGroup;
use crate::actor::write_handle_group::WriteActorGroup;
use crate::error::unit_error::UnitError;
use crate::observability::{self, ServerMetrics};
use crate::option::auto_config::{AutoConfig, SystemEnv};
use crate::option::unit_options::{ServerOptions, UnitOptions};
use crate::storage::blob::compaction::CompactionPipeline;
use crate::storage::blob::manager::SegmentManager;
use crate::storage::level_iterator::LevelIterator;
use crate::storage::index::{Storage, StorageOptions};
use crate::storage::retention::RetentionManager;
use crate::storage::write_cache::WriteCache;
use crate::unit::timeline_state::TimelineStateManager;
use crate::unit::unit_service::UnitService;
use crate::wal::checkpoint;
use crate::wal::wal::{Wal, WalOptions};
use catalog::Catalog;
use chronicle_proto::pb_catalog::{UnitInfo, UnitRegistration, UnitStatus};
use chronicle_proto::pb_ext::Event;
use chronicle_proto::pb_admin::admin_server::AdminServer;
use chronicle_proto::pb_ext::chronicle_server::ChronicleServer;
use futures_util::StreamExt;
use prost::Message;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tonic::transport::Server;
use tracing::{error, info, warn};

const DEFAULT_ACTOR_NUM: usize = 4;
const DEFAULT_INFLIGHT_NUM: usize = 4096;

const DISK_LOW_WATERMARK_PCT: f64 = 5.0;
const DISK_HIGH_WATERMARK_PCT: f64 = 10.0;

pub struct Unit {
    context: CancellationToken,
    external_handle: JoinHandle<()>,
    health_handle: JoinHandle<()>,
    compaction_pipeline: CompactionPipeline,
    retention_manager: Option<RetentionManager>,
    wal: Wal,
    catalog: Arc<Catalog>,
    address: String,
    zone: String,
    _meter_provider: opentelemetry_sdk::metrics::SdkMeterProvider,
    _observable_gauges: Vec<opentelemetry::metrics::ObservableGauge<u64>>,
}

impl Unit {
    pub async fn new(
        options: UnitOptions,
        catalog: Arc<Catalog>,
    ) -> Result<Self, UnitError> {
        info!("unit initializing");
        let context = CancellationToken::new();

        let env = SystemEnv::detect();
        let auto = AutoConfig::from_env_with_io(&env, options.io_mode);

        let (meter_provider, meter, prometheus_registry) = observability::init_meter_provider();
        let metrics = Arc::new(ServerMetrics::new(&meter));
        observability::set_global_metrics(metrics.clone());

        let resolved_compaction = options.compaction.resolve(&auto);
        let resolved_index = options.index.resolve(&auto);

        let storage = Storage::new(StorageOptions {
            path: options.storage.dir.clone(),
            index: Some(resolved_index),
        })?;
        info!(path = %options.storage.dir, "storage index opened");

        let mut observable_gauges = observability::register_rocksdb_gauges(&meter, storage.clone());
        observable_gauges.push(observability::register_disk_usage_gauge(&meter, vec![
            ("wal".to_string(), options.wal.dir.clone()),
            ("index".to_string(), options.storage.dir.clone()),
            ("blob".to_string(), options.segments.dir.clone()),
        ]));
        observable_gauges.push(observability::register_disk_capacity_gauge(&meter, options.storage.dir.clone()));

        let wal = Wal::new(WalOptions {
            dir: options.wal.dir.clone(),
            max_segment_size: None,
            io_mode: options.io_mode,
        })
        .await?;
        info!(dir = %options.wal.dir, "wal opened");

        let capacity = resolved_compaction.write_cache_capacity_mb * 1024 * 1024;
        let write_cache = WriteCache::new(capacity);

        let remote_store: Option<Arc<dyn crate::storage::blob::remote::RemoteStore>> =
            if let Some(ref offload_opts) = resolved_compaction.offload {
                let s3 = crate::storage::blob::remote::S3RemoteStore::new(
                    offload_opts.bucket.clone(),
                    offload_opts.prefix.clone(),
                    offload_opts.endpoint.clone(),
                    offload_opts.region.clone(),
                )
                .await;
                Some(Arc::new(s3))
            } else {
                None
            };

        let segments_dir = PathBuf::from(&options.segments.dir);
        let segment_manager = Arc::new(SegmentManager::recover_with_remote(
            segments_dir,
            options.io_mode,
            remote_store.clone(),
            64,
            storage.clone(),
        )?);
        info!(dir = %options.segments.dir, "segment manager recovered");

        let wal_checkpoint = checkpoint::read_checkpoint(&storage);
        info!(checkpoint_segment = wal_checkpoint.segment_id, "wal checkpoint loaded");

        info!("replaying wal into write cache");
        let mut stream = wal.read_stream_from(wal_checkpoint.segment_id).await;
        let mut replayed = 0u64;
        while let Some(result) = stream.next().await {
            match result {
                Ok(data) => {
                    if let Ok(event) = Event::decode(data.as_slice()) {
                        write_cache.put_direct(event, false);
                        replayed += 1;
                    }
                }
                Err(e) => {
                    warn!(error = ?e, "wal replay error reading record");
                    break;
                }
            }
        }
        drop(stream);
        info!(events = replayed, "wal replay complete");

        let merged_reader = LevelIterator::new(
            write_cache.clone(),
            storage.clone(),
            segment_manager.clone(),
        );

        let timeline_state = Arc::new(TimelineStateManager::new());

        let write_group = Arc::new(WriteActorGroup::new(
            DEFAULT_ACTOR_NUM,
            DEFAULT_INFLIGHT_NUM,
            wal.clone(),
            write_cache.clone(),
            timeline_state.clone(),
        ));
        let read_group = Arc::new(ReadHandleGroup::new(
            DEFAULT_ACTOR_NUM,
            DEFAULT_INFLIGHT_NUM,
            merged_reader,
        ));

        let compaction_pipeline = CompactionPipeline::spawn(
            write_cache,
            segment_manager.clone(),
            storage.clone(),
            context.clone(),
            Duration::from_millis(resolved_compaction.interval_ms),
            resolved_compaction.l1_compaction_trigger,
            resolved_compaction.l2_compaction_trigger,
            remote_store,
            Some(wal.clone()),
        );
        info!(
            interval_ms = resolved_compaction.interval_ms,
            "compaction pipeline started"
        );

        let retention_manager = options.retention.ttl_hours.map(|ttl_hours| {
            let ttl_ms = ttl_hours as i64 * 3600 * 1000;
            let interval = Duration::from_secs(options.retention.interval_secs);
            info!(ttl_hours, interval_secs = options.retention.interval_secs, "retention manager started");
            RetentionManager::spawn(
                segment_manager.clone(),
                storage.clone(),
                context.clone(),
                ttl_ms,
                interval,
            )
        });

        let unit_state = Arc::new(AtomicU8::new(STATE_WRITABLE));

        let unit_service = UnitService::new(write_group, read_group, timeline_state.clone(), unit_state.clone(), metrics);

        let admin_service = AdminService {
            wal: wal.clone(),
            segment_manager,
            index: storage.clone(),
            state: unit_state.clone(),
        };

        let address = options
            .server
            .advertise_address
            .clone()
            .unwrap_or_else(|| format!("http://{}", options.server.bind_address));

        let external_handle = bg_start_external_service(
            options.server.clone(),
            context.clone(),
            unit_service,
            admin_service,
            prometheus_registry,
        );

        let health_handle = bg_health_monitor(
            context.clone(),
            unit_state,
            options.storage.dir.clone(),
            catalog.clone(),
            address.clone(),
            options.server.zone.clone(),
        );

        let registration = UnitRegistration {
            unit: Some(UnitInfo {
                id: address.clone(),
                address: address.clone(),
            }),
            status: UnitStatus::Writable.into(),
            zone: options.server.zone.clone(),
            cpu_usage: 0.0,
            memory_usage: 0.0,
            disk_usage: 0.0,
        };
        let mut last_err = None;
        for attempt in 0..10 {
            let cat_start = std::time::Instant::now();
            let cat_attrs = [opentelemetry::KeyValue::new("op", "register_unit")];
            match catalog.register_unit(&registration).await {
                Ok(()) => {
                    if let Some(m) = observability::global_metrics() {
                        m.catalog_operations.add(1, &cat_attrs);
                        m.catalog_latency.record(cat_start.elapsed().as_secs_f64(), &cat_attrs);
                    }
                    info!(address = %address, "unit registered in catalog");
                    last_err = None;
                    break;
                }
                Err(e) => {
                    if let Some(m) = observability::global_metrics() {
                        m.catalog_operations.add(1, &cat_attrs);
                        m.catalog_errors.add(1, &cat_attrs);
                        m.catalog_latency.record(cat_start.elapsed().as_secs_f64(), &cat_attrs);
                    }
                    warn!(attempt, error = %e, "catalog registration failed, retrying");
                    last_err = Some(e);
                    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                }
            }
        }
        if let Some(e) = last_err {
            return Err(UnitError::Unavailable(format!("catalog registration failed after retries: {}", e)));
        }

        Ok(Self {
            context,
            external_handle,
            health_handle,
            compaction_pipeline,
            retention_manager,
            wal,
            catalog,
            address,
            zone: options.server.zone.clone(),
            _meter_provider: meter_provider,
            _observable_gauges: observable_gauges,
        })
    }

    pub async fn stop(self) {
        info!("unit shutting down");

        if let Err(err) = self.catalog.unregister_unit(&self.address, &self.zone).await {
            warn!(error = ?err, address = %self.address, "failed to unregister unit from catalog");
        } else {
            info!(address = %self.address, "unit unregistered from catalog");
        }

        self.wal.cancel();

        self.context.cancel();

        self.compaction_pipeline.shutdown().await;
        if let Some(retention) = self.retention_manager {
            retention.shutdown().await;
        }
        if let Err(err) = self.health_handle.await {
            error!(error = ?err, "unexpected error closing health monitor");
        }
        if let Err(err) = self.external_handle.await {
            error!(error = ?err, "unexpected error closing external service");
        }
        info!("unit stopped");
    }
}

fn bg_start_external_service(
    options: ServerOptions,
    context: CancellationToken,
    unit_service: UnitService,
    admin_service: AdminService,
    prometheus_registry: prometheus::Registry,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let (health_reporter, health_service) = tonic_health::server::health_reporter();
        health_reporter
            .set_serving::<ChronicleServer<UnitService>>()
            .await;

        let metrics_addr = std::net::SocketAddr::new(
            options.bind_address.ip(),
            options.bind_address.port() + 1,
        );
        let metrics_context = context.clone();
        tokio::spawn(async move {
            serve_prometheus(metrics_addr, prometheus_registry, metrics_context).await;
        });
        info!(addr = %metrics_addr, "prometheus metrics endpoint started");

        info!(addr = %options.bind_address, "grpc service starting");
        let serve_future = Server::builder()
            .add_service(health_service)
            .add_service(AdminServer::new(admin_service))
            .add_service(ChronicleServer::new(unit_service))
            .serve_with_shutdown(options.bind_address, context.cancelled());
        info!("unit ready");
        if let Err(err) = serve_future.await {
            error!(error = %err, "grpc service error");
        }
    })
}

fn bg_health_monitor(
    context: CancellationToken,
    state: Arc<AtomicU8>,
    data_dir: String,
    catalog: Arc<Catalog>,
    address: String,
    zone: String,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut sys = sysinfo::System::new();
        let mut ticker = tokio::time::interval(Duration::from_secs(30));
        loop {
            tokio::select! {
                _ = context.cancelled() => {
                    info!("health monitor stopped");
                    break;
                }
                _ = ticker.tick() => {
                    report_load(&mut sys, &state, &data_dir, &catalog, &address, &zone).await;
                }
            }
        }
    })
}

/// Compute system load and re-register with current CPU/memory/disk usage.
/// Also handles disk watermark transitions (WRITABLE <-> READONLY).
async fn report_load(
    sys: &mut sysinfo::System,
    state: &AtomicU8,
    data_dir: &str,
    catalog: &Arc<Catalog>,
    address: &str,
    zone: &str,
) {
    // CPU: refresh and compute average across all cores (0.0–1.0)
    sys.refresh_cpu_usage();
    let cpu_usage = if sys.cpus().is_empty() {
        0.0
    } else {
        let total: f32 = sys.cpus().iter().map(|c| c.cpu_usage()).sum();
        (total / sys.cpus().len() as f32 / 100.0) as f64
    };

    // Memory: used / total (0.0–1.0)
    sys.refresh_memory();
    let memory_usage = if sys.total_memory() == 0 {
        0.0
    } else {
        sys.used_memory() as f64 / sys.total_memory() as f64
    };

    // Disk: used / total (0.0–1.0) via statvfs
    let disk_usage = disk_usage_ratio(data_dir);

    // Disk watermark transitions
    let available_pct = (1.0 - disk_usage) * 100.0;
    let current = state.load(Ordering::Relaxed);

    if current == STATE_WRITABLE && available_pct < DISK_LOW_WATERMARK_PCT {
        warn!(
            available_pct = format!("{:.1}", available_pct),
            "disk low — switching to READONLY"
        );
        state.store(STATE_READONLY, Ordering::Relaxed);
    } else if current == STATE_READONLY && available_pct > DISK_HIGH_WATERMARK_PCT {
        info!(
            available_pct = format!("{:.1}", available_pct),
            "disk recovered — switching to WRITABLE"
        );
        state.store(STATE_WRITABLE, Ordering::Relaxed);
    }

    let status = if state.load(Ordering::Relaxed) == STATE_WRITABLE {
        UnitStatus::Writable
    } else {
        UnitStatus::Readonly
    };

    let reg = UnitRegistration {
        unit: Some(UnitInfo {
            id: address.to_string(),
            address: address.to_string(),
        }),
        status: status.into(),
        zone: zone.to_string(),
        cpu_usage,
        memory_usage,
        disk_usage,
    };
    if let Err(e) = catalog.register_unit(&reg).await {
        warn!(error = ?e, "failed to report load to catalog");
    }
}

fn disk_usage_ratio(data_dir: &str) -> f64 {
    let c_path = match std::ffi::CString::new(data_dir) {
        Ok(p) => p,
        Err(_) => return 0.0,
    };
    let mut stat: libc::statvfs = unsafe { std::mem::zeroed() };
    let ret = unsafe { libc::statvfs(c_path.as_ptr(), &mut stat) };
    if ret != 0 || stat.f_blocks == 0 {
        return 0.0;
    }
    let total = stat.f_blocks as u64 * stat.f_frsize as u64;
    let available = stat.f_bavail as u64 * stat.f_frsize as u64;
    if total == 0 {
        return 0.0;
    }
    1.0 - (available as f64 / total as f64)
}

async fn serve_prometheus(
    addr: std::net::SocketAddr,
    registry: prometheus::Registry,
    context: CancellationToken,
) {
    use hyper::service::service_fn;
    use hyper_util::rt::TokioIo;
    use http_body_util::Full;

    let listener = match tokio::net::TcpListener::bind(addr).await {
        Ok(l) => l,
        Err(e) => {
            error!(error = %e, addr = %addr, "failed to bind prometheus endpoint");
            return;
        }
    };

    loop {
        tokio::select! {
            _ = context.cancelled() => break,
            accepted = listener.accept() => {
                let (stream, _) = match accepted {
                    Ok(a) => a,
                    Err(_) => continue,
                };
                let registry = registry.clone();
                tokio::spawn(async move {
                    let svc = service_fn(move |_req| {
                        let registry = registry.clone();
                        async move {
                            let encoder = prometheus::TextEncoder::new();
                            let metric_families = registry.gather();
                            let body = encoder.encode_to_string(&metric_families)
                                .unwrap_or_default();
                            Ok::<_, hyper::Error>(
                                hyper::Response::builder()
                                    .header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
                                    .body(Full::new(hyper::body::Bytes::from(body)))
                                    .unwrap()
                            )
                        }
                    });
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), svc)
                        .await;
                });
            }
        }
    }
}
