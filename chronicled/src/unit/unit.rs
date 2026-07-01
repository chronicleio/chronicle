use crate::error::unit_error::UnitError;
use crate::option::unit_options::{ServerOptions, UnitOptions};
use crate::storage::Storage;
use crate::storage::wal::{Wal, WalOptions};
use crate::storage::write_cache::WriteCache;
use crate::unit::timeline_state::TimelineStateManager;
use crate::unit::write_path::{RecordStreamContext, run_record_stream};
use chronicle_proto::pb_ext::chronicle_server::{Chronicle, ChronicleServer};
use chronicle_proto::pb_ext::{
    Event, FenceRequest, FenceResponse, FetchEventsRequest, FetchEventsResponse,
    RecordEventsRequest, RecordEventsResponse, StatusCode,
};
use futures_util::StreamExt;
use prost::Message;
use std::future::Future;
use std::sync::{Arc, Mutex};
use tokio::sync::{Mutex as TokioMutex, mpsc};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::codegen::BoxStream;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};
use tracing::{error, info, warn};

const DEFAULT_INFLIGHT_NUM: usize = 4096;
const RESPONSE_BUFFER: usize = 4;

pub struct Unit {
    inner: Arc<UnitInner>,
    external_handle: Option<Arc<TokioMutex<Option<JoinHandle<()>>>>>,
}

struct UnitInner {
    context: CancellationToken,
    storage: Arc<dyn Storage>,
    write_cache: WriteCache,
    timeline_state: Arc<TimelineStateManager>,
    stream_handles: Mutex<Vec<JoinHandle<()>>>,
    inflight_capacity: usize,
}

impl UnitInner {
    fn spawn_stream<F>(&self, future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let handle = tokio::spawn(future);
        self.stream_handles.lock().unwrap().push(handle);
    }

    async fn shutdown_streams(&self) {
        self.context.cancel();
        loop {
            let handles = {
                let mut handles = self.stream_handles.lock().unwrap();
                if handles.is_empty() {
                    break;
                }
                std::mem::take(&mut *handles)
            };
            for handle in handles {
                if let Err(err) = handle.await {
                    warn!(error = ?err, "unit stream task join error");
                }
            }
        }
    }
}

impl Unit {
    pub async fn new(options: UnitOptions) -> Result<Self, UnitError> {
        info!("unit initializing");
        let context = CancellationToken::new();

        let wal = Wal::new(WalOptions {
            dir: options.wal.dir.clone(),
            max_segment_size: None,
            io_mode: options.io_mode,
        })
        .await?;
        let storage: Arc<dyn Storage> = Arc::new(wal);
        info!(dir = %options.wal.dir, "storage opened");

        let write_cache = WriteCache::new();

        info!("replaying storage into write cache");
        let mut stream = storage.read_stream();
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
                    warn!(error = ?e, "storage replay error reading record");
                    break;
                }
            }
        }
        drop(stream);
        info!(events = replayed, "storage replay complete");

        let timeline_state = Arc::new(TimelineStateManager::new());
        let inner = Arc::new(UnitInner {
            context,
            storage,
            write_cache,
            timeline_state,
            stream_handles: Mutex::new(Vec::new()),
            inflight_capacity: DEFAULT_INFLIGHT_NUM,
        });
        let external_handle = Arc::new(TokioMutex::new(None));
        let unit = Self {
            inner,
            external_handle: Some(external_handle.clone()),
        };

        let handle = bg_start_external_service(options.server.clone(), unit.service());
        *external_handle.lock().await = Some(handle);

        Ok(unit)
    }

    fn service(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            external_handle: None,
        }
    }

    fn record_stream_context(&self) -> RecordStreamContext {
        RecordStreamContext {
            storage: self.inner.storage.clone(),
            write_cache: self.inner.write_cache.clone(),
            timeline_state: self.inner.timeline_state.clone(),
            context: self.inner.context.clone(),
            inflight_capacity: self.inner.inflight_capacity,
        }
    }

    pub async fn stop(self) {
        info!("unit shutting down");

        self.inner.context.cancel();

        if let Some(external_handle) = self.external_handle {
            let handle = external_handle.lock().await.take();
            if let Some(handle) = handle
                && let Err(err) = handle.await
            {
                error!(error = ?err, "unexpected error closing external service");
            }
        }
        self.inner.shutdown_streams().await;
        self.inner.storage.shutdown().await;
        info!("unit stopped");
    }
}

#[tonic::async_trait]
impl Chronicle for Unit {
    type RecordStream = BoxStream<RecordEventsResponse>;

    async fn record(
        &self,
        request: Request<Streaming<RecordEventsRequest>>,
    ) -> Result<Response<Self::RecordStream>, Status> {
        let (tx, rx) = mpsc::channel(RESPONSE_BUFFER);
        self.inner.spawn_stream(run_record_stream(
            request.into_inner(),
            tx,
            self.record_stream_context(),
        ));

        let output_stream = ReceiverStream::new(rx);
        Ok(Response::new(Box::pin(output_stream) as Self::RecordStream))
    }

    type FetchStream = BoxStream<FetchEventsResponse>;

    async fn fetch(
        &self,
        _request: Request<Streaming<FetchEventsRequest>>,
    ) -> Result<Response<Self::FetchStream>, Status> {
        Err(Status::unimplemented("unit read path is disabled"))
    }

    async fn fence(
        &self,
        request: Request<FenceRequest>,
    ) -> Result<Response<FenceResponse>, Status> {
        let req = request.into_inner();
        match self.inner.timeline_state.fence(req.timeline_id, req.term) {
            Ok(lra) => Ok(Response::new(FenceResponse {
                code: StatusCode::Ok.into(),
                lra,
                term: req.term,
            })),
            Err(current_term) => Ok(Response::new(FenceResponse {
                code: StatusCode::Fenced.into(),
                lra: -1,
                term: current_term,
            })),
        }
    }
}

fn bg_start_external_service(options: ServerOptions, unit: Unit) -> JoinHandle<()> {
    let context = unit.inner.context.clone();
    tokio::spawn(async move {
        let (health_reporter, health_service) = tonic_health::server::health_reporter();
        health_reporter.set_serving::<ChronicleServer<Unit>>().await;

        info!(addr = %options.bind_address, "grpc service starting");
        let serve_future = Server::builder()
            .add_service(health_service)
            .add_service(ChronicleServer::new(unit))
            .serve_with_shutdown(options.bind_address, context.cancelled());
        info!("unit ready");
        if let Err(err) = serve_future.await {
            error!(error = %err, "grpc service error");
        }
    })
}
