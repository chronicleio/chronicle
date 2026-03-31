use super::ensemble::select_ensemble;
use crate::conn::conn_pool::ConnPool;
use crate::error::ChronicleError;
use crate::error_inner::InnerError;
use crate::{Event as UserEvent, Offset, TimelineOptions};
use backoff::future;
use catalog::Catalog;
use chronicle_proto::pb_catalog::{Segment, TimelineMeta, UnitInfo};
use chronicle_proto::pb_ext::{
    RecordEventsRequest, RecordEventsRequestItem,
};
use futures_util::future::{join_all, select_all};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use task::JoinHandle;
use tokio::{sync, task};
use tokio::sync::{mpsc, oneshot, watch};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

const EMPTY_UNITS: VecDeque<UnitInfo> = VecDeque::new();

struct RecordRequest {
    event: UserEvent,
    reply: oneshot::Sender<Result<Offset, ChronicleError>>,
}

struct State {
    name: String,
    meta: TimelineMeta,
    catalog: Arc<Catalog>,
    pool: Arc<ConnPool>,
    options: TimelineOptions,
    lrs: i64,
    needs_trunc: bool,
    ensemble: Vec<UnitInfo>,
    segment_start_offset: i64,
    segment_version: i64,
}

struct InflightBatch {
    max_offset: i64,
    callbacks: Vec<(i64, oneshot::Sender<Result<Offset, ChronicleError>>)>,
}

struct StateMachineInner {
    record_tx: mpsc::Sender<RecordRequest>,
    cancel: CancellationToken,
    task: sync::Mutex<Option<JoinHandle<()>>>,
}

#[derive(Clone)]
pub(crate) struct StateMachine {
    inner: Arc<StateMachineInner>,
}

impl StateMachine {
    pub async fn start(
        name: &str,
        catalog: Arc<Catalog>,
        pool: Arc<ConnPool>,
        options: &TimelineOptions,
    ) -> Result<(Self, TimelineMeta), ChronicleError> {
        let state = State {
            name: name.to_string(),
            meta: TimelineMeta::default(),
            catalog,
            pool,
            options: options.clone(),
            lrs: 0,
            needs_trunc: false,
            ensemble: Vec::new(),
            segment_start_offset: 0,
            segment_version: -1,
        };

        let max_batch_size = options.max_batch_size;
        let linger = options.linger;
        let cancel = CancellationToken::new();
        let (record_tx, record_rx) = mpsc::channel::<RecordRequest>(options.max_inflight);
        let (ready_tx, ready_rx) = oneshot::channel();

        let task = tokio::spawn(run(
            state,
            ready_tx,
            record_rx,
            cancel.clone(),
            max_batch_size,
            linger,
        ));

        let meta = ready_rx
            .await
            .map_err(|_| ChronicleError::Internal("state machine died during init".into()))??;

        Ok((
            Self {
                inner: Arc::new(StateMachineInner {
                    record_tx,
                    cancel,
                    task: sync::Mutex::new(Some(task)),
                }),
            },
            meta,
        ))
    }

    pub async fn record(&self, event: UserEvent) -> Result<Offset, ChronicleError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.inner
            .record_tx
            .send(RecordRequest {
                event,
                reply: reply_tx,
            })
            .await
            .map_err(|_| ChronicleError::Internal("state machine stopped".into()))?;
        reply_rx
            .await
            .map_err(|_| ChronicleError::Internal("state machine dropped".into()))?
    }

    pub async fn stop(&self) {
        self.inner.cancel.cancel();
        if let Some(task) = self.inner.task.lock().await.take() {
            let _ = task.await;
        }
    }
}

async fn init(state: &mut State) -> Result<Vec<(String, watch::Receiver<i64>)>, ChronicleError> {
    state.meta = state
        .catalog
        .tl_new_term(&state.name)
        .await
        .map_err(|e| ChronicleError::Internal(e.to_string()))?;

    let rf = state.options.replication_factor;
    let catalog = state.catalog.clone();
    let writable_segment = state
        .catalog
        .timeline_get_or_init_last_segment(&state.meta.name, || async {
            let units = catalog.list_writable_units().await?;
            select_ensemble(&units, &EMPTY_UNITS, &EMPTY_UNITS).ok_or_else(|| {
                catalog::error::CatalogError::NotFound(format!(
                    "need {} writable units, have {}",
                    rf,
                    units.len()
                ))
            })
        })
        .await
        .map_err(|e| ChronicleError::UnitNotEnough(e.to_string()))?;

    state.ensemble = writable_segment.value.ensemble.clone();
    state.segment_start_offset = writable_segment.value.start_offset;
    state.segment_version = writable_segment.version;

    info!(
        timeline_id = state.meta.timeline_id,
        term = state.meta.term,
        "fencing ensemble"
    );

    let wm_watches = fence_and_watch(state).await?;

    info!(
        timeline_id = state.meta.timeline_id,
        term = state.meta.term,
        lra = state.meta.lra,
        "complete"
    );

    Ok(wm_watches)
}

async fn fence_and_watch(
    state: &mut State,
) -> Result<Vec<(String, watch::Receiver<i64>)>, ChronicleError> {
    let ensemble = state.ensemble.clone();

    info!(
        timeline_id = state.meta.timeline_id,
        term = state.meta.term,
        "fencing ensemble"
    );

    let lra = fence_ensemble(state, &ensemble)
        .await
        .map_err(|e| ChronicleError::Internal(e.to_string()))?;

    if state.meta.lra != lra {
        state.meta.lra = lra;
        state.lrs = lra;
        state.meta = state
            .catalog
            .timeline_update(&state.meta, state.meta.version)
            .await
            .map_err(|e| ChronicleError::Internal(e.to_string()))?;
    }
    state.needs_trunc = lra > 0;

    let mut wm_watches = Vec::new();
    for unit in &state.ensemble {
        if let Ok(conn) = state.pool.get_or_connect(&unit.address) {
            let rx = conn.subscribe_watermark(state.meta.timeline_id, lra);
            wm_watches.push((unit.address.clone(), rx));
        }
    }

    Ok(wm_watches)
}

async fn run(
    mut state: State,
    ready_tx: oneshot::Sender<Result<TimelineMeta, ChronicleError>>,
    mut record_rx: mpsc::Receiver<RecordRequest>,
    cancel: CancellationToken,
    max_batch_size: usize,
    linger: Duration,
) {
    let mut wm_watches = match init(&mut state).await {
        Ok(watches) => watches,
        Err(e) => {
            let _ = ready_tx.send(Err(e));
            return;
        }
    };
    let _ = ready_tx.send(Ok(state.meta.clone()));

    let mut batch: Vec<RecordRequest> = Vec::with_capacity(max_batch_size);
    let mut inflight: VecDeque<InflightBatch> = VecDeque::new();
    let mut linger_tick = tokio::time::interval(linger);

    loop {
        tokio::select! {
            biased;

            _ = cancel.cancelled() => {
                for batch in inflight.drain(..) {
                    for (_, tx) in batch.callbacks {
                        let _ = tx.send(Err(ChronicleError::Canceled));
                    }
                }
                for req in batch.drain(..) {
                    let _ = req.reply.send(Err(ChronicleError::Canceled));
                }
                return;
            }

            lra = wait_watermark_advance(&mut wm_watches) => {
                handle_watermark_changed(&mut state, &mut inflight, lra);
            }

            count = record_rx.recv_many(&mut batch, max_batch_size) => {
                if count == 0 {
                    continue;
                }
                if batch.len() >= max_batch_size {
                    flush_batch(&mut state, &mut batch, &mut inflight, &mut wm_watches).await;
                }
            }

            _ = linger_tick.tick() => {
                if !batch.is_empty() {
                    flush_batch(&mut state, &mut batch, &mut inflight, &mut wm_watches).await;
                }
            }
        }
    }
}

async fn wait_watermark_advance(watches: &mut [(String, watch::Receiver<i64>)]) -> i64 {
    if watches.is_empty() {
        return std::future::pending().await;
    }
    let futs: Vec<_> = watches
        .iter_mut()
        .map(|(_, rx)| Box::pin(rx.changed()))
        .collect();
    let _ = select_all(futs).await;
    watches
        .iter()
        .map(|(_, rx)| *rx.borrow())
        .min()
        .unwrap_or(-1)
}

fn handle_watermark_changed(state: &mut State, inflight: &mut VecDeque<InflightBatch>, lra: i64) {
    while let Some(front) = inflight.front() {
        if front.max_offset > lra {
            break;
        }
        let batch = inflight.pop_front().unwrap();
        for (offset, tx) in batch.callbacks {
            if tx.send(Ok(Offset(offset))).is_err() {
                warn!(offset = offset, "record callback failed");
            }
        }
    }
    state.meta.lra = lra;
}

fn far_future() -> Duration {
    Duration::from_secs(86400)
}

async fn fence_ensemble(state: &State, ensemble: &[UnitInfo]) -> Result<i64, InnerError> {
    let mut lra = 0i64;
    let quarantined_candidates = Mutex::new(VecDeque::new());
    let mut fenced_candidates: VecDeque<UnitInfo> = VecDeque::new();
    let mut ensemble_candidates = ensemble.to_vec();

    loop {
        let fence_futures = ensemble_candidates.into_iter().map(|unit| {
            let pool = state.pool.clone();
            let timeout = state.options.request_timeout;
            let timeline_id = state.meta.timeline_id;
            let term = state.meta.term;
            async move {
                let result = match pool.get_or_connect(&unit.address) {
                    Ok(conn) => conn.fence_with_retry(timeline_id, term, timeout).await,
                    Err(e) => Err(e),
                };
                (unit, result)
            }
        });
        let fence_responses = join_all(fence_futures).await;
        for (unit, result) in fence_responses {
            match result {
                Ok(response) => {
                    if response.lra > lra {
                        lra = response.lra;
                    }
                    fenced_candidates.push_back(unit);
                }
                Err(e @ InnerError::InvalidTerm { .. }) => {
                    return Err(e);
                }
                Err(e) => {
                    warn!(unit = ?unit, error = %e, "fence failed, quarantining unit");
                    quarantined_candidates.lock().unwrap().push_back(unit);
                }
            }
        }
        if fenced_candidates.len() >= ensemble.len() {
            return Ok(lra);
        }
        let backoff = backoff::ExponentialBackoffBuilder::new()
            .with_max_elapsed_time(None)
            .build();
        let new_ensemble = future::retry_notify(
            backoff,
            || async {
                loop {
                    let candidates =
                        state.catalog.list_writable_units().await.map_err(|error| {
                            backoff::Error::transient(InnerError::Catalog(error))
                        })?;
                    let quarantined = quarantined_candidates.lock().unwrap();
                    match select_ensemble(&candidates, &fenced_candidates, &quarantined) {
                        None => {
                            if quarantined.is_empty() {
                                return Err(backoff::Error::transient(InnerError::UnitNotEnough(
                                    "not enough candidates for ensemble".into(),
                                )));
                            }
                            drop(quarantined);
                            quarantined_candidates.lock().unwrap().pop_front();
                            continue;
                        }
                        Some(ensemble) => return Ok(ensemble),
                    }
                }
            },
            |_e, retry_in| {
                warn!(retry_in = ?retry_in, "not enough candidates, retrying");
            },
        )
        .await?;
        ensemble_candidates = new_ensemble
            .into_iter()
            .filter(|unit| !fenced_candidates.contains(unit))
            .collect();
    }
}

fn prepare_batch(
    state: &mut State,
    events: Vec<UserEvent>,
) -> (Vec<RecordEventsRequestItem>, Vec<i64>) {
    let mut items = Vec::with_capacity(events.len());
    let mut offsets = Vec::with_capacity(events.len());

    for event in events {
        let offset = state.lrs + 1;
        state.lrs = offset;

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        let proto_event = chronicle_proto::pb_ext::Event {
            timeline_id: state.meta.timeline_id,
            term: state.meta.term,
            offset,
            payload: Some(event.payload.into()),
            crc32: None,
            timestamp: now,
            schema_id: 0,
        };

        let item = RecordEventsRequestItem {
            event: Some(proto_event),
            trunc: state.needs_trunc,
            lra: state.meta.lra,
        };
        state.needs_trunc = false;

        items.push(item);
        offsets.push(offset);
    }

    (items, offsets)
}

async fn flush_batch(
    state: &mut State,
    batch: &mut Vec<RecordRequest>,
    inflight: &mut VecDeque<InflightBatch>,
    wm_watches: &mut Vec<(String, watch::Receiver<i64>)>,
) {
    let mut events = Vec::with_capacity(batch.len());
    let mut replies = Vec::with_capacity(batch.len());
    for req in batch.drain(..) {
        events.push(req.event);
        replies.push(req.reply);
    }

    let (items, offsets) = prepare_batch(state, events);
    let max_offset = *offsets.last().unwrap();
    let request = RecordEventsRequest { items };

    let futs = state.ensemble.iter().map(|unit| {
        let unit = unit.clone();
        let request = request.clone();
        let pool = state.pool.clone();
        let timeout = state.options.request_timeout;
        async move {
            let result = match pool.get_or_connect(&unit.address) {
                Ok(conn) => conn.send_record_with_retry(request, timeout).await,
                Err(e) => Err(ChronicleError::Transport(e.to_string())),
            };
            (unit, result)
        }
    });
    let results = join_all(futs).await;

    let failed: Vec<UnitInfo> = results
        .into_iter()
        .filter_map(|(unit, result)| {
            if let Err(e) = result {
                warn!(unit = ?unit, error = %e, "flush_batch: send failed after retries");
                Some(unit)
            } else {
                None
            }
        })
        .collect();

    if !failed.is_empty() {
        if let Err(e) = recover_ensemble(state, &failed, wm_watches).await {
            warn!(error = %e, "ensemble recovery failed");
        }
    }

    let callbacks: Vec<_> = offsets.into_iter().zip(replies).collect();
    inflight.push_back(InflightBatch {
        max_offset,
        callbacks,
    });
}

async fn recover_ensemble(
    state: &mut State,
    failed: &[UnitInfo],
    wm_watches: &mut Vec<(String, watch::Receiver<i64>)>,
) -> Result<(), ChronicleError> {
    info!(
        failed_count = failed.len(),
        "recovering ensemble: replacing failed units"
    );

    let failed_deque: VecDeque<UnitInfo> = failed.iter().cloned().collect();
    let healthy: VecDeque<UnitInfo> = state
        .ensemble
        .iter()
        .filter(|u| !failed.contains(u))
        .cloned()
        .collect();

    let candidates = state
        .catalog
        .list_writable_units()
        .await
        .map_err(|e| ChronicleError::Internal(e.to_string()))?;

    let new_ensemble =
        select_ensemble(&candidates, &healthy, &failed_deque).ok_or_else(|| {
            ChronicleError::UnitNotEnough("not enough units to replace failed members".into())
        })?;

    let has_records = state.lrs >= state.segment_start_offset;
    let segment = Segment {
        ensemble: new_ensemble.clone(),
        start_offset: if has_records {
            state.meta.lra + 1
        } else {
            state.segment_start_offset
        },
    };
    let expected_version = if has_records {
        -1 // new segment
    } else {
        state.segment_version // update in place
    };

    let versioned = state
        .catalog
        .put_segment(&state.name, &segment, expected_version)
        .await
        .map_err(|e| ChronicleError::Internal(e.to_string()))?;

    state.ensemble = new_ensemble;
    state.segment_start_offset = segment.start_offset;
    state.segment_version = versioned.version;

    *wm_watches = fence_and_watch(state).await?;

    info!(
        timeline_id = state.meta.timeline_id,
        "ensemble recovery complete"
    );

    Ok(())
}