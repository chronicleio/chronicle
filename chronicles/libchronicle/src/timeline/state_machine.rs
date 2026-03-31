use super::ensemble::select_ensemble;
use crate::conn::conn_pool::ConnPool;
use crate::conn::Watermark;
use crate::error::ChronicleError;
use crate::error_inner::InnerError;
use crate::{Event as UserEvent, Offset, TimelineOptions};
use backoff::future;
use catalog::Catalog;
use chronicle_proto::pb_catalog::{TimelineMeta, UnitInfo};
use chronicle_proto::pb_ext::{
    FenceResponse, RecordEventsRequest, RecordEventsRequestItem, StatusCode,
};
use futures_util::future::join_all;
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
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
    unit_committed: HashMap<String, i64>,
    ensemble: Vec<UnitInfo>,
}

struct InflightBatch {
    max_offset: i64,
    callbacks: Vec<(i64, oneshot::Sender<Result<Offset, ChronicleError>>)>,
}

pub(crate) struct StateMachine {
    record_tx: mpsc::Sender<RecordRequest>,
    cancel: CancellationToken,
    task: tokio::task::JoinHandle<()>,
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
            unit_committed: HashMap::new(),
            ensemble: Vec::new(),
        };

        let max_batch_size = options.max_batch_size;
        let linger = options.linger;
        let cancel = CancellationToken::new();
        let (record_tx, record_rx) = mpsc::channel::<RecordRequest>(options.max_inflight);
        let (wm_tx, wm_rx) = mpsc::channel::<Watermark>(256);
        let (ready_tx, ready_rx) = oneshot::channel();

        let task = tokio::spawn(run(
            state,
            ready_tx,
            record_rx,
            wm_tx,
            wm_rx,
            cancel.clone(),
            max_batch_size,
            linger,
        ));

        let meta = ready_rx
            .await
            .map_err(|_| ChronicleError::Internal("state machine died during init".into()))??;

        Ok((Self { record_tx, cancel, task }, meta))
    }

    pub async fn record(&self, event: UserEvent) -> Result<Offset, ChronicleError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.record_tx
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

    pub async fn stop(self) {
        self.cancel.cancel();
        drop(self.record_tx);
        let _ = self.task.await;
    }
}

async fn init(
    state: &mut State,
    wm_tx: &mpsc::Sender<Watermark>,
) -> Result<(), ChronicleError> {
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

    info!(
        timeline_id = state.meta.timeline_id,
        term = state.meta.term,
        "new term: fencing ensemble"
    );

    let ensemble = state.ensemble.clone();
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

    for unit in &state.ensemble {
        state.unit_committed.insert(unit.address.clone(), lra);
        if let Ok(conn) = state.pool.get_or_connect(&unit.address) {
            conn.subscribe_watermark(state.meta.timeline_id, wm_tx.clone());
        }
    }

    info!(
        timeline_id = state.meta.timeline_id,
        term = state.meta.term,
        lra = lra,
        "new term: complete"
    );

    Ok(())
}

async fn run(
    mut state: State,
    ready_tx: oneshot::Sender<Result<TimelineMeta, ChronicleError>>,
    mut record_rx: mpsc::Receiver<RecordRequest>,
    wm_tx: mpsc::Sender<Watermark>,
    mut wm_rx: mpsc::Receiver<Watermark>,
    cancel: CancellationToken,
    max_batch_size: usize,
    linger: Duration,
) {
    if let Err(e) = init(&mut state, &wm_tx).await {
        let _ = ready_tx.send(Err(e));
        return;
    }
    let _ = ready_tx.send(Ok(state.meta.clone()));

    let mut batch: Vec<RecordRequest> = Vec::with_capacity(max_batch_size);
    let mut inflight: VecDeque<InflightBatch> = VecDeque::new();
    let linger_sleep = tokio::time::sleep(far_future());
    tokio::pin!(linger_sleep);
    let mut linger_active = false;

    loop {
        tokio::select! {
            biased;

            _ = cancel.cancelled() => {
                if !batch.is_empty() {
                    flush_batch(&mut state, &mut batch, &mut inflight).await;
                }
                drain_remaining(&mut state, &mut inflight, &mut wm_rx).await;
                return;
            }

            wm = wm_rx.recv() => {
                match wm {
                    Some(wm) => process_watermark(&mut state, &mut inflight, wm),
                    None => break,
                }
            }

            req = record_rx.recv() => {
                match req {
                    Some(req) => {
                        batch.push(req);
                        drain_records(&mut record_rx, &mut batch, max_batch_size);

                        if batch.len() >= max_batch_size {
                            flush_batch(&mut state, &mut batch, &mut inflight).await;
                            linger_sleep.as_mut().reset(tokio::time::Instant::now() + far_future());
                            linger_active = false;
                        } else if !linger_active {
                            linger_sleep
                                .as_mut()
                                .reset(tokio::time::Instant::now() + linger);
                            linger_active = true;
                        }
                    }
                    None => {
                        if !batch.is_empty() {
                            flush_batch(&mut state, &mut batch, &mut inflight).await;
                        }
                        drain_remaining(&mut state, &mut inflight, &mut wm_rx).await;
                        return;
                    }
                }
            }

            _ = &mut linger_sleep, if linger_active => {
                if !batch.is_empty() {
                    flush_batch(&mut state, &mut batch, &mut inflight).await;
                }
                linger_sleep.as_mut().reset(tokio::time::Instant::now() + far_future());
                linger_active = false;
            }
        }
    }
}

fn drain_records(
    record_rx: &mut mpsc::Receiver<RecordRequest>,
    batch: &mut Vec<RecordRequest>,
    max_batch_size: usize,
) {
    while batch.len() < max_batch_size {
        match record_rx.try_recv() {
            Ok(req) => batch.push(req),
            Err(_) => break,
        }
    }
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
        let fence_futures = ensemble_candidates.into_iter().map(|unit| async move {
            let fence_response = fence_unit(state, &unit).await;
            (unit, fence_response)
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
                                return Err(backoff::Error::transient(
                                    InnerError::UnitNotEnough(
                                        "not enough candidates for ensemble".into(),
                                    ),
                                ));
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

async fn fence_unit(state: &State, unit: &UnitInfo) -> Result<FenceResponse, InnerError> {
    let conn = state.pool.get_or_connect(&unit.address)?;
    let backoff = backoff::ExponentialBackoffBuilder::new()
        .with_max_elapsed_time(Some(state.options.request_timeout))
        .build();
    future::retry_notify(
        backoff,
        || async {
            match conn.fence(state.meta.timeline_id, state.meta.term).await {
                Ok(resp) => Ok(resp),
                Err(e @ InnerError::InvalidTerm { .. }) => Err(backoff::Error::permanent(e)),
                Err(e) => Err(backoff::Error::transient(e)),
            }
        },
        |e, retry_in| {
            warn!(
                unit = ?unit,
                error = %e,
                retry_in = ?retry_in,
                "fence unit failed, retrying"
            );
        },
    )
    .await
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

    for unit in &state.ensemble {
        if let Ok(conn) = state.pool.get_or_connect(&unit.address) {
            if let Err(e) = conn.send_record(request.clone()).await {
                warn!(unit = ?unit, error = %e, "flush_batch: failed to send");
            }
        }
    }

    let callbacks: Vec<_> = offsets.into_iter().zip(replies).collect();
    inflight.push_back(InflightBatch {
        max_offset,
        callbacks,
    });
}

fn process_watermark(
    state: &mut State,
    inflight: &mut VecDeque<InflightBatch>,
    wm: Watermark,
) {
    match wm.result {
        Ok(resp) => {
            if resp.code == StatusCode::Ok as i32 {
                state.unit_committed.insert(wm.endpoint, resp.commit_offset);
                drain_resolved(state, inflight);
            } else if resp.code == StatusCode::Fenced as i32 {
                fail_all_inflight(
                    inflight,
                    ChronicleError::Fenced {
                        timeline_id: resp.timeline_id,
                        term: resp.term,
                    },
                );
            } else {
                fail_all_inflight(
                    inflight,
                    ChronicleError::InvalidTerm {
                        current: resp.term,
                        requested: state.meta.term,
                    },
                );
            }
        }
        Err(e) => {
            warn!(endpoint = %wm.endpoint, error = %e, "unit stream error");
            fail_all_inflight(inflight, e);
        }
    }
}

fn drain_resolved(state: &mut State, inflight: &mut VecDeque<InflightBatch>) {
    let global_synced = state.unit_committed.values().copied().min().unwrap_or(-1);

    while let Some(front) = inflight.front() {
        if front.max_offset <= global_synced {
            let batch = inflight.pop_front().unwrap();
            for (offset, tx) in batch.callbacks {
                let _ = tx.send(Ok(Offset(offset)));
            }
            state.meta.lra = batch.max_offset;
        } else {
            break;
        }
    }
}

fn fail_all_inflight(inflight: &mut VecDeque<InflightBatch>, error: ChronicleError) {
    let msg = error.to_string();
    for batch in inflight.drain(..) {
        for (_, tx) in batch.callbacks {
            let _ = tx.send(Err(ChronicleError::Internal(msg.clone())));
        }
    }
}

async fn drain_remaining(
    state: &mut State,
    inflight: &mut VecDeque<InflightBatch>,
    wm_rx: &mut mpsc::Receiver<Watermark>,
) {
    while !inflight.is_empty() {
        match wm_rx.recv().await {
            Some(wm) => process_watermark(state, inflight, wm),
            None => {
                fail_all_inflight(
                    inflight,
                    ChronicleError::Internal("watermark channel closed".into()),
                );
                break;
            }
        }
    }
}
