use super::cursor::EventStream;
use super::ensemble::select_ensemble;
use crate::conn::conn_pool::ConnPool;
use crate::conn::{Conn, Watermark};
use crate::error::ChronicleError;
use crate::error_inner::InnerError;
use crate::{Event as UserEvent, FetchOptions, Offset, StartPosition, TimelineOptions};
use backoff::future;
use catalog::{Catalog, Versioned};
use chronicle_proto::pb_catalog::{Segment, TimelineMeta, UnitInfo};
use chronicle_proto::pb_ext::{
    FenceResponse, RecordEventsRequest, RecordEventsRequestItem, StatusCode,
};
use futures_util::future::join_all;
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{RwLock, mpsc, oneshot};
use tokio::time::Instant;
use tracing::{info, warn};

const EMPTY_UNITS: VecDeque<UnitInfo> = VecDeque::new();

enum Mode {
    ReadWrite {
        event_tx: Option<mpsc::Sender<PendingEvent>>,
        loop_task: Option<tokio::task::JoinHandle<()>>,
    },
    ReadOnly,
}

// ---------------------------------------------------------------------------
// Event loop state — owned by the spawned write task, no mutexes needed
// ---------------------------------------------------------------------------

struct LoopState {
    timeline_id: i64,
    term: i64,
    lrs: i64,
    lra: i64,
    needs_trunc: bool,
    catalog: Arc<Catalog>,
    pool: Arc<ConnPool>,
    timeline_name: String,
    conns: HashMap<String, Conn>,
    unit_committed: HashMap<String, i64>,
}

struct InflightBatch {
    max_offset: i64,
    callbacks: Vec<(i64, oneshot::Sender<Result<Offset, ChronicleError>>)>,
}

struct PendingEvent {
    event: UserEvent,
    tx: oneshot::Sender<Result<Offset, ChronicleError>>,
}

// ---------------------------------------------------------------------------
// Timeline
// ---------------------------------------------------------------------------

pub struct Timeline {
    #[allow(dead_code)]
    options: TimelineOptions,
    catalog: Arc<Catalog>,
    pool: Arc<ConnPool>,
    meta: TimelineMeta,
}

impl Timeline {
    async fn fence_ensemble(
        &self,
        timeline_id: i64,
        term: i64,
        ensemble: &[UnitInfo],
    ) -> Result<i64, InnerError> {
        let mut lra = 0i64;
        let quarantined_candidates = Mutex::new(VecDeque::new());
        let mut fenced_candidates: VecDeque<UnitInfo> = VecDeque::new();
        let mut ensemble_candidates = ensemble.to_vec();

        loop {
            let fence_futures = ensemble_candidates.into_iter().map(|unit| async move {
                let fence_response = self.fence_unit(timeline_id, &unit, term).await;
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
                        // unexpected invalid term, we should force client quit.
                        return Err(e);
                    }
                    Err(e) => {
                        warn!(unit = ?unit, error = %e, "fence failed, quarantining unit");
                        quarantined_candidates.lock().unwrap().push_back(unit)
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
                            self.catalog.list_writable_units().await.map_err(|error| {
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

    pub async fn open(
        catalog: Arc<Catalog>,
        pool: Arc<ConnPool>,
        name: &str,
        options: TimelineOptions,
    ) -> Result<Self, ChronicleError> {
        let tc = catalog.tl_new_term(name).await?;

        let writable_segment = catalog
            .timeline_get_or_init_last_segment(&tc.name, || async move {
                let units = &catalog.list_writable_units().await?;
                let ensemble =
                    select_ensemble(&units, &EMPTY_UNITS, &EMPTY_UNITS).ok_or_else(|| {
                        catalog::error::CatalogError::NotFound(format!(
                            "need {} writable units, have {}",
                            options.replication_factor.clone(),
                            units.len()
                        ))
                    })?;
                Ok(ensemble)
            })
            .await
            .map_err(|e| ChronicleError::UnitNotEnough(e.to_string()))?;

        let ensemble = writable_segment.value.ensemble.clone();
        info!(
            timeline_id = tc.timeline_id,
            term = tc.term,
            "new term: fencing ensemble"
        );
        let mut tl = Timeline {
            options,
            catalog,
            pool,
            meta: tc,
            writable_segment,
        };
        let lra = tl
            .fence_ensemble(tc.timeline_id, tc.term, &ensemble)
            .await
            .map_err(|e| ChronicleError::Internal(e.to_string()))?;
        if tl.meta.lra != lra {
            tl.meta.lra = lra;
            tl.meta = catalog.timeline_update(&tl.meta, tc.version).await?;
        }
        info!(
            timeline_id = tc.timeline_id,
            term = tc.term,
            lra = lra,
            "new term: complete"
        );

        for unit in &ensemble {
            let conn = pool
                .get_or_connect(&unit.address)
                .map_err(|e| ChronicleError::Transport(e.to_string()))?;
            conn.subscribe_watermark(tc.timeline_id, wm_tx.clone());
            conns.insert(unit.address.clone(), conn);
        }

        let unit_committed: HashMap<String, i64> =
            conns.keys().map(|ep| (ep.clone(), lra)).collect();

        let state = LoopState {
            timeline_id: tc.timeline_id,
            term: tc.term,
            lrs: lra,
            lra,
            needs_trunc,
            catalog: catalog.clone(),
            pool: pool.clone(),
            timeline_name: name.to_string(),
            conns,
            unit_committed,
        };

        let (event_tx, event_rx) = mpsc::channel::<PendingEvent>(max_batch_size * 2);
        let loop_task = tokio::spawn(event_loop(state, event_rx, wm_rx, max_batch_size, linger));

        info!(
            timeline_id = tc.timeline_id,
            timeline = %name,
            "timeline opened"
        );

        Ok(Self {
            mode: Mode::ReadWrite {
                event_tx: Some(event_tx),
                loop_task: Some(loop_task),
            },
            meta: tc,
            options,
            catalog,
            pool,
        })
    }

    pub async fn open_readonly(
        catalog: Arc<Catalog>,
        pool: Arc<ConnPool>,
        name: &str,
        options: TimelineOptions,
    ) -> Result<Self, ChronicleError> {
        let tc = catalog
            .get_timeline(name)
            .await
            .map_err(|_| ChronicleError::TimelineNotFound(name.to_string()))?;

        info!(
            timeline_id = tc.timeline_id,
            timeline = %name,
            "timeline opened (readonly)"
        );

        Ok(Self {
            mode: Mode::ReadOnly,
            meta: tc,
            options,
            catalog,
            pool,
        })
    }

    async fn fence_unit(
        &self,
        timeline_id: i64,
        unit: &UnitInfo,
        term: i64,
    ) -> Result<FenceResponse, InnerError> {
        let conn = self.pool.get_or_connect(&unit.address)?;
        let backoff = backoff::ExponentialBackoffBuilder::new()
            .with_max_elapsed_time(Some(self.options.request_timeout))
            .build();
        future::retry_notify(
            backoff,
            || async {
                match conn.fence(timeline_id, term).await {
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

    pub fn timeline_id(&self) -> i64 {
        self.meta.timeline_id
    }

    pub async fn record(&self, event: UserEvent) -> Result<Offset, ChronicleError> {
        let sender = match &self.mode {
            Mode::ReadWrite { event_tx, .. } => event_tx
                .as_ref()
                .ok_or_else(|| ChronicleError::Internal("timeline closed".into()))?,
            Mode::ReadOnly { .. } => {
                return Err(ChronicleError::Internal("timeline is read-only".into()));
            }
        };
        let (tx, rx) = oneshot::channel();
        sender
            .send(PendingEvent { event, tx })
            .await
            .map_err(|_| ChronicleError::Internal("timeline closed".into()))?;
        rx.await
            .map_err(|_| ChronicleError::Internal("watermark channel dropped".into()))?
    }

    pub async fn fetch(&self, options: FetchOptions) -> Result<EventStream, ChronicleError> {
        // Resolve start offset.
        let start_offset = match options.start {
            StartPosition::Earliest => 0,
            StartPosition::Latest => {
                let tc = self
                    .catalog
                    .get_timeline(&self.meta.name)
                    .await
                    .map_err(|e| {
                        ChronicleError::Internal(format!("failed to get timeline: {}", e))
                    })?;
                tc.lra + 1
            }
            StartPosition::Offset(offset) => offset,
            StartPosition::Index { .. } => {
                return Err(ChronicleError::Internal(
                    "index-based fetch not yet implemented".into(),
                ));
            }
        };

        let mut stream = EventStream::new(
            self.meta.timeline_id,
            self.meta.name.clone(),
            self.catalog.clone(),
            self.pool.clone(),
            start_offset,
        );

        if let Some(limit) = options.limit {
            stream = stream.with_limit(limit);
        }
        if let Some(timeout) = options.timeout {
            stream = stream.with_timeout(timeout);
        }
        if matches!(options.start, StartPosition::Latest) {
            stream = stream.with_tail();
        }

        Ok(stream)
    }

    pub async fn close(&mut self) {
        match &mut self.mode {
            Mode::ReadWrite {
                event_tx,
                loop_task,
            } => {
                // Drop the sender so the event loop sees channel closed and exits.
                event_tx.take();
                if let Some(task) = loop_task.take() {
                    let _ = task.await;
                }
            }
            Mode::ReadOnly => {}
        }
        info!(timeline_id = self.meta.timeline_id, "timeline closed");
    }
}

// ---------------------------------------------------------------------------
// Event loop — single-threaded, owns all mutable state
// ---------------------------------------------------------------------------

async fn event_loop(
    mut state: LoopState,
    mut event_rx: mpsc::Receiver<PendingEvent>,
    mut wm_rx: mpsc::Receiver<Watermark>,
    max_batch_size: usize,
    linger: Duration,
) {
    let mut batch: Vec<PendingEvent> = Vec::with_capacity(max_batch_size);
    let mut inflight: VecDeque<InflightBatch> = VecDeque::new();
    let linger_sleep = tokio::time::sleep(far_future());
    tokio::pin!(linger_sleep);
    let mut linger_active = false;

    loop {
        tokio::select! {
            // Prioritize watermark processing to keep the global watermark advancing.
            biased;

            wm = wm_rx.recv() => {
                match wm {
                    Some(wm) => {
                        process_watermark(&mut state, &mut inflight, wm);
                    }
                    None => break,
                }
            }

            event = event_rx.recv() => {
                match event {
                    Some(e) => {
                        if batch.is_empty() && !linger_active {
                            linger_sleep
                                .as_mut()
                                .reset(tokio::time::Instant::now() + linger);
                            linger_active = true;
                        }
                        batch.push(e);
                        if batch.len() >= max_batch_size {
                            flush_batch(&mut state, &mut batch, &mut inflight).await;
                            linger_sleep.as_mut().reset(tokio::time::Instant::now() + far_future());
                            linger_active = false;
                        }
                    }
                    None => {
                        // Channel closed — flush remaining and drain inflight.
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

fn far_future() -> Duration {
    Duration::from_secs(86400)
}

// ---------------------------------------------------------------------------
// Write path
// ---------------------------------------------------------------------------

fn prepare_batch(
    state: &mut LoopState,
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
            timeline_id: state.timeline_id,
            term: state.term,
            offset,
            payload: Some(event.payload.into()),
            crc32: None,
            timestamp: now,
            schema_id: 0,
        };

        let item = RecordEventsRequestItem {
            event: Some(proto_event),
            trunc: state.needs_trunc,
            lra: state.lra,
        };
        state.needs_trunc = false;

        items.push(item);
        offsets.push(offset);
    }

    (items, offsets)
}

async fn flush_batch(
    state: &mut LoopState,
    batch: &mut Vec<PendingEvent>,
    inflight: &mut VecDeque<InflightBatch>,
) {
    let mut events = Vec::with_capacity(batch.len());
    let mut txs = Vec::with_capacity(batch.len());
    for p in batch.drain(..) {
        events.push(p.event);
        txs.push(p.tx);
    }

    let (items, offsets) = prepare_batch(state, events);
    let max_offset = *offsets.last().unwrap();
    let request = RecordEventsRequest { items };

    // Fire to all unit connections (lazy stream init).
    for (ep, conn) in &state.conns {
        if let Err(e) = conn.send_record(request.clone()).await {
            warn!(endpoint = %ep, error = %e, "flush_batch: failed to send");
        }
    }

    let callbacks: Vec<_> = offsets.into_iter().zip(txs).collect();
    inflight.push_back(InflightBatch {
        max_offset,
        callbacks,
    });
}

// ---------------------------------------------------------------------------
// Watermark processing — resolution by global minimum
// ---------------------------------------------------------------------------

fn process_watermark(state: &mut LoopState, inflight: &mut VecDeque<InflightBatch>, wm: Watermark) {
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
                        requested: state.term,
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

fn drain_resolved(state: &mut LoopState, inflight: &mut VecDeque<InflightBatch>) {
    let global_synced = state.unit_committed.values().copied().min().unwrap_or(-1);

    while let Some(front) = inflight.front() {
        if front.max_offset <= global_synced {
            let batch = inflight.pop_front().unwrap();
            for (offset, tx) in batch.callbacks {
                let _ = tx.send(Ok(Offset(offset)));
            }
            state.lra = batch.max_offset;
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

/// After event channel closes, keep processing watermarks until inflight is empty.
async fn drain_remaining(
    state: &mut LoopState,
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

// ---------------------------------------------------------------------------
// Quarantine — generic priority queue with timed entries
// ---------------------------------------------------------------------------

struct QuarantineEntry<T> {
    value: T,
    ready_at: Instant,
}

impl<T> Eq for QuarantineEntry<T> {}

impl<T> PartialEq for QuarantineEntry<T> {
    fn eq(&self, other: &Self) -> bool {
        self.ready_at == other.ready_at
    }
}

impl<T> Ord for QuarantineEntry<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.ready_at.cmp(&other.ready_at)
    }
}

impl<T> PartialOrd for QuarantineEntry<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

async fn update_segment(
    state: &LoopState,
    old_endpoint: &str,
    new_endpoint: &str,
) -> Result<(), ChronicleError> {
    let catalog = &state.catalog;
    let name = &state.timeline_name;

    let catalog_segments = catalog.list_segments(name).await?;
    if let Some(last) = catalog_segments.last() {
        let mut new_ensemble = last.value.ensemble.clone();
        if let Some(pos) = new_ensemble.iter().position(|e| e == old_endpoint) {
            new_ensemble[pos] = new_endpoint.to_string();
        }

        if last.value.start_offset > state.lra {
            let updated = Segment {
                ensemble: new_ensemble,
                start_offset: last.value.start_offset,
            };
            catalog.put_segment(name, &updated, last.version).await?;
        } else {
            let new_seg = Segment {
                ensemble: new_ensemble,
                start_offset: state.lra + 1,
            };
            catalog.put_segment(name, &new_seg, -1).await?;
        }
    }

    Ok(())
}
