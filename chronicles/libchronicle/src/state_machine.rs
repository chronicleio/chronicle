use crate::conn::{Conn, ConnPool, RecordStream, Watermark};
use crate::ensemble::select_ensemble;
use crate::error::ChronicleError;
use crate::Event as UserEvent;
use crate::Offset;
use catalog::Catalog;
use chronicle_proto::pb_catalog::Segment;
use chronicle_proto::pb_ext::{
    RecordEventsRequest, RecordEventsRequestItem, StatusCode,
};
use futures_util::future::join_all;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tracing::{info, warn};

const MAX_FENCE_ATTEMPTS: u64 = 5;

// ---------------------------------------------------------------------------
// Event loop state — owned by the spawned task, no mutexes needed
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
    streams: HashMap<String, RecordStream>,
    unit_synced: HashMap<String, i64>,
    wm_tx: mpsc::Sender<Watermark>,
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
// StateMachine — a handle to the single-threaded event loop
// ---------------------------------------------------------------------------

pub(crate) struct StateMachine {
    event_tx: Option<mpsc::Sender<PendingEvent>>,
    loop_task: Option<tokio::task::JoinHandle<()>>,
    timeline_id: i64,
}

impl StateMachine {
    pub async fn open(
        catalog: Arc<Catalog>,
        pool: Arc<ConnPool>,
        name: &str,
        replication_factor: usize,
        _schema_id: Option<String>,
        max_batch_size: usize,
        linger: Duration,
    ) -> Result<Self, ChronicleError> {
        let tc = catalog.tl_new_term(name).await?;

        let catalog_ref = &catalog;
        let writable_seg = catalog
            .tl_fetch_or_insert_w_seg(&tc.name, || async move {
                let units = catalog_ref.list_writable_units().await?;
                select_ensemble(&units, replication_factor, &[], &[]).ok_or_else(|| {
                    catalog::error::CatalogError::Internal(format!(
                        "need {} writable units, have {}",
                        replication_factor,
                        units.len()
                    ))
                })
            })
            .await?;
        let previous: Vec<String> = writable_seg.value.ensemble.clone();

        let units = catalog.list_writable_units().await?;
        let ensemble =
            select_ensemble(&units, replication_factor, &previous, &[]).ok_or_else(|| {
                ChronicleError::UnitNotEnough(format!(
                    "need {} writable units, have {}",
                    replication_factor,
                    units.len()
                ))
            })?;

        info!(
            timeline_id = tc.timeline_id,
            term = tc.term,
            "new term: fencing ensemble"
        );

        // Create the shared watermark channel.
        let (wm_tx, wm_rx) = mpsc::channel::<Watermark>(256);

        // Fence ensemble and open record streams.
        let (ensemble, lra, streams) =
            fence_ensemble(&pool, &wm_tx, ensemble, tc.timeline_id, tc.term).await?;

        let needs_trunc = lra > 0;

        // Update the writable segment with the selected ensemble.
        let updated_seg = Segment {
            ensemble: ensemble.clone(),
            start_offset: lra + 1,
        };
        catalog
            .put_segment(&tc.name, &updated_seg, writable_seg.version)
            .await?;

        // Persist LRA.
        let mut updated = tc.clone();
        updated.lra = lra;
        let _updated = catalog.put_timeline(&updated, tc.version).await?;

        info!(
            timeline_id = tc.timeline_id,
            term = tc.term,
            lra = lra,
            "new term: complete"
        );

        // Initialize per-unit synced watermarks.
        let unit_synced: HashMap<String, i64> = streams
            .keys()
            .map(|ep| (ep.clone(), lra))
            .collect();

        // Build loop state and spawn the event loop.
        let state = LoopState {
            timeline_id: tc.timeline_id,
            term: tc.term,
            lrs: lra,
            lra,
            needs_trunc,
            catalog,
            pool,
            timeline_name: name.to_string(),
            streams,
            unit_synced,
            wm_tx,
        };

        let (event_tx, event_rx) = mpsc::channel::<PendingEvent>(max_batch_size * 2);
        let loop_task = tokio::spawn(event_loop(state, event_rx, wm_rx, max_batch_size, linger));

        Ok(Self {
            event_tx: Some(event_tx),
            loop_task: Some(loop_task),
            timeline_id: tc.timeline_id,
        })
    }

    pub fn timeline_id(&self) -> i64 {
        self.timeline_id
    }

    pub async fn record(&self, event: UserEvent) -> Result<Offset, ChronicleError> {
        let sender = self
            .event_tx
            .as_ref()
            .ok_or_else(|| ChronicleError::Internal("timeline closed".into()))?;
        let (tx, rx) = oneshot::channel();
        sender
            .send(PendingEvent { event, tx })
            .await
            .map_err(|_| ChronicleError::Internal("timeline closed".into()))?;
        rx.await
            .map_err(|_| ChronicleError::Internal("watermark channel dropped".into()))?
    }

    pub async fn close(&mut self) {
        // Drop the sender so the event loop sees channel closed and exits.
        self.event_tx.take();
        if let Some(task) = self.loop_task.take() {
            let _ = task.await;
        }
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

    // Fire to all unit streams (non-blocking).
    for (ep, stream) in &state.streams {
        if let Err(e) = stream.send(request.clone()).await {
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

fn process_watermark(
    state: &mut LoopState,
    inflight: &mut VecDeque<InflightBatch>,
    wm: Watermark,
) {
    match wm.result {
        Ok(resp) => {
            if resp.code == StatusCode::Ok as i32 {
                state
                    .unit_synced
                    .insert(wm.endpoint, resp.synced_offset);
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

fn drain_resolved(
    state: &mut LoopState,
    inflight: &mut VecDeque<InflightBatch>,
) {
    let global_synced = state.unit_synced.values().copied().min().unwrap_or(-1);

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

fn fail_all_inflight(
    inflight: &mut VecDeque<InflightBatch>,
    error: ChronicleError,
) {
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
// Ensemble setup — fencing + stream opening
// ---------------------------------------------------------------------------

/// Fence all ensemble members and open record streams.
/// Returns (ensemble, max_lra, streams).
async fn fence_ensemble(
    pool: &Arc<ConnPool>,
    wm_tx: &mpsc::Sender<Watermark>,
    ensemble: Vec<String>,
    timeline_id: i64,
    term: i64,
) -> Result<(Vec<String>, i64, HashMap<String, RecordStream>), ChronicleError> {
    let futs = ensemble.iter().map(|ep| {
        let ep = ep.clone();
        let pool = pool.clone();
        let wm_tx = wm_tx.clone();
        async move {
            let conn = pool.get_or_connect(&ep)?;
            let lra = fence_unit(&conn, &ep, timeline_id, term).await?;
            let stream = conn.open_record_stream(wm_tx).await?;
            Ok::<_, ChronicleError>((ep, lra, stream))
        }
    });

    let results = join_all(futs).await;

    let mut max_lra: i64 = 0;
    let mut streams = HashMap::new();
    let mut final_ensemble = Vec::new();
    for result in results {
        let (ep, lra, stream) = result?;
        if lra > max_lra {
            max_lra = lra;
        }
        streams.insert(ep.clone(), stream);
        final_ensemble.push(ep);
    }

    Ok((final_ensemble, max_lra, streams))
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
            catalog
                .put_segment(name, &updated, last.version)
                .await?;
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

// ---------------------------------------------------------------------------
// Fencing
// ---------------------------------------------------------------------------

async fn fence_unit(
    conn: &Conn,
    endpoint: &str,
    timeline_id: i64,
    term: i64,
) -> Result<i64, ChronicleError> {
    let mut attempts: u64 = 0;
    let response = loop {
        attempts += 1;
        match conn.fence(timeline_id, term).await {
            Ok(resp) => break resp,
            Err(e) if attempts < MAX_FENCE_ATTEMPTS => {
                warn!(
                    endpoint = endpoint,
                    attempt = attempts,
                    error = %e,
                    "fence: failed, retrying"
                );
                tokio::time::sleep(Duration::from_millis(100 * attempts)).await;
            }
            Err(e) => {
                return Err(ChronicleError::ReconciliationFailed(format!(
                    "failed to fence unit {}: {}",
                    endpoint, e
                )));
            }
        }
    };

    if response.code == StatusCode::Ok as i32 {
        info!(endpoint = endpoint, lra = response.lra, "fence: unit fenced");
        Ok(response.lra)
    } else if response.code == StatusCode::Fenced as i32 {
        Err(ChronicleError::Fenced {
            timeline_id,
            term: response.term,
        })
    } else {
        Err(ChronicleError::InvalidTerm {
            current: response.term,
            requested: term,
        })
    }
}
