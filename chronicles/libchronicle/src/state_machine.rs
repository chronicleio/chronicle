use crate::conn::{Conn, ConnPool, RecordHandle};
use crate::ensemble::select_ensemble;
use crate::error::ChronicleError;
use crate::Event as UserEvent;
use catalog::Catalog;
use chronicle_proto::pb_catalog::Segment;
use chronicle_proto::pb_ext::{
    RecordEventsRequest, RecordEventsRequestItem, StatusCode,
};
use futures_util::future::join_all;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing::{info, warn};

const MAX_FENCE_ATTEMPTS: u64 = 5;
const MAX_REPLACE_ATTEMPTS: u32 = 3;
const FENCE_TIMEOUT: Duration = Duration::from_secs(30);
const APPLY_TIMEOUT: Duration = Duration::from_secs(5);

// ---------------------------------------------------------------------------
// Broadcast — run an operation on all endpoints concurrently with timeout
// ---------------------------------------------------------------------------

async fn broadcast<F, Fut, T>(
    endpoints: &[String],
    op: F,
    timeout: Duration,
) -> (Vec<(String, T)>, Vec<(String, ChronicleError)>)
where
    F: Fn(String) -> Fut,
    Fut: std::future::Future<Output = Result<T, ChronicleError>> + Send + 'static,
    T: Send + 'static,
{
    let futs = endpoints.iter().map(|ep| {
        let ep = ep.clone();
        let fut = op(ep.clone());
        async move {
            match tokio::time::timeout(timeout, fut).await {
                Ok(result) => (ep, result),
                Err(_) => (
                    ep.clone(),
                    Err(ChronicleError::Transport(format!(
                        "operation timed out for {}",
                        ep
                    ))),
                ),
            }
        }
    });

    let results = join_all(futs).await;

    let mut successes = Vec::new();
    let mut failures = Vec::new();
    for (ep, result) in results {
        match result {
            Ok(val) => successes.push((ep, val)),
            Err(e) => failures.push((ep, e)),
        }
    }

    (successes, failures)
}

// ---------------------------------------------------------------------------
// Private state
// ---------------------------------------------------------------------------

struct State {
    timeline_id: i64,
    term: i64,
    lrs: i64,
    lra: i64,
    replication_factor: usize,
    needs_trunc: bool,
}

struct Inner {
    state: Mutex<State>,
    catalog: Arc<Catalog>,
    pool: Arc<ConnPool>,
    timeline_name: String,
    /// Ensemble endpoints → record handles.
    handles: Mutex<HashMap<String, RecordHandle>>,
}

// ---------------------------------------------------------------------------
// StateMachine
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub(crate) struct StateMachine {
    inner: Arc<Inner>,
}

impl StateMachine {
    pub async fn open(
        catalog: Arc<Catalog>,
        pool: Arc<ConnPool>,
        name: &str,
        replication_factor: usize,
        _schema_id: Option<String>,
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

        let sm = Self {
            inner: Arc::new(Inner {
                state: Mutex::new(State {
                    timeline_id: tc.timeline_id,
                    term: tc.term,
                    lrs: 0,
                    lra: 0,
                    replication_factor,
                    needs_trunc: false,
                }),
                catalog: catalog.clone(),
                pool: pool.clone(),
                timeline_name: name.to_string(),
                handles: Mutex::new(HashMap::new()),
            }),
        };

        let (ensemble, lra) = sm
            .fence_ensemble(ensemble, tc.timeline_id, tc.term)
            .await?;

        {
            let mut s = sm.inner.state.lock().unwrap();
            s.lrs = lra;
            s.lra = lra;
            s.needs_trunc = lra > 0;
        }

        // Update the writable segment with the selected ensemble.
        let updated_seg = Segment {
            ensemble,
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

        Ok(sm)
    }

    // -- Getters -------------------------------------------------------------

    pub fn timeline_id(&self) -> i64 {
        self.inner.state.lock().unwrap().timeline_id
    }

    pub fn lra(&self) -> i64 {
        self.inner.state.lock().unwrap().lra
    }

    // -- Write path ----------------------------------------------------------

    /// Assign offsets and build proto items. Returns `(items, offsets)`.
    pub(crate) fn prepare_batch(
        &self,
        events: Vec<UserEvent>,
    ) -> (Vec<RecordEventsRequestItem>, Vec<i64>) {
        let mut s = self.inner.state.lock().unwrap();
        let mut items = Vec::with_capacity(events.len());
        let mut offsets = Vec::with_capacity(events.len());

        for event in events {
            let offset = s.lrs + 1;
            s.lrs = offset;

            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64;

            let proto_event = chronicle_proto::pb_ext::Event {
                timeline_id: s.timeline_id,
                term: s.term,
                offset,
                payload: Some(event.payload.into()),
                crc32: None,
                timestamp: now,
                schema_id: 0,
            };

            let item = RecordEventsRequestItem {
                event: Some(proto_event),
                trunc: s.needs_trunc,
                lra: s.lra,
            };
            s.needs_trunc = false;

            items.push(item);
            offsets.push(offset);
        }

        (items, offsets)
    }

    /// Send items to all ensemble members concurrently. Blocks until all
    /// units respond. On failure, replaces the unit and retries.
    /// Advances LRA on success.
    pub(crate) async fn apply(
        &self,
        items: Vec<RecordEventsRequestItem>,
    ) -> Result<(), ChronicleError> {
        let mut endpoints: Vec<String> = {
            self.inner.handles.lock().unwrap().keys().cloned().collect()
        };

        if endpoints.is_empty() {
            return Err(ChronicleError::UnitNotEnough("no connected units".into()));
        }

        let request = RecordEventsRequest { items };
        let handles = self.inner.handles.lock().unwrap().clone();

        let results = self
            .broadcast_with_replace(
                &mut endpoints,
                |ep| {
                    let handle = handles.get(&ep).cloned();
                    let req = request.clone();
                    async move {
                        let h = handle.ok_or_else(|| {
                            ChronicleError::Transport(format!("no handle for {}", ep))
                        })?;
                        h.record(req).await
                    }
                },
                APPLY_TIMEOUT,
            )
            .await?;

        // All units responded. Check responses and advance LRA.
        let mut max_offset: i64 = 0;
        for (_ep, response) in &results {
            for item in &response.items {
                if item.code != StatusCode::Ok as i32 {
                    if item.code == StatusCode::Fenced as i32 {
                        let s = self.inner.state.lock().unwrap();
                        return Err(ChronicleError::Fenced {
                            timeline_id: s.timeline_id,
                            term: s.term,
                        });
                    }
                    warn!(code = item.code, "apply: unit returned error code");
                }
                if let Some(ref event) = item.event {
                    if event.offset > max_offset {
                        max_offset = event.offset;
                    }
                }
            }
        }

        if max_offset > 0 {
            let mut s = self.inner.state.lock().unwrap();
            s.lra = max_offset;
        }

        Ok(())
    }

    // -- Ensemble management -------------------------------------------------

    /// Select a replacement unit, fence it, open a record handle.
    /// Returns `(new_endpoint, lra)`.
    async fn replace_unit(
        &self,
        failed_endpoint: &str,
    ) -> Result<(String, i64), ChronicleError> {
        let (timeline_id, term) = {
            let s = self.inner.state.lock().unwrap();
            (s.timeline_id, s.term)
        };

        let exclude: Vec<String> = {
            let handles = self.inner.handles.lock().unwrap();
            let mut ex: Vec<String> = handles.keys().cloned().collect();
            if !ex.contains(&failed_endpoint.to_string()) {
                ex.push(failed_endpoint.to_string());
            }
            ex
        };

        let units = self.inner.catalog.list_writable_units().await?;
        let replacement = select_ensemble(&units, 1, &[], &exclude).ok_or_else(|| {
            ChronicleError::UnitNotEnough("no available unit to replace failed unit".into())
        })?;
        let new_ep = replacement[0].clone();

        let conn = self.inner.pool.get_or_connect(&new_ep)?;
        let lra = fence_unit(&conn, &new_ep, timeline_id, term).await?;
        let handle = conn.open_record_handle().await?;

        {
            let mut handles = self.inner.handles.lock().unwrap();
            handles.remove(failed_endpoint);
            handles.insert(new_ep.clone(), handle);
        }

        // Update segment in catalog.
        self.update_segment(failed_endpoint, &new_ep).await?;

        info!(old = %failed_endpoint, new = %new_ep, "replace_unit: replaced");
        Ok((new_ep, lra))
    }

    /// Broadcast with automatic replacement on failure.
    async fn broadcast_with_replace<F, Fut, T>(
        &self,
        endpoints: &mut Vec<String>,
        op: F,
        timeout: Duration,
    ) -> Result<Vec<(String, T)>, ChronicleError>
    where
        F: Fn(String) -> Fut + Clone,
        Fut: std::future::Future<Output = Result<T, ChronicleError>> + Send + 'static,
        T: Send + 'static,
    {
        let mut all_successes = Vec::new();
        let mut pending = endpoints.clone();

        for round in 0..=MAX_REPLACE_ATTEMPTS {
            if pending.is_empty() {
                break;
            }
            if round > 0 {
                warn!(round = round, "broadcast_with_replace: retrying");
            }

            let (successes, failures) = broadcast(&pending, op.clone(), timeout).await;
            all_successes.extend(successes);

            if failures.is_empty() {
                break;
            }

            for (ep, e) in &failures {
                warn!(endpoint = %ep, error = %e, "broadcast_with_replace: unit failed");
            }

            if round == MAX_REPLACE_ATTEMPTS {
                return Err(ChronicleError::UnitNotEnough(format!(
                    "failed on {} units after {} rounds",
                    failures.len(),
                    MAX_REPLACE_ATTEMPTS
                )));
            }

            let mut next_pending = Vec::new();
            for (failed_ep, _) in failures {
                let (new_ep, _lra) = self.replace_unit(&failed_ep).await?;
                if let Some(pos) = endpoints.iter().position(|e| e == &failed_ep) {
                    endpoints[pos] = new_ep.clone();
                }
                next_pending.push(new_ep);
            }
            pending = next_pending;
        }

        Ok(all_successes)
    }

    /// Fence all ensemble members concurrently. Returns (ensemble, max_lra).
    async fn fence_ensemble(
        &self,
        mut ensemble: Vec<String>,
        timeline_id: i64,
        term: i64,
    ) -> Result<(Vec<String>, i64), ChronicleError> {
        let pool = self.inner.pool.clone();
        let results = self
            .broadcast_with_replace(
                &mut ensemble,
                |ep| {
                    let pool = pool.clone();
                    async move {
                        let conn = pool.get_or_connect(&ep)?;
                        let lra = fence_unit(&conn, &ep, timeline_id, term).await?;
                        let handle = conn.open_record_handle().await?;
                        Ok((lra, handle))
                    }
                },
                FENCE_TIMEOUT,
            )
            .await?;

        let mut max_lra: i64 = 0;
        for (ep, (lra, handle)) in results {
            if lra > max_lra {
                max_lra = lra;
            }
            self.inner.handles.lock().unwrap().insert(ep, handle);
        }

        Ok((ensemble, max_lra))
    }

    /// Update segment in catalog when a unit is replaced.
    /// Empty segment → replace ensemble in-place.
    /// Non-empty → create new segment.
    async fn update_segment(
        &self,
        old_endpoint: &str,
        new_endpoint: &str,
    ) -> Result<(), ChronicleError> {
        let lra = self.inner.state.lock().unwrap().lra;
        let catalog = &self.inner.catalog;
        let name = &self.inner.timeline_name;

        let catalog_segments = catalog.list_segments(name).await?;
        if let Some(last) = catalog_segments.last() {
            let mut new_ensemble = last.value.ensemble.clone();
            if let Some(pos) = new_ensemble.iter().position(|e| e == old_endpoint) {
                new_ensemble[pos] = new_endpoint.to_string();
            }

            if last.value.start_offset > lra {
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
                    start_offset: lra + 1,
                };
                catalog.put_segment(name, &new_seg, -1).await?;
            }
        }

        Ok(())
    }

    // -- Lifecycle -----------------------------------------------------------

    pub async fn close(&self) {
        self.inner.handles.lock().unwrap().clear();
    }
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
