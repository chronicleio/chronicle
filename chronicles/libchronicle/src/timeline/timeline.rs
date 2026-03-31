use super::cursor::EventStream;
use super::state_machine::StateMachine;
use crate::conn::conn_pool::ConnPool;
use crate::error::ChronicleError;
use crate::{Event as UserEvent, FetchOptions, Offset, StartPosition, TimelineOptions};
use catalog::Catalog;
use std::sync::Arc;
use tracing::info;

pub struct Timeline {
    timeline_id: i64,
    timeline_name: String,
    catalog: Arc<Catalog>,
    pool: Arc<ConnPool>,
    #[allow(dead_code)]
    options: TimelineOptions,
    state_machine: Option<StateMachine>,
}

impl Timeline {
    pub async fn open(
        catalog: Arc<Catalog>,
        pool: Arc<ConnPool>,
        name: &str,
        options: TimelineOptions,
    ) -> Result<Self, ChronicleError> {
        let (sm, meta) = StateMachine::start(name, catalog.clone(), pool.clone(), &options).await?;

        info!(
            timeline_id = meta.timeline_id,
            timeline = %name,
            "timeline opened"
        );

        Ok(Self {
            timeline_id: meta.timeline_id,
            timeline_name: name.to_string(),
            catalog,
            pool,
            options,
            state_machine: Some(sm),
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
            timeline_id: tc.timeline_id,
            timeline_name: name.to_string(),
            catalog,
            pool,
            options,
            state_machine: None,
        })
    }

    pub async fn record(&self, event: UserEvent) -> Result<Offset, ChronicleError> {
        let sm = self
            .state_machine
            .as_ref()
            .ok_or_else(|| ChronicleError::Internal("timeline is read-only".into()))?;
        sm.record(event).await
    }

    pub async fn fetch(&self, options: FetchOptions) -> Result<EventStream, ChronicleError> {
        let start_offset = match options.start {
            StartPosition::Earliest => 0,
            StartPosition::Latest => {
                let tc = self
                    .catalog
                    .get_timeline(&self.timeline_name)
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
            self.timeline_id,
            self.timeline_name.clone(),
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
        if let Some(sm) = self.state_machine.take() {
            sm.stop().await;
        }
        info!(timeline_id = self.timeline_id, "timeline closed");
    }
}
