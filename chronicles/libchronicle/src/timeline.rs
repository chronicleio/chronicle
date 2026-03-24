use crate::conn::ConnPool;
use crate::cursor::EventStream;
use crate::error::ChronicleError;
use crate::state_machine::StateMachine;
use crate::{Event as UserEvent, FetchOptions, Offset, TimelineOptions, Writer};
use catalog::Catalog;
use std::sync::Arc;
use tracing::info;

pub struct Timeline {
    state_machine: StateMachine,
    #[allow(dead_code)]
    options: TimelineOptions,
    #[allow(dead_code)]
    catalog: Arc<Catalog>,
    #[allow(dead_code)]
    pool: Arc<ConnPool>,
}

impl Timeline {
    pub async fn open(
        catalog: Arc<Catalog>,
        pool: Arc<ConnPool>,
        name: &str,
        options: TimelineOptions,
    ) -> Result<Self, ChronicleError> {
        let state_machine = StateMachine::open(
            catalog.clone(),
            pool.clone(),
            name,
            options.replication_factor,
            options.schema_id.clone(),
            options.max_batch_size,
            options.linger,
        )
        .await?;

        Ok(Self {
            state_machine,
            options,
            catalog,
            pool,
        })
    }

    pub async fn open_readonly(
        catalog: Arc<Catalog>,
        pool: Arc<ConnPool>,
        name: &str,
    ) -> Result<Self, ChronicleError> {
        let state_machine = StateMachine::open_readonly(
            catalog.clone(),
            pool.clone(),
            name,
        )
        .await?;

        Ok(Self {
            state_machine,
            options: TimelineOptions::default(),
            catalog,
            pool,
        })
    }

    pub async fn fetch(&self, options: FetchOptions) -> Result<EventStream, ChronicleError> {
        self.state_machine.fetch(options).await
    }

    pub async fn close(&mut self) {
        let timeline_id = self.state_machine.timeline_id();
        self.state_machine.close().await;
        info!(timeline_id = timeline_id, "timeline closed");
    }
}

#[async_trait::async_trait]
impl Writer for Timeline {
    async fn record(&self, event: UserEvent) -> Result<Offset, ChronicleError> {
        self.state_machine.record(event).await
    }
}
