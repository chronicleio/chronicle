use crate::conn::ConnPool;
use crate::error::ChronicleError;
use crate::state_machine::StateMachine;
use crate::write_group::WriteGroup;
use crate::{Event as UserEvent, Offset, TimelineOptions, Writer};
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
    write_group: WriteGroup,
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
        )
        .await?;

        let write_group = WriteGroup::start(
            state_machine.clone(),
            options.max_batch_size,
            options.linger,
        );

        Ok(Self {
            state_machine,
            options,
            catalog,
            pool,
            write_group,
        })
    }

    // TODO: fetch() will query catalog for segments dynamically.
    // Reader implementation is a follow-up.

    pub async fn close(&self) {
        self.write_group.close().await;
        self.state_machine.close().await;
        info!(timeline_id = self.state_machine.timeline_id(), "timeline closed");
    }
}

#[async_trait::async_trait]
impl Writer for Timeline {
    async fn record(&self, event: UserEvent) -> Result<Offset, ChronicleError> {
        self.write_group.record(event).await
    }
}
