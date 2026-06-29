use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use super::manager::SegmentManager;
use super::remote::RemoteStore;
use crate::error::unit_error::UnitError;

pub(crate) struct L4OffloadTask {
    pub segment_manager: Arc<SegmentManager>,
    pub remote_store: Arc<dyn RemoteStore>,
    pub interval: Duration,
}

impl L4OffloadTask {
    pub async fn run(&self, context: CancellationToken) {
        let mut ticker = tokio::time::interval(self.interval);
        loop {
            tokio::select! {
                _ = context.cancelled() => {
                    let _ = self.offload().await;
                    info!("L4 offload task stopped");
                    break;
                }
                _ = ticker.tick() => {
                    if let Err(e) = self.offload().await {
                        warn!(error = ?e, "L4 offload failed");
                    }
                }
            }
        }
    }

    pub async fn offload(&self) -> Result<(), UnitError> {
        let l3_segments = self.segment_manager.segments_at_level(3);
        if l3_segments.is_empty() {
            return Ok(());
        }

        for meta in &l3_segments {
            let local_path = match self.segment_manager.segment_path_for(meta.id) {
                Some(p) => p,
                None => continue,
            };

            if !local_path.exists() {
                continue;
            }

            let filename = local_path
                .file_name()
                .and_then(|f| f.to_str())
                .ok_or_else(|| UnitError::Storage("invalid segment path".into()))?;

            let key = format!("chronicle/segments/{}", filename);

            self.remote_store.upload(&local_path, &key).await?;
            self.segment_manager.mark_remote(meta.id, key.clone());

            info!(
                segment_id = meta.id,
                key = %key,
                "L3 → L4 segment offloaded to remote"
            );
        }

        Ok(())
    }
}
