use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::storage::blob::manager::SegmentManager;
use crate::storage::index::Storage;

pub struct RetentionManager {
    handle: JoinHandle<()>,
}

impl RetentionManager {
    pub fn spawn(
        segment_manager: Arc<SegmentManager>,
        index: Storage,
        context: CancellationToken,
        ttl_ms: i64,
        interval: Duration,
    ) -> Self {
        let handle = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            loop {
                tokio::select! {
                    _ = context.cancelled() => {
                        info!("retention manager stopped");
                        break;
                    }
                    _ = ticker.tick() => {
                        run_retention(&segment_manager, &index, ttl_ms);
                    }
                }
            }
        });
        Self { handle }
    }

    pub async fn shutdown(self) {
        if let Err(err) = self.handle.await {
            warn!(error = ?err, "retention manager join error");
        }
    }
}

fn run_retention(segment_manager: &SegmentManager, index: &Storage, ttl_ms: i64) {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let cutoff = now - ttl_ms;

    let mut expired_ids = Vec::new();
    for level in 3..=4 {
        for meta in segment_manager.segments_at_level(level) {
            if meta.created_at > 0 && meta.created_at < cutoff {
                expired_ids.push(meta.id);
            }
        }
    }

    if expired_ids.is_empty() {
        return;
    }

    info!(
        count = expired_ids.len(),
        "retention: evicting expired segments"
    );

    let expired_set: HashSet<u64> = expired_ids.iter().copied().collect();
    let entries = index.scan_by_segment_ids(&expired_set);
    if !entries.is_empty() {
        let keys: Vec<(i64, i64)> = entries.iter().map(|((t, o), _)| (*t, *o)).collect();
        if let Err(e) = index.delete_index_batch(&keys) {
            warn!(error = %e, "retention: failed to delete index entries");
            return;
        }
        info!(
            entries = entries.len(),
            "retention: deleted expired index entries"
        );
    }

    segment_manager.remove_segments(&expired_ids);
    info!(
        segments = expired_ids.len(),
        "retention: removed expired segments"
    );
}
