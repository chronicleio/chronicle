use std::sync::Arc;
use std::time::Instant;

use chronicle_proto::pb_admin::admin_server::Admin;
use chronicle_proto::pb_admin::{
    DiskUsage, GetStatusRequest, GetStatusResponse, ListSegmentsRequest, ListSegmentsResponse,
    SegmentInfo, TriggerCompactionRequest, TriggerCompactionResponse,
};
use opentelemetry::KeyValue;
use tonic::{Request, Response, Status};

use crate::storage::blob::manager::{SegmentLocation, SegmentManager};
use crate::storage::index::Storage;
use crate::wal::checkpoint;
use crate::wal::wal::Wal;

pub struct AdminService {
    pub wal: Wal,
    pub segment_manager: Arc<SegmentManager>,
    pub index: Storage,
    pub state: Arc<std::sync::atomic::AtomicU8>,
}

pub const STATE_WRITABLE: u8 = 0;
pub const STATE_READONLY: u8 = 1;

#[tonic::async_trait]
impl Admin for AdminService {
    async fn get_status(
        &self,
        _request: Request<GetStatusRequest>,
    ) -> Result<Response<GetStatusResponse>, Status> {
        let start = Instant::now();
        let attrs = [KeyValue::new("method", "get_status")];
        if let Some(m) = crate::observability::global_metrics() {
            m.admin_requests.add(1, &attrs);
        }
        let wal_segment_id = self.wal.current_segment_id().await;
        let wal_checkpoint = checkpoint::read_checkpoint(&self.index);
        let state_val = self.state.load(std::sync::atomic::Ordering::Relaxed);

        let state_str = match state_val {
            STATE_WRITABLE => "WRITABLE",
            STATE_READONLY => "READONLY",
            _ => "UNKNOWN",
        };

        if let Some(m) = crate::observability::global_metrics() {
            m.admin_latency
                .record(start.elapsed().as_secs_f64(), &attrs);
        }
        Ok(Response::new(GetStatusResponse {
            state: state_str.to_string(),
            wal_segment_id,
            wal_checkpoint_segment: wal_checkpoint.segment_id,
            write_cache_entries: 0,
            disk: Some(DiskUsage::default()),
        }))
    }

    async fn list_segments(
        &self,
        request: Request<ListSegmentsRequest>,
    ) -> Result<Response<ListSegmentsResponse>, Status> {
        let level_filter = request.into_inner().level;

        let levels: Vec<u32> = if level_filter == 0 {
            vec![1, 2, 3, 4]
        } else {
            vec![level_filter]
        };

        let mut segments = Vec::new();
        for level in levels {
            for meta in self.segment_manager.segments_at_level(level) {
                segments.push(SegmentInfo {
                    id: meta.id,
                    level: meta.level,
                    size: meta.size,
                    entry_count: meta.entry_count,
                    remote: matches!(meta.location, SegmentLocation::Remote { .. }),
                });
            }
        }

        Ok(Response::new(ListSegmentsResponse { segments }))
    }

    async fn trigger_compaction(
        &self,
        _request: Request<TriggerCompactionRequest>,
    ) -> Result<Response<TriggerCompactionResponse>, Status> {
        Ok(Response::new(TriggerCompactionResponse {
            triggered: false,
            message: "manual compaction not yet implemented".to_string(),
        }))
    }
}
