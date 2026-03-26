use crate::actor::Envelope;
use crate::actor::read_handle_group::ReadHandleGroup;
use crate::actor::write_handle_group::WriteActorGroup;
use crate::observability::ServerMetrics;
use crate::unit::admin_service::STATE_READONLY;
use crate::unit::timeline_state::TimelineStateManager;
use chronicle_proto::pb_ext::chronicle_server::Chronicle;
use chronicle_proto::pb_ext::{
    FenceRequest, FenceResponse, FetchEventsRequest, FetchEventsResponse, RecordEventsRequest,
    RecordEventsResponse, StatusCode,
};
use futures_util::StreamExt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::Instant;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::codegen::BoxStream;
use tonic::{Request, Response, Status, Streaming};

pub struct UnitService {
    write_group: Arc<WriteActorGroup>,
    read_group: Arc<ReadHandleGroup>,
    timeline_state: Arc<TimelineStateManager>,
    unit_state: Arc<AtomicU8>,
    metrics: Arc<ServerMetrics>,
}

impl UnitService {
    pub fn new(
        write_group: Arc<WriteActorGroup>,
        read_group: Arc<ReadHandleGroup>,
        timeline_state: Arc<TimelineStateManager>,
        unit_state: Arc<AtomicU8>,
        metrics: Arc<ServerMetrics>,
    ) -> Self {
        Self {
            write_group,
            read_group,
            timeline_state,
            unit_state,
            metrics,
        }
    }
}

#[tonic::async_trait]
impl Chronicle for UnitService {
    type RecordStream = BoxStream<RecordEventsResponse>;

    async fn record(
        &self,
        request: Request<Streaming<RecordEventsRequest>>,
    ) -> Result<Response<Self::RecordStream>, Status> {
        if self.unit_state.load(Ordering::Relaxed) == STATE_READONLY {
            return Err(Status::unavailable("unit is readonly"));
        }

        let mut stream = request.into_inner();
        let (tx, rx) = mpsc::channel(4);
        let write_group = self.write_group.clone();
        let metrics = self.metrics.clone();

        tokio::spawn(async move {
            while let Some(request) = stream.next().await {
                match request {
                    Ok(req) => {
                        let start = Instant::now();
                        let item_count = req.items.len();

                        // Extract timeline_id and term from first item.
                        let (timeline_id, term) = req.items.first()
                            .and_then(|i| i.event.as_ref())
                            .map(|e| (e.timeline_id, e.term))
                            .unwrap_or((0, 0));

                        metrics.write_requests.add(1, &[]);
                        metrics.write_queue_depth.add(item_count as i64, &[]);

                        // Phase 1: dispatch all items, collect response channels.
                        let mut total_bytes: u64 = 0;
                        let mut channels = Vec::with_capacity(item_count);
                        let mut dispatch_failed = false;

                        for item in req.items {
                            total_bytes += item.event.as_ref()
                                .and_then(|e| e.payload.as_ref())
                                .map_or(0, |p| p.len() as u64);

                            let (item_tx, item_rx) = mpsc::channel(1);
                            let envelope = Envelope {
                                request: item,
                                res_tx: item_tx,
                            };
                            if let Err(_e) = write_group.dispatch(envelope).await {
                                metrics.write_queue_depth.add(-(item_count as i64), &[]);
                                metrics.write_errors.add(1, &[]);
                                metrics.failed_requests.add(1, &[]);
                                let response = RecordEventsResponse {
                                    code: StatusCode::InvalidTerm.into(),
                                    commit_offset: -1,
                                    timeline_id,
                                    term,
                                };
                                if tx.send(Ok(response)).await.is_err() {
                                    return;
                                }
                                dispatch_failed = true;
                                break;
                            }
                            channels.push(item_rx);
                        }

                        if dispatch_failed {
                            continue;
                        }

                        // Phase 2: wait for all responses, track max synced offset.
                        let mut max_offset: i64 = -1;
                        let mut error_code: Option<i32> = None;
                        let error_term: i64 = term;

                        for mut item_rx in channels {
                            if let Some(result) = item_rx.recv().await {
                                match result {
                                    Ok(event) => {
                                        if event.offset > max_offset {
                                            max_offset = event.offset;
                                        }
                                    }
                                    Err(status) => {
                                        metrics.write_errors.add(1, &[]);
                                        // Parse error — check if it's a term/fence issue.
                                        let msg = status.message();
                                        if msg.contains("INVALID_TERM") {
                                            error_code = Some(StatusCode::InvalidTerm.into());
                                        } else {
                                            error_code = Some(StatusCode::Fenced.into());
                                        }
                                        break;
                                    }
                                }
                            }
                        }

                        metrics.write_queue_depth.add(-(item_count as i64), &[]);

                        // Phase 3: send one watermark ack response.
                        let response = RecordEventsResponse {
                            code: error_code.unwrap_or(StatusCode::Ok.into()),
                            commit_offset: max_offset,
                            timeline_id,
                            term: error_term,
                        };
                        metrics.write_latency.record(start.elapsed().as_secs_f64(), &[]);
                        metrics.write_bytes.add(total_bytes, &[]);
                        if tx.send(Ok(response)).await.is_err() {
                            return;
                        }
                    }
                    Err(e) => {
                        if tx.send(Err(e)).await.is_err() {
                            return;
                        }
                    }
                }
            }
        });

        let output_stream = ReceiverStream::new(rx);
        Ok(Response::new(Box::pin(output_stream) as Self::RecordStream))
    }

    type FetchStream = BoxStream<FetchEventsResponse>;

    async fn fetch(
        &self,
        request: Request<Streaming<FetchEventsRequest>>,
    ) -> Result<Response<Self::FetchStream>, Status> {
        let mut stream = request.into_inner();
        let (tx, rx) = mpsc::channel(4);
        let read_group = self.read_group.clone();
        let metrics = self.metrics.clone();

        tokio::spawn(async move {
            while let Some(request) = stream.next().await {
                match request {
                    Ok(req) => {
                        let start = Instant::now();
                        metrics.read_requests.add(1, &[]);
                        metrics.read_queue_depth.add(1, &[]);

                        let (item_tx, mut item_rx) = mpsc::channel(16);
                        let envelope = Envelope {
                            request: req,
                            res_tx: item_tx,
                        };
                        if let Err(e) = read_group.dispatch(envelope).await {
                            metrics.read_queue_depth.add(-1, &[]);
                            metrics.read_errors.add(1, &[]);
                            metrics.failed_requests.add(1, &[]);
                            if tx.send(Err(e)).await.is_err() {
                                return;
                            }
                            continue;
                        }
                        let mut event_count = 0u64;
                        while let Some(result) = item_rx.recv().await {
                            if let Ok(ref resp) = result {
                                event_count += resp.event.len() as u64;
                            }
                            if tx.send(result).await.is_err() {
                                return;
                            }
                        }
                        metrics.read_queue_depth.add(-1, &[]);
                        metrics.read_latency.record(start.elapsed().as_secs_f64(), &[]);
                        metrics.read_events.add(event_count, &[]);
                    }
                    Err(e) => {
                        if tx.send(Err(e)).await.is_err() {
                            return;
                        }
                    }
                }
            }
        });

        let output_stream = ReceiverStream::new(rx);
        Ok(Response::new(Box::pin(output_stream) as Self::FetchStream))
    }

    async fn fence(
        &self,
        request: Request<FenceRequest>,
    ) -> Result<Response<FenceResponse>, Status> {
        let req = request.into_inner();
        match self.timeline_state.fence(req.timeline_id, req.term) {
            Ok(lra) => Ok(Response::new(FenceResponse {
                code: StatusCode::Ok.into(),
                lra,
                term: req.term,
            })),
            Err(current_term) => Ok(Response::new(FenceResponse {
                code: StatusCode::Fenced.into(),
                lra: -1,
                term: current_term,
            })),
        }
    }
}
