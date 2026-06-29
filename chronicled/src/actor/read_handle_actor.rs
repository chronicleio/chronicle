use crate::actor::Envelope;
use crate::error::unit_error::UnitError;
use crate::storage::TimelineReader;
use crate::storage::level_iterator::LevelIterator;
use chronicle_proto::pb_ext::{ChunkType, FetchEventsRequest, FetchEventsResponse, StatusCode};
use tokio::select;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tonic::Status;
use tracing::{info, warn};

pub struct ReadActor {
    pub context: CancellationToken,
    pub worker_handle: JoinHandle<()>,
    pub mailbox: Sender<Envelope<FetchEventsRequest, FetchEventsResponse, Status>>,
}

impl ReadActor {
    pub fn new(_index: i32, inflight_num: usize, reader: LevelIterator) -> Self {
        let context = CancellationToken::new();
        let (mailbox_tx, mailbox_rx) = tokio::sync::mpsc::channel(inflight_num);
        let handle = tokio::spawn(Self::bg_worker(context.clone(), mailbox_rx, reader));
        Self {
            context,
            worker_handle: handle,
            mailbox: mailbox_tx,
        }
    }

    async fn bg_worker(
        context: CancellationToken,
        mut mailbox_rx: Receiver<Envelope<FetchEventsRequest, FetchEventsResponse, Status>>,
        reader: LevelIterator,
    ) {
        let mut inflight_read = Vec::new();
        loop {
            select! {
                _ = context.cancelled() => {
                    info!("read actor stopped");
                    break;
                }
                count = mailbox_rx.recv_many(&mut inflight_read, 1024) => {
                    if count == 0 {
                        info!("read actor stopped, mailbox closed");
                        break;
                    }
                    'envelope_loop: for envelope in inflight_read.drain(..) {
                        let request = envelope.request;
                        let e_offset = request.end_offset;

                        let reader_clone = reader.clone();
                        let batches = match tokio::task::spawn_blocking(move || {
                            reader_clone.fetch_batches(
                                request.timeline_id,
                                request.start_offset,
                                request.end_offset,
                            ).collect::<Vec<_>>()
                        }).await {
                            Ok(b) => b,
                            Err(e) => {
                                warn!(error = ?e, "spawn_blocking failed for read");
                                let _ = envelope.res_tx.send(Err(
                                    Status::internal("read task failed")
                                )).await;
                                continue 'envelope_loop;
                            }
                        };

                        let mut latest_advanced_offset = -1;
                        let mut sent_first = false;

                        for (offset, advanced_offset, events) in batches {
                            latest_advanced_offset = advanced_offset;
                            let chunk_type = if offset != e_offset {
                                if !sent_first {
                                    sent_first = true;
                                    ChunkType::First
                            } else {
                                ChunkType::Middle
                            }
                        } else if !sent_first {
                            ChunkType::Full
                        } else {
                            ChunkType::Last
                        };

                            let res = FetchEventsResponse {
                                code: StatusCode::Ok.into(),
                                r#type: chunk_type.into(),
                                timeline_id: request.timeline_id,
                                event: events,
                                advanced_offset,
                            };

                            let is_final = matches!(chunk_type, ChunkType::Full | ChunkType::Last);

                            if let Err(err) = envelope.res_tx.send(Ok(res)).await {
                                warn!("Send response to client failed: {:?}", err);
                                continue 'envelope_loop;
                            }

                            if is_final {
                                continue 'envelope_loop;
                            }
                        }

                        let chunk_type = if sent_first {
                            ChunkType::Last
                        } else {
                            ChunkType::Full
                        };
                        let res = FetchEventsResponse {
                            code: StatusCode::Ok.into(),
                            r#type: chunk_type.into(),
                            timeline_id: request.timeline_id,
                            event: vec![],
                            advanced_offset: latest_advanced_offset,
                        };
                        if let Err(err) = envelope.res_tx.send(Ok(res)).await {
                            warn!("Send response to client failed: {:?}", err);
                        }
                    }
                }
            }
        }
    }

    pub async fn send(
        &self,
        event: Envelope<FetchEventsRequest, FetchEventsResponse, Status>,
    ) -> Result<(), UnitError> {
        self.mailbox.send(event).await.map_err(|err| {
            UnitError::Unavailable(format!("unexpected actor mailbox status. {:?}", err))
        })
    }
}
