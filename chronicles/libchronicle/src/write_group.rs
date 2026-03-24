use crate::error::ChronicleError;
use crate::state_machine::StateMachine;
use crate::{Event as UserEvent, Offset};
use std::sync::Mutex;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tracing::warn;

struct PendingEvent {
    event: UserEvent,
    tx: oneshot::Sender<Result<Offset, ChronicleError>>,
}

/// A write shard — owns a batch loop that feeds into the state machine.
pub(crate) struct WriteGroup {
    event_tx: Mutex<Option<mpsc::Sender<PendingEvent>>>,
    group_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl WriteGroup {
    pub fn start(
        sm: StateMachine,
        max_batch_size: usize,
        linger: Duration,
    ) -> Self {
        let (event_tx, event_rx) = mpsc::channel::<PendingEvent>(max_batch_size * 2);

        let group_task = tokio::spawn(async move {
            batch_loop(sm, event_rx, max_batch_size, linger).await;
        });

        Self {
            event_tx: Mutex::new(Some(event_tx)),
            group_task: Mutex::new(Some(group_task)),
        }
    }

    pub async fn record(&self, event: UserEvent) -> Result<Offset, ChronicleError> {
        let sender = {
            let tx = self.event_tx.lock().unwrap();
            tx.clone()
                .ok_or_else(|| ChronicleError::Internal("timeline closed".into()))?
        };
        let (tx, rx) = oneshot::channel();
        sender
            .send(PendingEvent { event, tx })
            .await
            .map_err(|_| ChronicleError::Internal("timeline closed".into()))?;
        rx.await
            .map_err(|_| ChronicleError::Internal("ack channel dropped".into()))?
    }

    pub async fn close(&self) {
        let task = {
            self.event_tx.lock().unwrap().take();
            self.group_task.lock().unwrap().take()
        };
        if let Some(task) = task {
            let _ = task.await;
        }
    }
}

// --- batching loop ---

async fn batch_loop(
    sm: StateMachine,
    mut event_rx: mpsc::Receiver<PendingEvent>,
    max_batch_size: usize,
    linger: Duration,
) {
    let mut batch: Vec<PendingEvent> = Vec::with_capacity(max_batch_size);

    loop {
        batch.clear();

        match event_rx.recv().await {
            Some(first) => batch.push(first),
            None => return,
        }

        let deadline = tokio::time::sleep(linger);
        tokio::pin!(deadline);

        loop {
            tokio::select! {
                biased;
                event = event_rx.recv() => {
                    match event {
                        Some(e) => {
                            batch.push(e);
                            if batch.len() >= max_batch_size {
                                break;
                            }
                        }
                        None => break,
                    }
                }
                _ = &mut deadline => break,
            }
        }

        if batch.is_empty() {
            continue;
        }

        let mut events = Vec::with_capacity(batch.len());
        let mut txs = Vec::with_capacity(batch.len());
        for p in batch.drain(..) {
            events.push(p.event);
            txs.push(p.tx);
        }

        let (items, offsets) = sm.prepare_batch(events);

        match sm.apply(items).await {
            Ok(()) => {
                for (offset, tx) in offsets.into_iter().zip(txs) {
                    let _ = tx.send(Ok(Offset(offset)));
                }
            }
            Err(e) => {
                let msg = e.to_string();
                warn!(error = %msg, "failed to apply batch");
                for tx in txs {
                    let _ = tx.send(Err(ChronicleError::Internal(msg.clone())));
                }
            }
        }
    }
}
