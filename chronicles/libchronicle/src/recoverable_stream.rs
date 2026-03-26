use crate::error::ChronicleError;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::sync::mpsc;
use tokio::task::AbortHandle;

// ---------------------------------------------------------------------------
// StreamState — active sender + its background reader, lifecycle-tied
// ---------------------------------------------------------------------------

struct StreamState<Req: Send + 'static> {
    tx: mpsc::Sender<Req>,
    reader: AbortHandle,
}

impl<Req: Send + 'static> StreamState<Req> {
    fn is_alive(&self) -> bool {
        !self.tx.is_closed() && !self.reader.is_finished()
    }
}

impl<Req: Send + 'static> Drop for StreamState<Req> {
    fn drop(&mut self) {
        self.reader.abort();
    }
}

// ---------------------------------------------------------------------------
// Factory type — async closure that opens a new stream
// ---------------------------------------------------------------------------

/// Returns `(request_sender, response_reader_handle)`.
pub(crate) type StreamFactory<Req> = Arc<
    dyn Fn()
            -> Pin<
                Box<
                    dyn Future<
                            Output = Result<
                                (mpsc::Sender<Req>, tokio::task::JoinHandle<()>),
                                ChronicleError,
                            >,
                        > + Send,
                >,
            > + Send
        + Sync,
>;

// ---------------------------------------------------------------------------
// RecoverableStream — lazy-init, self-recovering bidirectional gRPC stream
// ---------------------------------------------------------------------------

/// A self-recovering bidirectional gRPC stream.
///
/// Lazily opens on first [`send`]. If the stream or its response reader dies,
/// the next [`send`] transparently reopens both sides via the factory.
///
/// Clone is cheap (Arc internals) — all clones share the same underlying
/// stream and factory.
pub(crate) struct RecoverableStream<Req: Send + 'static> {
    state: Arc<Mutex<Option<StreamState<Req>>>>,
    factory: StreamFactory<Req>,
}

impl<Req: Send + 'static> Clone for RecoverableStream<Req> {
    fn clone(&self) -> Self {
        Self {
            state: self.state.clone(),
            factory: self.factory.clone(),
        }
    }
}

impl<Req: Send + 'static> RecoverableStream<Req> {
    pub fn new(factory: StreamFactory<Req>) -> Self {
        Self {
            state: Arc::new(Mutex::new(None)),
            factory,
        }
    }

    /// Send a request, lazily opening or recovering the stream as needed.
    pub async fn send(&self, request: Req) -> Result<(), ChronicleError> {
        let mut guard = self.state.lock().await;
        if guard.as_ref().is_none_or(|s| !s.is_alive()) {
            // Drop the old stream — aborts the reader task.
            guard.take();
            let (tx, handle) = (self.factory)().await?;
            *guard = Some(StreamState {
                tx,
                reader: handle.abort_handle(),
            });
        }
        guard
            .as_ref()
            .unwrap()
            .tx
            .send(request)
            .await
            .map_err(|_| ChronicleError::Transport("stream closed".into()))
    }
}
