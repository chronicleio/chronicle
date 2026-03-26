use crate::error::ChronicleError;
use crate::recoverable_stream::RecoverableStream;
use chronicle_proto::pb_ext::{
    FenceRequest, FenceResponse, FetchEventsRequest, FetchEventsResponse, RecordEventsRequest,
    RecordEventsResponse,
    chronicle_client::ChronicleClient,
};
use dashmap::DashMap;
use futures_util::Stream;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;
use tracing::warn;

// ---------------------------------------------------------------------------
// ConnOptions
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct ConnOptions {
    pub conns_per_unit: usize,
    pub connect_timeout: Duration,
    pub request_timeout: Duration,
    pub keep_alive_interval: Duration,
    pub keep_alive_timeout: Duration,
}

impl Default for ConnOptions {
    fn default() -> Self {
        let cpus = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);
        Self {
            conns_per_unit: cpus.max(1),
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(30),
            keep_alive_interval: Duration::from_secs(10),
            keep_alive_timeout: Duration::from_secs(5),
        }
    }
}

// ---------------------------------------------------------------------------
// Watermark — a write-ack received from a unit, carrying its synced offset
// ---------------------------------------------------------------------------

pub struct Watermark {
    pub endpoint: String,
    pub result: Result<RecordEventsResponse, ChronicleError>,
}

// ---------------------------------------------------------------------------
// Conn — one logical connection with its own record and fetch streams
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct Conn {
    endpoint: String,
    client: ChronicleClient<Channel>,
    record_stream: RecoverableStream<RecordEventsRequest>,
    fetch_stream: RecoverableStream<FetchEventsRequest>,
    wm_subscribers: Arc<DashMap<i64, mpsc::Sender<Watermark>>>,
    fetch_subscribers: Arc<DashMap<i64, mpsc::Sender<Result<FetchEventsResponse, ChronicleError>>>>,
}

impl Conn {
    pub(crate) fn new(endpoint: String, client: ChronicleClient<Channel>) -> Self {
        let wm_subscribers = Arc::new(DashMap::new());
        let fetch_subscribers: Arc<DashMap<i64, mpsc::Sender<Result<FetchEventsResponse, ChronicleError>>>> =
            Arc::new(DashMap::new());

        let record_stream = {
            let client = client.clone();
            let ep = endpoint.clone();
            let subs = wm_subscribers.clone();
            RecoverableStream::new(Arc::new(move || {
                let mut client = client.clone();
                let ep = ep.clone();
                let subs = subs.clone();
                Box::pin(async move {
                    let (tx, rx) = mpsc::channel::<RecordEventsRequest>(64);
                    let stream = ReceiverStream::new(rx);
                    let response = client
                        .record(stream)
                        .await
                        .map_err(|e| ChronicleError::Transport(e.to_string()))?;
                    let handle =
                        tokio::spawn(record_response_reader(ep, response.into_inner(), subs));
                    Ok((tx, handle))
                })
            }))
        };

        let fetch_stream = {
            let client = client.clone();
            let ep = endpoint.clone();
            let subs = fetch_subscribers.clone();
            RecoverableStream::new(Arc::new(move || {
                let mut client = client.clone();
                let ep = ep.clone();
                let subs = subs.clone();
                Box::pin(async move {
                    let (tx, rx) = mpsc::channel::<FetchEventsRequest>(64);
                    let stream = ReceiverStream::new(rx);
                    let response = client
                        .fetch(stream)
                        .await
                        .map_err(|e| ChronicleError::Transport(e.to_string()))?;
                    let handle =
                        tokio::spawn(fetch_response_reader(ep, response.into_inner(), subs));
                    Ok((tx, handle))
                })
            }))
        };

        Self {
            endpoint,
            client,
            record_stream,
            fetch_stream,
            wm_subscribers,
            fetch_subscribers,
        }
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    // -- watermark subscribers ------------------------------------------------

    pub fn subscribe_watermark(&self, timeline_id: i64, tx: mpsc::Sender<Watermark>) {
        self.wm_subscribers.insert(timeline_id, tx);
    }

    pub fn unsubscribe_watermark(&self, timeline_id: i64) {
        self.wm_subscribers.remove(&timeline_id);
    }

    // -- RPC ------------------------------------------------------------------

    pub async fn fence(
        &self,
        timeline_id: i64,
        term: i64,
    ) -> Result<FenceResponse, ChronicleError> {
        let mut client = self.client.clone();
        let response = client
            .fence(FenceRequest { timeline_id, term })
            .await
            .map_err(|e| ChronicleError::Transport(e.to_string()))?;
        Ok(response.into_inner())
    }

    /// Send a record request. The gRPC stream is lazily opened on first call
    /// and automatically reconnected if the previous stream died.
    pub async fn send_record(
        &self,
        request: RecordEventsRequest,
    ) -> Result<(), ChronicleError> {
        self.record_stream.send(request).await
    }

    /// Start a fetch. Subscribes for responses, sends the request through the
    /// shared fetch stream, and returns a [`FetchStream`] that yields
    /// responses. Automatically unsubscribes when dropped.
    pub async fn fetch(
        &self,
        request: FetchEventsRequest,
    ) -> Result<FetchStream, ChronicleError> {
        let timeline_id = request.timeline_id;
        let (tx, rx) = mpsc::channel::<Result<FetchEventsResponse, ChronicleError>>(64);
        self.fetch_subscribers.insert(timeline_id, tx);
        self.fetch_stream.send(request).await?;
        Ok(FetchStream {
            rx,
            timeline_id,
            subscribers: self.fetch_subscribers.clone(),
        })
    }
}

// ---------------------------------------------------------------------------
// FetchStream — Stream wrapper that unsubscribes on drop
// ---------------------------------------------------------------------------

pub struct FetchStream {
    rx: mpsc::Receiver<Result<FetchEventsResponse, ChronicleError>>,
    timeline_id: i64,
    subscribers: Arc<DashMap<i64, mpsc::Sender<Result<FetchEventsResponse, ChronicleError>>>>,
}

impl Stream for FetchStream {
    type Item = Result<FetchEventsResponse, ChronicleError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx)
    }
}

impl Drop for FetchStream {
    fn drop(&mut self) {
        self.subscribers.remove(&self.timeline_id);
    }
}

// ---------------------------------------------------------------------------
// Record response reader — demuxes watermarks by timeline_id to subscribers
// ---------------------------------------------------------------------------

async fn record_response_reader(
    endpoint: String,
    mut stream: tonic::Streaming<RecordEventsResponse>,
    subscribers: Arc<DashMap<i64, mpsc::Sender<Watermark>>>,
) {
    let reason = loop {
        match stream.message().await {
            Ok(Some(resp)) => {
                let timeline_id = resp.timeline_id;
                if let Some(tx) = subscribers.get(&timeline_id) {
                    let _ = tx
                        .send(Watermark {
                            endpoint: endpoint.clone(),
                            result: Ok(resp),
                        })
                        .await;
                }
            }
            Ok(None) => break "stream ended".to_string(),
            Err(e) => {
                warn!(endpoint = %endpoint, error = %e, "record_response_reader: error");
                break e.to_string();
            }
        }
    };
    // Notify all subscribers so they don't block forever.
    for entry in subscribers.iter() {
        let _ = entry
            .value()
            .send(Watermark {
                endpoint: endpoint.clone(),
                result: Err(ChronicleError::Transport(reason.clone())),
            })
            .await;
    }
    warn!(endpoint = %endpoint, reason = %reason, "record_response_reader: ended");
}

// ---------------------------------------------------------------------------
// Fetch response reader — demuxes fetch responses by timeline_id to subscribers
// ---------------------------------------------------------------------------

async fn fetch_response_reader(
    endpoint: String,
    mut stream: tonic::Streaming<FetchEventsResponse>,
    subscribers: Arc<DashMap<i64, mpsc::Sender<Result<FetchEventsResponse, ChronicleError>>>>,
) {
    let reason = loop {
        match stream.message().await {
            Ok(Some(resp)) => {
                let timeline_id = resp.timeline_id;
                if let Some(tx) = subscribers.get(&timeline_id) {
                    match tx.try_send(Ok(resp)) {
                        Ok(()) => {}
                        Err(mpsc::error::TrySendError::Closed(_)) => {
                            subscribers.remove(&timeline_id);
                        }
                        Err(mpsc::error::TrySendError::Full(_)) => {
                            warn!(
                                endpoint = %endpoint,
                                timeline_id = timeline_id,
                                "fetch subscriber full, dropping response"
                            );
                        }
                    }
                }
            }
            Ok(None) => break "stream ended".to_string(),
            Err(e) => {
                warn!(endpoint = %endpoint, error = %e, "fetch_response_reader: error");
                break e.to_string();
            }
        }
    };
    // Notify all subscribers with the error, then clear.
    for entry in subscribers.iter() {
        let _ = entry
            .value()
            .try_send(Err(ChronicleError::Transport(reason.clone())));
    }
    subscribers.clear();
    warn!(endpoint = %endpoint, reason = %reason, "fetch_response_reader: ended");
}
