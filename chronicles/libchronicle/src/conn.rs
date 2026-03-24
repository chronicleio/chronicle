use crate::error::ChronicleError;
use chronicle_proto::pb_ext::{
    FenceRequest, FenceResponse, FetchEventsRequest, FetchEventsResponse, RecordEventsRequest,
    RecordEventsResponse,
    chronicle_client::ChronicleClient,
};
use dashmap::DashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::{Channel, Endpoint};
use tracing::{info, warn};

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

#[derive(Clone)]
pub struct Conn {
    endpoint: String,
    client: ChronicleClient<Channel>,
}

impl Conn {
    pub fn connect(endpoint: &str, opts: &ConnOptions) -> Result<Self, ChronicleError> {
        let channel = Self::build_channel(endpoint, opts)?;
        let client = ChronicleClient::new(channel);
        Ok(Self {
            endpoint: endpoint.to_string(),
            client,
        })
    }

    fn build_channel(endpoint: &str, opts: &ConnOptions) -> Result<Channel, ChronicleError> {
        let channel = Endpoint::from_shared(endpoint.to_string())
            .map_err(|e| ChronicleError::Transport(format!("{}: {}", endpoint, e)))?
            .connect_timeout(opts.connect_timeout)
            .timeout(opts.request_timeout)
            .http2_keep_alive_interval(opts.keep_alive_interval)
            .keep_alive_timeout(opts.keep_alive_timeout)
            .keep_alive_while_idle(true)
            .connect_lazy();
        Ok(channel)
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

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

    /// Open a fire-and-forget record stream. Responses are forwarded to `ack_tx`
    /// by a background reader task.
    pub async fn open_record_stream(
        &self,
        ack_tx: mpsc::Sender<UnitAck>,
    ) -> Result<RecordStream, ChronicleError> {
        let mut client = self.client.clone();
        let (stream_tx, stream_rx) = mpsc::channel::<RecordEventsRequest>(64);
        let stream = ReceiverStream::new(stream_rx);
        let response = client
            .record(stream)
            .await
            .map_err(|e| ChronicleError::Transport(e.to_string()))?;
        let response_stream = response.into_inner();

        let ep = self.endpoint.clone();
        tokio::spawn(response_reader(ep, response_stream, ack_tx));

        Ok(RecordStream { tx: stream_tx })
    }

    pub async fn open_fetch_stream(
        &self,
        buffer: usize,
    ) -> Result<
        (
            mpsc::Sender<FetchEventsRequest>,
            tonic::Streaming<FetchEventsResponse>,
        ),
        ChronicleError,
    > {
        let mut client = self.client.clone();
        let (tx, rx) = mpsc::channel(buffer);
        let stream = ReceiverStream::new(rx);
        let response = client
            .fetch(stream)
            .await
            .map_err(|e| ChronicleError::Transport(e.to_string()))?;
        Ok((tx, response.into_inner()))
    }
}

// ---------------------------------------------------------------------------
// RecordStream — fire-and-forget sender into a per-unit gRPC stream
// ---------------------------------------------------------------------------

pub struct RecordStream {
    tx: mpsc::Sender<RecordEventsRequest>,
}

impl RecordStream {
    /// Send a batch into the stream without waiting for ack.
    pub async fn send(&self, request: RecordEventsRequest) -> Result<(), ChronicleError> {
        self.tx
            .send(request)
            .await
            .map_err(|_| ChronicleError::Transport("record stream closed".into()))
    }
}

// ---------------------------------------------------------------------------
// UnitAck — a cumulative ack received from a unit
// ---------------------------------------------------------------------------

pub struct UnitAck {
    pub endpoint: String,
    pub result: Result<RecordEventsResponse, ChronicleError>,
}

/// Background task: reads cumulative ack responses from a unit's gRPC stream
/// and forwards them to the shared ack channel.
async fn response_reader(
    endpoint: String,
    mut stream: tonic::Streaming<RecordEventsResponse>,
    ack_tx: mpsc::Sender<UnitAck>,
) {
    loop {
        match stream.message().await {
            Ok(Some(resp)) => {
                if ack_tx
                    .send(UnitAck {
                        endpoint: endpoint.clone(),
                        result: Ok(resp),
                    })
                    .await
                    .is_err()
                {
                    break;
                }
            }
            Ok(None) => {
                let _ = ack_tx
                    .send(UnitAck {
                        endpoint: endpoint.clone(),
                        result: Err(ChronicleError::Transport("stream ended".into())),
                    })
                    .await;
                break;
            }
            Err(e) => {
                let _ = ack_tx
                    .send(UnitAck {
                        endpoint: endpoint.clone(),
                        result: Err(ChronicleError::Transport(e.to_string())),
                    })
                    .await;
                break;
            }
        }
    }
    warn!(endpoint = %endpoint, "response_reader: ended");
}

// ---------------------------------------------------------------------------
// ConnPool
// ---------------------------------------------------------------------------

struct ConnGroup {
    conns: Vec<Conn>,
    next: AtomicUsize,
}

pub struct ConnPool {
    entries: DashMap<String, ConnGroup>,
    opts: ConnOptions,
}

impl ConnPool {
    pub fn new(opts: ConnOptions) -> Self {
        Self {
            entries: DashMap::new(),
            opts,
        }
    }

    pub fn get_or_connect(&self, endpoint: &str) -> Result<Conn, ChronicleError> {
        if let Some(entry) = self.entries.get(endpoint) {
            let idx = entry.next.fetch_add(1, Ordering::Relaxed) % entry.conns.len();
            return Ok(entry.conns[idx].clone());
        }
        let mut conns = Vec::with_capacity(self.opts.conns_per_unit);
        for _ in 0..self.opts.conns_per_unit {
            conns.push(Conn::connect(endpoint, &self.opts)?);
        }
        info!(address = %endpoint, count = self.opts.conns_per_unit, "connected to unit");
        let entry = ConnGroup {
            conns,
            next: AtomicUsize::new(0),
        };
        let conn = entry.conns[0].clone();
        self.entries.insert(endpoint.to_string(), entry);
        Ok(conn)
    }
}
