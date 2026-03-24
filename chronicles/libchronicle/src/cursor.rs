use crate::conn::Conn;
use crate::error::ChronicleError;
use crate::Event;
use chronicle_proto::pb_catalog::Segment;
use chronicle_proto::pb_ext::{ChunkType, FetchEventsRequest};
use futures_util::Stream;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::warn;

type InnerStream = Pin<Box<dyn Stream<Item = Result<Event, ChronicleError>> + Send>>;

const MAX_RETRIES: usize = 5;
const DEFAULT_POLL_INTERVAL: Duration = Duration::from_millis(500);

pub struct EventStream {
    timeline_id: i64,
    segments: Vec<Segment>,
    /// Shared segment list updated by the segment subscription watcher.
    /// Used to refresh segments in tail mode.
    shared_segments: Arc<RwLock<Vec<Segment>>>,
    conns: HashMap<String, Conn>,
    position: Arc<AtomicI64>,
    inner: Option<InnerStream>,
    tail: bool,
    limit: Option<usize>,
    yielded: usize,
    timeout: Option<Duration>,
    started_at: Instant,
    poll_interval: Duration,
    current_backoff: Duration,
    retries: usize,
    /// When true, we need to refresh segments before reopening inner stream.
    needs_segment_refresh: bool,
}

impl EventStream {
    pub(crate) fn new(
        timeline_id: i64,
        segments: Vec<Segment>,
        conns: &HashMap<String, Conn>,
        start_offset: i64,
        shared_segments: Arc<RwLock<Vec<Segment>>>,
    ) -> Self {
        Self {
            timeline_id,
            segments,
            shared_segments,
            conns: conns.clone(),
            position: Arc::new(AtomicI64::new(start_offset)),
            inner: None,
            tail: false,
            limit: None,
            yielded: 0,
            timeout: None,
            started_at: Instant::now(),
            poll_interval: DEFAULT_POLL_INTERVAL,
            current_backoff: DEFAULT_POLL_INTERVAL,
            retries: 0,
            needs_segment_refresh: false,
        }
    }

    pub(crate) fn with_tail(mut self) -> Self {
        self.tail = true;
        self
    }

    pub(crate) fn with_limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    pub(crate) fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    fn segment_for_offset(segments: &[Segment], offset: i64) -> Result<&Segment, ChronicleError> {
        segments
            .iter()
            .rev()
            .find(|seg| seg.start_offset <= offset)
            .ok_or_else(|| {
                ChronicleError::Internal(format!("no segment covers offset {}", offset))
            })
    }

    fn pick_conn<'a>(
        conns: &'a HashMap<String, Conn>,
        segment: &Segment,
    ) -> Result<&'a Conn, ChronicleError> {
        for ep in &segment.ensemble {
            if let Some(conn) = conns.get(ep) {
                return Ok(conn);
            }
        }
        Err(ChronicleError::UnitNotEnough(
            "no reachable unit in segment ensemble".into(),
        ))
    }

    async fn open_inner(
        timeline_id: i64,
        segments: &[Segment],
        conns: &HashMap<String, Conn>,
        position: &Arc<AtomicI64>,
    ) -> Result<InnerStream, ChronicleError> {
        let start = position.load(Ordering::Relaxed);
        let segment = Self::segment_for_offset(segments, start)?;
        let conn = Self::pick_conn(conns, segment)?;

        let (tx, mut response_stream) = conn.open_fetch_stream(64).await?;

        tx.send(FetchEventsRequest {
            timeline_id,
            start_offset: start,
            end_offset: i64::MAX,
        })
        .await
        .map_err(|_| ChronicleError::Transport("fetch stream closed".into()))?;

        let position = position.clone();

        let stream = async_stream::try_stream! {
            let _tx = tx;
            while let Some(response) = response_stream
                .message()
                .await
                .map_err(|e| ChronicleError::Transport(e.to_string()))?
            {
                let is_final = matches!(
                    response.r#type(),
                    ChunkType::Full | ChunkType::Last
                );
                for proto_event in response.event {
                    let evt = Event {
                        offset: Some(proto_event.offset),
                        timestamp: Some(proto_event.timestamp),
                        payload: proto_event.payload.map(|b| b.to_vec()).unwrap_or_default(),
                        key: None,
                        txn_id: None,
                    };
                    position.store(proto_event.offset + 1, Ordering::Relaxed);
                    yield evt;
                }
                if is_final {
                    break;
                }
            }
        };

        Ok(Box::pin(stream))
    }

    fn is_timed_out(&self) -> bool {
        if let Some(timeout) = self.timeout {
            self.started_at.elapsed() >= timeout
        } else {
            false
        }
    }

    fn is_limit_reached(&self) -> bool {
        if let Some(limit) = self.limit {
            self.yielded >= limit
        } else {
            false
        }
    }
}

impl Stream for EventStream {
    type Item = Result<Event, ChronicleError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            if self.is_limit_reached() || self.is_timed_out() {
                return Poll::Ready(None);
            }

            // Refresh segments from the shared list if needed (tail mode).
            if self.needs_segment_refresh {
                let shared = self.shared_segments.clone();
                let mut fut = Box::pin(async move { shared.read().await.clone() });
                match fut.as_mut().poll(cx) {
                    Poll::Ready(new_segments) => {
                        self.segments = new_segments;
                        self.needs_segment_refresh = false;
                    }
                    Poll::Pending => return Poll::Pending,
                }
            }

            if self.inner.is_none() {
                let timeline_id = self.timeline_id;
                let segments = self.segments.clone();
                let conns = self.conns.clone();
                let position = self.position.clone();

                let mut fut = Box::pin(Self::open_inner(timeline_id, &segments, &conns, &position));
                match fut.as_mut().poll(cx) {
                    Poll::Ready(Ok(stream)) => {
                        self.inner = Some(stream);
                    }
                    Poll::Ready(Err(e)) => {
                        return Poll::Ready(Some(Err(e)));
                    }
                    Poll::Pending => return Poll::Pending,
                }
            }

            let stream = self.inner.as_mut().unwrap();
            match stream.as_mut().poll_next(cx) {
                Poll::Ready(Some(Ok(event))) => {
                    self.retries = 0;
                    self.current_backoff = self.poll_interval;
                    self.yielded += 1;
                    return Poll::Ready(Some(Ok(event)));
                }
                Poll::Ready(Some(Err(e))) => {
                    self.retries += 1;
                    if self.retries > MAX_RETRIES {
                        return Poll::Ready(Some(Err(e)));
                    }
                    warn!(
                        error = %e,
                        retry = self.retries,
                        "fetch stream error, reconnecting"
                    );
                    self.inner = None;
                    continue;
                }
                Poll::Ready(None) => {
                    if self.tail {
                        self.inner = None;
                        self.needs_segment_refresh = true;
                        cx.waker().wake_by_ref();
                        return Poll::Pending;
                    }
                    return Poll::Ready(None);
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}
