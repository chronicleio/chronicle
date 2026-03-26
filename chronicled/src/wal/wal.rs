use crate::error::unit_error::UnitError;
use crate::option::unit_options::IoMode;
use crate::segment::Segment;
use crate::segment::direct::DirectSegment;
use crate::segment::mmap::MmapSegment;
use crate::segment::record::{Record, RecordBatch};
use crate::segment::standard::StandardSegment;
use crate::wal::INVALID_OFFSET;
use async_stream::stream;
use futures_util::stream::Stream;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::select;
use tokio::sync::Mutex;
use tokio::sync::mpsc::{Receiver as MpscReceiver, channel};
use tokio::sync::oneshot;
use tokio::sync::watch;
use tokio::sync::watch::Receiver;
use tokio::task::JoinHandle;
use tokio::time::interval;
use tokio::{sync, task};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

const BATCH_FLUSH_INTERVAL_MS: u64 = 1;
const MAX_BATCH_SIZE: usize = 512;
const DEFAULT_MAX_SEGMENT_SIZE: u64 = 64 * 1024 * 1024;

fn segment_filename(segment_id: u64) -> String {
    format!("{:08}.log", segment_id)
}

fn parse_segment_id(filename: &str) -> Option<u64> {
    let stem = filename.strip_suffix(".log")?;
    stem.parse::<u64>().ok()
}

async fn open_segment(path: PathBuf, io_mode: IoMode) -> Result<Box<dyn Segment>, UnitError> {
    match io_mode {
        IoMode::Advanced => {
            let ds = DirectSegment::new(path)
                .await
                .map_err(|e| UnitError::Storage(e.to_string()))?;
            Ok(Box::new(ds))
        }
        IoMode::Basic => {
            let s = StandardSegment::new(path)
                .await
                .map_err(|e| UnitError::Storage(e.to_string()))?;
            Ok(Box::new(s))
        }
        IoMode::Mmap => {
            let ms = MmapSegment::new(path)
                .await
                .map_err(|e| UnitError::Storage(e.to_string()))?;
            Ok(Box::new(ms))
        }
    }
}

fn discover_segments(dir: &Path) -> Vec<(u64, PathBuf)> {
    let mut segments = Vec::new();
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let name = entry.file_name();
            if let Some(name_str) = name.to_str() {
                if let Some(id) = parse_segment_id(name_str) {
                    segments.push((id, entry.path()));
                }
            }
        }
    }
    segments.sort_by_key(|(id, _)| *id);
    segments
}

struct WalState {
    dir: PathBuf,
    io_mode: IoMode,
    max_segment_size: u64,
    current_segment: Box<dyn Segment>,
    current_segment_id: u64,
}

impl WalState {
    async fn rotate(&mut self) -> Result<u64, UnitError> {
        if let Err(e) = self.current_segment.sync().await {
            warn!(error = ?e, "failed to sync segment before rotation");
        }

        let new_id = self.current_segment_id + 1;
        let path = self.dir.join(segment_filename(new_id));
        let new_segment = open_segment(path, self.io_mode).await?;
        self.current_segment = new_segment;
        self.current_segment_id = new_id;
        info!(segment_id = new_id, "wal rotated to new segment");
        Ok(new_id)
    }

    fn needs_rotation(&self, additional_bytes: usize) -> bool {
        self.current_segment.offset() + additional_bytes as u64 > self.max_segment_size
    }

    fn global_offset(&self, local_offset: u64) -> i64 {
        ((self.current_segment_id as i64) << 32) | (local_offset as i64 & 0xFFFF_FFFF)
    }
}

struct Inner {
    buffer: sync::mpsc::Sender<(Vec<u8>, oneshot::Sender<i64>)>,
    commit_offset: Receiver<i64>,
    state: Mutex<WalState>,
}

impl Inner {
    async fn sync_data(&self) {
        if let Err(e) = self.state.lock().await.current_segment.sync().await {
            warn!(error = ?e, "failed to sync writable segment");
        }
    }
}

pub struct WalOptions {
    pub dir: String,
    pub max_segment_size: Option<u64>,
    pub io_mode: IoMode,
}

#[derive(Clone)]
pub struct Wal {
    context: CancellationToken,
    inner: Arc<Inner>,
    wal_writer_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
    wal_syncer_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
    dir: PathBuf,
    io_mode: IoMode,
}

impl Wal {
    pub async fn new(options: WalOptions) -> Result<Wal, UnitError> {
        let (buf_tx, buf_rx) = channel::<(Vec<u8>, oneshot::Sender<i64>)>(1024);

        let (advanced_offset_tx, advanced_offset_rx) = watch::channel(INVALID_OFFSET);
        let (commit_offset_tx, commit_offset_rx) = watch::channel(INVALID_OFFSET);

        let dir = PathBuf::from(&options.dir);

        if let Err(e) = tokio::fs::create_dir_all(&options.dir).await {
            return Err(UnitError::Storage(format!(
                "Failed to create WAL directory: {}",
                e
            )));
        }

        let max_segment_size = options.max_segment_size.unwrap_or(DEFAULT_MAX_SEGMENT_SIZE);

        let existing = discover_segments(&dir);
        let (current_segment_id, segment) = if let Some((last_id, last_path)) = existing.last() {
            let seg = open_segment(last_path.clone(), options.io_mode).await?;
            (*last_id, seg)
        } else {
            let path = dir.join(segment_filename(0));
            let seg = open_segment(path, options.io_mode).await?;
            (0, seg)
        };

        info!(
            segment_id = current_segment_id,
            segments_found = existing.len(),
            "wal initialized with multi-segment support"
        );

        let context = CancellationToken::new();

        let wal_state = WalState {
            dir: dir.clone(),
            io_mode: options.io_mode,
            max_segment_size,
            current_segment: segment,
            current_segment_id,
        };

        let inner = Arc::new(Inner {
            buffer: buf_tx,
            commit_offset: commit_offset_rx,
            state: Mutex::new(wal_state),
        });

        let wal_writer_handle = task::spawn(bg_wal_writer(
            context.clone(),
            inner.clone(),
            buf_rx,
            advanced_offset_tx,
        ));
        let wal_syncer_handle = task::spawn(bg_wal_syncer(
            inner.clone(),
            advanced_offset_rx,
            commit_offset_tx,
        ));

        Ok(Wal {
            context,
            inner,
            wal_writer_handle: Arc::new(Mutex::new(Some(wal_writer_handle))),
            wal_syncer_handle: Arc::new(Mutex::new(Some(wal_syncer_handle))),
            dir,
            io_mode: options.io_mode,
        })
    }

    pub async fn append(&self, data: Vec<u8>) -> Result<i64, UnitError> {
        let (tx, rx) = oneshot::channel();
        self.inner
            .buffer
            .send((data, tx))
            .await
            .map_err(|_| UnitError::Wal)?;
        rx.await.map_err(|_| UnitError::Wal)
    }

    pub fn watch_synced(&self) -> Receiver<i64> {
        self.inner.commit_offset.clone()
    }

    pub async fn read_stream(
        &self,
    ) -> Pin<Box<dyn Stream<Item = Result<Vec<u8>, UnitError>> + Send + '_>> {
        self.read_stream_from(0).await
    }

    pub async fn read_stream_from(
        &self,
        from_segment_id: u64,
    ) -> Pin<Box<dyn Stream<Item = Result<Vec<u8>, UnitError>> + Send + '_>> {
        let segments = discover_segments(&self.dir);
        let io_mode = self.io_mode;

        let replay_segments: Vec<(u64, PathBuf)> = segments
            .into_iter()
            .filter(|(id, _)| *id >= from_segment_id)
            .collect();

        Box::pin(stream! {
            for (seg_id, path) in replay_segments {
                let seg_result = open_segment(path.clone(), io_mode).await;
                let mut seg = match seg_result {
                    Ok(s) => s,
                    Err(e) => {
                        warn!(segment_id = seg_id, error = ?e, "failed to open wal segment for replay");
                        yield Err(e);
                        return;
                    }
                };

                let data = match seg.read_all().await {
                    Ok(d) => d,
                    Err(e) => {
                        warn!(segment_id = seg_id, error = ?e, "failed to read wal segment");
                        yield Err(UnitError::Storage(e.to_string()));
                        return;
                    }
                };

                let mut offset = 0;
                while offset < data.len() {
                    match Record::decode(&data[offset..]) {
                        Ok((record, size)) => {
                            yield Ok(record.data);
                            offset += size;
                        }
                        Err(e) => {
                            warn!(
                                segment_id = seg_id,
                                offset = offset,
                                error = %e,
                                "failed to decode wal record"
                            );
                            break;
                        }
                    }
                }
            }
        })
    }

    pub async fn trim(&self, below_segment_id: u64) -> Result<u64, UnitError> {
        let segments = discover_segments(&self.dir);
        let mut trimmed = 0u64;

        for (seg_id, path) in segments {
            if seg_id >= below_segment_id {
                break;
            }
            match tokio::fs::remove_file(&path).await {
                Ok(_) => {
                    trimmed += 1;
                    info!(segment_id = seg_id, "wal segment trimmed");
                }
                Err(e) => {
                    warn!(segment_id = seg_id, error = ?e, "failed to trim wal segment");
                }
            }
        }

        Ok(trimmed)
    }

    pub async fn current_segment_id(&self) -> u64 {
        self.inner.state.lock().await.current_segment_id
    }

    pub fn cancel(&self) {
        self.context.cancel();
    }
}

async fn bg_wal_writer(
    context: CancellationToken,
    inner: Arc<Inner>,
    mut buf_rx: MpscReceiver<(Vec<u8>, oneshot::Sender<i64>)>,
    advanced_offset_tx: watch::Sender<i64>,
) {
    let mut batch_timer = interval(Duration::from_millis(BATCH_FLUSH_INTERVAL_MS));
    batch_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let mut pending_batch = RecordBatch::new();
    let mut pending_senders = Vec::new();

    loop {
        select! {
            _ = context.cancelled() => {
                if !pending_batch.is_empty() {
                    flush_batch(&inner, pending_batch, pending_senders, &advanced_offset_tx).await;
                }
                info!("wal writer stopped");
                break;
            }
            _ = batch_timer.tick() => {
                if !pending_batch.is_empty() {
                    let batch = std::mem::replace(&mut pending_batch, RecordBatch::new());
                    let senders = std::mem::take(&mut pending_senders);
                    flush_batch(&inner, batch, senders, &advanced_offset_tx).await;
                }
            }
            Some((data, offset_tx)) = buf_rx.recv() => {
                let record = Record::new(data);
                pending_batch.add(record);
                pending_senders.push(offset_tx);

                while pending_batch.len() < MAX_BATCH_SIZE {
                    match buf_rx.try_recv() {
                        Ok((data, tx)) => {
                            pending_batch.add(Record::new(data));
                            pending_senders.push(tx);
                        }
                        Err(_) => break,
                    }
                }

                if pending_batch.len() >= MAX_BATCH_SIZE {
                    let batch = std::mem::replace(&mut pending_batch, RecordBatch::new());
                    let senders = std::mem::take(&mut pending_senders);
                    flush_batch(&inner, batch, senders, &advanced_offset_tx).await;
                }
            }
        }
    }
}

async fn flush_batch(
    inner: &Arc<Inner>,
    batch: RecordBatch,
    senders: Vec<oneshot::Sender<i64>>,
    advanced_offset_tx: &watch::Sender<i64>,
) {
    let record_sizes: Result<Vec<usize>, _> = batch
        .records
        .iter()
        .map(|r| r.encode().map(|e| e.len()))
        .collect();

    let record_sizes = match record_sizes {
        Ok(sizes) => sizes,
        Err(e) => {
            warn!(error = ?e, "failed to encode records in batch");
            return;
        }
    };

    let encoded = match batch.encode() {
        Ok(e) => e,
        Err(e) => {
            warn!(error = ?e, "failed to encode batch");
            return;
        }
    };

    let mut state = inner.state.lock().await;

    if state.needs_rotation(encoded.len()) {
        if let Err(e) = state.rotate().await {
            warn!(error = ?e, "failed to rotate wal segment");
            return;
        }
    }

    let encoded_len = encoded.len();
    match state.current_segment.write(&encoded).await {
        Ok(base_offset) => {
            if let Some(m) = crate::observability::global_metrics() {
                m.wal_writes.add(1, &[]);
                m.wal_bytes.add(encoded_len as u64, &[]);
            }

            let global_base = state.global_offset(base_offset);

            let mut current_offset = global_base;
            for (sender, size) in senders.into_iter().zip(record_sizes.iter()) {
                if sender.send(current_offset).is_err() {
                    warn!("failed to send offset back to caller");
                }
                current_offset += *size as i64;
            }

            let final_offset = global_base + encoded_len as i64;
            if advanced_offset_tx.send(final_offset).is_err() {
                warn!("no active subscriber for advanced offset");
            }
        }
        Err(e) => {
            warn!(error = ?e, "failed to write batch to segment");
        }
    }
}

async fn bg_wal_syncer(
    inner: Arc<Inner>,
    mut advanced_offset_rx: watch::Receiver<i64>,
    commit_offset_tx: watch::Sender<i64>,
) {
    loop {
        match advanced_offset_rx.changed().await {
            Ok(_) => {
                let advanced_offset = *advanced_offset_rx.borrow();
                let start = std::time::Instant::now();
                inner.sync_data().await;
                if let Some(m) = crate::observability::global_metrics() {
                    m.wal_sync_latency.record(start.elapsed().as_secs_f64(), &[]);
                }
                if let Err(err) = commit_offset_tx.send(advanced_offset) {
                    warn!(error = ?err, "no active subscriber for synced offset");
                }
            }
            Err(err) => {
                warn!(error = ?err, "advanced offset watch channel closed");
                break;
            }
        };
    }
}
