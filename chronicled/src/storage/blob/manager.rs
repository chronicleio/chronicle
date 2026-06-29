use std::collections::HashMap;
use std::fs;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use chronicle_proto::pb_ext::Event;
use chronicle_proto::pb_storage;
use lru::LruCache;
use prost::Message;

use std::time::{SystemTime, UNIX_EPOCH};

use super::remote::RemoteStore;
use super::{BlobReader, BlobWriter};
use crate::error::unit_error::UnitError;
use crate::option::unit_options::IoMode;
use crate::segment::Segment;
use crate::segment::direct::DirectSegment;
use crate::segment::mmap::MmapSegment;
use crate::segment::standard::StandardSegment;
use crate::storage::index::{IndexEntry, Storage};

#[derive(Debug, Clone)]
pub enum SegmentLocation {
    Local { path: String },
    Remote { key: String },
}

#[derive(Debug, Clone)]
pub struct SegmentMeta {
    pub id: u64,
    pub level: u32,
    pub size: u64,
    pub entry_count: u64,
    pub location: SegmentLocation,
    pub created_at: i64,
}

impl SegmentMeta {
    fn encode(&self) -> Vec<u8> {
        let proto = pb_storage::SegmentMeta {
            id: self.id,
            level: self.level,
            size: self.size,
            entry_count: self.entry_count,
            location: Some(match &self.location {
                SegmentLocation::Local { path } => pb_storage::SegmentLocation {
                    kind: Some(pb_storage::segment_location::Kind::Local(
                        pb_storage::LocalSegment { path: path.clone() },
                    )),
                },
                SegmentLocation::Remote { key } => pb_storage::SegmentLocation {
                    kind: Some(pb_storage::segment_location::Kind::Remote(
                        pb_storage::RemoteSegment { key: key.clone() },
                    )),
                },
            }),
            created_at: self.created_at,
        };
        proto.encode_to_vec()
    }

    fn decode(data: &[u8]) -> Option<Self> {
        let proto = pb_storage::SegmentMeta::decode(data).ok()?;
        let location = match proto.location?.kind? {
            pb_storage::segment_location::Kind::Local(l) => SegmentLocation::Local { path: l.path },
            pb_storage::segment_location::Kind::Remote(r) => SegmentLocation::Remote { key: r.key },
        };
        Some(SegmentMeta {
            id: proto.id,
            level: proto.level,
            size: proto.size,
            entry_count: proto.entry_count,
            location,
            created_at: proto.created_at,
        })
    }
}

struct CachedRemoteSegment {
    reader: BlobReader,
    cached_path: PathBuf,
}

pub struct SegmentManager {
    segments_dir: PathBuf,
    cache_dir: PathBuf,
    readers: RwLock<HashMap<u64, BlobReader>>,
    next_segment_id: AtomicU64,
    io_mode: IoMode,
    index: Storage,
    remote_cache: Mutex<LruCache<u64, CachedRemoteSegment>>,
    remote_store: Option<Arc<dyn RemoteStore>>,
}

fn parse_segment_filename(name: &str) -> Option<(u32, u64)> {
    if let Some(rest) = name.strip_suffix(".cseg") {
        if let Some(rest) = rest.strip_prefix("L") {
            let parts: Vec<&str> = rest.splitn(2, '_').collect();
            if parts.len() == 2 {
                let level = parts[0].parse::<u32>().ok()?;
                let id = parts[1].parse::<u64>().ok()?;
                return Some((level, id));
            }
        }
        if let Some(id_str) = rest.strip_prefix("segment_") {
            let id = id_str.parse::<u64>().ok()?;
            return Some((0, id));
        }
    }
    None
}

fn segment_filename(level: u32, id: u64) -> String {
    format!("L{}_{:06}.cseg", level, id)
}

const DEFAULT_REMOTE_CACHE_SIZE: usize = 64;

fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

impl SegmentManager {
    pub fn recover(
        segments_dir: PathBuf,
        io_mode: IoMode,
        index: Storage,
    ) -> Result<Self, UnitError> {
        Self::recover_with_remote(
            segments_dir,
            io_mode,
            None,
            DEFAULT_REMOTE_CACHE_SIZE,
            index,
        )
    }

    pub fn recover_with_remote(
        segments_dir: PathBuf,
        io_mode: IoMode,
        remote_store: Option<Arc<dyn RemoteStore>>,
        remote_cache_capacity: usize,
        index: Storage,
    ) -> Result<Self, UnitError> {
        fs::create_dir_all(&segments_dir)
            .map_err(|e| UnitError::Storage(format!("failed to create segments dir: {}", e)))?;

        let cache_dir = segments_dir.join(".remote_cache");
        fs::create_dir_all(&cache_dir)
            .map_err(|e| UnitError::Storage(format!("failed to create cache dir: {}", e)))?;

        let mut max_id = 0u64;

        for entry in fs::read_dir(&segments_dir)
            .map_err(|e| UnitError::Storage(format!("failed to read segments dir: {}", e)))?
        {
            let entry = entry.map_err(|e| UnitError::Storage(e.to_string()))?;
            let name = entry.file_name();
            let name_str = name.to_string_lossy();

            if let Some((level, id)) = parse_segment_filename(&name_str) {
                let file_size = entry.metadata().map(|m| m.len()).unwrap_or(0);

                let seg_path = segments_dir.join(&*name_str);
                let meta = SegmentMeta {
                    id,
                    level,
                    size: file_size,
                    entry_count: 0,
                    location: SegmentLocation::Local {
                        path: seg_path.to_string_lossy().to_string(),
                    },
                    created_at: now_millis(),
                };
                index.put_segment_meta_raw(id, &meta.encode())?;

                max_id = max_id.max(id + 1);
            }
        }

        for (id, _) in index.all_segment_meta_raw() {
            max_id = max_id.max(id + 1);
        }

        let cap = NonZeroUsize::new(remote_cache_capacity.max(1)).unwrap();

        Ok(Self {
            segments_dir,
            cache_dir,
            readers: RwLock::new(HashMap::new()),
            next_segment_id: AtomicU64::new(max_id),
            io_mode,
            index,
            remote_cache: Mutex::new(LruCache::new(cap)),
            remote_store,
        })
    }

    pub fn set_remote_store(&mut self, remote_store: Arc<dyn RemoteStore>) {
        self.remote_store = Some(remote_store);
    }

    pub async fn new_writer_at_level(&self, level: u32) -> Result<BlobWriter, UnitError> {
        let id = self.next_segment_id.fetch_add(1, Ordering::Relaxed);
        let path = self.segments_dir.join(segment_filename(level, id));

        let segment: Box<dyn Segment> = match self.io_mode {
            IoMode::Advanced => {
                let ds = DirectSegment::new(path.clone())
                    .await
                    .map_err(|e| UnitError::Storage(e.to_string()))?;
                Box::new(ds)
            }
            IoMode::Basic => {
                let s = StandardSegment::new(path.clone())
                    .await
                    .map_err(|e| UnitError::Storage(e.to_string()))?;
                Box::new(s)
            }
            IoMode::Mmap => {
                let ms = MmapSegment::new(path.clone())
                    .await
                    .map_err(|e| UnitError::Storage(e.to_string()))?;
                Box::new(ms)
            }
        };

        let meta = SegmentMeta {
            id,
            level,
            size: 0,
            entry_count: 0,
            location: SegmentLocation::Local {
                path: path.to_string_lossy().to_string(),
            },
            created_at: now_millis(),
        };
        self.index.put_segment_meta_raw(id, &meta.encode())?;

        Ok(BlobWriter::new(segment, id))
    }

    pub async fn new_writer(&self) -> Result<BlobWriter, UnitError> {
        self.new_writer_at_level(0).await
    }

    pub fn update_meta(&self, id: u64, size: u64, entry_count: u64) {
        if let Some(mut meta) = self.segment_meta(id) {
            meta.size = size;
            meta.entry_count = entry_count;
            let _ = self.index.put_segment_meta_raw(id, &meta.encode());
        }
    }

    pub fn segments_at_level(&self, level: u32) -> Vec<SegmentMeta> {
        self.index
            .all_segment_meta_raw()
            .into_iter()
            .filter_map(|(_, data)| SegmentMeta::decode(&data))
            .filter(|m| m.level == level && matches!(m.location, SegmentLocation::Local { .. }))
            .collect()
    }

    pub fn remove_segments(&self, ids: &[u64]) {
        let mut readers = self.readers.write().unwrap();

        for &id in ids {
            readers.remove(&id);
            if let Some(meta) = self.segment_meta(id) {
                if let SegmentLocation::Local { ref path } = meta.location {
                    let _ = fs::remove_file(path);
                }
                let _ = self.index.delete_segment_meta(id);
            }
        }
    }

    pub fn mark_remote(&self, id: u64, key: String) {
        let mut readers = self.readers.write().unwrap();
        readers.remove(&id);

        if let Some(mut meta) = self.segment_meta(id) {
            if let SegmentLocation::Local { ref path } = meta.location {
                let _ = fs::remove_file(path);
            }

            meta.location = SegmentLocation::Remote { key };
            let _ = self.index.put_segment_meta_raw(id, &meta.encode());
        }
    }

    pub fn cleanup_orphans(&self, referenced_ids: &std::collections::HashSet<u64>) {
        let all_meta: Vec<u64> = self
            .index
            .all_segment_meta_raw()
            .into_iter()
            .map(|(id, _)| id)
            .collect();

        let orphan_ids: Vec<u64> = all_meta
            .into_iter()
            .filter(|id| !referenced_ids.contains(id))
            .collect();

        if !orphan_ids.is_empty() {
            self.remove_segments(&orphan_ids);
        }
    }

    pub fn segment_path_for(&self, id: u64) -> Option<PathBuf> {
        self.segment_meta(id).and_then(|m| match &m.location {
            SegmentLocation::Local { path } => Some(PathBuf::from(path)),
            _ => None,
        })
    }

    pub fn segment_meta(&self, id: u64) -> Option<SegmentMeta> {
        self.index
            .get_segment_meta_raw(id)
            .and_then(|data| SegmentMeta::decode(&data))
    }

    pub fn read_event(&self, entry: &IndexEntry) -> Result<Event, UnitError> {
        {
            let readers = self.readers.read().unwrap();
            if let Some(reader) = readers.get(&entry.segment_id) {
                return reader.read_event(entry.byte_offset, entry.length);
            }
        }

        let meta = self.segment_meta(entry.segment_id);

        match meta.as_ref().map(|m| &m.location) {
            Some(SegmentLocation::Local { path }) => {
                let path = PathBuf::from(path);
                let reader = BlobReader::open(&path)?;
                let event = reader.read_event(entry.byte_offset, entry.length)?;
                self.readers
                    .write()
                    .unwrap()
                    .insert(entry.segment_id, reader);
                Ok(event)
            }
            Some(SegmentLocation::Remote { .. }) => {
                let mut cache = self.remote_cache.lock().unwrap();
                if let Some(cached) = cache.get(&entry.segment_id) {
                    return cached.reader.read_event(entry.byte_offset, entry.length);
                }

                Err(UnitError::Storage(format!(
                    "segment {} is remote; use read_event_async for remote reads",
                    entry.segment_id,
                )))
            }
            None => {
                let path = self.find_local_path(entry.segment_id)?;
                let reader = BlobReader::open(&path)?;
                let event = reader.read_event(entry.byte_offset, entry.length)?;
                self.readers
                    .write()
                    .unwrap()
                    .insert(entry.segment_id, reader);
                Ok(event)
            }
        }
    }

    pub async fn read_event_async(&self, entry: &IndexEntry) -> Result<Event, UnitError> {
        match self.read_event(entry) {
            Ok(event) => return Ok(event),
            Err(UnitError::Storage(msg)) if msg.contains("is remote") => {}
            Err(e) => return Err(e),
        }

        let meta = self
            .segment_meta(entry.segment_id)
            .ok_or_else(|| UnitError::Storage(format!("segment {} not found", entry.segment_id)))?;

        let remote_key = match &meta.location {
            SegmentLocation::Remote { key } => key.clone(),
            _ => {
                return Err(UnitError::Storage(format!(
                    "segment {} not found as remote",
                    entry.segment_id
                )));
            }
        };

        let remote_store = self
            .remote_store
            .as_ref()
            .ok_or_else(|| UnitError::Storage("no remote store configured".into()))?;

        let data = remote_store.download(&remote_key).await?;
        let cached_path = self.cache_dir.join(segment_filename(meta.level, meta.id));
        fs::write(&cached_path, &data)
            .map_err(|e| UnitError::Storage(format!("failed to write cached segment: {}", e)))?;

        let reader = BlobReader::open(&cached_path)?;
        let event = reader.read_event(entry.byte_offset, entry.length)?;

        let mut cache = self.remote_cache.lock().unwrap();
        let evicted = cache.push(
            entry.segment_id,
            CachedRemoteSegment {
                reader,
                cached_path,
            },
        );
        if let Some((_, old)) = evicted {
            let _ = fs::remove_file(&old.cached_path);
        }

        Ok(event)
    }

    fn find_local_path(&self, id: u64) -> Result<PathBuf, UnitError> {
        if let Some(path) = self.segment_path_for(id)
            && path.exists()
        {
            return Ok(path);
        }
        let legacy = self.segments_dir.join(format!("segment_{:06}.cseg", id));
        if legacy.exists() {
            return Ok(legacy);
        }
        Err(UnitError::Storage(format!(
            "segment file not found for id {}",
            id
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::index::StorageOptions;

    fn test_index(dir: &std::path::Path) -> Storage {
        Storage::new(StorageOptions {
            path: dir.join("index").to_string_lossy().to_string(),
            index: None,
        })
        .unwrap()
    }

    #[test]
    fn test_parse_segment_filename() {
        assert_eq!(parse_segment_filename("L0_000001.cseg"), Some((0, 1)));
        assert_eq!(parse_segment_filename("L1_000042.cseg"), Some((1, 42)));
        assert_eq!(parse_segment_filename("L2_000100.cseg"), Some((2, 100)));
        assert_eq!(parse_segment_filename("segment_000003.cseg"), Some((0, 3)));
        assert_eq!(parse_segment_filename("garbage.txt"), None);
    }

    #[test]
    fn test_segment_filename_format() {
        assert_eq!(segment_filename(0, 1), "L0_000001.cseg");
        assert_eq!(segment_filename(2, 42), "L2_000042.cseg");
    }

    #[test]
    fn test_segment_meta_encode_decode() {
        let local = SegmentMeta {
            id: 42,
            level: 2,
            size: 1024,
            entry_count: 10,
            location: SegmentLocation::Local {
                path: "/tmp/segments/L2_000042.cseg".into(),
            },
            created_at: 1000,
        };
        let encoded = local.encode();
        let decoded = SegmentMeta::decode(&encoded).unwrap();
        assert_eq!(decoded.level, 2);
        assert_eq!(decoded.size, 1024);
        assert_eq!(decoded.entry_count, 10);
        assert!(
            matches!(decoded.location, SegmentLocation::Local { ref path } if path == "/tmp/segments/L2_000042.cseg")
        );

        let remote = SegmentMeta {
            id: 7,
            level: 3,
            size: 2048,
            entry_count: 20,
            location: SegmentLocation::Remote {
                key: "chronicle/segments/L3_000007.cseg".into(),
            },
            created_at: 2000,
        };
        let encoded = remote.encode();
        let decoded = SegmentMeta::decode(&encoded).unwrap();
        assert_eq!(decoded.level, 3);
        assert!(
            matches!(decoded.location, SegmentLocation::Remote { key } if key == "chronicle/segments/L3_000007.cseg")
        );
    }

    #[test]
    fn test_segment_manager_recover_empty() {
        let dir = tempfile::tempdir().unwrap();
        let index = test_index(dir.path());
        let mgr =
            SegmentManager::recover(dir.path().join("segments"), IoMode::Basic, index).unwrap();
        assert_eq!(mgr.next_segment_id.load(Ordering::Relaxed), 0);
        assert!(mgr.segments_at_level(0).is_empty());
    }

    #[tokio::test]
    async fn test_segment_manager_write_and_read() {
        let dir = tempfile::tempdir().unwrap();
        let index = test_index(dir.path());
        let mgr =
            SegmentManager::recover(dir.path().join("segments"), IoMode::Basic, index).unwrap();

        let event = Event {
            timeline_id: 5,
            term: 1,
            offset: 42,
            payload: Some(b"test_payload".to_vec().into()),
            crc32: None,
            timestamp: 999,
            schema_id: 0,
        };

        let mut writer = mgr.new_writer().await.unwrap();
        let seg_id = writer.segment_id();
        let (byte_offset, length) = writer.write_entry(&event).await.unwrap();
        writer.finish().await.unwrap();

        let entry = IndexEntry {
            segment_id: seg_id,
            byte_offset,
            length,
        };

        let read_event = mgr.read_event(&entry).unwrap();
        assert_eq!(read_event.timeline_id, 5);
        assert_eq!(read_event.offset, 42);
        assert_eq!(read_event.payload, Some(b"test_payload".to_vec().into()));
    }

    #[tokio::test]
    async fn test_segment_manager_recover_existing() {
        let dir = tempfile::tempdir().unwrap();

        {
            let index = test_index(dir.path());
            let mgr =
                SegmentManager::recover(dir.path().join("segments"), IoMode::Basic, index).unwrap();
            let writer = mgr.new_writer().await.unwrap();
            writer.finish().await.unwrap();
            let writer = mgr.new_writer().await.unwrap();
            writer.finish().await.unwrap();
        }

        let index = test_index(dir.path());
        let mgr =
            SegmentManager::recover(dir.path().join("segments"), IoMode::Basic, index).unwrap();
        assert_eq!(mgr.next_segment_id.load(Ordering::Relaxed), 2);
        assert_eq!(mgr.segments_at_level(0).len(), 2);
    }

    #[tokio::test]
    async fn test_segment_manager_level_aware_writer() {
        let dir = tempfile::tempdir().unwrap();
        let index = test_index(dir.path());
        let mgr =
            SegmentManager::recover(dir.path().join("segments"), IoMode::Basic, index).unwrap();

        let writer = mgr.new_writer_at_level(1).await.unwrap();
        let id = writer.segment_id();
        writer.finish().await.unwrap();

        assert_eq!(mgr.segments_at_level(1).len(), 1);
        assert_eq!(mgr.segments_at_level(0).len(), 0);

        let path = mgr.segment_path_for(id).unwrap();
        assert!(
            path.file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("L1_")
        );
    }

    #[tokio::test]
    async fn test_segment_manager_remove_segments() {
        let dir = tempfile::tempdir().unwrap();
        let index = test_index(dir.path());
        let mgr =
            SegmentManager::recover(dir.path().join("segments"), IoMode::Basic, index).unwrap();

        let w1 = mgr.new_writer().await.unwrap();
        let id1 = w1.segment_id();
        w1.finish().await.unwrap();

        let w2 = mgr.new_writer().await.unwrap();
        let id2 = w2.segment_id();
        w2.finish().await.unwrap();

        assert_eq!(mgr.segments_at_level(0).len(), 2);

        mgr.remove_segments(&[id1]);
        assert_eq!(mgr.segments_at_level(0).len(), 1);
        assert!(mgr.segment_path_for(id1).is_none());
        assert!(mgr.segment_path_for(id2).is_some());
    }

    #[tokio::test]
    async fn test_segment_manager_mark_remote() {
        let dir = tempfile::tempdir().unwrap();
        let index = test_index(dir.path());
        let mgr =
            SegmentManager::recover(dir.path().join("segments"), IoMode::Basic, index).unwrap();

        let event = Event {
            timeline_id: 1,
            term: 1,
            offset: 0,
            payload: Some(b"data".to_vec().into()),
            crc32: None,
            timestamp: 100,
            schema_id: 0,
        };

        let mut writer = mgr.new_writer_at_level(2).await.unwrap();
        let seg_id = writer.segment_id();
        writer.write_entry(&event).await.unwrap();
        writer.finish().await.unwrap();

        assert_eq!(mgr.segments_at_level(2).len(), 1);
        assert!(mgr.segment_path_for(seg_id).is_some());

        mgr.mark_remote(seg_id, "chronicle/segments/L2_000000.cseg".into());

        assert_eq!(mgr.segments_at_level(2).len(), 0);
        assert!(mgr.segment_path_for(seg_id).is_none());

        let meta = mgr.segment_meta(seg_id).unwrap();
        assert!(matches!(meta.location, SegmentLocation::Remote { .. }));
    }

    #[tokio::test]
    async fn test_segment_manager_mmap_write_and_read() {
        let dir = tempfile::tempdir().unwrap();
        let index = test_index(dir.path());
        let mgr =
            SegmentManager::recover(dir.path().join("segments"), IoMode::Mmap, index).unwrap();

        let event = Event {
            timeline_id: 7,
            term: 2,
            offset: 99,
            payload: Some(b"mmap_test".to_vec().into()),
            crc32: None,
            timestamp: 555,
            schema_id: 0,
        };

        let mut writer = mgr.new_writer().await.unwrap();
        let seg_id = writer.segment_id();
        let (byte_offset, length) = writer.write_entry(&event).await.unwrap();
        writer.finish().await.unwrap();

        let entry = IndexEntry {
            segment_id: seg_id,
            byte_offset,
            length,
        };

        let read_event = mgr.read_event(&entry).unwrap();
        assert_eq!(read_event.timeline_id, 7);
        assert_eq!(read_event.offset, 99);
        assert_eq!(read_event.payload, Some(b"mmap_test".to_vec().into()));
    }

    #[tokio::test]
    async fn test_remote_cache_lru_eviction() {
        let dir = tempfile::tempdir().unwrap();
        let index = test_index(dir.path());
        let mgr = SegmentManager::recover_with_remote(
            dir.path().join("segments"),
            IoMode::Basic,
            None,
            2,
            index,
        )
        .unwrap();

        let mut paths = Vec::new();
        for i in 0..3u64 {
            let cached_path = mgr.cache_dir.join(format!("cached_{}.cseg", i));
            fs::write(&cached_path, [0u8; 10]).unwrap();
            let reader = BlobReader::open(&cached_path).unwrap();
            paths.push(cached_path.clone());

            let mut cache = mgr.remote_cache.lock().unwrap();
            let evicted = cache.push(
                i,
                CachedRemoteSegment {
                    reader,
                    cached_path,
                },
            );
            if let Some((_, old)) = evicted {
                let _ = fs::remove_file(&old.cached_path);
            }
        }

        let cache = mgr.remote_cache.lock().unwrap();
        assert_eq!(cache.len(), 2);
        assert!(!cache.contains(&0));
        assert!(cache.contains(&1));
        assert!(cache.contains(&2));
        drop(cache);

        assert!(!paths[0].exists());
        assert!(paths[1].exists());
        assert!(paths[2].exists());
    }

    #[test]
    fn test_segment_meta_persisted_in_rocksdb() {
        let dir = tempfile::tempdir().unwrap();
        let index = test_index(dir.path());

        let meta = SegmentMeta {
            id: 1,
            level: 2,
            size: 4096,
            entry_count: 50,
            location: SegmentLocation::Local {
                path: "/tmp/L2_000001.cseg".into(),
            },
            created_at: 3000,
        };
        index.put_segment_meta_raw(1, &meta.encode()).unwrap();

        let remote_meta = SegmentMeta {
            id: 2,
            level: 3,
            size: 8192,
            entry_count: 100,
            location: SegmentLocation::Remote {
                key: "s3/key".into(),
            },
            created_at: 4000,
        };
        index
            .put_segment_meta_raw(2, &remote_meta.encode())
            .unwrap();

        let retrieved = index.get_segment_meta_raw(1).unwrap();
        let decoded = SegmentMeta::decode(&retrieved).unwrap();
        assert_eq!(decoded.level, 2);
        assert_eq!(decoded.size, 4096);

        let all = index.all_segment_meta_raw();
        assert_eq!(all.len(), 2);

        index.delete_segment_meta(1).unwrap();
        assert!(index.get_segment_meta_raw(1).is_none());
        assert_eq!(index.all_segment_meta_raw().len(), 1);
    }
}
