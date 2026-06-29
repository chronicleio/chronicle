use std::sync::Arc;
use std::time::Duration;

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use super::compaction_l1::L1FlushTask;
use super::compaction_l2::L2MergeTask;
use super::compaction_l3::L3SplitTask;
use super::compaction_l4::L4OffloadTask;
use super::compaction_level::CompactionLevel;
use super::manager::SegmentManager;
use super::remote::RemoteStore;
use crate::storage::index::Storage;
use crate::storage::write_cache::WriteCache;
use crate::wal::wal::Wal;

pub struct CompactionPipeline {
    handles: Vec<JoinHandle<()>>,
}

pub struct CompactionPipelineConfig {
    pub write_cache: WriteCache,
    pub segment_manager: Arc<SegmentManager>,
    pub index: Storage,
    pub context: CancellationToken,
    pub interval: Duration,
    pub l1_compaction_trigger: usize,
    pub l2_compaction_trigger: usize,
    pub remote_store: Option<Arc<dyn RemoteStore>>,
    pub wal: Option<Wal>,
}

impl CompactionPipeline {
    pub fn spawn(config: CompactionPipelineConfig) -> Self {
        let CompactionPipelineConfig {
            write_cache,
            segment_manager,
            index,
            context,
            interval,
            l1_compaction_trigger,
            l2_compaction_trigger,
            remote_store,
            wal,
        } = config;

        let flush_notify = write_cache.flush_notify();

        let l1_handle = {
            let segment_manager = segment_manager.clone();
            let index = index.clone();
            let context = context.clone();
            tokio::spawn(async move {
                let task = L1FlushTask {
                    write_cache,
                    segment_manager,
                    index,
                    flush_notify,
                    wal,
                };
                task.run(context, interval).await;
            })
        };

        let l2_handle = {
            let segment_manager = segment_manager.clone();
            let index = index.clone();
            let context = context.clone();
            tokio::spawn(async move {
                let task = L2MergeTask {
                    segment_manager,
                    index,
                    trigger: l1_compaction_trigger,
                    interval,
                };
                task.run(context).await;
            })
        };

        let l3_handle = {
            let segment_manager = segment_manager.clone();
            let index = index.clone();
            let context = context.clone();
            tokio::spawn(async move {
                let task = L3SplitTask {
                    segment_manager,
                    index,
                    trigger: l2_compaction_trigger,
                    interval,
                };
                task.run(context).await;
            })
        };

        let l4_handle = if let Some(remote_store) = remote_store {
            let segment_manager = segment_manager.clone();
            let context = context.clone();
            Some(tokio::spawn(async move {
                let task = L4OffloadTask {
                    segment_manager,
                    remote_store,
                    interval,
                };
                task.run(context).await;
            }))
        } else {
            None
        };

        let mut handles = vec![l1_handle, l2_handle, l3_handle];
        if let Some(h) = l4_handle {
            handles.push(h);
        }

        Self { handles }
    }

    pub async fn shutdown(self) {
        for handle in self.handles {
            if let Err(err) = handle.await {
                warn!(error = ?err, "compaction task join error");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::compaction_level::CompactionLevel;
    use super::*;
    use crate::option::unit_options::IoMode;
    use crate::storage::index::StorageOptions;
    use chronicle_proto::pb_ext::Event;

    struct TestHarness {
        write_cache: WriteCache,
        segment_manager: Arc<SegmentManager>,
        index: Storage,
        l1: L1FlushTask,
        l2: L2MergeTask,
        l3: L3SplitTask,
    }

    fn setup_test(dir: &std::path::Path, l1_trigger: usize, l2_trigger: usize) -> TestHarness {
        let segments_dir = dir.join("segments");
        let index_dir = dir.join("index");

        let write_cache = WriteCache::new(64 * 1024 * 1024);
        let index = Storage::new(StorageOptions {
            path: index_dir.to_string_lossy().to_string(),
            index: None,
        })
        .unwrap();
        let segment_manager =
            Arc::new(SegmentManager::recover(segments_dir, IoMode::Basic, index.clone()).unwrap());

        let flush_notify = write_cache.flush_notify();

        let l1 = L1FlushTask {
            write_cache: write_cache.clone(),
            segment_manager: segment_manager.clone(),
            index: index.clone(),
            flush_notify,
            wal: None,
        };

        let l2 = L2MergeTask {
            segment_manager: segment_manager.clone(),
            index: index.clone(),
            trigger: l1_trigger,
            interval: Duration::from_secs(1),
        };

        let l3 = L3SplitTask {
            segment_manager: segment_manager.clone(),
            index: index.clone(),
            trigger: l2_trigger,
            interval: Duration::from_secs(1),
        };

        TestHarness {
            write_cache,
            segment_manager,
            index,
            l1,
            l2,
            l3,
        }
    }

    #[tokio::test]
    async fn test_flush_to_l1() {
        let dir = tempfile::tempdir().unwrap();
        let h = setup_test(dir.path(), 4, 4);

        for i in 0..5 {
            let event = Event {
                timeline_id: 1,
                term: 1,
                offset: i,
                payload: Some(format!("data_{}", i).into_bytes().into()),
                crc32: None,
                timestamp: i * 100,
                schema_id: 0,
            };
            h.write_cache.put_direct(event, true);
        }

        h.write_cache.try_seal();
        h.l1.flush().await.unwrap();

        assert_eq!(h.segment_manager.segments_at_level(1).len(), 1);
        let entries = h.index.scan_index(1, 0, 10);
        assert_eq!(entries.len(), 5);
    }

    #[tokio::test]
    async fn test_l1_to_l2_merge() {
        let dir = tempfile::tempdir().unwrap();
        let h = setup_test(dir.path(), 4, 100);

        for batch in 0..4 {
            for i in 0..3 {
                let offset = batch * 3 + i;
                let event = Event {
                    timeline_id: 1,
                    term: 1,
                    offset,
                    payload: Some(format!("batch{}_{}", batch, i).into_bytes().into()),
                    crc32: None,
                    timestamp: offset * 100,
                    schema_id: 0,
                };
                h.write_cache.put_direct(event, true);
            }
            h.write_cache.try_seal();
            h.l1.flush().await.unwrap();
        }

        assert_eq!(h.segment_manager.segments_at_level(1).len(), 4);

        h.l2.compact().await.unwrap();

        assert_eq!(h.segment_manager.segments_at_level(1).len(), 0);
        assert_eq!(h.segment_manager.segments_at_level(2).len(), 1);

        let entries = h.index.scan_index(1, 0, 100);
        assert_eq!(entries.len(), 12);

        for (offset, entry) in &entries {
            let event = h.segment_manager.read_event(entry).unwrap();
            assert_eq!(event.offset, *offset);
            assert_eq!(event.timeline_id, 1);
        }
    }

    #[tokio::test]
    async fn test_l2_to_l3_split() {
        let dir = tempfile::tempdir().unwrap();
        let h = setup_test(dir.path(), 2, 2);

        for batch in 0..2 {
            for timeline in 1..=3i64 {
                for i in 0..2 {
                    let offset = batch * 2 + i;
                    let event = Event {
                        timeline_id: timeline,
                        term: 1,
                        offset,
                        payload: Some(
                            format!("t{}_b{}_{}", timeline, batch, i)
                                .into_bytes()
                                .into(),
                        ),
                        crc32: None,
                        timestamp: offset * 100,
                        schema_id: 0,
                    };
                    h.write_cache.put_direct(event, true);
                }
            }
            h.write_cache.try_seal();
            h.l1.flush().await.unwrap();
        }

        h.l2.compact().await.unwrap();
        assert!(h.segment_manager.segments_at_level(1).is_empty());

        for batch in 0..2 {
            for timeline in 1..=3i64 {
                let offset = 10 + batch;
                let event = Event {
                    timeline_id: timeline,
                    term: 1,
                    offset,
                    payload: Some(format!("extra_t{}_{}", timeline, batch).into_bytes().into()),
                    crc32: None,
                    timestamp: offset * 100,
                    schema_id: 0,
                };
                h.write_cache.put_direct(event, true);
            }
            h.write_cache.try_seal();
            h.l1.flush().await.unwrap();
        }
        h.l2.compact().await.unwrap();

        let l2_count = h.segment_manager.segments_at_level(2).len();
        assert!(l2_count >= 2, "expected >= 2 L2 segments, got {}", l2_count);

        h.l3.compact().await.unwrap();

        assert_eq!(h.segment_manager.segments_at_level(2).len(), 0);
        assert_eq!(h.segment_manager.segments_at_level(3).len(), 3);

        for timeline in 1..=3i64 {
            let entries = h.index.scan_index(timeline, 0, 100);
            assert!(
                !entries.is_empty(),
                "timeline {} should have entries",
                timeline
            );
            for (_, entry) in &entries {
                let event = h.segment_manager.read_event(entry).unwrap();
                assert_eq!(event.timeline_id, timeline);
            }
        }
    }

    #[tokio::test]
    async fn test_recovery_with_leveled_filenames() {
        let dir = tempfile::tempdir().unwrap();
        let segments_dir = dir.path().join("segments");
        let index_dir = dir.path().join("index_recovery");

        {
            let index = Storage::new(StorageOptions {
                path: index_dir.to_string_lossy().to_string(),
                index: None,
            })
            .unwrap();
            let mgr = SegmentManager::recover(segments_dir.clone(), IoMode::Basic, index).unwrap();
            let w = mgr.new_writer_at_level(1).await.unwrap();
            w.finish().await.unwrap();
            let w = mgr.new_writer_at_level(2).await.unwrap();
            w.finish().await.unwrap();
            let w = mgr.new_writer_at_level(3).await.unwrap();
            w.finish().await.unwrap();
        }

        let index = Storage::new(StorageOptions {
            path: index_dir.to_string_lossy().to_string(),
            index: None,
        })
        .unwrap();
        let mgr = SegmentManager::recover(segments_dir, IoMode::Basic, index).unwrap();
        assert_eq!(mgr.segments_at_level(1).len(), 1);
        assert_eq!(mgr.segments_at_level(2).len(), 1);
        assert_eq!(mgr.segments_at_level(3).len(), 1);
    }

    #[tokio::test]
    async fn test_full_compaction_pipeline() {
        let dir = tempfile::tempdir().unwrap();
        let h = setup_test(dir.path(), 2, 2);

        for batch in 0..4 {
            for timeline in 1..=2i64 {
                let event = Event {
                    timeline_id: timeline,
                    term: 1,
                    offset: batch,
                    payload: Some(format!("t{}_off{}", timeline, batch).into_bytes().into()),
                    crc32: None,
                    timestamp: batch * 100,
                    schema_id: 0,
                };
                h.write_cache.put_direct(event, true);
            }
            h.write_cache.try_seal();
            h.l1.flush().await.unwrap();
            h.l2.compact().await.unwrap();
            h.l3.compact().await.unwrap();
        }

        let l3 = h.segment_manager.segments_at_level(3);
        assert_eq!(l3.len(), 2, "expected 2 L3 segments (one per timeline)");

        for timeline in 1..=2i64 {
            let entries = h.index.scan_index(timeline, 0, 10);
            assert_eq!(entries.len(), 4);
            for (_, entry) in &entries {
                let event = h.segment_manager.read_event(entry).unwrap();
                assert_eq!(event.timeline_id, timeline);
            }
        }
    }

    #[tokio::test]
    async fn test_pipeline_spawn_and_shutdown() {
        let dir = tempfile::tempdir().unwrap();
        let segments_dir = dir.path().join("segments");
        let index_dir = dir.path().join("index");

        let write_cache = WriteCache::new(64 * 1024 * 1024);
        let index = Storage::new(StorageOptions {
            path: index_dir.to_string_lossy().to_string(),
            index: None,
        })
        .unwrap();
        let segment_manager =
            Arc::new(SegmentManager::recover(segments_dir, IoMode::Basic, index.clone()).unwrap());

        let context = CancellationToken::new();

        let pipeline = CompactionPipeline::spawn(CompactionPipelineConfig {
            write_cache: write_cache.clone(),
            segment_manager: segment_manager.clone(),
            index: index.clone(),
            context: context.clone(),
            interval: Duration::from_millis(50),
            l1_compaction_trigger: 2,
            l2_compaction_trigger: 2,
            remote_store: None,
            wal: None,
        });

        for i in 0..3 {
            let event = Event {
                timeline_id: 1,
                term: 1,
                offset: i,
                payload: Some(format!("data_{}", i).into_bytes().into()),
                crc32: None,
                timestamp: i * 100,
                schema_id: 0,
            };
            write_cache.put_direct(event, true);
        }
        write_cache.try_seal();

        tokio::time::sleep(Duration::from_millis(200)).await;

        assert!(!segment_manager.segments_at_level(1).is_empty());

        context.cancel();
        pipeline.shutdown().await;
    }
}
