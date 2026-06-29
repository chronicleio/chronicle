use std::sync::Arc;
use std::time::Duration;

use super::compaction_level::{CompactionLevel, IndexedEntry};
use super::manager::SegmentManager;
use crate::storage::index::Storage;

pub(crate) struct L2MergeTask {
    pub segment_manager: Arc<SegmentManager>,
    pub index: Storage,
    pub trigger: usize,
    pub interval: Duration,
}

impl CompactionLevel for L2MergeTask {
    fn name(&self) -> &'static str {
        "L1 → L2 merge"
    }
    fn source_level(&self) -> u32 {
        1
    }
    fn target_level(&self) -> u32 {
        2
    }
    fn trigger(&self) -> usize {
        self.trigger
    }
    fn interval(&self) -> Duration {
        self.interval
    }
    fn segment_manager(&self) -> &Arc<SegmentManager> {
        &self.segment_manager
    }
    fn index(&self) -> &Storage {
        &self.index
    }

    fn group_entries(&self, mut entries: Vec<IndexedEntry>) -> Vec<Vec<IndexedEntry>> {
        entries.sort_by_key(|&((tid, off), _)| (tid, off));
        vec![entries]
    }
}
