use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use super::compaction_level::{CompactionLevel, IndexedEntry};
use super::manager::SegmentManager;
use crate::storage::index::Storage;

pub(crate) struct L3SplitTask {
    pub segment_manager: Arc<SegmentManager>,
    pub index: Storage,
    pub trigger: usize,
    pub interval: Duration,
}

impl CompactionLevel for L3SplitTask {
    fn name(&self) -> &'static str {
        "L2 → L3 split"
    }
    fn source_level(&self) -> u32 {
        2
    }
    fn target_level(&self) -> u32 {
        3
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

    fn group_entries(&self, entries: Vec<IndexedEntry>) -> Vec<Vec<IndexedEntry>> {
        let mut by_timeline: HashMap<i64, Vec<IndexedEntry>> = HashMap::new();
        for entry in entries {
            by_timeline.entry(entry.0.0).or_default().push(entry);
        }

        by_timeline
            .into_values()
            .map(|mut group| {
                group.sort_by_key(|&((_, off), _)| off);
                group
            })
            .collect()
    }
}
