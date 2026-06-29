use crate::error::unit_error::UnitError;
use crate::storage::index::Storage;

const WAL_CHECKPOINT_KEY: [u8; 1] = [0xFE];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalCheckpoint {
    pub segment_id: u64,
}

impl WalCheckpoint {
    pub fn new(segment_id: u64) -> Self {
        Self { segment_id }
    }

    fn encode(&self) -> [u8; 8] {
        self.segment_id.to_be_bytes()
    }

    fn decode(bytes: &[u8]) -> Option<Self> {
        if bytes.len() >= 8 {
            let segment_id = u64::from_be_bytes(bytes[0..8].try_into().ok()?);
            Some(Self { segment_id })
        } else {
            None
        }
    }
}

pub fn write_checkpoint(index: &Storage, checkpoint: &WalCheckpoint) -> Result<(), UnitError> {
    index.put_raw(&WAL_CHECKPOINT_KEY, &checkpoint.encode())
}

pub fn read_checkpoint(index: &Storage) -> WalCheckpoint {
    index
        .get_raw(&WAL_CHECKPOINT_KEY)
        .and_then(|v| WalCheckpoint::decode(&v))
        .unwrap_or(WalCheckpoint { segment_id: 0 })
}
