#[allow(clippy::module_inception)]
pub mod wal;

pub use wal::{Wal, WalOptions};

pub const INVALID_OFFSET: i64 = -1;
