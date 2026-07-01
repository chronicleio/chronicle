use crate::error::unit_error::UnitError;
use async_trait::async_trait;
use futures_util::stream::Stream;
use std::pin::Pin;
use tokio::sync::watch;

pub mod wal;
pub mod write_cache;

pub type StorageReadStream<'a> =
    Pin<Box<dyn Stream<Item = Result<Vec<u8>, UnitError>> + Send + 'a>>;

#[async_trait]
pub trait Storage: Send + Sync {
    async fn append(&self, data: Vec<u8>) -> Result<i64, UnitError>;

    fn watch_synced(&self) -> watch::Receiver<i64>;

    fn read_stream(&self) -> StorageReadStream<'_>;

    async fn shutdown(&self);
}
