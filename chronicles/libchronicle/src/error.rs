use catalog::error::CatalogError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ChronicleError {
    #[error("Timeline not found: {0}")]
    TimelineNotFound(String),

    #[error("Timeline already exists: {0}")]
    TimelineAlreadyExists(String),

    #[error("Invalid term: current={current}, requested={requested}")]
    InvalidTerm { current: i64, requested: i64 },

    #[error("Fenced: timeline_id={timeline_id}, term={term}")]
    Fenced { timeline_id: i64, term: i64 },

    #[error("Reconciliation failed: {0}")]
    ReconciliationFailed(String),

    #[error("Unit not enough: {0}")]
    UnitNotEnough(String),

    #[error("Catalog error: {0}")]
    Catalog(#[from] CatalogError),

    #[error("Transport error: {0}")]
    Transport(String),

    #[error("Internal error: {0}")]
    Internal(String),
}

impl From<tonic::Status> for ChronicleError {
    fn from(status: tonic::Status) -> Self {
        ChronicleError::Transport(status.to_string())
    }
}
