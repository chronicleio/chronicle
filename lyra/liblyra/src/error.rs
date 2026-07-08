use crate::error_inner::InnerError;
use catalog::error::CatalogError;
use libxunit::error::XunitClientError;
use thiserror::Error;

#[derive(Error, Debug, Clone)]
pub enum LyraError {
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

    #[error("XUnit error: {0}")]
    Xunit(#[from] XunitClientError),

    #[error("Transport error: {0}")]
    Transport(String),

    #[error("Internal error: {0}")]
    Internal(String),

    #[error("Canceled")]
    Canceled,
}

impl From<tonic::Status> for LyraError {
    fn from(status: tonic::Status) -> Self {
        LyraError::Transport(status.to_string())
    }
}

impl From<InnerError> for LyraError {
    fn from(value: InnerError) -> Self {
        match value {
            InnerError::FenceFailed(message) => LyraError::ReconciliationFailed(message),
            InnerError::Transport(message) => LyraError::Transport(message),
            InnerError::InvalidTerm { expect, actual } => LyraError::InvalidTerm {
                current: actual,
                requested: expect,
            },
            InnerError::Catalog(error) => LyraError::Catalog(error),
            InnerError::UnitNotEnough(message) => LyraError::UnitNotEnough(message),
            InnerError::Canceled => LyraError::Canceled,
        }
    }
}
