use catalog::error::CatalogError;
use libxunit::error::XunitClientError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum LensError {
    #[error("Invalid SQL statement: {0}")]
    InvalidStatement(String),
    #[error("Unsupported SQL statement: {0}")]
    UnsupportedStatement(String),
    #[error("XUnit client is required for this SQL statement")]
    MissingXunitClient,
    #[error("Catalog error: {0}")]
    Catalog(#[from] CatalogError),
    #[error("XUnit error: {0}")]
    Xunit(#[from] XunitClientError),
}
