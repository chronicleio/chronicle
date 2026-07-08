use catalog::error::CatalogError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum LensError {
    #[error("Invalid SQL statement: {0}")]
    InvalidStatement(String),
    #[error("Unsupported SQL statement: {0}")]
    UnsupportedStatement(String),
    #[error("Catalog error: {0}")]
    Catalog(#[from] CatalogError),
}
