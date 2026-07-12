use catalog::error::CatalogError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConnectorError {
    #[error("Catalog error: {0}")]
    Catalog(#[from] CatalogError),
}
