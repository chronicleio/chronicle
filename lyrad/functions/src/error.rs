use catalog::error::CatalogError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum FunctionsError {
    #[error("Catalog error: {0}")]
    Catalog(#[from] CatalogError),
}
