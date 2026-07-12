pub mod error;

use catalog::CatalogRef;
use error::FunctionsError;
use tracing::info;

#[derive(Debug, Clone)]
pub struct FunctionsOptions {
    pub worker_id: String,
}

impl Default for FunctionsOptions {
    fn default() -> Self {
        Self {
            worker_id: "functions-0".to_string(),
        }
    }
}

pub struct FunctionsRuntime {
    catalog: CatalogRef,
    options: FunctionsOptions,
}

impl FunctionsRuntime {
    pub fn new(catalog: CatalogRef, options: FunctionsOptions) -> Self {
        Self { catalog, options }
    }

    pub async fn start(&self) -> Result<(), FunctionsError> {
        let _ = &self.catalog;
        info!(worker_id = %self.options.worker_id, "functions starting");
        Ok(())
    }
}
