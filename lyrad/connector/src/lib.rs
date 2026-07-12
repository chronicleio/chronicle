pub mod error;

use catalog::CatalogRef;
use error::ConnectorError;
use tracing::info;

#[derive(Debug, Clone)]
pub struct ConnectorOptions {
    pub worker_id: String,
}

impl Default for ConnectorOptions {
    fn default() -> Self {
        Self {
            worker_id: "connector-0".to_string(),
        }
    }
}

pub struct Connector {
    catalog: CatalogRef,
    options: ConnectorOptions,
}

impl Connector {
    pub fn new(catalog: CatalogRef, options: ConnectorOptions) -> Self {
        Self { catalog, options }
    }

    pub async fn start(&self) -> Result<(), ConnectorError> {
        let _ = &self.catalog;
        info!(worker_id = %self.options.worker_id, "connector starting");
        Ok(())
    }
}
