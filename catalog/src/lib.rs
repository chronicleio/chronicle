pub mod error;
pub mod oxia_catalog;

use error::CatalogError;
use oxia_catalog::OxiaCatalog;
use serde::Deserialize;
use tracing::info;

/// Wraps a value with its catalog version for CAS operations.
#[derive(Debug, Clone)]
pub struct Versioned<T> {
    pub value: T,
    pub version: i64,
}

impl<T> Versioned<T> {
    pub fn new(value: T, version: i64) -> Self {
        Self { value, version }
    }
}

pub const SEGMENT_KEY_PAD: usize = 19;

pub fn segment_key(name: &str, start_offset: i64) -> String {
    format!("/chronicle/timelines/{}/seg-{:0>width$}", name, start_offset, width = SEGMENT_KEY_PAD)
}

pub fn segment_key_prefix(name: &str) -> String {
    format!("/chronicle/timelines/{}/seg-", name)
}

pub fn segment_key_max(name: &str) -> String {
    format!("/chronicle/timelines/{}/seg-{}", name, "9".repeat(SEGMENT_KEY_PAD))
}

pub type Catalog = OxiaCatalog;

#[derive(Debug, Deserialize, Clone)]
pub struct CatalogOptions {
    #[serde(default = "default_service_address")]
    pub service_address: String,
    #[serde(default = "default_namespace")]
    pub namespace: String,
}

impl Default for CatalogOptions {
    fn default() -> Self {
        Self {
            service_address: default_service_address(),
            namespace: default_namespace(),
        }
    }
}

fn default_service_address() -> String {
    "localhost:6648".to_string()
}

fn default_namespace() -> String {
    "default".to_string()
}

pub async fn build_catalog(
    options: &CatalogOptions,
) -> Result<Catalog, CatalogError> {
    info!(
        address = %options.service_address,
        namespace = %options.namespace,
        "connecting to oxia catalog"
    );
    OxiaCatalog::new(options.service_address.clone(), options.namespace.clone()).await
}
