use crate::conn::ConnOptions;
use crate::conn::conn_pool::ConnPool;
use crate::error::ChronicleError;
use crate::timeline::Timeline;
use crate::TimelineOptions;
use catalog::Catalog;
use opentelemetry::metrics::Meter;
use std::sync::Arc;
use std::time::Duration;

pub struct ChronicleOptions {
    pub(crate) conn_opts: ConnOptions,
    #[allow(dead_code)]
    meter: Option<Meter>,
}

impl Default for ChronicleOptions {
    fn default() -> Self {
        Self {
            conn_opts: ConnOptions::default(),
            meter: None,
        }
    }
}

impl ChronicleOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn conns_per_unit(mut self, n: usize) -> Self {
        self.conn_opts.conns_per_unit = n.max(1);
        self
    }

    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.conn_opts.connect_timeout = timeout;
        self
    }

    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.conn_opts.request_timeout = timeout;
        self
    }

    pub fn keep_alive_interval(mut self, interval: Duration) -> Self {
        self.conn_opts.keep_alive_interval = interval;
        self
    }

    pub fn keep_alive_timeout(mut self, timeout: Duration) -> Self {
        self.conn_opts.keep_alive_timeout = timeout;
        self
    }

    pub fn meter(mut self, meter: Meter) -> Self {
        self.meter = Some(meter);
        self
    }
}

pub struct Chronicle {
    catalog: Arc<Catalog>,
    pool: Arc<ConnPool>,
}

impl Chronicle {
    pub fn new(
        catalog: Arc<Catalog>,
        options: ChronicleOptions,
    ) -> Self {
        Self {
            pool: Arc::new(ConnPool::new(options.conn_opts.clone())),
            catalog,
        }
    }

    pub async fn open_timeline(&self, name: &str, options: TimelineOptions) -> Result<Timeline, ChronicleError> {
        Timeline::open(
            self.catalog.clone(),
            self.pool.clone(),
            name,
            options,
        )
        .await
    }

    pub async fn drop_timeline(&self, name: &str) -> Result<(), ChronicleError> {
        let tc = self.catalog.get_timeline(name).await?;
        self.catalog.delete_timeline(name, tc.version).await?;
        Ok(())
    }
}
