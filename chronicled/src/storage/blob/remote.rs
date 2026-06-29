use std::path::Path;

use async_trait::async_trait;

use crate::error::unit_error::UnitError;

#[async_trait]
pub trait RemoteStore: Send + Sync {
    async fn upload(&self, local_path: &Path, key: &str) -> Result<(), UnitError>;

    async fn download(&self, key: &str) -> Result<Vec<u8>, UnitError>;

    async fn delete(&self, key: &str) -> Result<(), UnitError>;
}

pub struct S3RemoteStore {
    client: aws_sdk_s3::Client,
    bucket: String,
    prefix: String,
}

impl S3RemoteStore {
    pub async fn new(
        bucket: String,
        prefix: String,
        endpoint: Option<String>,
        region: Option<String>,
    ) -> Self {
        let mut config_loader = aws_config::from_env();

        if let Some(region) = region {
            config_loader = config_loader.region(aws_config::Region::new(region));
        }

        if let Some(endpoint) = endpoint {
            config_loader = config_loader.endpoint_url(endpoint);
        }

        let sdk_config = config_loader.load().await;

        let s3_config = aws_sdk_s3::config::Builder::from(&sdk_config)
            .force_path_style(true)
            .build();

        let client = aws_sdk_s3::Client::from_conf(s3_config);

        Self {
            client,
            bucket,
            prefix,
        }
    }

    pub fn segment_key(&self, segment_filename: &str) -> String {
        format!("{}/{}", self.prefix, segment_filename)
    }
}

#[async_trait]
impl RemoteStore for S3RemoteStore {
    async fn upload(&self, local_path: &Path, key: &str) -> Result<(), UnitError> {
        let body = aws_sdk_s3::primitives::ByteStream::from_path(local_path)
            .await
            .map_err(|e| {
                UnitError::Storage(format!("failed to read segment file for upload: {}", e))
            })?;

        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(body)
            .send()
            .await
            .map_err(|e| UnitError::Storage(format!("S3 upload failed: {}", e)))?;

        Ok(())
    }

    async fn download(&self, key: &str) -> Result<Vec<u8>, UnitError> {
        let resp = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| UnitError::Storage(format!("S3 download failed: {}", e)))?;

        let data = resp
            .body
            .collect()
            .await
            .map_err(|e| UnitError::Storage(format!("S3 body read failed: {}", e)))?
            .into_bytes()
            .to_vec();

        Ok(data)
    }

    async fn delete(&self, key: &str) -> Result<(), UnitError> {
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| UnitError::Storage(format!("S3 delete failed: {}", e)))?;

        Ok(())
    }
}
