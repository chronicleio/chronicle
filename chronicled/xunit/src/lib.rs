pub mod error;

use catalog::{Action, ActionRequest, CatalogRef, DatasetName, Offset, PartitionId, Versioned};
use error::XunitError;
use libxunit::{
    AppendRowsRequest, AppendRowsResponse, RowBatch, RowData, ScanRequest, ScanResponse,
    XunitClient, error::XunitClientError,
};
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, RwLock};

pub struct Xunit {
    catalog: Option<CatalogRef>,
    storage: Arc<RwLock<XunitStorage>>,
}

impl Xunit {
    pub fn new(catalog: CatalogRef) -> Self {
        Self {
            catalog: Some(catalog),
            storage: Arc::default(),
        }
    }

    pub fn in_memory() -> Self {
        Self {
            catalog: None,
            storage: Arc::default(),
        }
    }

    pub async fn submit_action(
        &self,
        request: ActionRequest,
    ) -> Result<Versioned<Action>, XunitError> {
        let catalog = self.catalog.as_ref().ok_or_else(|| {
            XunitClientError::Internal("xunit action submission requires a catalog".into())
        })?;
        Ok(catalog.submit_action(request).await?)
    }
}

#[derive(Default)]
struct XunitStorage {
    partitions: HashMap<(DatasetName, PartitionId), PartitionData>,
}

#[derive(Default)]
struct PartitionData {
    rows: BTreeMap<Offset, StoredRow>,
    committed_offset: Option<Offset>,
}

#[derive(Clone, PartialEq, Eq)]
struct StoredRow {
    schema_id: i64,
    payload: Vec<u8>,
}

#[async_trait::async_trait]
impl XunitClient for Xunit {
    async fn append_rows(
        &self,
        request: AppendRowsRequest,
    ) -> Result<AppendRowsResponse, XunitClientError> {
        validate_append_request(&request)?;

        let mut storage = self
            .storage
            .write()
            .map_err(|_| XunitClientError::Internal("xunit storage lock poisoned".into()))?;
        let partition = storage
            .partitions
            .entry((request.dataset.clone(), request.partition.clone()))
            .or_default();

        if let Some(committed_offset) = partition.committed_offset {
            let next_expected = committed_offset + 1;
            if request.offset_range.start > next_expected {
                return Err(XunitClientError::InvalidRequest(format!(
                    "append creates offset gap: committed={}, start={}",
                    committed_offset, request.offset_range.start
                )));
            }
        }

        for row in &request.rows {
            let incoming = StoredRow {
                schema_id: request.schema_id,
                payload: row.payload.clone(),
            };
            match partition.rows.get(&row.offset) {
                Some(existing) if existing == &incoming => {}
                Some(_) => {
                    return Err(XunitClientError::InvalidRequest(format!(
                        "conflicting row at offset {}",
                        row.offset
                    )));
                }
                None => {
                    partition.rows.insert(row.offset, incoming);
                }
            }
        }

        let committed_offset = request.offset_range.end - 1;
        partition.committed_offset = Some(
            partition
                .committed_offset
                .map_or(committed_offset, |current| current.max(committed_offset)),
        );

        Ok(AppendRowsResponse {
            committed_offset: partition.committed_offset.unwrap_or(committed_offset),
        })
    }

    async fn scan(&self, request: ScanRequest) -> Result<ScanResponse, XunitClientError> {
        if !request.projection.is_empty() {
            return Err(XunitClientError::InvalidRequest(
                "xunit MVP scan only supports full-row projection".into(),
            ));
        }
        if !request.filters.is_empty() {
            return Err(XunitClientError::InvalidRequest(
                "xunit MVP scan does not support filters yet".into(),
            ));
        }

        let storage = self
            .storage
            .read()
            .map_err(|_| XunitClientError::Internal("xunit storage lock poisoned".into()))?;
        let mut rows_by_schema = BTreeMap::new();
        let mut rows_returned = 0usize;

        for ((dataset, _partition), partition) in &storage.partitions {
            if dataset != &request.dataset {
                continue;
            }
            for (offset, row) in &partition.rows {
                rows_by_schema
                    .entry(row.schema_id)
                    .or_insert_with(Vec::new)
                    .push(RowData::new(*offset, row.payload.clone()));
                rows_returned += 1;
                if request.limit.is_some_and(|limit| rows_returned >= limit) {
                    break;
                }
            }
            if request.limit.is_some_and(|limit| rows_returned >= limit) {
                break;
            }
        }

        Ok(ScanResponse {
            batches: rows_by_schema
                .into_iter()
                .map(|(schema_id, rows)| RowBatch { schema_id, rows })
                .collect(),
        })
    }

    async fn submit_action(
        &self,
        request: ActionRequest,
    ) -> Result<Versioned<Action>, XunitClientError> {
        let catalog = self.catalog.as_ref().ok_or_else(|| {
            XunitClientError::Internal("xunit action submission requires a catalog".into())
        })?;
        catalog
            .submit_action(request)
            .await
            .map_err(|error| XunitClientError::Internal(error.to_string()))
    }
}

fn validate_append_request(request: &AppendRowsRequest) -> Result<(), XunitClientError> {
    if request.dataset.is_empty() {
        return Err(XunitClientError::InvalidRequest(
            "dataset name is required".into(),
        ));
    }
    if request.partition.is_empty() {
        return Err(XunitClientError::InvalidRequest(
            "partition id is required".into(),
        ));
    }
    if request.offset_range.start >= request.offset_range.end {
        return Err(XunitClientError::InvalidRequest(format!(
            "invalid offset range [{}, {})",
            request.offset_range.start, request.offset_range.end
        )));
    }
    let expected_len = (request.offset_range.end - request.offset_range.start) as usize;
    if request.rows.len() != expected_len {
        return Err(XunitClientError::InvalidRequest(format!(
            "offset range expects {} rows, got {}",
            expected_len,
            request.rows.len()
        )));
    }
    for (idx, row) in request.rows.iter().enumerate() {
        let expected_offset = request.offset_range.start + idx as i64;
        if row.offset != expected_offset {
            return Err(XunitClientError::InvalidRequest(format!(
                "row offset mismatch at index {}: expected {}, got {}",
                idx, expected_offset, row.offset
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use catalog::OffsetRange;

    #[tokio::test]
    async fn append_and_scan_rows() {
        let xunit = Xunit::in_memory();
        let response = xunit
            .append_rows(AppendRowsRequest::new(
                "events",
                "default",
                1,
                OffsetRange::new(0, 2),
                vec![RowData::new(0, b"one"), RowData::new(1, b"two")],
            ))
            .await
            .unwrap();

        assert_eq!(response.committed_offset, 1);

        let scan = xunit.scan(ScanRequest::all("events")).await.unwrap();
        assert_eq!(scan.batches.len(), 1);
        assert_eq!(scan.batches[0].rows.len(), 2);
        assert_eq!(scan.batches[0].rows[0].payload, b"one");
        assert_eq!(scan.batches[0].rows[1].payload, b"two");
    }

    #[tokio::test]
    async fn duplicate_append_is_idempotent() {
        let xunit = Xunit::in_memory();
        let request = AppendRowsRequest::new(
            "events",
            "default",
            1,
            OffsetRange::new(0, 1),
            vec![RowData::new(0, b"one")],
        );

        xunit.append_rows(request.clone()).await.unwrap();
        xunit.append_rows(request).await.unwrap();

        let scan = xunit.scan(ScanRequest::all("events")).await.unwrap();
        assert_eq!(scan.batches[0].rows.len(), 1);
    }

    #[tokio::test]
    async fn conflicting_replay_is_rejected() {
        let xunit = Xunit::in_memory();
        xunit
            .append_rows(AppendRowsRequest::new(
                "events",
                "default",
                1,
                OffsetRange::new(0, 1),
                vec![RowData::new(0, b"one")],
            ))
            .await
            .unwrap();

        let error = xunit
            .append_rows(AppendRowsRequest::new(
                "events",
                "default",
                1,
                OffsetRange::new(0, 1),
                vec![RowData::new(0, b"changed")],
            ))
            .await
            .unwrap_err();
        assert!(matches!(error, XunitClientError::InvalidRequest(_)));
    }
}
