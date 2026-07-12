use catalog::{Dataset, DatasetName, DatasetSchema};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CreateDatasetStatement {
    pub name: DatasetName,
    pub schema: DatasetSchema,
}

impl CreateDatasetStatement {
    pub fn into_dataset(self) -> Dataset {
        Dataset::new(self.name, self.schema)
    }
}
