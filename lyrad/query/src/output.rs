use catalog::{Dataset, DatasetName, Versioned};

#[derive(Debug, Clone)]
pub enum QueryOutput {
    Datasets(Vec<Versioned<Dataset>>),
    DeletedDataset(DatasetName),
}
