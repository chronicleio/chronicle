use catalog::{Dataset, DatasetName, Versioned};

#[derive(Debug, Clone)]
pub enum LensOutput {
    Datasets(Vec<Versioned<Dataset>>),
    DeletedDataset(DatasetName),
}
