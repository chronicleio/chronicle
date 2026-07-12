use catalog::{Dataset, DatasetName, Versioned};

#[derive(Debug, Clone)]
pub enum OrchestratorOutput {
    Datasets(Vec<Versioned<Dataset>>),
    DeletedDataset(DatasetName),
}
