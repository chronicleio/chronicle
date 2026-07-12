use crate::error::OrchestratorError;
use crate::output::OrchestratorOutput;
use crate::planner::{OrchestratorCommand, plan_statement};
use catalog::{CatalogRef, Dataset, DatasetField};

pub struct Orchestrator {
    catalog: CatalogRef,
}

impl Orchestrator {
    pub fn new(catalog: CatalogRef) -> Self {
        Self { catalog }
    }

    pub async fn execute(&self, sql: &str) -> Result<OrchestratorOutput, OrchestratorError> {
        match plan_statement(sql)? {
            OrchestratorCommand::CreateDataset(statement) => {
                Ok(OrchestratorOutput::Datasets(vec![
                    self.catalog
                        .create_dataset(statement.into_dataset())
                        .await?,
                ]))
            }
            OrchestratorCommand::AlterDataset(statement) => {
                let current = self.catalog.get_dataset(&statement.name).await?;
                let dataset = apply_alteration(current.value, statement.operation)?;
                Ok(OrchestratorOutput::Datasets(vec![
                    self.catalog
                        .update_dataset(dataset, current.version)
                        .await?,
                ]))
            }
            OrchestratorCommand::DeleteDataset(name) => {
                let current = self.catalog.get_dataset(&name).await?;
                self.catalog.delete_dataset(&name, current.version).await?;
                Ok(OrchestratorOutput::DeletedDataset(name))
            }
        }
    }
}

fn apply_alteration(
    mut dataset: Dataset,
    operation: crate::ddl::AlterDatasetOperation,
) -> Result<Dataset, OrchestratorError> {
    match operation {
        crate::ddl::AlterDatasetOperation::AddField(field) => add_field(&mut dataset, field)?,
        crate::ddl::AlterDatasetOperation::DropField(name) => drop_field(&mut dataset, &name)?,
    }
    dataset.schema.version += 1;
    Ok(dataset)
}

fn add_field(dataset: &mut Dataset, field: DatasetField) -> Result<(), OrchestratorError> {
    if dataset
        .schema
        .fields
        .iter()
        .any(|existing| existing.name == field.name)
    {
        return Err(OrchestratorError::InvalidStatement(format!(
            "dataset '{}' already has field '{}'",
            dataset.name, field.name
        )));
    }
    dataset.schema.fields.push(field);
    Ok(())
}

fn drop_field(dataset: &mut Dataset, name: &str) -> Result<(), OrchestratorError> {
    let original_len = dataset.schema.fields.len();
    dataset.schema.fields.retain(|field| field.name != name);
    if dataset.schema.fields.len() == original_len {
        return Err(OrchestratorError::InvalidStatement(format!(
            "dataset '{}' does not have field '{}'",
            dataset.name, name
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use catalog::build_memory_catalog;

    #[tokio::test]
    async fn create_dataset_uses_catalog_sql() {
        let catalog = build_memory_catalog();
        let orchestrator = Orchestrator::new(catalog);

        let output = orchestrator
            .execute("create dataset events (id int64 not null, payload json)")
            .await
            .unwrap();

        match output {
            OrchestratorOutput::Datasets(datasets) => {
                assert_eq!(datasets.len(), 1);
                assert_eq!(datasets[0].value.name, "events");
                assert_eq!(datasets[0].value.schema.fields.len(), 2);
                assert!(!datasets[0].value.schema.fields[0].nullable);
            }
            other => panic!("unexpected output: {other:?}"),
        }
    }

    #[tokio::test]
    async fn alter_dataset_adds_field() {
        let catalog = build_memory_catalog();
        let orchestrator = Orchestrator::new(catalog);

        orchestrator
            .execute("create dataset events (payload json)")
            .await
            .unwrap();
        let output = orchestrator
            .execute("alter dataset events add field user_id string not null")
            .await
            .unwrap();

        match output {
            OrchestratorOutput::Datasets(datasets) => {
                let dataset = &datasets[0].value;
                assert_eq!(dataset.schema.version, 2);
                assert_eq!(dataset.schema.fields.len(), 2);
                assert_eq!(dataset.schema.fields[1].name, "user_id");
                assert!(!dataset.schema.fields[1].nullable);
            }
            other => panic!("unexpected output: {other:?}"),
        }
    }

    #[tokio::test]
    async fn alter_dataset_drops_field() {
        let catalog = build_memory_catalog();
        let orchestrator = Orchestrator::new(catalog);

        orchestrator
            .execute("create dataset events (payload json, user_id string)")
            .await
            .unwrap();
        let output = orchestrator
            .execute("alter dataset events drop field user_id")
            .await
            .unwrap();

        match output {
            OrchestratorOutput::Datasets(datasets) => {
                let dataset = &datasets[0].value;
                assert_eq!(dataset.schema.version, 2);
                assert_eq!(dataset.schema.fields.len(), 1);
                assert_eq!(dataset.schema.fields[0].name, "payload");
            }
            other => panic!("unexpected output: {other:?}"),
        }
    }

    #[tokio::test]
    async fn delete_dataset_removes_catalog_entry() {
        let catalog = build_memory_catalog();
        let orchestrator = Orchestrator::new(catalog.clone());

        orchestrator
            .execute("create dataset events (payload json)")
            .await
            .unwrap();
        let output = orchestrator.execute("delete dataset events").await.unwrap();

        match output {
            OrchestratorOutput::DeletedDataset(name) => assert_eq!(name, "events"),
            other => panic!("unexpected output: {other:?}"),
        }
        assert!(catalog.get_dataset("events").await.is_err());
    }

    #[tokio::test]
    async fn rejects_non_dataset_ddl_sql() {
        let catalog = build_memory_catalog();
        let orchestrator = Orchestrator::new(catalog);

        let error = orchestrator.execute("show datasets").await.unwrap_err();

        assert_eq!(
            error.to_string(),
            "Unsupported SQL statement: show datasets"
        );
    }
}
