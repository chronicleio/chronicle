use crate::error::QueryError;
use crate::output::QueryOutput;
use crate::planner::{QueryCommand, plan_statement};
use catalog::{CatalogRef, Dataset, DatasetField};

pub struct Query {
    catalog: CatalogRef,
}

impl Query {
    pub fn new(catalog: CatalogRef) -> Self {
        Self { catalog }
    }

    pub async fn execute(&self, sql: &str) -> Result<QueryOutput, QueryError> {
        match plan_statement(sql)? {
            QueryCommand::CreateDataset(dataset) => Ok(QueryOutput::Datasets(vec![
                self.catalog.create_dataset(dataset).await?,
            ])),
            QueryCommand::AlterDataset(statement) => {
                let current = self.catalog.get_dataset(&statement.name).await?;
                let dataset = apply_alteration(current.value, statement.operation)?;
                Ok(QueryOutput::Datasets(vec![
                    self.catalog
                        .update_dataset(dataset, current.version)
                        .await?,
                ]))
            }
            QueryCommand::DeleteDataset(name) => {
                let current = self.catalog.get_dataset(&name).await?;
                self.catalog.delete_dataset(&name, current.version).await?;
                Ok(QueryOutput::DeletedDataset(name))
            }
        }
    }
}

fn apply_alteration(
    mut dataset: Dataset,
    operation: crate::ddl::AlterDatasetOperation,
) -> Result<Dataset, QueryError> {
    match operation {
        crate::ddl::AlterDatasetOperation::AddField(field) => add_field(&mut dataset, field)?,
        crate::ddl::AlterDatasetOperation::DropField(name) => drop_field(&mut dataset, &name)?,
    }
    dataset.schema.version += 1;
    Ok(dataset)
}

fn add_field(dataset: &mut Dataset, field: DatasetField) -> Result<(), QueryError> {
    if dataset
        .schema
        .fields
        .iter()
        .any(|existing| existing.name == field.name)
    {
        return Err(QueryError::InvalidStatement(format!(
            "dataset '{}' already has field '{}'",
            dataset.name, field.name
        )));
    }
    dataset.schema.fields.push(field);
    Ok(())
}

fn drop_field(dataset: &mut Dataset, name: &str) -> Result<(), QueryError> {
    let original_len = dataset.schema.fields.len();
    dataset.schema.fields.retain(|field| field.name != name);
    if dataset.schema.fields.len() == original_len {
        return Err(QueryError::InvalidStatement(format!(
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
        let query = Query::new(catalog);

        let output = query
            .execute("create dataset events (id int64 not null, payload json)")
            .await
            .unwrap();

        match output {
            QueryOutput::Datasets(datasets) => {
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
        let query = Query::new(catalog);

        query
            .execute("create dataset events (payload json)")
            .await
            .unwrap();
        let output = query
            .execute("alter dataset events add field user_id string not null")
            .await
            .unwrap();

        match output {
            QueryOutput::Datasets(datasets) => {
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
        let query = Query::new(catalog);

        query
            .execute("create dataset events (payload json, user_id string)")
            .await
            .unwrap();
        let output = query
            .execute("alter dataset events drop field user_id")
            .await
            .unwrap();

        match output {
            QueryOutput::Datasets(datasets) => {
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
        let query = Query::new(catalog.clone());

        query
            .execute("create dataset events (payload json)")
            .await
            .unwrap();
        let output = query.execute("delete dataset events").await.unwrap();

        match output {
            QueryOutput::DeletedDataset(name) => assert_eq!(name, "events"),
            other => panic!("unexpected output: {other:?}"),
        }
        assert!(catalog.get_dataset("events").await.is_err());
    }

    #[tokio::test]
    async fn rejects_non_dataset_ddl_sql() {
        let catalog = build_memory_catalog();
        let query = Query::new(catalog);

        let error = query.execute("show datasets").await.unwrap_err();

        assert_eq!(
            error.to_string(),
            "Unsupported SQL statement: show datasets"
        );
    }
}
