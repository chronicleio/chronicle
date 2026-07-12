use crate::ddl::{
    AlterDatasetStatement, parse_alter_dataset, parse_create_dataset, parse_delete_dataset,
};
use crate::error::QueryError;
use catalog::{Dataset, DatasetName};

pub(crate) enum QueryCommand {
    CreateDataset(Dataset),
    AlterDataset(AlterDatasetStatement),
    DeleteDataset(DatasetName),
}

pub(crate) fn plan_statement(sql: &str) -> Result<QueryCommand, QueryError> {
    let statement = sql.trim().trim_end_matches(';').trim();
    if let Some(dataset) = parse_create_dataset(statement)? {
        return Ok(QueryCommand::CreateDataset(dataset));
    }

    if let Some(statement) = parse_alter_dataset(statement)? {
        return Ok(QueryCommand::AlterDataset(statement));
    }

    if let Some(name) = parse_delete_dataset(statement)? {
        return Ok(QueryCommand::DeleteDataset(name));
    }

    Err(QueryError::UnsupportedStatement(statement.to_string()))
}
