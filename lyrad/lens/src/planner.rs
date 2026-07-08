use crate::ddl::{
    AlterDatasetStatement, parse_alter_dataset, parse_create_dataset, parse_delete_dataset,
};
use crate::error::LensError;
use catalog::{Dataset, DatasetName};

pub(crate) enum LensCommand {
    CreateDataset(Dataset),
    AlterDataset(AlterDatasetStatement),
    DeleteDataset(DatasetName),
}

pub(crate) fn plan_statement(sql: &str) -> Result<LensCommand, LensError> {
    let statement = sql.trim().trim_end_matches(';').trim();
    if let Some(dataset) = parse_create_dataset(statement)? {
        return Ok(LensCommand::CreateDataset(dataset));
    }

    if let Some(statement) = parse_alter_dataset(statement)? {
        return Ok(LensCommand::AlterDataset(statement));
    }

    if let Some(name) = parse_delete_dataset(statement)? {
        return Ok(LensCommand::DeleteDataset(name));
    }

    Err(LensError::UnsupportedStatement(statement.to_string()))
}
