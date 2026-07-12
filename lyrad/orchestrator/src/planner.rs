use crate::ddl::{
    AlterDatasetStatement, parse_alter_dataset, parse_create_dataset, parse_delete_dataset,
};
use crate::error::OrchestratorError;
use crate::statement::CreateDatasetStatement;
use catalog::DatasetName;

#[derive(Debug)]
pub(crate) enum OrchestratorCommand {
    CreateDataset(CreateDatasetStatement),
    AlterDataset(AlterDatasetStatement),
    DeleteDataset(DatasetName),
}

pub(crate) fn plan_statement(sql: &str) -> Result<OrchestratorCommand, OrchestratorError> {
    let trimmed = sql.trim();
    let statement = trimmed.strip_suffix(';').unwrap_or(trimmed).trim();
    if statement.contains(';') {
        return Err(OrchestratorError::InvalidStatement(
            "multiple SQL statements are not supported".to_string(),
        ));
    }

    if let Some(dataset) = parse_create_dataset(statement)? {
        return Ok(OrchestratorCommand::CreateDataset(dataset));
    }

    if let Some(statement) = parse_alter_dataset(statement)? {
        return Ok(OrchestratorCommand::AlterDataset(statement));
    }

    if let Some(name) = parse_delete_dataset(statement)? {
        return Ok(OrchestratorCommand::DeleteDataset(name));
    }

    Err(OrchestratorError::UnsupportedStatement(
        statement.to_string(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_create_dataset_with_single_trailing_semicolon() {
        let command = plan_statement("create dataset events (payload json);").unwrap();

        match command {
            OrchestratorCommand::CreateDataset(statement) => assert_eq!(statement.name, "events"),
            _ => panic!("unexpected command"),
        }
    }

    #[test]
    fn rejects_multiple_statements() {
        let error = plan_statement("create dataset events (payload json); delete dataset events")
            .unwrap_err()
            .to_string();

        assert_eq!(
            error,
            "Invalid SQL statement: multiple SQL statements are not supported"
        );
    }
}
