use crate::error::OrchestratorError;
use crate::statement::CreateDatasetStatement;
use catalog::{DataType, DatasetField, DatasetName, DatasetSchema};

#[derive(Debug, Clone)]
pub(crate) struct AlterDatasetStatement {
    pub name: DatasetName,
    pub operation: AlterDatasetOperation,
}

#[derive(Debug, Clone)]
pub(crate) enum AlterDatasetOperation {
    AddField(DatasetField),
    DropField(String),
}

pub(crate) fn parse_create_dataset(
    statement: &str,
) -> Result<Option<CreateDatasetStatement>, OrchestratorError> {
    let lower = statement.to_ascii_lowercase();
    if !lower.starts_with("create dataset ") {
        return Ok(None);
    }

    let rest = statement["create dataset ".len()..].trim();
    let open = rest
        .find('(')
        .ok_or_else(|| OrchestratorError::InvalidStatement(statement.to_string()))?;
    let close = rest
        .rfind(')')
        .ok_or_else(|| OrchestratorError::InvalidStatement(statement.to_string()))?;
    if close <= open {
        return Err(OrchestratorError::InvalidStatement(statement.to_string()));
    }
    if !rest[close + 1..].trim().is_empty() {
        return Err(OrchestratorError::InvalidStatement(statement.to_string()));
    }

    let name = parse_identifier(rest[..open].trim(), "dataset", statement)?;

    let fields_sql = &rest[open + 1..close];
    let mut fields = Vec::new();
    for field_sql in fields_sql.split(',') {
        let tokens: Vec<_> = field_sql.split_whitespace().collect();
        let field = parse_field(&tokens, statement)?;
        if fields
            .iter()
            .any(|existing: &DatasetField| existing.name.eq_ignore_ascii_case(&field.name))
        {
            return Err(OrchestratorError::InvalidStatement(format!(
                "duplicate field: {}",
                field.name
            )));
        }
        fields.push(field);
    }

    if fields.is_empty() {
        return Err(OrchestratorError::InvalidStatement(statement.to_string()));
    }

    Ok(Some(CreateDatasetStatement {
        name,
        schema: DatasetSchema::new(fields),
    }))
}

pub(crate) fn parse_alter_dataset(
    statement: &str,
) -> Result<Option<AlterDatasetStatement>, OrchestratorError> {
    let lower = statement.to_ascii_lowercase();
    if !lower.starts_with("alter dataset ") {
        return Ok(None);
    }

    let tokens: Vec<_> = statement.split_whitespace().collect();
    if tokens.len() < 5 {
        return Err(OrchestratorError::InvalidStatement(statement.to_string()));
    }
    if !tokens[0].eq_ignore_ascii_case("alter") || !tokens[1].eq_ignore_ascii_case("dataset") {
        return Err(OrchestratorError::InvalidStatement(statement.to_string()));
    }

    let name = parse_identifier(tokens[2], "dataset", statement)?;
    if tokens[3].eq_ignore_ascii_case("add") && tokens[4].eq_ignore_ascii_case("field") {
        if tokens.len() < 7 {
            return Err(OrchestratorError::InvalidStatement(statement.to_string()));
        }
        let field = parse_field(&tokens[5..], statement)?;
        return Ok(Some(AlterDatasetStatement {
            name,
            operation: AlterDatasetOperation::AddField(field),
        }));
    }

    if tokens[3].eq_ignore_ascii_case("drop") && tokens[4].eq_ignore_ascii_case("field") {
        if tokens.len() != 6 {
            return Err(OrchestratorError::InvalidStatement(statement.to_string()));
        }
        return Ok(Some(AlterDatasetStatement {
            name,
            operation: AlterDatasetOperation::DropField(parse_identifier(
                tokens[5], "field", statement,
            )?),
        }));
    }

    Err(OrchestratorError::InvalidStatement(statement.to_string()))
}

pub(crate) fn parse_delete_dataset(
    statement: &str,
) -> Result<Option<DatasetName>, OrchestratorError> {
    let tokens: Vec<_> = statement.split_whitespace().collect();
    if tokens.is_empty() || !tokens[0].eq_ignore_ascii_case("delete") {
        return Ok(None);
    }
    if tokens.len() != 3 || !tokens[1].eq_ignore_ascii_case("dataset") {
        return Err(OrchestratorError::InvalidStatement(statement.to_string()));
    }
    Ok(Some(parse_identifier(tokens[2], "dataset", statement)?))
}

fn parse_field(tokens: &[&str], statement: &str) -> Result<DatasetField, OrchestratorError> {
    if tokens.len() < 2 {
        return Err(OrchestratorError::InvalidStatement(statement.to_string()));
    }
    let name = parse_identifier(tokens[0], "field", statement)?;
    let mut field = DatasetField::new(name, parse_data_type(tokens[1])?);
    if tokens.len() == 4
        && tokens[2].eq_ignore_ascii_case("not")
        && tokens[3].eq_ignore_ascii_case("null")
    {
        field.nullable = false;
    } else if tokens.len() != 2 {
        return Err(OrchestratorError::InvalidStatement(statement.to_string()));
    }
    Ok(field)
}

fn parse_identifier(value: &str, kind: &str, statement: &str) -> Result<String, OrchestratorError> {
    let mut chars = value.chars();
    let Some(first) = chars.next() else {
        return Err(OrchestratorError::InvalidStatement(statement.to_string()));
    };
    if !(first.is_ascii_alphabetic() || first == '_')
        || !chars.all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
    {
        return Err(OrchestratorError::InvalidStatement(format!(
            "invalid {kind} name: {value}"
        )));
    }
    Ok(value.to_string())
}

pub(crate) fn parse_data_type(value: &str) -> Result<DataType, OrchestratorError> {
    match value.to_ascii_lowercase().as_str() {
        "bool" | "boolean" => Ok(DataType::Boolean),
        "int" | "int32" | "integer" => Ok(DataType::Int32),
        "bigint" | "int64" | "long" => Ok(DataType::Int64),
        "float" | "float32" => Ok(DataType::Float32),
        "double" | "float64" => Ok(DataType::Float64),
        "string" | "text" => Ok(DataType::String),
        "binary" | "bytes" => Ok(DataType::Binary),
        "date" => Ok(DataType::Date),
        "timestamp" => Ok(DataType::Timestamp),
        "json" => Ok(DataType::Json),
        _ => Err(OrchestratorError::InvalidStatement(format!(
            "unsupported data type: {value}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_create_dataset_schema() {
        let statement = parse_create_dataset(
            "create dataset events (id int64 not null, ts timestamp, payload json)",
        )
        .unwrap()
        .unwrap();

        assert_eq!(statement.name, "events");
        assert_eq!(statement.schema.fields.len(), 3);
        assert_eq!(statement.schema.fields[0].name, "id");
        assert_eq!(statement.schema.fields[0].data_type, DataType::Int64);
        assert!(!statement.schema.fields[0].nullable);
        assert_eq!(statement.schema.fields[1].data_type, DataType::Timestamp);
        assert_eq!(statement.schema.fields[2].data_type, DataType::Json);
    }

    #[test]
    fn rejects_unknown_create_dataset_type() {
        let error = parse_create_dataset("create dataset events (payload variant)")
            .unwrap_err()
            .to_string();

        assert_eq!(
            error,
            "Invalid SQL statement: unsupported data type: variant"
        );
    }

    #[test]
    fn rejects_create_dataset_trailing_tokens() {
        let error = parse_create_dataset("create dataset events (payload json) trailing")
            .unwrap_err()
            .to_string();

        assert_eq!(
            error,
            "Invalid SQL statement: create dataset events (payload json) trailing"
        );
    }

    #[test]
    fn rejects_create_dataset_malformed_field_modifiers() {
        let error = parse_create_dataset("create dataset events (payload json required)")
            .unwrap_err()
            .to_string();

        assert_eq!(
            error,
            "Invalid SQL statement: create dataset events (payload json required)"
        );
    }

    #[test]
    fn rejects_create_dataset_duplicate_fields() {
        let error = parse_create_dataset("create dataset events (payload json, Payload string)")
            .unwrap_err()
            .to_string();

        assert_eq!(error, "Invalid SQL statement: duplicate field: Payload");
    }

    #[test]
    fn parses_alter_dataset_add_field() {
        let statement =
            parse_alter_dataset("alter dataset events add field user_id string not null")
                .unwrap()
                .unwrap();

        assert_eq!(statement.name, "events");
        match statement.operation {
            AlterDatasetOperation::AddField(field) => {
                assert_eq!(field.name, "user_id");
                assert_eq!(field.data_type, DataType::String);
                assert!(!field.nullable);
            }
            other => panic!("unexpected operation: {other:?}"),
        }
    }

    #[test]
    fn parses_alter_dataset_drop_field() {
        let statement = parse_alter_dataset("alter dataset events drop field user_id")
            .unwrap()
            .unwrap();

        assert_eq!(statement.name, "events");
        match statement.operation {
            AlterDatasetOperation::DropField(field) => assert_eq!(field, "user_id"),
            other => panic!("unexpected operation: {other:?}"),
        }
    }

    #[test]
    fn parses_delete_dataset() {
        let name = parse_delete_dataset("delete dataset events")
            .unwrap()
            .unwrap();
        assert_eq!(name, "events");
    }
}
