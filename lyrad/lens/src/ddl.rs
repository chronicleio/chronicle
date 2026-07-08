use crate::error::LensError;
use catalog::{DataType, Dataset, DatasetField, DatasetName, DatasetSchema};

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

pub(crate) fn parse_create_dataset(statement: &str) -> Result<Option<Dataset>, LensError> {
    let lower = statement.to_ascii_lowercase();
    if !lower.starts_with("create dataset ") {
        return Ok(None);
    }

    let rest = statement["create dataset ".len()..].trim();
    let open = rest
        .find('(')
        .ok_or_else(|| LensError::InvalidStatement(statement.to_string()))?;
    let close = rest
        .rfind(')')
        .ok_or_else(|| LensError::InvalidStatement(statement.to_string()))?;
    if close <= open {
        return Err(LensError::InvalidStatement(statement.to_string()));
    }

    let name = rest[..open].trim();
    if name.is_empty() {
        return Err(LensError::InvalidStatement(statement.to_string()));
    }

    let fields_sql = &rest[open + 1..close];
    let mut fields = Vec::new();
    for field_sql in fields_sql.split(',') {
        let tokens: Vec<_> = field_sql.split_whitespace().collect();
        if tokens.len() < 2 {
            return Err(LensError::InvalidStatement(statement.to_string()));
        }
        let mut field = DatasetField::new(tokens[0], parse_data_type(tokens[1])?);
        if tokens.len() >= 4
            && tokens[2].eq_ignore_ascii_case("not")
            && tokens[3].eq_ignore_ascii_case("null")
        {
            field.nullable = false;
        }
        fields.push(field);
    }

    if fields.is_empty() {
        return Err(LensError::InvalidStatement(statement.to_string()));
    }

    Ok(Some(Dataset::new(name, DatasetSchema::new(fields))))
}

pub(crate) fn parse_alter_dataset(
    statement: &str,
) -> Result<Option<AlterDatasetStatement>, LensError> {
    let lower = statement.to_ascii_lowercase();
    if !lower.starts_with("alter dataset ") {
        return Ok(None);
    }

    let tokens: Vec<_> = statement.split_whitespace().collect();
    if tokens.len() < 5 {
        return Err(LensError::InvalidStatement(statement.to_string()));
    }
    if !tokens[0].eq_ignore_ascii_case("alter") || !tokens[1].eq_ignore_ascii_case("dataset") {
        return Err(LensError::InvalidStatement(statement.to_string()));
    }

    let name = tokens[2].to_string();
    if tokens[3].eq_ignore_ascii_case("add") && tokens[4].eq_ignore_ascii_case("field") {
        if tokens.len() < 7 {
            return Err(LensError::InvalidStatement(statement.to_string()));
        }
        let field = parse_field(&tokens[5..], statement)?;
        return Ok(Some(AlterDatasetStatement {
            name,
            operation: AlterDatasetOperation::AddField(field),
        }));
    }

    if tokens[3].eq_ignore_ascii_case("drop") && tokens[4].eq_ignore_ascii_case("field") {
        if tokens.len() != 6 {
            return Err(LensError::InvalidStatement(statement.to_string()));
        }
        return Ok(Some(AlterDatasetStatement {
            name,
            operation: AlterDatasetOperation::DropField(tokens[5].to_string()),
        }));
    }

    Err(LensError::InvalidStatement(statement.to_string()))
}

pub(crate) fn parse_delete_dataset(statement: &str) -> Result<Option<DatasetName>, LensError> {
    let tokens: Vec<_> = statement.split_whitespace().collect();
    if tokens.is_empty() || !tokens[0].eq_ignore_ascii_case("delete") {
        return Ok(None);
    }
    if tokens.len() != 3 || !tokens[1].eq_ignore_ascii_case("dataset") {
        return Err(LensError::InvalidStatement(statement.to_string()));
    }
    if tokens[2].is_empty() {
        return Err(LensError::InvalidStatement(statement.to_string()));
    }
    Ok(Some(tokens[2].to_string()))
}

fn parse_field(tokens: &[&str], statement: &str) -> Result<DatasetField, LensError> {
    if tokens.len() < 2 {
        return Err(LensError::InvalidStatement(statement.to_string()));
    }
    let mut field = DatasetField::new(tokens[0], parse_data_type(tokens[1])?);
    if tokens.len() == 4
        && tokens[2].eq_ignore_ascii_case("not")
        && tokens[3].eq_ignore_ascii_case("null")
    {
        field.nullable = false;
    } else if tokens.len() != 2 {
        return Err(LensError::InvalidStatement(statement.to_string()));
    }
    Ok(field)
}

pub(crate) fn parse_data_type(value: &str) -> Result<DataType, LensError> {
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
        _ => Err(LensError::InvalidStatement(format!(
            "unsupported data type: {value}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_create_dataset_schema() {
        let dataset = parse_create_dataset(
            "create dataset events (id int64 not null, ts timestamp, payload json)",
        )
        .unwrap()
        .unwrap();

        assert_eq!(dataset.name, "events");
        assert_eq!(dataset.schema.fields.len(), 3);
        assert_eq!(dataset.schema.fields[0].name, "id");
        assert_eq!(dataset.schema.fields[0].data_type, DataType::Int64);
        assert!(!dataset.schema.fields[0].nullable);
        assert_eq!(dataset.schema.fields[1].data_type, DataType::Timestamp);
        assert_eq!(dataset.schema.fields[2].data_type, DataType::Json);
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
