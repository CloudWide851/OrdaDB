use std::fs::{self, File};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use ordadb_catalog::TableDefinition;
use ordadb_engine::Engine;
use ordadb_types::{DbError, Identifier, QueryEvent, Result, ScalarType, Value};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Number};
use tempfile::NamedTempFile;
use uuid::Uuid;

const DEFAULT_NULL_TOKEN: &str = r"\N";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum TransferFormat {
    Csv,
    JsonLines,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TableTransferRequest {
    pub schema: String,
    pub table: String,
    pub path: PathBuf,
    pub format: TransferFormat,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransferLimits {
    pub max_file_bytes: u64,
    pub max_rows: u64,
    pub max_record_bytes: usize,
}

impl Default for TransferLimits {
    fn default() -> Self {
        Self {
            max_file_bytes: 64 * 1024 * 1024 * 1024,
            max_rows: 100_000_000,
            max_record_bytes: 64 * 1024 * 1024,
        }
    }
}

impl TransferLimits {
    fn validate(self) -> Result<Self> {
        if self.max_file_bytes == 0 || self.max_rows == 0 || self.max_record_bytes == 0 {
            return Err(invalid("table transfer limits must be non-zero"));
        }
        Ok(self)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransferSummary {
    pub operation_id: Uuid,
    pub schema: String,
    pub table: String,
    pub path: PathBuf,
    pub format: TransferFormat,
    pub rows: u64,
    pub bytes: u64,
}

pub fn resolve_operation_path(
    operations_root: impl AsRef<Path>,
    requested: impl AsRef<Path>,
    output: bool,
) -> Result<PathBuf> {
    let root = operations_root
        .as_ref()
        .canonicalize()
        .map_err(|error| io_error("failed to resolve operations root", error))?;
    if !root.is_dir() {
        return Err(invalid("operations root must be a directory"));
    }
    let requested = requested.as_ref();
    if requested.as_os_str().is_empty() {
        return Err(invalid("operation path must not be empty"));
    }
    let candidate = if requested.is_absolute() {
        requested.to_path_buf()
    } else {
        root.join(requested)
    };
    let resolved = if output {
        let parent = candidate
            .parent()
            .ok_or_else(|| invalid("operation output path has no parent directory"))?
            .canonicalize()
            .map_err(|error| io_error("failed to resolve operation output parent", error))?;
        let name = candidate
            .file_name()
            .ok_or_else(|| invalid("operation output path has no file name"))?;
        parent.join(name)
    } else {
        candidate
            .canonicalize()
            .map_err(|error| io_error("failed to resolve operation input path", error))?
    };
    if !resolved.starts_with(&root) {
        return Err(DbError::new(
            "42501",
            "operation path escapes the configured operations root",
        ));
    }
    Ok(resolved)
}

pub fn import_table(
    engine: &Engine,
    operations_root: impl AsRef<Path>,
    request: &TableTransferRequest,
    limits: TransferLimits,
    cancellation: Option<&AtomicBool>,
) -> Result<TransferSummary> {
    let limits = limits.validate()?;
    let path = resolve_operation_path(operations_root, &request.path, false)?;
    let bytes = fs::metadata(&path)
        .map_err(|error| io_error("failed to inspect table import", error))?
        .len();
    if bytes > limits.max_file_bytes {
        return Err(resource_limit(format!(
            "table import is {bytes} bytes; limit is {}",
            limits.max_file_bytes
        )));
    }
    let (schema, table) = resolve_table(engine, &request.schema, &request.table)?;
    let insert_sql = insert_sql(&schema, &table)?;
    let mut session = engine.connect()?;
    let mut transaction = session.begin()?;
    let mut rows = 0_u64;
    match request.format {
        TransferFormat::Csv => {
            let mut reader = csv::ReaderBuilder::new()
                .has_headers(true)
                .flexible(false)
                .from_path(&path)
                .map_err(|error| transfer_error("failed to open CSV import", error))?;
            let headers = reader
                .headers()
                .map_err(|error| transfer_error("failed to read CSV header", error))?
                .clone();
            validate_headers(&headers, &table)?;
            for record in reader.records() {
                check_cancelled(cancellation)?;
                rows = next_row_count(rows, limits)?;
                let record =
                    record.map_err(|error| transfer_error("failed to read CSV row", error))?;
                if record.as_slice().len() > limits.max_record_bytes {
                    return Err(resource_limit("CSV row exceeds the configured byte limit"));
                }
                let values = record
                    .iter()
                    .zip(table.columns())
                    .map(|(value, column)| csv_to_value(value, &column.data_type))
                    .collect::<Result<Vec<_>>>()?;
                transaction.execute(&insert_sql, &values)?;
            }
        }
        TransferFormat::JsonLines => {
            let file = File::open(&path)
                .map_err(|error| io_error("failed to open JSON Lines import", error))?;
            let mut reader = BufReader::new(file);
            loop {
                check_cancelled(cancellation)?;
                let Some(line) = read_bounded_line(&mut reader, limits.max_record_bytes)? else {
                    break;
                };
                if line.iter().all(u8::is_ascii_whitespace) {
                    continue;
                }
                rows = next_row_count(rows, limits)?;
                let object: Map<String, serde_json::Value> = serde_json::from_slice(&line)
                    .map_err(|error| json_error("failed to decode JSON Lines row", error))?;
                if object.len() != table.columns().len() {
                    return Err(invalid(
                        "JSON Lines row must contain every table column exactly once",
                    ));
                }
                let values = table
                    .columns()
                    .iter()
                    .map(|column| {
                        object
                            .get(column.name.as_str())
                            .ok_or_else(|| {
                                DbError::new(
                                    "42703",
                                    format!("JSON Lines row is missing column {}", column.name),
                                )
                            })
                            .and_then(|value| json_to_value(value, &column.data_type))
                    })
                    .collect::<Result<Vec<_>>>()?;
                transaction.execute(&insert_sql, &values)?;
            }
        }
    }
    transaction.commit()?;
    Ok(TransferSummary {
        operation_id: Uuid::new_v4(),
        schema: schema.as_str().to_owned(),
        table: table.name.as_str().to_owned(),
        path,
        format: request.format,
        rows,
        bytes,
    })
}

pub fn export_table(
    engine: &Engine,
    operations_root: impl AsRef<Path>,
    request: &TableTransferRequest,
    limits: TransferLimits,
    cancellation: Option<&AtomicBool>,
) -> Result<TransferSummary> {
    let limits = limits.validate()?;
    let path = resolve_operation_path(operations_root, &request.path, true)?;
    if path.exists() {
        return Err(
            DbError::new("55000", "table export destination already exists")
                .with_hint("choose a new path; existing exports are never overwritten"),
        );
    }
    let (schema, table) = resolve_table(engine, &request.schema, &request.table)?;
    let parent = path
        .parent()
        .ok_or_else(|| invalid("table export destination has no parent directory"))?;
    let mut output = NamedTempFile::new_in(parent)
        .map_err(|error| io_error("failed to create table export", error))?;
    let select_sql = format!("SELECT * FROM {schema}.{}", table.name);
    let mut session = engine.connect()?;
    let stream = session.execute_stream(&select_sql, &[])?;
    let mut rows = 0_u64;
    match request.format {
        TransferFormat::Csv => {
            let mut writer = csv::WriterBuilder::new()
                .has_headers(false)
                .from_writer(BufWriter::new(output.as_file_mut()));
            writer
                .write_record(table.columns().iter().map(|column| column.name.as_str()))
                .map_err(|error| transfer_error("failed to write CSV header", error))?;
            for event in stream {
                check_cancelled(cancellation)?;
                if let QueryEvent::Batch(batch) = event? {
                    for row in batch.rows {
                        rows = next_row_count(rows, limits)?;
                        writer
                            .write_record(row.values.iter().map(value_to_csv))
                            .map_err(|error| transfer_error("failed to write CSV row", error))?;
                    }
                }
            }
            writer
                .flush()
                .map_err(|error| io_error("failed to flush CSV export", error))?;
        }
        TransferFormat::JsonLines => {
            let mut writer = BufWriter::new(output.as_file_mut());
            for event in stream {
                check_cancelled(cancellation)?;
                if let QueryEvent::Batch(batch) = event? {
                    for row in batch.rows {
                        rows = next_row_count(rows, limits)?;
                        let object = table
                            .columns()
                            .iter()
                            .zip(row.values.iter())
                            .map(|(column, value)| {
                                (column.name.as_str().to_owned(), value_to_json(value))
                            })
                            .collect::<Map<_, _>>();
                        serde_json::to_writer(&mut writer, &object)
                            .map_err(|error| json_error("failed to write JSON Lines row", error))?;
                        writer.write_all(b"\n").map_err(|error| {
                            io_error("failed to terminate JSON Lines row", error)
                        })?;
                    }
                }
            }
            writer
                .flush()
                .map_err(|error| io_error("failed to flush JSON Lines export", error))?;
        }
    }
    output
        .as_file_mut()
        .sync_all()
        .map_err(|error| io_error("failed to synchronize table export", error))?;
    let bytes = output
        .as_file()
        .metadata()
        .map_err(|error| io_error("failed to inspect table export", error))?
        .len();
    if bytes > limits.max_file_bytes {
        return Err(resource_limit(format!(
            "table export is {bytes} bytes; limit is {}",
            limits.max_file_bytes
        )));
    }
    output
        .persist(&path)
        .map_err(|error| io_error("failed to publish table export", error.error))?;
    Ok(TransferSummary {
        operation_id: Uuid::new_v4(),
        schema: schema.as_str().to_owned(),
        table: table.name.as_str().to_owned(),
        path,
        format: request.format,
        rows,
        bytes,
    })
}

fn resolve_table(
    engine: &Engine,
    schema_name: &str,
    table_name: &str,
) -> Result<(Identifier, TableDefinition)> {
    if schema_name.is_empty() || table_name.is_empty() {
        return Err(invalid("schema and table names must not be empty"));
    }
    let schema_name = Identifier::unquoted(schema_name);
    let table_name = Identifier::unquoted(table_name);
    let catalog = engine.catalog_snapshot()?;
    let schema = catalog
        .schema(&schema_name)
        .ok_or_else(|| DbError::new("3F000", format!("schema {schema_name} does not exist")))?;
    let table = schema.table(&table_name).cloned().ok_or_else(|| {
        DbError::new(
            "42P01",
            format!("table {schema_name}.{table_name} does not exist"),
        )
    })?;
    Ok((schema_name, table))
}

fn insert_sql(schema: &Identifier, table: &TableDefinition) -> Result<String> {
    if table.columns().is_empty() {
        return Err(DbError::new(
            "0A000",
            "table transfer does not support zero-column tables",
        ));
    }
    let columns = table
        .columns()
        .iter()
        .map(|column| column.name.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    let parameters = (1..=table.columns().len())
        .map(|index| format!("${index}"))
        .collect::<Vec<_>>()
        .join(", ");
    Ok(format!(
        "INSERT INTO {schema}.{} ({columns}) VALUES ({parameters})",
        table.name
    ))
}

fn validate_headers(headers: &csv::StringRecord, table: &TableDefinition) -> Result<()> {
    if headers.len() != table.columns().len()
        || headers
            .iter()
            .zip(table.columns())
            .any(|(header, column)| header != column.name.as_str())
    {
        return Err(invalid(
            "CSV header must match the complete table column order",
        ));
    }
    Ok(())
}

fn csv_to_value(value: &str, data_type: &ScalarType) -> Result<Value> {
    if value == DEFAULT_NULL_TOKEN {
        return Ok(Value::Null);
    }
    text_to_value(value, data_type)
}

fn json_to_value(value: &serde_json::Value, data_type: &ScalarType) -> Result<Value> {
    if value.is_null() {
        return Ok(Value::Null);
    }
    match data_type {
        ScalarType::Boolean => value
            .as_bool()
            .map(Value::Boolean)
            .ok_or_else(|| type_error("JSON boolean required")),
        ScalarType::Int16 => json_integer(value, "INT2")?
            .try_into()
            .map(Value::Int16)
            .map_err(|_| type_error("JSON integer is outside INT2 range")),
        ScalarType::Int32 => json_integer(value, "INT4")?
            .try_into()
            .map(Value::Int32)
            .map_err(|_| type_error("JSON integer is outside INT4 range")),
        ScalarType::Int64 => json_integer(value, "INT8").map(Value::Int64),
        ScalarType::Float32 => {
            json_float(value, "FLOAT4").map(|value| Value::Float32(value as f32))
        }
        ScalarType::Float64 => json_float(value, "FLOAT8").map(Value::Float64),
        ScalarType::Json => Ok(Value::Json(value.clone())),
        ScalarType::Jsonb => Ok(Value::Jsonb(value.clone())),
        ScalarType::Vector { dimensions } => {
            let values = value
                .as_array()
                .ok_or_else(|| type_error("JSON vector array required"))?
                .iter()
                .map(|value| {
                    json_float(value, "VECTOR element").and_then(|value| {
                        let value = value as f32;
                        if value.is_finite() {
                            Ok(value)
                        } else {
                            Err(type_error("VECTOR element must be finite"))
                        }
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            if dimensions.is_some_and(|dimensions| dimensions != values.len()) {
                return Err(type_error("VECTOR dimension does not match its column"));
            }
            Ok(Value::Vector(values))
        }
        _ => value
            .as_str()
            .ok_or_else(|| type_error("JSON string required for this column type"))
            .and_then(|value| text_to_value(value, data_type)),
    }
}

fn text_to_value(value: &str, data_type: &ScalarType) -> Result<Value> {
    let parsed = match data_type {
        ScalarType::Boolean => Value::Boolean(match value.to_ascii_lowercase().as_str() {
            "true" | "t" | "1" => true,
            "false" | "f" | "0" => false,
            _ => return Err(type_error("boolean text must be true/false, t/f, or 1/0")),
        }),
        ScalarType::Int16 => Value::Int16(parse_text(value, "INT2")?),
        ScalarType::Int32 => Value::Int32(parse_text(value, "INT4")?),
        ScalarType::Int64 => Value::Int64(parse_text(value, "INT8")?),
        ScalarType::Float32 => {
            let value: f32 = parse_text(value, "FLOAT4")?;
            if !value.is_finite() {
                return Err(type_error("FLOAT4 must be finite"));
            }
            Value::Float32(value)
        }
        ScalarType::Float64 => {
            let value: f64 = parse_text(value, "FLOAT8")?;
            if !value.is_finite() {
                return Err(type_error("FLOAT8 must be finite"));
            }
            Value::Float64(value)
        }
        ScalarType::Decimal { .. } => Value::Decimal(
            Decimal::from_str(value).map_err(|_| type_error("invalid DECIMAL text"))?,
        ),
        ScalarType::Char { .. } | ScalarType::Varchar { .. } | ScalarType::Text => {
            Value::Text(value.to_owned())
        }
        ScalarType::Binary => Value::Binary(
            BASE64
                .decode(value)
                .map_err(|_| type_error("binary text must use base64"))?,
        ),
        ScalarType::Date => Value::Date(
            NaiveDate::parse_from_str(value, "%Y-%m-%d")
                .map_err(|_| type_error("DATE text must use YYYY-MM-DD"))?,
        ),
        ScalarType::Time => Value::Time(
            NaiveTime::parse_from_str(value, "%H:%M:%S%.f")
                .map_err(|_| type_error("TIME text must use HH:MM:SS[.fraction]"))?,
        ),
        ScalarType::Timestamp { .. } => Value::Timestamp(
            NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S%.f")
                .or_else(|_| NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f"))
                .map_err(|_| type_error("TIMESTAMP text is invalid"))?,
        ),
        ScalarType::Json => Value::Json(
            serde_json::from_str(value).map_err(|error| json_error("invalid JSON text", error))?,
        ),
        ScalarType::Jsonb => Value::Jsonb(
            serde_json::from_str(value).map_err(|error| json_error("invalid JSONB text", error))?,
        ),
        ScalarType::Uuid => {
            Value::Uuid(Uuid::parse_str(value).map_err(|_| type_error("invalid UUID text"))?)
        }
        ScalarType::Vector { dimensions } => {
            let values: Vec<f32> = serde_json::from_str(value)
                .map_err(|error| json_error("VECTOR text must be a JSON array", error))?;
            if values.iter().any(|value| !value.is_finite()) {
                return Err(type_error("VECTOR elements must be finite"));
            }
            if dimensions.is_some_and(|dimensions| dimensions != values.len()) {
                return Err(type_error("VECTOR dimension does not match its column"));
            }
            Value::Vector(values)
        }
    };
    Ok(parsed)
}

fn value_to_csv(value: &Value) -> String {
    match value {
        Value::Null => DEFAULT_NULL_TOKEN.to_owned(),
        Value::Boolean(value) => value.to_string(),
        Value::Int16(value) => value.to_string(),
        Value::Int32(value) => value.to_string(),
        Value::Int64(value) => value.to_string(),
        Value::Float32(value) => value.to_string(),
        Value::Float64(value) => value.to_string(),
        Value::Decimal(value) => value.to_string(),
        Value::Text(value) => value.clone(),
        Value::Binary(value) => BASE64.encode(value),
        Value::Date(value) => value.format("%Y-%m-%d").to_string(),
        Value::Time(value) => value.format("%H:%M:%S%.f").to_string(),
        Value::Timestamp(value) => value.format("%Y-%m-%dT%H:%M:%S%.f").to_string(),
        Value::Json(value) | Value::Jsonb(value) => value.to_string(),
        Value::Uuid(value) => value.to_string(),
        Value::Vector(value) => serde_json::to_string(value).unwrap_or_else(|_| "[]".into()),
    }
}

fn value_to_json(value: &Value) -> serde_json::Value {
    match value {
        Value::Null => serde_json::Value::Null,
        Value::Boolean(value) => serde_json::Value::Bool(*value),
        Value::Int16(value) => Number::from(*value).into(),
        Value::Int32(value) => Number::from(*value).into(),
        Value::Int64(value) => Number::from(*value).into(),
        Value::Float32(value) => {
            Number::from_f64(f64::from(*value)).map_or(serde_json::Value::Null, Into::into)
        }
        Value::Float64(value) => {
            Number::from_f64(*value).map_or(serde_json::Value::Null, Into::into)
        }
        Value::Decimal(value) => serde_json::Value::String(value.to_string()),
        Value::Text(value) => serde_json::Value::String(value.clone()),
        Value::Binary(value) => serde_json::Value::String(BASE64.encode(value)),
        Value::Date(value) => serde_json::Value::String(value.format("%Y-%m-%d").to_string()),
        Value::Time(value) => serde_json::Value::String(value.format("%H:%M:%S%.f").to_string()),
        Value::Timestamp(value) => {
            serde_json::Value::String(value.format("%Y-%m-%dT%H:%M:%S%.f").to_string())
        }
        Value::Json(value) | Value::Jsonb(value) => value.clone(),
        Value::Uuid(value) => serde_json::Value::String(value.to_string()),
        Value::Vector(value) => serde_json::to_value(value).unwrap_or(serde_json::Value::Null),
    }
}

fn read_bounded_line(
    reader: &mut impl BufRead,
    max_record_bytes: usize,
) -> Result<Option<Vec<u8>>> {
    let mut line = Vec::new();
    loop {
        let available = reader
            .fill_buf()
            .map_err(|error| io_error("failed to read JSON Lines import", error))?;
        if available.is_empty() {
            return if line.is_empty() {
                Ok(None)
            } else {
                Ok(Some(line))
            };
        }
        let take = available
            .iter()
            .position(|byte| *byte == b'\n')
            .map_or(available.len(), |position| position + 1);
        if line.len().saturating_add(take) > max_record_bytes {
            return Err(resource_limit(
                "JSON Lines row exceeds the configured byte limit",
            ));
        }
        line.extend_from_slice(&available[..take]);
        let terminated = available[take - 1] == b'\n';
        reader.consume(take);
        if terminated {
            while matches!(line.last(), Some(b'\n' | b'\r')) {
                line.pop();
            }
            return Ok(Some(line));
        }
    }
}

fn check_cancelled(cancellation: Option<&AtomicBool>) -> Result<()> {
    if cancellation.is_some_and(|cancelled| cancelled.load(Ordering::Acquire)) {
        return Err(DbError::new("57014", "table transfer was cancelled"));
    }
    Ok(())
}

fn next_row_count(rows: u64, limits: TransferLimits) -> Result<u64> {
    let rows = rows
        .checked_add(1)
        .ok_or_else(|| resource_limit("table transfer row count overflowed"))?;
    if rows > limits.max_rows {
        return Err(resource_limit(format!(
            "table transfer exceeds the row limit of {}",
            limits.max_rows
        )));
    }
    Ok(rows)
}

fn json_integer(value: &serde_json::Value, label: &str) -> Result<i64> {
    value
        .as_i64()
        .ok_or_else(|| type_error(format!("JSON {label} integer required")))
}

fn json_float(value: &serde_json::Value, label: &str) -> Result<f64> {
    let value = value
        .as_f64()
        .ok_or_else(|| type_error(format!("JSON {label} number required")))?;
    if !value.is_finite() {
        return Err(type_error(format!("JSON {label} must be finite")));
    }
    Ok(value)
}

fn parse_text<T: FromStr>(value: &str, label: &str) -> Result<T> {
    value
        .parse()
        .map_err(|_| type_error(format!("invalid {label} text")))
}

fn invalid(message: impl Into<String>) -> DbError {
    DbError::new("22023", message)
}

fn type_error(message: impl Into<String>) -> DbError {
    DbError::new("22P02", message)
}

fn resource_limit(message: impl Into<String>) -> DbError {
    DbError::new("54000", message)
}

fn io_error(context: impl Into<String>, error: std::io::Error) -> DbError {
    DbError::new("58030", context).with_detail(error.to_string())
}

fn json_error(context: impl Into<String>, error: serde_json::Error) -> DbError {
    DbError::new("22P02", context).with_detail(error.to_string())
}

fn transfer_error(context: impl Into<String>, error: csv::Error) -> DbError {
    DbError::new("22P02", context).with_detail(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use ordadb_engine::EngineConfig;
    use ordadb_types::QueryEvent;
    use tempfile::tempdir;

    fn engine(data_dir: &Path) -> Engine {
        let engine = Engine::open(EngineConfig::new(data_dir)).expect("engine");
        let mut session = engine.connect().expect("session");
        session
            .execute(
                "CREATE TABLE items (id BIGINT PRIMARY KEY, label TEXT NOT NULL, enabled BOOLEAN)",
                &[],
            )
            .expect("create");
        engine
    }

    fn row_count(engine: &Engine) -> usize {
        let mut session = engine.connect().expect("session");
        session
            .execute("SELECT * FROM items", &[])
            .expect("select")
            .filter_map(|event| match event {
                QueryEvent::Batch(batch) => Some(batch.rows.len()),
                _ => None,
            })
            .sum()
    }

    #[test]
    fn csv_and_json_lines_round_trip_through_atomic_files() {
        let directory = tempdir().expect("tempdir");
        let operations = directory.path().join("operations");
        fs::create_dir(&operations).expect("operations");
        let source = engine(&directory.path().join("source"));
        fs::write(
            operations.join("items.csv"),
            "id,label,enabled\r\n1,alpha,true\r\n2,beta,\\N\r\n",
        )
        .expect("csv");
        let request = TableTransferRequest {
            schema: "public".into(),
            table: "items".into(),
            path: "items.csv".into(),
            format: TransferFormat::Csv,
        };
        let imported = import_table(
            &source,
            &operations,
            &request,
            TransferLimits::default(),
            None,
        )
        .expect("import");
        assert_eq!(imported.rows, 2);

        let export = TableTransferRequest {
            path: "items.jsonl".into(),
            format: TransferFormat::JsonLines,
            ..request
        };
        let exported = export_table(
            &source,
            &operations,
            &export,
            TransferLimits::default(),
            None,
        )
        .expect("export");
        assert_eq!(exported.rows, 2);
        let destination = engine(&directory.path().join("destination"));
        import_table(
            &destination,
            &operations,
            &export,
            TransferLimits::default(),
            None,
        )
        .expect("JSON import");
        assert_eq!(row_count(&destination), 2);
    }

    #[test]
    fn malformed_or_cancelled_import_rolls_back_every_row() {
        let directory = tempdir().expect("tempdir");
        let operations = directory.path().join("operations");
        fs::create_dir(&operations).expect("operations");
        let engine = engine(&directory.path().join("data"));
        fs::write(
            operations.join("bad.csv"),
            "id,label,enabled\n1,alpha,true\n1,duplicate,false\n",
        )
        .expect("bad csv");
        let request = TableTransferRequest {
            schema: "public".into(),
            table: "items".into(),
            path: "bad.csv".into(),
            format: TransferFormat::Csv,
        };
        assert_eq!(
            import_table(
                &engine,
                &operations,
                &request,
                TransferLimits::default(),
                None,
            )
            .expect_err("duplicate")
            .sql_state,
            "23505"
        );
        assert_eq!(row_count(&engine), 0);

        let cancelled = AtomicBool::new(true);
        assert_eq!(
            import_table(
                &engine,
                &operations,
                &request,
                TransferLimits::default(),
                Some(&cancelled),
            )
            .expect_err("cancelled")
            .sql_state,
            "57014"
        );
        assert_eq!(row_count(&engine), 0);
    }

    #[test]
    fn operation_paths_cannot_escape_the_root() {
        let directory = tempdir().expect("tempdir");
        let root = directory.path().join("operations");
        fs::create_dir(&root).expect("root");
        assert_eq!(
            resolve_operation_path(&root, directory.path().join("outside.csv"), true)
                .expect_err("outside")
                .sql_state,
            "42501"
        );
    }
}
