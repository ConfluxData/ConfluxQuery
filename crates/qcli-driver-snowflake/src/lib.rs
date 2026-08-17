//! Snowflake adapter backed by `snowflakedb-rs`'s native JSON streaming path.

use arrow_array::{ArrayRef, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;
use futures_util::StreamExt;
use qcli_auth::UsernamePasswordCredential;
use qcli_driver_api::{
    AdapterCapabilities, AdapterCapability, CatalogMetadata, ColumnMetadata, DriverError,
    EngineAdapter, IdentifierCapabilities, IdentifierCase, MetadataRequest, ObjectKind,
    ObjectMetadata, QueryEvent, QueryRequest, QuerySink, QueryState, SchemaMetadata,
};
use snowflakedb_rs::auth::AuthStrategy;
use snowflakedb_rs::{
    ArrowSnowflakeConnection, Column, Executor, Query, QueryResult, Row,
    SnowflakeConnectionOptsBuilder,
};
use std::collections::BTreeMap;
use std::sync::Arc;

const BATCH_ROWS: usize = 1_000;

#[derive(Debug, Default)]
pub struct SnowflakeAdapter;

#[async_trait]
impl EngineAdapter for SnowflakeAdapter {
    fn engine(&self) -> &'static str {
        "snowflake"
    }

    fn capabilities(&self) -> AdapterCapabilities {
        AdapterCapabilities::from_supported([
            AdapterCapability::StreamResults,
            AdapterCapability::ListCatalogs,
            AdapterCapability::ListSchemas,
            AdapterCapability::ListObjects,
            AdapterCapability::DescribeObject,
            AdapterCapability::PreparedStatements,
        ])
    }

    fn identifier_capabilities(&self) -> IdentifierCapabilities {
        IdentifierCapabilities {
            unquoted: IdentifierCase::Upper,
            quoted: IdentifierCase::Mixed,
            quote: "\"".into(),
        }
    }

    async fn execute(&self, request: QueryRequest, sink: QuerySink) -> Result<(), DriverError> {
        let mut connection = connect(&request.properties).await?;
        sink.events
            .send(QueryEvent::State(QueryState::Running))
            .await
            .ok();
        let query = connection.query(&request.sql).await.map_err(sf_error)?;
        let result = query.execute().await.map_err(sf_error)?;
        let expected = result.expected_result_length();
        let columns = result.columns();
        let mut stream = result.rows();
        let mut buffered = Vec::with_capacity(BATCH_ROWS);
        let mut total = 0;
        let mut producing = false;
        while let Some(row) = stream.next().await {
            if sink.cancellation.is_cancelled() {
                return Err(DriverError::new(
                    "cancellation_unavailable",
                    "Snowflake cancellation requires upstream query-ID support",
                ));
            }
            buffered.push(row.map_err(sf_error)?);
            if buffered.len() == BATCH_ROWS {
                producing = send_batch(&sink, &columns, &mut buffered, producing).await?;
                total += BATCH_ROWS;
            }
        }
        if !buffered.is_empty() {
            total += buffered.len();
            let _ = send_batch(&sink, &columns, &mut buffered, producing).await?;
        }
        validate_row_count(expected, total)?;
        if let Some(properties) = session_update(&request.sql) {
            sink.events
                .send(QueryEvent::SessionProperties(properties))
                .await
                .ok();
        }
        sink.events.send(QueryEvent::RowsProduced(total)).await.ok();
        Ok(())
    }

    async fn list_catalogs(
        &self,
        request: MetadataRequest,
    ) -> Result<Vec<CatalogMetadata>, DriverError> {
        Ok(metadata_rows(&request.properties, "SHOW DATABASES")
            .await?
            .into_iter()
            .filter_map(|row| cell(&row, 1).or_else(|| cell(&row, 0)))
            .map(|name| CatalogMetadata { name })
            .collect())
    }

    async fn list_schemas(
        &self,
        request: MetadataRequest,
    ) -> Result<Vec<SchemaMetadata>, DriverError> {
        let sql = request.catalog.as_ref().map_or_else(
            || "SHOW SCHEMAS".to_owned(),
            |catalog| format!("SHOW SCHEMAS IN DATABASE {}", identifier(catalog)),
        );
        Ok(metadata_rows(&request.properties, &sql)
            .await?
            .into_iter()
            .filter_map(|row| cell(&row, 1).or_else(|| cell(&row, 0)))
            .map(|name| SchemaMetadata {
                catalog: request.catalog.clone(),
                name,
            })
            .collect())
    }

    async fn list_objects(
        &self,
        request: MetadataRequest,
    ) -> Result<Vec<ObjectMetadata>, DriverError> {
        let database = context(&request, "database", request.catalog.as_deref())?;
        let schema = context(&request, "schema", request.schema.as_deref())?;
        let sql = format!(
            "SHOW TERSE OBJECTS IN SCHEMA {}.{}",
            identifier(database),
            identifier(schema)
        );
        let pattern = request.pattern.as_deref();
        Ok(metadata_rows(&request.properties, &sql)
            .await?
            .into_iter()
            .filter_map(|row| {
                let name = cell(&row, 0).or_else(|| cell(&row, 1))?;
                if !glob_matches(pattern, &name) {
                    return None;
                }
                let kind = match cell(&row, 1).or_else(|| cell(&row, 2)).as_deref() {
                    Some("VIEW" | "MATERIALIZED VIEW") => ObjectKind::View,
                    Some("TABLE") => ObjectKind::Table,
                    _ => ObjectKind::Other,
                };
                Some(ObjectMetadata {
                    catalog: Some(database.to_owned()),
                    schema: Some(schema.to_owned()),
                    name,
                    kind,
                })
            })
            .collect())
    }

    async fn describe_object(
        &self,
        request: MetadataRequest,
        object: &str,
    ) -> Result<Vec<ColumnMetadata>, DriverError> {
        let sql = format!("DESCRIBE TABLE {}", qualified(&request, object));
        Ok(metadata_rows(&request.properties, &sql)
            .await?
            .into_iter()
            .filter_map(|row| {
                Some(ColumnMetadata {
                    name: cell(&row, 0)?,
                    data_type: cell(&row, 1).unwrap_or_else(|| "UNKNOWN".into()),
                    nullable: cell(&row, 3).map(|value| value.eq_ignore_ascii_case("Y")),
                    comment: cell(&row, 8).filter(|value| !value.is_empty()),
                })
            })
            .collect())
    }
}

async fn connect(
    properties: &BTreeMap<String, String>,
) -> Result<ArrowSnowflakeConnection, DriverError> {
    let auth_type = properties
        .get("auth_type")
        .map_or("password", String::as_str);
    if auth_type != "password" {
        return Err(DriverError::new(
            "authentication",
            format!("Snowflake authentication method '{auth_type}' is not supported yet"),
        ));
    }
    let credential = UsernamePasswordCredential::new(
        required(properties, "user")?,
        required(properties, "password")?,
    );
    let mut builder = SnowflakeConnectionOptsBuilder::default();
    builder
        .pool_size(1)
        .account_id(required(properties, "account")?)
        .username(credential.username())
        .strategy(AuthStrategy::Password(
            credential.password().expose().to_owned(),
        ))
        .download_chunks_in_parallel(4_usize)
        .download_chunks_in_order(true);
    if let Some(value) = properties.get("warehouse") {
        builder.warehouse(value);
    }
    if let Some(value) = properties.get("database") {
        builder.database(value);
    }
    if let Some(value) = properties.get("schema") {
        builder.schema(value);
    }
    if let Some(value) = properties.get("role") {
        builder.role(value);
    }
    let options = builder
        .build()
        .map_err(|error| DriverError::new("configuration", error.to_string()))?;
    let pool = options.connect_arrow().await.map_err(sf_error)?;
    pool.get().await.map_err(sf_error)
}

fn validate_row_count(expected: i64, actual: usize) -> Result<(), DriverError> {
    let expected = usize::try_from(expected).map_err(|_| {
        DriverError::new(
            "snowflake_result",
            format!("Snowflake returned invalid expected row count {expected}"),
        )
    })?;
    if expected != actual {
        return Err(DriverError::new(
            "snowflake_result",
            format!("Snowflake reported {expected} rows but the driver decoded {actual}"),
        ));
    }
    Ok(())
}

async fn send_batch(
    sink: &QuerySink,
    columns: &[Arc<Column>],
    rows: &mut Vec<Row>,
    producing: bool,
) -> Result<bool, DriverError> {
    if !producing {
        sink.events
            .send(QueryEvent::State(QueryState::ProducingResults))
            .await
            .ok();
    }
    let batch = rows_to_batch(columns, std::mem::take(rows))?;
    sink.batches
        .send(batch)
        .await
        .map_err(|_| DriverError::new("consumer_closed", "result consumer closed"))?;
    Ok(true)
}

fn rows_to_batch(columns: &[Arc<Column>], rows: Vec<Row>) -> Result<RecordBatch, DriverError> {
    let fields = columns
        .iter()
        .map(|column| {
            let mut metadata = std::collections::HashMap::new();
            metadata.insert("snowflake.type".into(), column.col_type.name());
            if let Some(precision) = column.precision {
                metadata.insert("snowflake.precision".into(), precision.to_string());
            }
            if let Some(scale) = column.scale {
                metadata.insert("snowflake.scale".into(), scale.to_string());
            }
            Field::new(&column.name, DataType::Utf8, column.nullable).with_metadata(metadata)
        })
        .collect::<Vec<_>>();
    let mut values = vec![Vec::with_capacity(rows.len()); columns.len()];
    for row in rows {
        for (index, result) in row.into_iter().enumerate() {
            let cell = result.map_err(sf_error)?;
            let value: Option<String> = cell.value.into();
            values[index].push(value);
        }
    }
    let arrays = values
        .into_iter()
        .map(|values| Arc::new(StringArray::from(values)) as ArrayRef)
        .collect::<Vec<_>>();
    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
        .map_err(|error| DriverError::new("snowflake_type", error.to_string()))
}

async fn metadata_rows(
    properties: &BTreeMap<String, String>,
    sql: &str,
) -> Result<Vec<Row>, DriverError> {
    let mut connection = connect(properties).await?;
    connection.fetch_all(sql).await.map_err(sf_error)
}

fn cell(row: &Row, index: usize) -> Option<String> {
    let cell = row.get(index).ok()?;
    let value: Option<String> = cell.value.into();
    value
}

fn required<'a>(
    properties: &'a BTreeMap<String, String>,
    name: &str,
) -> Result<&'a str, DriverError> {
    properties
        .get(name)
        .map(String::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            DriverError::new(
                "configuration",
                format!("Snowflake target requires '{name}'"),
            )
        })
}

fn sf_error(error: impl std::fmt::Display) -> DriverError {
    let message = error.to_string();
    let code = if message.to_ascii_lowercase().contains("auth")
        || message.to_ascii_lowercase().contains("password")
    {
        "authentication"
    } else {
        "snowflake"
    };
    DriverError::new(code, message)
}

fn identifier(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

fn context<'a>(
    request: &'a MetadataRequest,
    property: &str,
    selected: Option<&'a str>,
) -> Result<&'a str, DriverError> {
    selected
        .or_else(|| request.properties.get(property).map(String::as_str))
        .ok_or_else(|| {
            DriverError::new(
                "missing_context",
                format!("metadata discovery requires a {property}"),
            )
        })
}

fn qualified(request: &MetadataRequest, object: &str) -> String {
    [
        request
            .catalog
            .as_deref()
            .or_else(|| request.properties.get("database").map(String::as_str)),
        request
            .schema
            .as_deref()
            .or_else(|| request.properties.get("schema").map(String::as_str)),
        Some(object),
    ]
    .into_iter()
    .flatten()
    .map(identifier)
    .collect::<Vec<_>>()
    .join(".")
}

fn glob_matches(pattern: Option<&str>, value: &str) -> bool {
    pattern.is_none_or(|pattern| {
        pattern
            .strip_suffix('*')
            .map_or_else(|| pattern == value, |prefix| value.starts_with(prefix))
    })
}

fn session_update(sql: &str) -> Option<BTreeMap<String, String>> {
    let words = sql
        .trim()
        .trim_end_matches(';')
        .split_whitespace()
        .collect::<Vec<_>>();
    let [use_word, kind, value] = words.as_slice() else {
        return None;
    };
    if !use_word.eq_ignore_ascii_case("use") {
        return None;
    }
    let property = if kind.eq_ignore_ascii_case("database") {
        "database"
    } else if kind.eq_ignore_ascii_case("schema") {
        "schema"
    } else if kind.eq_ignore_ascii_case("warehouse") {
        "warehouse"
    } else if kind.eq_ignore_ascii_case("role") {
        "role"
    } else {
        return None;
    };
    Some(BTreeMap::from([(
        property.into(),
        value.trim_matches('"').into(),
    )]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn password_credentials_are_redacted() {
        let credential = UsernamePasswordCredential::new("alice", "secret-password");
        assert!(!format!("{credential:?}").contains("secret-password"));
    }

    #[test]
    fn identifiers_are_safely_quoted() {
        assert_eq!(identifier("a\"b"), "\"a\"\"b\"");
    }

    #[test]
    fn patterns_support_prefix_globs() {
        assert!(glob_matches(Some("NAT*"), "NATION"));
        assert!(!glob_matches(Some("NAT"), "NATION"));
    }

    #[test]
    fn tracks_snowflake_context_changes() {
        assert_eq!(
            session_update("USE WAREHOUSE REPORTING_WH;").unwrap()["warehouse"],
            "REPORTING_WH"
        );
    }

    #[test]
    fn detects_silently_dropped_snowflake_rows() {
        let error = validate_row_count(10, 0).unwrap_err();
        assert_eq!(error.code, "snowflake_result");
        assert!(error.message.contains("reported 10 rows"));
    }

    #[test]
    fn accepts_fully_decoded_snowflake_rows() {
        validate_row_count(10, 10).unwrap();
    }
}
