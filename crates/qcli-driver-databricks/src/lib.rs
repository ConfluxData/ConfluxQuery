//! Databricks SQL adapter using the official Statement Execution REST API.

use arrow_array::{ArrayRef, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;
use qcli_auth::{BearerCredentialProvider, StaticBearerCredential};
use qcli_driver_api::{
    AdapterCapabilities, AdapterCapability, CatalogMetadata, ColumnMetadata, DriverError,
    EngineAdapter, MetadataRequest, ObjectKind, ObjectMetadata, QueryEvent, QueryRequest,
    QuerySink, QueryState, SchemaMetadata,
};
use reqwest::{Client, Method, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, Default)]
pub struct DatabricksAdapter;

#[async_trait]
impl EngineAdapter for DatabricksAdapter {
    fn engine(&self) -> &'static str {
        "databricks"
    }

    fn capabilities(&self) -> AdapterCapabilities {
        AdapterCapabilities::from_supported([
            AdapterCapability::StreamResults,
            AdapterCapability::CancelQuery,
            AdapterCapability::ListCatalogs,
            AdapterCapability::ListSchemas,
            AdapterCapability::ListObjects,
            AdapterCapability::DescribeObject,
        ])
    }

    async fn execute(&self, request: QueryRequest, sink: QuerySink) -> Result<(), DriverError> {
        let connection = Connection::from_properties(&request.properties)?;
        sink.events
            .send(QueryEvent::State(QueryState::Running))
            .await
            .ok();
        let mut response = connection.submit(&request.sql).await?;
        let statement_id = response.statement_id.clone().ok_or_else(|| {
            DriverError::new(
                "databricks_protocol",
                "response did not contain a statement ID",
            )
        })?;
        sink.events
            .send(QueryEvent::EngineQueryId(statement_id.clone()))
            .await
            .ok();
        let mut rows = 0;
        let mut producing = false;
        loop {
            if sink.cancellation.is_cancelled() {
                connection.cancel(&statement_id).await?;
                return Err(DriverError::new("cancelled", "query was cancelled"));
            }
            match response.state()? {
                "PENDING" | "RUNNING" => {
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    response = connection.status(&statement_id).await?;
                }
                "SUCCEEDED" => {
                    let columns = response.columns().to_vec();
                    if let Some(result) = response.result.take() {
                        let mut chunk = result;
                        loop {
                            if !chunk.data_array.is_empty() {
                                if !producing {
                                    sink.events
                                        .send(QueryEvent::State(QueryState::ProducingResults))
                                        .await
                                        .ok();
                                    producing = true;
                                }
                                let batch = result_batch(&columns, &chunk.data_array)?;
                                rows += batch.num_rows();
                                sink.batches.send(batch).await.map_err(|_| {
                                    DriverError::new("consumer_closed", "result consumer closed")
                                })?;
                            }
                            let Some(link) = chunk.next_chunk_internal_link else {
                                break;
                            };
                            chunk = connection.chunk(&link).await?;
                        }
                    }
                    if let Some(properties) = session_update(&request.sql) {
                        sink.events
                            .send(QueryEvent::SessionProperties(properties))
                            .await
                            .ok();
                    }
                    sink.events.send(QueryEvent::RowsProduced(rows)).await.ok();
                    return Ok(());
                }
                "CANCELED" | "CLOSED" => {
                    return Err(DriverError::new("cancelled", "query was cancelled"));
                }
                "FAILED" => return Err(response.execution_error()),
                state => {
                    return Err(DriverError::new(
                        "databricks_protocol",
                        format!("unknown statement state '{state}'"),
                    ));
                }
            }
        }
    }

    async fn list_catalogs(
        &self,
        request: MetadataRequest,
    ) -> Result<Vec<CatalogMetadata>, DriverError> {
        Ok(metadata_rows(&request, "SHOW CATALOGS")
            .await?
            .into_iter()
            .filter_map(|r| value(&r, 0))
            .map(|name| CatalogMetadata { name })
            .collect())
    }

    async fn list_schemas(
        &self,
        request: MetadataRequest,
    ) -> Result<Vec<SchemaMetadata>, DriverError> {
        let sql = request.catalog.as_ref().map_or_else(
            || "SHOW SCHEMAS".to_owned(),
            |catalog| format!("SHOW SCHEMAS IN {}", identifier(catalog)),
        );
        Ok(metadata_rows(&request, &sql)
            .await?
            .into_iter()
            .filter_map(|r| value(&r, 0))
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
        let catalog = context(&request, "catalog")?;
        let schema = context(&request, "schema")?;
        let sql = format!(
            "SHOW TABLES IN {}.{}",
            identifier(catalog),
            identifier(schema)
        );
        let pattern = request.pattern.clone();
        Ok(metadata_rows(&request, &sql)
            .await?
            .into_iter()
            .filter_map(|row| {
                let name = value(&row, 1).or_else(|| value(&row, 0))?;
                glob_matches(pattern.as_deref(), &name).then(|| ObjectMetadata {
                    catalog: Some(catalog.to_owned()),
                    schema: Some(schema.to_owned()),
                    name,
                    kind: ObjectKind::Table,
                })
            })
            .collect())
    }

    async fn describe_object(
        &self,
        request: MetadataRequest,
        object: &str,
    ) -> Result<Vec<ColumnMetadata>, DriverError> {
        let sql = format!("DESCRIBE {}", qualified(&request, object));
        Ok(metadata_rows(&request, &sql)
            .await?
            .into_iter()
            .filter_map(|row| {
                Some(ColumnMetadata {
                    name: value(&row, 0)?,
                    data_type: value(&row, 1).unwrap_or_else(|| "unknown".into()),
                    nullable: None,
                    comment: value(&row, 2),
                })
            })
            .collect())
    }
}

struct Connection {
    host: String,
    warehouse_id: String,
    catalog: Option<String>,
    schema: Option<String>,
    client: Client,
    credential: Arc<dyn BearerCredentialProvider>,
}

impl Connection {
    fn from_properties(properties: &BTreeMap<String, String>) -> Result<Self, DriverError> {
        let auth_type = properties.get("auth_type").map_or("pat", String::as_str);
        if auth_type != "pat" {
            return Err(DriverError::new(
                "authentication",
                format!("Databricks authentication method '{auth_type}' is not supported yet"),
            ));
        }
        let host = required(properties, "host")?
            .trim_end_matches('/')
            .to_owned();
        if !host.starts_with("https://")
            && !host.starts_with("http://127.0.0.1")
            && !host.starts_with("http://localhost")
        {
            return Err(DriverError::new(
                "insecure_authentication",
                "Databricks credentials require HTTPS",
            ));
        }
        let http_path = required(properties, "http_path")?;
        let warehouse_id = http_path
            .trim_end_matches('/')
            .rsplit('/')
            .next()
            .filter(|v| !v.is_empty())
            .ok_or_else(|| {
                DriverError::new("configuration", "http_path does not contain a warehouse ID")
            })?
            .to_owned();
        let token = required(properties, "token")?.to_owned();
        let timeout = properties
            .get("connect_timeout")
            .and_then(|v| parse_duration(v))
            .unwrap_or(Duration::from_secs(10));
        let client = Client::builder()
            .connect_timeout(timeout)
            .build()
            .map_err(|error| http_error(&error))?;
        Ok(Self {
            host,
            warehouse_id,
            catalog: properties.get("catalog").cloned(),
            schema: properties.get("schema").cloned(),
            client,
            credential: Arc::new(StaticBearerCredential::new("pat", token)),
        })
    }

    async fn submit(&self, statement: &str) -> Result<StatementResponse, DriverError> {
        self.request(
            Method::POST,
            "/api/2.0/sql/statements",
            Some(&SubmitRequest {
                statement,
                warehouse_id: &self.warehouse_id,
                catalog: self.catalog.as_deref(),
                schema: self.schema.as_deref(),
                wait_timeout: "10s",
                on_wait_timeout: "CONTINUE",
                disposition: "INLINE",
                format: "JSON_ARRAY",
            }),
        )
        .await
    }

    async fn status(&self, id: &str) -> Result<StatementResponse, DriverError> {
        self.request::<(), _>(Method::GET, &format!("/api/2.0/sql/statements/{id}"), None)
            .await
    }

    async fn chunk(&self, link: &str) -> Result<ResultData, DriverError> {
        self.request::<(), _>(Method::GET, link, None).await
    }

    async fn cancel(&self, id: &str) -> Result<(), DriverError> {
        let _: Value = self
            .request::<(), _>(
                Method::POST,
                &format!("/api/2.0/sql/statements/{id}/cancel"),
                None,
            )
            .await?;
        Ok(())
    }

    async fn request<B: Serialize + ?Sized, R: for<'de> Deserialize<'de>>(
        &self,
        method: Method,
        path: &str,
        body: Option<&B>,
    ) -> Result<R, DriverError> {
        let token = self
            .credential
            .credential()
            .await
            .map_err(|e| DriverError::new("authentication", e.to_string()))?;
        let mut request = self
            .client
            .request(method, format!("{}{}", self.host, path))
            .bearer_auth(token.expose());
        if let Some(body) = body {
            request = request.json(body);
        }
        let response = request.send().await.map_err(|error| http_error(&error))?;
        let status = response.status();
        let bytes = response.bytes().await.map_err(|error| http_error(&error))?;
        if !status.is_success() {
            let message = serde_json::from_slice::<ApiError>(&bytes)
                .ok()
                .and_then(|e| e.message)
                .unwrap_or_else(|| {
                    status
                        .canonical_reason()
                        .unwrap_or("request failed")
                        .to_owned()
                });
            let code = if matches!(status, StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN) {
                "authentication"
            } else {
                "databricks_http"
            };
            return Err(DriverError::new(code, message));
        }
        serde_json::from_slice(&bytes)
            .map_err(|e| DriverError::new("databricks_protocol", e.to_string()))
    }
}

#[derive(Serialize)]
struct SubmitRequest<'a> {
    statement: &'a str,
    warehouse_id: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    catalog: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    schema: Option<&'a str>,
    wait_timeout: &'a str,
    on_wait_timeout: &'a str,
    disposition: &'a str,
    format: &'a str,
}

#[derive(Debug, Deserialize)]
struct StatementResponse {
    statement_id: Option<String>,
    status: Option<StatementStatus>,
    manifest: Option<Manifest>,
    result: Option<ResultData>,
}

impl StatementResponse {
    fn state(&self) -> Result<&str, DriverError> {
        self.status
            .as_ref()
            .map(|s| s.state.as_str())
            .ok_or_else(|| {
                DriverError::new(
                    "databricks_protocol",
                    "response did not contain statement state",
                )
            })
    }
    fn columns(&self) -> &[Column] {
        self.manifest
            .as_ref()
            .and_then(|m| m.schema.as_ref())
            .map_or(&[], |s| s.columns.as_slice())
    }
    fn execution_error(&self) -> DriverError {
        let error = self.status.as_ref().and_then(|s| s.error.as_ref());
        DriverError::new(
            error
                .and_then(|e| e.error_code.clone())
                .unwrap_or_else(|| "databricks_query".into()),
            error
                .and_then(|e| e.message.clone())
                .unwrap_or_else(|| "Databricks statement failed".into()),
        )
    }
}

#[derive(Debug, Deserialize)]
struct StatementStatus {
    state: String,
    error: Option<StatementError>,
}
#[derive(Debug, Deserialize)]
struct StatementError {
    error_code: Option<String>,
    message: Option<String>,
}
#[derive(Debug, Deserialize)]
struct Manifest {
    schema: Option<ResultSchema>,
}
#[derive(Debug, Deserialize)]
struct ResultSchema {
    #[serde(default)]
    columns: Vec<Column>,
}
#[derive(Debug, Clone, Deserialize)]
struct Column {
    name: String,
    #[serde(default)]
    type_text: String,
}
#[derive(Debug, Deserialize)]
struct ResultData {
    #[serde(default)]
    data_array: Vec<Vec<Option<Value>>>,
    next_chunk_internal_link: Option<String>,
}
#[derive(Debug, Deserialize)]
struct ApiError {
    message: Option<String>,
}

fn result_batch(
    columns: &[Column],
    rows: &[Vec<Option<Value>>],
) -> Result<RecordBatch, DriverError> {
    let width = columns.len().max(rows.first().map_or(0, Vec::len));
    let fields = (0..width)
        .map(|index| {
            let column = columns.get(index);
            let mut metadata = std::collections::HashMap::new();
            if let Some(column) = column {
                metadata.insert("databricks.type".into(), column.type_text.clone());
            }
            Field::new(
                column.map_or_else(|| format!("column_{}", index + 1), |c| c.name.clone()),
                DataType::Utf8,
                true,
            )
            .with_metadata(metadata)
        })
        .collect::<Vec<_>>();
    let arrays = (0..width)
        .map(|index| {
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| row.get(index).and_then(Option::as_ref).map(display_value))
                    .collect::<Vec<_>>(),
            )) as ArrayRef
        })
        .collect::<Vec<_>>();
    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
        .map_err(|e| DriverError::new("databricks_type", e.to_string()))
}

fn display_value(value: &Value) -> String {
    value
        .as_str()
        .map_or_else(|| value.to_string(), ToOwned::to_owned)
}
fn required<'a>(p: &'a BTreeMap<String, String>, name: &str) -> Result<&'a str, DriverError> {
    p.get(name)
        .map(String::as_str)
        .filter(|v| !v.is_empty())
        .ok_or_else(|| {
            DriverError::new(
                "configuration",
                format!("Databricks target requires '{name}'"),
            )
        })
}
fn parse_duration(value: &str) -> Option<Duration> {
    value
        .strip_suffix("ms")
        .and_then(|v| v.parse().ok())
        .map(Duration::from_millis)
        .or_else(|| {
            value
                .strip_suffix('s')
                .and_then(|v| v.parse().ok())
                .map(Duration::from_secs)
        })
}
fn http_error(error: &reqwest::Error) -> DriverError {
    if error.is_timeout() {
        DriverError::new("timeout", "Databricks request timed out")
    } else {
        DriverError::new("connection", error.to_string())
    }
}
fn identifier(value: &str) -> String {
    format!("`{}`", value.replace('`', "``"))
}
fn qualified(request: &MetadataRequest, object: &str) -> String {
    [
        request.catalog.as_deref(),
        request.schema.as_deref(),
        Some(object),
    ]
    .into_iter()
    .flatten()
    .map(identifier)
    .collect::<Vec<_>>()
    .join(".")
}
fn context<'a>(request: &'a MetadataRequest, name: &str) -> Result<&'a str, DriverError> {
    match name {
        "catalog" => request
            .catalog
            .as_deref()
            .or_else(|| request.properties.get(name).map(String::as_str)),
        "schema" => request
            .schema
            .as_deref()
            .or_else(|| request.properties.get(name).map(String::as_str)),
        _ => None,
    }
    .ok_or_else(|| {
        DriverError::new(
            "missing_context",
            format!("metadata discovery requires a {name}"),
        )
    })
}
fn value(row: &[Option<Value>], index: usize) -> Option<String> {
    row.get(index)?.as_ref().map(display_value)
}
fn glob_matches(pattern: Option<&str>, value: &str) -> bool {
    pattern.is_none_or(|p| {
        p.strip_suffix('*')
            .map_or_else(|| p == value, |prefix| value.starts_with(prefix))
    })
}

fn session_update(sql: &str) -> Option<BTreeMap<String, String>> {
    let statement = sql.trim().trim_end_matches(';').trim();
    let words = statement.split_whitespace().collect::<Vec<_>>();
    match words.as_slice() {
        [use_word, kind, value]
            if use_word.eq_ignore_ascii_case("use") && kind.eq_ignore_ascii_case("catalog") =>
        {
            Some(BTreeMap::from([
                ("catalog".into(), clean_identifier(value)),
                ("schema".into(), "default".into()),
            ]))
        }
        [use_word, kind, value]
            if use_word.eq_ignore_ascii_case("use") && kind.eq_ignore_ascii_case("schema") =>
        {
            Some(BTreeMap::from([("schema".into(), clean_identifier(value))]))
        }
        [use_word, kind, value]
            if use_word.eq_ignore_ascii_case("use") && kind.eq_ignore_ascii_case("database") =>
        {
            let names = value
                .split_once('.')
                .map_or_else(
                    || vec![("schema".into(), clean_identifier(value))],
                    |(catalog, schema)| {
                        vec![
                            ("catalog".into(), clean_identifier(catalog)),
                            ("schema".into(), clean_identifier(schema)),
                        ]
                    },
                )
                .into_iter()
                .collect();
            Some(names)
        }
        _ => None,
    }
}

fn clean_identifier(value: &str) -> String {
    value.trim_matches(['`', '"', '\'']).into()
}

async fn metadata_rows(
    request: &MetadataRequest,
    sql: &str,
) -> Result<Vec<Vec<Option<Value>>>, DriverError> {
    let connection = Connection::from_properties(&request.properties)?;
    let mut response = connection.submit(sql).await?;
    let id = response.statement_id.clone().ok_or_else(|| {
        DriverError::new(
            "databricks_protocol",
            "metadata response omitted statement ID",
        )
    })?;
    let mut rows = Vec::new();
    loop {
        match response.state()? {
            "PENDING" | "RUNNING" => {
                tokio::time::sleep(Duration::from_millis(100)).await;
                response = connection.status(&id).await?;
            }
            "SUCCEEDED" => {
                if let Some(result) = response.result.take() {
                    let mut chunk = result;
                    loop {
                        rows.extend(chunk.data_array);
                        let Some(link) = chunk.next_chunk_internal_link else {
                            break;
                        };
                        chunk = connection.chunk(&link).await?;
                    }
                }
                return Ok(rows);
            }
            "FAILED" => return Err(response.execution_error()),
            state => {
                return Err(DriverError::new(
                    "databricks_query",
                    format!("metadata statement ended in state '{state}'"),
                ));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn properties() -> BTreeMap<String, String> {
        BTreeMap::from([
            ("auth_type".into(), "pat".into()),
            ("host".into(), "https://dbc.example.test".into()),
            (
                "http_path".into(),
                "/sql/1.0/warehouses/warehouse-123".into(),
            ),
            ("token".into(), "secret-token".into()),
        ])
    }

    #[test]
    fn validates_pat_and_derives_warehouse() {
        let connection = Connection::from_properties(&properties()).unwrap();
        assert_eq!(connection.warehouse_id, "warehouse-123");
        assert!(!format!("{:?}", connection.credential.method()).contains("secret-token"));
    }

    #[test]
    fn rejects_unknown_authentication_before_network_access() {
        let mut values = properties();
        values.insert("auth_type".into(), "oauth-m2m".into());
        let error = Connection::from_properties(&values).err().unwrap();
        assert_eq!(error.code, "authentication");
    }

    #[test]
    fn preserves_values_and_native_type_metadata() {
        let columns = vec![
            Column {
                name: "amount".into(),
                type_text: "DECIMAL(38,18)".into(),
            },
            Column {
                name: "payload".into(),
                type_text: "STRUCT<x:STRING>".into(),
            },
        ];
        let batch = result_batch(
            &columns,
            &[vec![
                Some(Value::String("1234567890.123456789012345678".into())),
                Some(serde_json::json!({"x": "value"})),
            ]],
        )
        .unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(
            batch.schema().field(0).metadata()["databricks.type"],
            "DECIMAL(38,18)"
        );
        let amount = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(amount.value(0), "1234567890.123456789012345678");
    }

    #[test]
    fn redacts_pat_provider() {
        let provider = StaticBearerCredential::new("pat", "secret-token");
        assert!(!format!("{provider:?}").contains("secret-token"));
    }

    #[test]
    fn tracks_databricks_context_changes() {
        let catalog = session_update("USE CATALOG main;").unwrap();
        assert_eq!(catalog["catalog"], "main");
        assert_eq!(catalog["schema"], "default");

        let qualified = session_update("USE DATABASE hive_metastore.tpch_1;").unwrap();
        assert_eq!(qualified["catalog"], "hive_metastore");
        assert_eq!(qualified["schema"], "tpch_1");
    }

    #[test]
    fn accepts_successful_commands_without_result_columns() {
        let response: StatementResponse = serde_json::from_value(serde_json::json!({
            "statement_id": "statement-1",
            "status": { "state": "SUCCEEDED" },
            "manifest": {
                "format": "JSON_ARRAY",
                "schema": { "column_count": 0 }
            },
            "result": { "data_array": [] }
        }))
        .unwrap();

        assert_eq!(response.state().unwrap(), "SUCCEEDED");
        assert!(response.columns().is_empty());
    }
}
