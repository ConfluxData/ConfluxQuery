//! Trino adapter backed by `trino-rust-client`'s low-level page API.

use arrow_array::RecordBatch;
use arrow_json::ReaderBuilder;
use arrow_schema::{DataType, Field, Fields, Schema};
use async_trait::async_trait;
use qcli_driver_api::{
    AdapterCapabilities, DriverError, EngineAdapter, QueryEvent, QueryProgress, QueryRequest,
    QuerySink, QueryState,
};
use serde::Deserialize;
use serde_json::{Map, Value};
use std::collections::BTreeMap;
use std::io::{BufReader, Cursor};
use std::sync::Arc;
use std::time::Duration;
use trino_rust_client::auth::Auth;
use trino_rust_client::error::Error as TrinoClientError;
use trino_rust_client::{Client, ClientBuilder, QueryResult, QueryResultData, Row, Stat};
use url::Url;

#[derive(Debug, Default)]
pub struct TrinoAdapter;

#[async_trait]
impl EngineAdapter for TrinoAdapter {
    fn engine(&self) -> &'static str {
        "trino"
    }

    fn capabilities(&self) -> AdapterCapabilities {
        AdapterCapabilities {
            stream_results: true,
            cancel_query: true,
        }
    }

    async fn execute(&self, request: QueryRequest, sink: QuerySink) -> Result<(), DriverError> {
        let client = build_client(&request.properties)?;
        sink.events
            .send(QueryEvent::State(QueryState::Running))
            .await
            .ok();

        let mut next_uri: Option<String> = None;
        let mut query_id: Option<String> = None;
        let mut columns = None;
        let mut total_rows = 0;
        let mut producing_results = false;

        loop {
            let page = if let Some(uri) = next_uri.take() {
                get_or_cancel(&client, &uri, query_id.as_deref(), &sink).await?
            } else {
                get_or_cancel(&client, &request.sql, None, &sink).await?
            };
            let final_page = page.next_uri.is_none();
            if query_id.is_none() {
                sink.events
                    .send(QueryEvent::EngineQueryId(page.id.clone()))
                    .await
                    .ok();
                query_id = Some(page.id.clone());
            }
            if final_page {
                sink.events
                    .try_send(QueryEvent::Progress(progress(&page.stats)))
                    .ok();
            }
            if let Some(error) = page.error {
                return Err(DriverError::new(
                    error.error_name,
                    format!("{}: {}", error.error_type, error.message),
                ));
            }
            if let Some(page_columns) = page.columns {
                columns = Some(convert_columns(page_columns)?);
            }
            if let Some(data) = page.data {
                let data = direct_rows(data)?;
                if !data.is_empty() {
                    let schema_columns = columns.as_ref().ok_or_else(|| {
                        DriverError::new("trino_protocol", "result data arrived without columns")
                    })?;
                    let batch = page_to_batch(schema_columns, data)?;
                    total_rows += batch.num_rows();
                    if batch.num_rows() > 0 {
                        if !producing_results {
                            sink.events
                                .send(QueryEvent::State(QueryState::ProducingResults))
                                .await
                                .ok();
                            producing_results = true;
                        }
                        sink.batches.send(batch).await.map_err(|_| {
                            DriverError::new("consumer_closed", "result consumer closed")
                        })?;
                    }
                }
            }
            next_uri = page.next_uri;
            if next_uri.is_none() {
                sink.events
                    .send(QueryEvent::RowsProduced(total_rows))
                    .await
                    .ok();
                return Ok(());
            }
        }
    }
}

async fn get_or_cancel(
    client: &Client,
    value: &str,
    query_id: Option<&str>,
    sink: &QuerySink,
) -> Result<QueryResult<Row>, DriverError> {
    tokio::select! {
        response = page_with_retry(client, value, query_id.is_some()) => response,
        () = wait_for_cancellation(&sink.cancellation) => {
            sink.events.send(QueryEvent::State(QueryState::Cancelling)).await.ok();
            let id = query_id.ok_or_else(|| DriverError::new(
                "cancel_unconfirmed",
                "query was cancelled before Trino returned its query ID",
            ))?;
            client.cancel(id).await.map_err(|error| {
                DriverError::new("cancel_unconfirmed", format!("Trino cancellation request failed: {error}"))
            })?;
            Err(DriverError::new("cancelled", "query cancellation was confirmed by Trino"))
        }
    }
}

async fn page_with_retry(
    client: &Client,
    value: &str,
    next: bool,
) -> Result<QueryResult<Row>, DriverError> {
    for attempt in 0..4 {
        let result = if next {
            client.get_next::<Row>(value).await
        } else {
            client.get::<Row>(value).await
        };
        let transient = matches!(
            &result,
            Err(TrinoClientError::HttpNotOk(status, _))
                if matches!(status.as_u16(), 429 | 502 | 503 | 504)
        );
        if transient && attempt < 3 {
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        }
        return result.map_err(|error| map_client_error(&error));
    }
    unreachable!("retry loop returns on its final attempt")
}

async fn wait_for_cancellation(signal: &qcli_driver_api::CancellationSignal) {
    while !signal.is_cancelled() {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn build_client(properties: &BTreeMap<String, String>) -> Result<Client, DriverError> {
    let url = Url::parse(required(properties, "url")?).map_err(|error| {
        DriverError::new("configuration", format!("invalid Trino URL: {error}"))
    })?;
    let host = url
        .host_str()
        .ok_or_else(|| DriverError::new("configuration", "Trino URL requires a host"))?;
    let user = required(properties, "user")?;
    let password = properties.get("password");
    let token = properties.get("token");
    if password.is_some() && token.is_some() {
        return Err(DriverError::new(
            "configuration",
            "configure either Trino password or token authentication, not both",
        ));
    }
    if (password.is_some() || token.is_some()) && url.scheme() != "https" {
        return Err(DriverError::new(
            "insecure_authentication",
            "qcli refuses to send Trino credentials over plain HTTP",
        ));
    }
    let mut builder = ClientBuilder::new(user, host)
        .port(url.port_or_known_default().unwrap_or(8080))
        .secure(url.scheme() == "https")
        .source(properties.get("source").map_or("qcli", String::as_str))
        .no_verify(!boolean(properties, "tls_verify", true)?)
        .max_attempt(4);
    if let Some(value) = properties.get("catalog") {
        builder = builder.catalog(value);
    }
    if let Some(value) = properties.get("schema") {
        builder = builder.schema(value);
    }
    if let Some(value) = properties.get("client_tags") {
        for tag in value
            .split(',')
            .map(str::trim)
            .filter(|tag| !tag.is_empty())
        {
            builder = builder.client_tag(tag);
        }
    }
    for (name, value) in properties {
        if let Some(name) = name.strip_prefix("session.") {
            builder = builder.property(name, value);
        }
    }
    if let Some(timeout) =
        duration(properties, "query_timeout")?.or(duration(properties, "connect_timeout")?)
    {
        builder = builder.client_request_timeout(timeout);
    }
    if let Some(token) = token {
        builder = builder.auth(Auth::new_jwt(token));
    } else if let Some(password) = password {
        builder = builder.auth(Auth::new_basic(user, Some(password)));
    }
    builder.build().map_err(|error| map_client_error(&error))
}

fn direct_rows(data: QueryResultData<Row>) -> Result<Vec<Vec<Value>>, DriverError> {
    match data {
        QueryResultData::Direct(rows) => Ok(rows.into_iter().map(Row::into_json).collect()),
        QueryResultData::Spooled(_) => Err(DriverError::new(
            "trino_spooling",
            "Trino returned spooled results; qcli currently requires the direct protocol",
        )),
    }
}

fn convert_columns(columns: Vec<trino_rust_client::Column>) -> Result<Vec<Column>, DriverError> {
    serde_json::from_value(
        serde_json::to_value(columns)
            .map_err(|error| DriverError::new("trino_protocol", error.to_string()))?,
    )
    .map_err(|error| DriverError::new("trino_protocol", error.to_string()))
}

fn progress(stats: &Stat) -> QueryProgress {
    QueryProgress {
        state: Some(stats.state.clone()),
        scheduled: Some(stats.scheduled),
        completed_splits: Some(stats.completed_splits.into()),
        total_splits: Some(stats.total_splits.into()),
        processed_rows: Some(stats.processed_rows),
        processed_bytes: Some(stats.processed_bytes),
        elapsed_millis: Some(stats.elapsed_time_millis),
    }
}

fn map_client_error(error: &TrinoClientError) -> DriverError {
    match error {
        TrinoClientError::BasicAuthWithHttp => {
            DriverError::new("insecure_authentication", error.to_string())
        }
        TrinoClientError::Forbidden { .. } => DriverError::new("authentication", error.to_string()),
        TrinoClientError::HttpError(source) if source.is_timeout() => {
            DriverError::new("timeout", "Trino request timed out")
        }
        TrinoClientError::HttpError(_) => DriverError::new("connection", error.to_string()),
        TrinoClientError::HttpNotOk(status, _)
            if status.as_u16() == 401 || status.as_u16() == 403 =>
        {
            DriverError::new("authentication", error.to_string())
        }
        TrinoClientError::HttpNotOk(_, _) => DriverError::new("trino_http", error.to_string()),
        TrinoClientError::Query(query) => DriverError::new(
            query.error_name.clone(),
            format!("{}: {}", query.error_type, query.message),
        ),
        TrinoClientError::Decode(_)
        | TrinoClientError::Protocol(_)
        | TrinoClientError::InconsistentData => {
            DriverError::new("trino_protocol", error.to_string())
        }
        TrinoClientError::Tls(_) => DriverError::new("tls", error.to_string()),
        _ => DriverError::new("trino_client", error.to_string()),
    }
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
            DriverError::new("configuration", format!("Trino requires property '{name}'"))
        })
}

fn boolean(
    properties: &BTreeMap<String, String>,
    name: &str,
    fallback: bool,
) -> Result<bool, DriverError> {
    properties
        .get(name)
        .map_or(Ok(fallback), |value| match value.as_str() {
            "true" => Ok(true),
            "false" => Ok(false),
            _ => Err(DriverError::new(
                "configuration",
                format!("property '{name}' requires true or false"),
            )),
        })
}

fn duration(
    properties: &BTreeMap<String, String>,
    name: &str,
) -> Result<Option<Duration>, DriverError> {
    properties
        .get(name)
        .map(|value| parse_duration(name, value))
        .transpose()
}

fn parse_duration(name: &str, value: &str) -> Result<Duration, DriverError> {
    let (number, multiplier) = if let Some(number) = value.strip_suffix("ms") {
        (number, 1_u64)
    } else if let Some(number) = value.strip_suffix('s') {
        (number, 1_000)
    } else if let Some(number) = value.strip_suffix('m') {
        (number, 60_000)
    } else if let Some(number) = value.strip_suffix('h') {
        (number, 3_600_000)
    } else {
        return Err(DriverError::new(
            "configuration",
            format!("property '{name}' has invalid duration"),
        ));
    };
    let number = number.parse::<u64>().map_err(|_| {
        DriverError::new(
            "configuration",
            format!("property '{name}' has invalid duration"),
        )
    })?;
    Ok(Duration::from_millis(number.saturating_mul(multiplier)))
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Column {
    name: String,
    #[serde(rename = "type")]
    display_type: String,
    #[serde(default)]
    type_signature: Option<TypeSignature>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TypeSignature {
    raw_type: String,
    #[serde(default)]
    arguments: Vec<TypeArgument>,
}

#[derive(Debug, Clone, Deserialize)]
struct TypeArgument {
    kind: String,
    value: Value,
}

fn page_to_batch(columns: &[Column], rows: Vec<Vec<Value>>) -> Result<RecordBatch, DriverError> {
    let fields = columns
        .iter()
        .map(column_field)
        .collect::<Result<Vec<_>, _>>()?;
    let schema = Arc::new(Schema::new(fields));
    let mut encoded = Vec::new();
    for row in rows {
        if row.len() != columns.len() {
            return Err(DriverError::new(
                "trino_protocol",
                "result row has the wrong column count",
            ));
        }
        let mut object = Map::new();
        for (column, value) in columns.iter().zip(row) {
            object.insert(column.name.clone(), normalize_value(column, value)?);
        }
        serde_json::to_writer(&mut encoded, &Value::Object(object))
            .map_err(|error| DriverError::new("arrow_conversion", error.to_string()))?;
        encoded.push(b'\n');
    }
    let mut reader = ReaderBuilder::new(schema)
        .with_batch_size(encoded.len().max(1))
        .build(BufReader::new(Cursor::new(encoded)))
        .map_err(|error| DriverError::new("arrow_conversion", error.to_string()))?;
    reader
        .next()
        .transpose()
        .map_err(|error| DriverError::new("arrow_conversion", error.to_string()))?
        .ok_or_else(|| DriverError::new("arrow_conversion", "empty Trino result page"))
}

fn column_field(column: &Column) -> Result<Field, DriverError> {
    let data_type = column
        .type_signature
        .as_ref()
        .map_or_else(|| display_type(&column.display_type), signature_type)?;
    Ok(Field::new(&column.name, data_type, true))
}

fn display_type(value: &str) -> Result<DataType, DriverError> {
    let lower = value.to_ascii_lowercase();
    if let Some(arguments) = lower
        .strip_prefix("decimal(")
        .and_then(|value| value.strip_suffix(')'))
    {
        let (precision, scale) = arguments
            .split_once(',')
            .ok_or_else(|| DriverError::new("trino_type", value))?;
        return decimal_type(precision.trim(), scale.trim());
    }
    scalar_type(&lower)
}

fn signature_type(signature: &TypeSignature) -> Result<DataType, DriverError> {
    match signature.raw_type.as_str() {
        "decimal" => {
            let precision = long_argument(signature, 0)?;
            let scale = long_argument(signature, 1)?;
            decimal_type(&precision.to_string(), &scale.to_string())
        }
        "array" => Ok(DataType::List(Arc::new(Field::new(
            "item",
            signature_type(&type_argument(signature, 0)?)?,
            true,
        )))),
        "map" => {
            let key = signature_type(&type_argument(signature, 0)?)?;
            let value = signature_type(&type_argument(signature, 1)?)?;
            Ok(DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(Fields::from(vec![
                        Field::new("keys", key, false),
                        Field::new("values", value, true),
                    ])),
                    false,
                )),
                false,
            ))
        }
        "row" => {
            let fields = signature
                .arguments
                .iter()
                .enumerate()
                .map(|(index, argument)| named_field(argument, index))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(DataType::Struct(Fields::from(fields)))
        }
        raw => scalar_type(raw),
    }
}

fn scalar_type(raw: &str) -> Result<DataType, DriverError> {
    match raw {
        "boolean" => Ok(DataType::Boolean),
        "tinyint" => Ok(DataType::Int8),
        "smallint" => Ok(DataType::Int16),
        "integer" => Ok(DataType::Int32),
        "bigint" => Ok(DataType::Int64),
        "real" => Ok(DataType::Float32),
        "double" => Ok(DataType::Float64),
        "varbinary" => Ok(DataType::Binary),
        "varchar"
        | "char"
        | "json"
        | "date"
        | "time"
        | "time with time zone"
        | "timestamp"
        | "timestamp with time zone"
        | "interval year to month"
        | "interval day to second"
        | "ipaddress"
        | "uuid" => Ok(DataType::Utf8),
        _ => Err(DriverError::new(
            "trino_type",
            format!("unsupported Trino type '{raw}'"),
        )),
    }
}

fn decimal_type(precision: &str, scale: &str) -> Result<DataType, DriverError> {
    let precision = precision
        .parse::<u8>()
        .map_err(|_| DriverError::new("trino_type", "invalid decimal precision"))?;
    let scale = scale
        .parse::<i8>()
        .map_err(|_| DriverError::new("trino_type", "invalid decimal scale"))?;
    Ok(DataType::Decimal128(precision, scale))
}

fn long_argument(signature: &TypeSignature, index: usize) -> Result<u64, DriverError> {
    signature
        .arguments
        .get(index)
        .and_then(|argument| argument.value.as_u64())
        .ok_or_else(|| {
            DriverError::new(
                "trino_type",
                format!(
                    "{} is missing numeric type argument {index}",
                    signature.raw_type
                ),
            )
        })
}

fn type_argument(signature: &TypeSignature, index: usize) -> Result<TypeSignature, DriverError> {
    let argument = signature
        .arguments
        .get(index)
        .ok_or_else(|| DriverError::new("trino_type", "missing type argument"))?;
    serde_json::from_value::<TypeSignature>(argument.value.clone())
        .map_err(|error| DriverError::new("trino_type", error.to_string()))
}

fn named_field(argument: &TypeArgument, index: usize) -> Result<Field, DriverError> {
    if argument.kind != "NAMED_TYPE" {
        return Err(DriverError::new(
            "trino_type",
            "row argument is not NAMED_TYPE",
        ));
    }
    let signature: TypeSignature = serde_json::from_value(argument.value["typeSignature"].clone())
        .map_err(|error| DriverError::new("trino_type", error.to_string()))?;
    let name = argument.value["fieldName"]["name"]
        .as_str()
        .map_or_else(|| format!("field_{index}"), str::to_owned);
    Ok(Field::new(name, signature_type(&signature)?, true))
}

fn normalize_value(column: &Column, value: Value) -> Result<Value, DriverError> {
    column
        .type_signature
        .as_ref()
        .map_or(Ok(value.clone()), |signature| {
            normalize_signature(signature, value)
        })
}

fn normalize_signature(signature: &TypeSignature, value: Value) -> Result<Value, DriverError> {
    if value.is_null() {
        return Ok(value);
    }
    match signature.raw_type.as_str() {
        "array" => {
            let item = type_argument(signature, 0)?;
            let values = value
                .as_array()
                .ok_or_else(|| DriverError::new("trino_protocol", "array value is not an array"))?;
            values
                .iter()
                .cloned()
                .map(|value| normalize_signature(&item, value))
                .collect::<Result<Vec<_>, _>>()
                .map(Value::Array)
        }
        "row" => {
            let values = value
                .as_array()
                .ok_or_else(|| DriverError::new("trino_protocol", "row value is not an array"))?;
            let mut object = Map::new();
            for (index, (argument, value)) in signature.arguments.iter().zip(values).enumerate() {
                let field = named_field(argument, index)?;
                let nested: TypeSignature =
                    serde_json::from_value(argument.value["typeSignature"].clone())
                        .map_err(|error| DriverError::new("trino_type", error.to_string()))?;
                object.insert(
                    field.name().clone(),
                    normalize_signature(&nested, value.clone())?,
                );
            }
            Ok(Value::Object(object))
        }
        _ => Ok(value),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Array, Decimal128Array, ListArray, MapArray, StructArray};
    use qcli_driver_api::{CancellationSignal, QuerySink};
    use std::sync::Mutex;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tokio::sync::mpsc;

    #[derive(Clone)]
    struct Reply {
        status: u16,
        body: String,
        delay: Duration,
    }

    async fn server(replies: Vec<Reply>) -> (Url, Arc<Mutex<Vec<String>>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let base = format!("http://{address}");
        let replies = replies
            .into_iter()
            .map(|mut reply| {
                reply.body = reply.body.replace("{BASE}", &base);
                reply
            })
            .collect::<Vec<_>>();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let captured = Arc::clone(&requests);
        tokio::spawn(async move {
            for reply in replies {
                let (mut stream, _) = listener.accept().await.unwrap();
                let captured = Arc::clone(&captured);
                tokio::spawn(async move {
                    let mut bytes = vec![0; 16_384];
                    let read = stream.read(&mut bytes).await.unwrap();
                    captured
                        .lock()
                        .unwrap()
                        .push(String::from_utf8_lossy(&bytes[..read]).into_owned());
                    tokio::time::sleep(reply.delay).await;
                    let reason = if reply.status == 200 {
                        "OK"
                    } else {
                        "No Content"
                    };
                    let response = format!(
                        "HTTP/1.1 {} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        reply.status,
                        reply.body.len(),
                        reply.body
                    );
                    stream.write_all(response.as_bytes()).await.ok();
                });
            }
        });
        (Url::parse(&format!("http://{address}")).unwrap(), requests)
    }

    fn request(url: &Url, sql: &str) -> QueryRequest {
        QueryRequest {
            qcli_query_id: "q1".into(),
            session_id: "s1".into(),
            session_version: 1,
            target: "test".into(),
            engine: "trino".into(),
            sql: sql.into(),
            properties: BTreeMap::from([
                ("url".into(), url.to_string()),
                ("user".into(), "alice".into()),
                ("catalog".into(), "hive".into()),
                ("schema".into(), "analytics".into()),
                ("session.query_max_run_time".into(), "30m".into()),
            ]),
        }
    }

    const RUNNING_STATS: &str = r#"{"state":"RUNNING","queued":false,"scheduled":true,"nodes":1,"totalSplits":1,"queuedSplits":0,"runningSplits":1,"completedSplits":0,"cpuTimeMillis":0,"wallTimeMillis":0,"queuedTimeMillis":0,"elapsedTimeMillis":0,"processedRows":0,"processedBytes":0,"peakMemoryBytes":0,"spilledBytes":0}"#;
    const FINISHED_STATS: &str = r#"{"state":"FINISHED","queued":false,"scheduled":true,"nodes":1,"totalSplits":1,"queuedSplits":0,"runningSplits":0,"completedSplits":1,"cpuTimeMillis":1,"wallTimeMillis":1,"queuedTimeMillis":0,"elapsedTimeMillis":1,"processedRows":1,"processedBytes":8,"peakMemoryBytes":0,"spilledBytes":0}"#;

    fn result(id: &str, stats: &str, fields: &str) -> String {
        format!(
            r#"{{"id":"{id}","infoUri":"http://localhost/ui/query.html?{id}","stats":{stats},"warnings":[]{fields}}}"#
        )
    }

    fn sink() -> (
        QuerySink,
        mpsc::Receiver<QueryEvent>,
        mpsc::Receiver<RecordBatch>,
        CancellationSignal,
    ) {
        let (event_tx, event_rx) = mpsc::channel(32);
        let (batch_tx, batch_rx) = mpsc::channel(8);
        let cancellation = CancellationSignal::default();
        (
            QuerySink {
                events: event_tx,
                batches: batch_tx,
                cancellation: cancellation.clone(),
            },
            event_rx,
            batch_rx,
            cancellation,
        )
    }

    #[tokio::test]
    async fn submits_native_sql_follows_pages_and_sends_context() {
        let (url, requests) = server(vec![
            Reply {
                status: 200,
                body: result("query-1", RUNNING_STATS, r#","nextUri":"{BASE}/next""#),
                delay: Duration::ZERO,
            },
            Reply {
                status: 200,
                body: result("query-1", FINISHED_STATS, r#","columns":[{"name":"answer","type":"bigint","typeSignature":{"rawType":"bigint","arguments":[]}}],"data":[[42]]"#),
                delay: Duration::ZERO,
            },
        ]).await;
        let (sink, mut events, mut batches, _) = sink();
        TrinoAdapter
            .execute(request(&url, "SELECT 21 + 21"), sink)
            .await
            .unwrap();
        assert_eq!(batches.recv().await.unwrap().num_rows(), 1);
        let events = std::iter::from_fn(|| events.try_recv().ok()).collect::<Vec<_>>();
        assert!(events.contains(&QueryEvent::EngineQueryId("query-1".into())));
        assert!(events.iter().any(|event| matches!(event, QueryEvent::Progress(progress) if progress.processed_rows == Some(1))));
        let requests = requests.lock().unwrap();
        assert!(requests[0].starts_with("POST /v1/statement HTTP/1.1"));
        assert!(requests[0].contains("SELECT 21 + 21"));
        assert!(
            requests[0]
                .to_ascii_lowercase()
                .contains("x-trino-user: alice")
        );
        assert!(
            requests[0]
                .to_ascii_lowercase()
                .contains("x-trino-catalog: hive")
        );
        assert!(
            requests[0]
                .to_ascii_lowercase()
                .contains("x-trino-session: query_max_run_time=30m")
        );
        assert!(requests[1].starts_with("GET /next HTTP/1.1"));
    }

    #[test]
    fn converts_decimal_array_map_and_row_to_arrow() {
        let columns: Vec<Column> = serde_json::from_value(serde_json::json!([
            {"name":"price","type":"decimal(20,6)","typeSignature":{"rawType":"decimal","arguments":[{"kind":"LONG","value":20},{"kind":"LONG","value":6}]}},
            {"name":"items","type":"array(bigint)","typeSignature":{"rawType":"array","arguments":[{"kind":"TYPE","value":{"rawType":"bigint","arguments":[]}}]}},
            {"name":"labels","type":"map(varchar,bigint)","typeSignature":{"rawType":"map","arguments":[{"kind":"TYPE","value":{"rawType":"varchar","arguments":[]}},{"kind":"TYPE","value":{"rawType":"bigint","arguments":[]}}]}},
            {"name":"point","type":"row(x bigint, y varchar)","typeSignature":{"rawType":"row","arguments":[{"kind":"NAMED_TYPE","value":{"fieldName":{"name":"x"},"typeSignature":{"rawType":"bigint","arguments":[]}}},{"kind":"NAMED_TYPE","value":{"fieldName":{"name":"y"},"typeSignature":{"rawType":"varchar","arguments":[]}}}]}}
        ])).unwrap();
        let batch = page_to_batch(
            &columns,
            vec![vec![
                Value::String("12345678901234.123456".into()),
                serde_json::json!([1, 2]),
                serde_json::json!({"a": 7}),
                serde_json::json!([9, "north"]),
            ]],
        )
        .unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .unwrap()
                .value(0),
            12_345_678_901_234_123_456
        );
        assert_eq!(
            batch
                .column(1)
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap()
                .value_length(0),
            2
        );
        assert_eq!(
            batch
                .column(2)
                .as_any()
                .downcast_ref::<MapArray>()
                .unwrap()
                .value_length(0),
            1
        );
        assert_eq!(
            batch
                .column(3)
                .as_any()
                .downcast_ref::<StructArray>()
                .unwrap()
                .num_columns(),
            2
        );
    }

    #[tokio::test]
    async fn cancellation_uses_delete_and_is_confirmed() {
        // Build the first page with the actual dynamic address in two stages.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let url = Url::parse(&format!("http://{address}")).unwrap();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let captured = Arc::clone(&requests);
        tokio::spawn(async move {
            for index in 0..3 {
                let (mut stream, _) = listener.accept().await.unwrap();
                let captured = Arc::clone(&captured);
                tokio::spawn(async move {
                    let mut bytes = vec![0; 16_384];
                    let read = stream.read(&mut bytes).await.unwrap();
                    captured
                        .lock()
                        .unwrap()
                        .push(String::from_utf8_lossy(&bytes[..read]).into_owned());
                    if index == 1 {
                        tokio::time::sleep(Duration::from_secs(5)).await;
                    }
                    let (status, body) = if index == 0 {
                        (
                            200,
                            result(
                                "query-cancel",
                                RUNNING_STATS,
                                &format!(r#","nextUri":"http://{address}/next""#),
                            ),
                        )
                    } else if index == 2 {
                        (204, String::new())
                    } else {
                        (200, result("query-cancel", FINISHED_STATS, ""))
                    };
                    let response = format!(
                        "HTTP/1.1 {status} OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    stream.write_all(response.as_bytes()).await.ok();
                });
            }
        });
        let (sink, mut events, _batches, cancellation) = sink();
        let task = tokio::spawn(async move {
            TrinoAdapter
                .execute(request(&url, "SELECT slow"), sink)
                .await
        });
        while !matches!(events.recv().await, Some(QueryEvent::EngineQueryId(_))) {}
        tokio::time::sleep(Duration::from_millis(50)).await;
        cancellation.cancel();
        let error = task.await.unwrap().unwrap_err();
        assert_eq!(error.code, "cancelled");
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(
            requests
                .lock()
                .unwrap()
                .iter()
                .any(|request| request.starts_with("DELETE /v1/query/query-cancel"))
        );
    }

    #[tokio::test]
    async fn retries_transient_gateway_response() {
        let (url, requests) = server(vec![
            Reply {
                status: 503,
                body: String::new(),
                delay: Duration::ZERO,
            },
            Reply {
                status: 200,
                body: result("query-retry", FINISHED_STATS, r#","columns":[{"name":"answer","type":"bigint","typeSignature":{"rawType":"bigint","arguments":[]}}],"data":[[1]]"#),
                delay: Duration::ZERO,
            },
        ])
        .await;
        let (sink, _events, mut batches, _) = sink();
        TrinoAdapter
            .execute(request(&url, "SELECT 1"), sink)
            .await
            .unwrap();
        assert_eq!(batches.recv().await.unwrap().num_rows(), 1);
        assert_eq!(requests.lock().unwrap().len(), 2);
    }

    #[test]
    fn accepts_basic_and_bearer_auth_configuration() {
        let base = BTreeMap::from([
            ("url".into(), "https://trino.test".into()),
            ("user".into(), "alice".into()),
        ]);
        let mut basic = base.clone();
        basic.insert("password".into(), "secret".into());
        build_client(&basic).unwrap();

        let mut bearer = base;
        bearer.insert("token".into(), "token-value".into());
        build_client(&bearer).unwrap();
    }

    #[test]
    fn rejects_credentials_over_plain_http_without_leaking_them() {
        let properties = BTreeMap::from([
            ("url".into(), "http://trino.test".into()),
            ("user".into(), "alice".into()),
            ("password".into(), "very-secret".into()),
        ]);
        let error = build_client(&properties).err().unwrap();
        assert_eq!(error.code, "insecure_authentication");
        assert!(!error.to_string().contains("very-secret"));
    }

    #[tokio::test]
    #[ignore = "requires QCLI_TRINO_URL pointing at a live Trino coordinator"]
    async fn live_trino_cancellation_is_confirmed() {
        let url = Url::parse(&std::env::var("QCLI_TRINO_URL").expect("QCLI_TRINO_URL is required"))
            .unwrap();
        let (sink, mut events, _batches, cancellation) = sink();
        let task = tokio::spawn(async move {
            TrinoAdapter
                .execute(
                    request(&url, "SELECT count(*) FROM tpch.sf100000.lineitem"),
                    sink,
                )
                .await
        });
        while !matches!(events.recv().await, Some(QueryEvent::EngineQueryId(_))) {}
        tokio::time::sleep(Duration::from_millis(100)).await;
        cancellation.cancel();
        let error = tokio::time::timeout(Duration::from_secs(10), task)
            .await
            .expect("cancellation timed out")
            .unwrap()
            .unwrap_err();
        assert_eq!(error.code, "cancelled");
    }

    #[tokio::test]
    #[ignore = "requires QCLI_TRINO_URL pointing at a live Trino coordinator"]
    async fn live_trino_streams_multiple_result_pages() {
        let url = Url::parse(&std::env::var("QCLI_TRINO_URL").expect("QCLI_TRINO_URL is required"))
            .unwrap();
        let (sink, _events, mut batches, _) = sink();
        let task = tokio::spawn(async move {
            TrinoAdapter
                .execute(
                    request(&url, "SELECT * FROM tpch.sf1.lineitem LIMIT 100000"),
                    sink,
                )
                .await
        });
        let mut rows = 0;
        let mut pages = 0;
        while let Some(batch) = batches.recv().await {
            rows += batch.num_rows();
            pages += 1;
        }
        task.await.unwrap().unwrap();
        assert_eq!(rows, 100_000);
        assert!(pages > 1, "expected more than one Trino result page");
    }
}
