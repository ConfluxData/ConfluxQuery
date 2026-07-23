//! Versioned localhost HTTP transport over qcli's shared session/query core.

use arrow_array::RecordBatch;
use arrow_ipc::reader::FileReader;
use arrow_ipc::writer::{FileWriter, StreamWriter};
use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, patch, post};
use axum::{Json, Router};
use base64::Engine;
use futures_util::StreamExt;
use qcli_config::{Config, ResolvedTarget};
use qcli_core::{CoreError, QueryItem, QueryService, SessionManager, SessionSnapshot};
use qcli_driver_api::{CancellationSignal, EngineAdapter, QueryEvent, QueryProgress, QueryState};
use qcli_output::{DisplayOptions, OutputFormat, StreamOutput};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::{BTreeMap, HashMap};
use std::convert::Infallible;
use std::io::Cursor;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tokio_stream::wrappers::ReceiverStream;
use uuid::Uuid;

const LOCAL_OWNER: &str = "local";

#[derive(Debug, Clone)]
pub struct HttpLimits {
    pub max_queries: usize,
    pub memory_result_bytes_per_query: usize,
    pub max_result_bytes_per_query: usize,
    pub result_ttl: Duration,
    pub default_page_rows: usize,
    pub max_page_rows: usize,
    pub max_sql_bytes: usize,
}

impl Default for HttpLimits {
    fn default() -> Self {
        Self {
            max_queries: 128,
            memory_result_bytes_per_query: 1024 * 1024,
            max_result_bytes_per_query: 64 * 1024 * 1024,
            result_ttl: Duration::from_secs(15 * 60),
            default_page_rows: 1_000,
            max_page_rows: 10_000,
            max_sql_bytes: 1024 * 1024,
        }
    }
}

#[derive(Clone)]
struct AppState {
    config: Arc<Config>,
    sessions: Arc<SessionManager>,
    queries: Arc<QueryService>,
    records: Arc<Mutex<HashMap<String, Arc<QueryRecord>>>>,
    limits: HttpLimits,
    page_secret: u64,
}

struct QueryRecord {
    id: String,
    owner: &'static str,
    session_id: String,
    session_version: u64,
    target: String,
    engine: String,
    cancel: CancellationSignal,
    data: Mutex<QueryData>,
    events: broadcast::Sender<EventEntry>,
}

struct QueryData {
    state: String,
    engine_query_id: Option<String>,
    rows: usize,
    retained_bytes: usize,
    storage: ResultStorage,
    events: Vec<EventEntry>,
    next_event_id: u64,
    error: Option<ApiErrorBody>,
    completed_at: Option<Instant>,
}

enum ResultStorage {
    Memory(Vec<RecordBatch>),
    Spill {
        path: PathBuf,
        writer: Option<Box<FileWriter<std::fs::File>>>,
    },
}

impl Drop for QueryRecord {
    fn drop(&mut self) {
        if let ResultStorage::Spill { path, .. } = &self
            .data
            .lock()
            .expect("query record mutex poisoned")
            .storage
        {
            std::fs::remove_file(path).ok();
        }
    }
}

#[derive(Debug, Clone, Serialize)]
struct EventEntry {
    id: u64,
    event: String,
    data: Value,
    terminal: bool,
}

#[derive(Debug, Clone, Serialize)]
struct ApiErrorBody {
    code: String,
    message: String,
}

#[derive(Debug)]
struct ApiError {
    status: StatusCode,
    body: ApiErrorBody,
}

impl ApiError {
    fn new(status: StatusCode, code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            status,
            body: ApiErrorBody {
                code: code.into(),
                message: message.into(),
            },
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.status, Json(json!({ "error": self.body }))).into_response()
    }
}

#[derive(Clone)]
pub struct HttpService {
    state: AppState,
}

impl HttpService {
    #[must_use]
    pub fn new(
        config: Config,
        adapters: impl IntoIterator<Item = Arc<dyn EngineAdapter>>,
        limits: HttpLimits,
    ) -> Self {
        let uuid = Uuid::new_v4();
        let bytes = uuid.as_bytes();
        let page_secret = u64::from_le_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]);
        Self {
            state: AppState {
                config: Arc::new(config),
                sessions: Arc::new(SessionManager::default()),
                queries: Arc::new(QueryService::new(adapters, 8)),
                records: Arc::new(Mutex::new(HashMap::new())),
                limits,
                page_secret,
            },
        }
    }

    pub fn router(&self) -> Router {
        Router::new()
            .route("/v1/sessions", post(create_session))
            .route(
                "/v1/sessions/{session_id}",
                get(get_session)
                    .patch(update_session)
                    .delete(delete_session),
            )
            .route(
                "/v1/sessions/{session_id}/target",
                post(switch_session_target),
            )
            .route(
                "/v1/sessions/{session_id}/properties",
                patch(update_session),
            )
            .route("/v1/sessions/{session_id}/options", patch(update_session))
            .route(
                "/v1/sessions/{session_id}/queries",
                post(submit_session_query),
            )
            .route("/v1/queries", post(submit_stateless_query))
            .route("/v1/queries/{query_id}", get(get_query))
            .route("/v1/queries/{query_id}/results", get(get_results))
            .route("/v1/queries/{query_id}/events", get(get_events))
            .route("/v1/queries/{query_id}/cancel", post(cancel_query))
            .with_state(self.state.clone())
    }

    /// Serve until the listener fails or the task is cancelled.
    ///
    /// # Errors
    ///
    /// Returns an I/O error from accepting or serving a connection.
    pub async fn serve(self, listener: TcpListener) -> std::io::Result<()> {
        axum::serve(listener, self.router()).await
    }
}

#[derive(Debug, Deserialize)]
struct CreateSessionRequest {
    target: String,
    #[serde(default)]
    context: BTreeMap<String, Value>,
    #[serde(default)]
    properties: BTreeMap<String, Value>,
    #[serde(default)]
    options: BTreeMap<String, Value>,
}

#[derive(Debug, Deserialize)]
struct UpdateSessionRequest {
    expected_version: u64,
    #[serde(default)]
    context: BTreeMap<String, Value>,
    #[serde(default)]
    properties: BTreeMap<String, Value>,
    #[serde(default)]
    options: BTreeMap<String, Value>,
}

#[derive(Debug, Deserialize)]
struct SwitchTargetRequest {
    expected_version: u64,
    target: String,
}

#[derive(Debug, Deserialize)]
struct QueryRequest {
    sql: String,
}

#[derive(Debug, Deserialize)]
struct StatelessQueryRequest {
    target: String,
    sql: String,
    #[serde(default)]
    context: BTreeMap<String, Value>,
    #[serde(default)]
    properties: BTreeMap<String, Value>,
}

#[derive(Debug, Serialize)]
struct SessionResponse {
    id: String,
    version: u64,
    target: String,
    engine: String,
}

impl From<SessionSnapshot> for SessionResponse {
    fn from(snapshot: SessionSnapshot) -> Self {
        Self {
            id: snapshot.id,
            version: snapshot.version,
            target: snapshot.target,
            engine: snapshot.engine,
        }
    }
}

#[derive(Debug, Serialize)]
struct QueryResponse {
    id: String,
    session_id: String,
    session_version: u64,
    target: String,
    engine: String,
    engine_query_id: Option<String>,
    state: String,
    rows: usize,
    error: Option<ApiErrorBody>,
}

async fn create_session(
    State(state): State<AppState>,
    Json(request): Json<CreateSessionRequest>,
) -> Result<(StatusCode, Json<SessionResponse>), ApiError> {
    let target = target(&state, &request.target)?;
    let mut overrides = values(request.context)?;
    overrides.extend(values(request.properties)?);
    overrides.extend(values(request.options)?);
    let snapshot = state.sessions.create_with_overrides(target, overrides);
    Ok((StatusCode::CREATED, Json(snapshot.into())))
}

async fn get_session(
    State(state): State<AppState>,
    Path(session_id): Path<String>,
) -> Result<Json<SessionResponse>, ApiError> {
    Ok(Json(
        state
            .sessions
            .snapshot(&session_id)
            .map_err(|error| core_error(&error))?
            .into(),
    ))
}

async fn update_session(
    State(state): State<AppState>,
    Path(session_id): Path<String>,
    Json(request): Json<UpdateSessionRequest>,
) -> Result<Json<SessionResponse>, ApiError> {
    let mut overrides = values(request.context)?;
    overrides.extend(values(request.properties)?);
    overrides.extend(values(request.options)?);
    let snapshot = state
        .sessions
        .set_options(&session_id, request.expected_version, overrides)
        .map_err(|error| core_error(&error))?;
    Ok(Json(snapshot.into()))
}

async fn switch_session_target(
    State(state): State<AppState>,
    Path(session_id): Path<String>,
    Json(request): Json<SwitchTargetRequest>,
) -> Result<Json<SessionResponse>, ApiError> {
    let target = target(&state, &request.target)?;
    let snapshot = state
        .sessions
        .switch_target(&session_id, request.expected_version, target)
        .map_err(|error| core_error(&error))?;
    Ok(Json(snapshot.into()))
}

async fn delete_session(
    State(state): State<AppState>,
    Path(session_id): Path<String>,
) -> Result<StatusCode, ApiError> {
    state
        .sessions
        .close(&session_id)
        .map_err(|error| core_error(&error))?;
    Ok(StatusCode::NO_CONTENT)
}

async fn submit_session_query(
    State(state): State<AppState>,
    Path(session_id): Path<String>,
    Json(request): Json<QueryRequest>,
) -> Result<(StatusCode, Json<QueryResponse>), ApiError> {
    let snapshot = state
        .sessions
        .snapshot(&session_id)
        .map_err(|error| core_error(&error))?;
    submit_query(&state, &snapshot, request.sql, false)
}

async fn submit_stateless_query(
    State(state): State<AppState>,
    Json(request): Json<StatelessQueryRequest>,
) -> Result<(StatusCode, Json<QueryResponse>), ApiError> {
    let target = target(&state, &request.target)?;
    let mut overrides = values(request.context)?;
    overrides.extend(values(request.properties)?);
    let snapshot = state.sessions.create_with_overrides(target, overrides);
    submit_query(&state, &snapshot, request.sql, true)
}

fn submit_query(
    state: &AppState,
    snapshot: &SessionSnapshot,
    sql: String,
    close_session: bool,
) -> Result<(StatusCode, Json<QueryResponse>), ApiError> {
    if sql.is_empty() || sql.len() > state.limits.max_sql_bytes {
        return Err(ApiError::new(
            StatusCode::PAYLOAD_TOO_LARGE,
            "invalid_sql_size",
            format!("SQL must contain 1..={} bytes", state.limits.max_sql_bytes),
        ));
    }
    cleanup_expired(state);
    {
        let records = state.records.lock().expect("query registry mutex poisoned");
        if records.len() >= state.limits.max_queries {
            return Err(ApiError::new(
                StatusCode::TOO_MANY_REQUESTS,
                "query_limit",
                "local retained-query limit reached",
            ));
        }
    }
    let handle = state
        .queries
        .submit(snapshot.clone(), sql)
        .map_err(|error| core_error(&error))?;
    let cancellation = handle.cancellation_signal();
    let id = format!("qcli_{}", Uuid::new_v4().simple());
    let (events, _) = broadcast::channel(128);
    let record = Arc::new(QueryRecord {
        id: id.clone(),
        owner: LOCAL_OWNER,
        session_id: snapshot.id.clone(),
        session_version: snapshot.version,
        target: snapshot.target.clone(),
        engine: snapshot.engine.clone(),
        cancel: cancellation,
        data: Mutex::new(QueryData {
            state: "submitted".into(),
            engine_query_id: None,
            rows: 0,
            retained_bytes: 0,
            storage: ResultStorage::Memory(Vec::new()),
            events: Vec::new(),
            next_event_id: 1,
            error: None,
            completed_at: None,
        }),
        events,
    });
    state
        .records
        .lock()
        .expect("query registry mutex poisoned")
        .insert(id, record.clone());
    let response = query_response(&record);
    tokio::spawn(collect_query(
        state.sessions.clone(),
        record,
        handle,
        state.limits.clone(),
        close_session,
    ));
    Ok((StatusCode::ACCEPTED, Json(response)))
}

async fn collect_query(
    sessions: Arc<SessionManager>,
    record: Arc<QueryRecord>,
    mut handle: qcli_core::QueryHandle,
    limits: HttpLimits,
    close_session: bool,
) {
    let mut overflow = false;
    let mut session_updates = BTreeMap::new();
    let mut terminal_event = None;
    while let Some(item) = handle.next_item().await {
        match item {
            QueryItem::Event(event) => {
                if let QueryEvent::SessionProperties(properties) = &event {
                    session_updates.extend(properties.clone());
                }
                if matches!(
                    event,
                    QueryEvent::State(
                        QueryState::Completed | QueryState::Cancelled | QueryState::Failed
                    )
                ) {
                    terminal_event = Some(event);
                } else {
                    record_event(&record, event);
                }
            }
            QueryItem::Batch(batch) => {
                let mut data = record.data.lock().expect("query record mutex poisoned");
                if let Err(error) = store_batch(&record.id, &mut data, batch, &limits) {
                    overflow = true;
                    data.error = Some(error);
                    drop(data);
                    record.cancel.cancel();
                }
            }
        }
    }
    let finish = handle.finish().await;
    if finish.is_ok() && !close_session && !session_updates.is_empty() {
        sessions
            .set_options(&record.session_id, record.session_version, session_updates)
            .ok();
    }
    if let Err(error) = finish_results(&record) {
        let mut data = record.data.lock().expect("query record mutex poisoned");
        data.error = Some(error);
        overflow = true;
    }
    if let Err(error) = finish {
        if matches!(&error, CoreError::Driver(driver) if driver.code == "cancelled") {
            record_event(
                &record,
                terminal_event.unwrap_or(QueryEvent::State(QueryState::Cancelled)),
            );
            if close_session {
                sessions.close(&record.session_id).ok();
            }
            return;
        }
        let mut data = record.data.lock().expect("query record mutex poisoned");
        if data.error.is_none() {
            data.error = Some(ApiErrorBody {
                code: "query_failed".into(),
                message: error.to_string(),
            });
        }
        drop(data);
        push_event(&record, "state", json!({ "state": "failed" }), true);
    } else if overflow {
        push_event(&record, "state", json!({ "state": "failed" }), true);
    } else {
        record_event(
            &record,
            terminal_event.unwrap_or(QueryEvent::State(QueryState::Completed)),
        );
    }
    if close_session {
        sessions.close(&record.session_id).ok();
    }
}

fn record_event(record: &QueryRecord, event: QueryEvent) {
    match event {
        QueryEvent::State(state) => {
            let state = state_name(state);
            let terminal = matches!(state, "completed" | "cancelled" | "failed");
            push_event(record, "state", json!({ "state": state }), terminal);
        }
        QueryEvent::EngineQueryId(id) => {
            record
                .data
                .lock()
                .expect("query record mutex poisoned")
                .engine_query_id = Some(id.clone());
            push_event(
                record,
                "engine_query_id",
                json!({ "engine_query_id": id }),
                false,
            );
        }
        QueryEvent::RowsProduced(rows) => {
            push_event(record, "rows", json!({ "rows": rows }), false);
        }
        QueryEvent::Progress(progress) => {
            push_event(record, "progress", progress_json(&progress), false);
        }
        QueryEvent::SessionProperties(properties) => {
            let properties = properties
                .into_iter()
                .map(|(name, value)| {
                    let value = if is_sensitive_name(&name) {
                        "<redacted>".into()
                    } else {
                        value
                    };
                    (name, value)
                })
                .collect::<BTreeMap<_, _>>();
            push_event(
                record,
                "session_properties",
                json!({ "properties": properties }),
                false,
            );
        }
    }
}

fn is_sensitive_name(name: &str) -> bool {
    let name = name.to_ascii_lowercase();
    ["password", "token", "secret", "credential", "private_key"]
        .iter()
        .any(|part| name.contains(part))
}

fn push_event(record: &QueryRecord, event: &str, value: Value, terminal: bool) {
    let entry = {
        let mut data = record.data.lock().expect("query record mutex poisoned");
        if event == "state" {
            if let Some(state) = value.get("state").and_then(Value::as_str) {
                data.state = state.into();
                if terminal {
                    data.completed_at = Some(Instant::now());
                }
            }
        }
        let entry = EventEntry {
            id: data.next_event_id,
            event: event.into(),
            data: value,
            terminal,
        };
        data.next_event_id += 1;
        data.events.push(entry.clone());
        entry
    };
    record.events.send(entry).ok();
}

async fn get_query(
    State(state): State<AppState>,
    Path(query_id): Path<String>,
) -> Result<Json<QueryResponse>, ApiError> {
    let record = record(&state, &query_id)?;
    Ok(Json(query_response(record.as_ref())))
}

async fn cancel_query(
    State(state): State<AppState>,
    Path(query_id): Path<String>,
) -> Result<(StatusCode, Json<QueryResponse>), ApiError> {
    let record = record(&state, &query_id)?;
    record.cancel.cancel();
    push_event(&record, "state", json!({ "state": "cancelling" }), false);
    Ok((StatusCode::ACCEPTED, Json(query_response(&record))))
}

#[derive(Debug, Deserialize)]
struct ResultsQuery {
    page_token: Option<String>,
    limit: Option<usize>,
}

async fn get_results(
    State(state): State<AppState>,
    Path(query_id): Path<String>,
    Query(query): Query<ResultsQuery>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let record = record(&state, &query_id)?;
    let (state_name, error, rows, source) = {
        let data = record.data.lock().expect("query record mutex poisoned");
        let source = match &data.storage {
            ResultStorage::Memory(batches) => ResultSource::Memory(batches.clone()),
            ResultStorage::Spill { path, writer } => {
                if writer.is_some() {
                    return Err(ApiError::new(
                        StatusCode::CONFLICT,
                        "query_running",
                        "results are available after query completion",
                    ));
                }
                ResultSource::Spill(path.clone())
            }
        };
        (data.state.clone(), data.error.clone(), data.rows, source)
    };
    if let Some(error) = error {
        return Err(ApiError {
            status: StatusCode::UNPROCESSABLE_ENTITY,
            body: error,
        });
    }
    if !matches!(state_name.as_str(), "completed" | "cancelled" | "failed") {
        return Err(ApiError::new(
            StatusCode::CONFLICT,
            "query_running",
            "results are available after query completion",
        ));
    }
    let offset = query
        .page_token
        .as_deref()
        .map(|token| decode_page_token(token, state.page_secret))
        .transpose()?
        .unwrap_or(0);
    let limit = query
        .limit
        .unwrap_or(state.limits.default_page_rows)
        .clamp(1, state.limits.max_page_rows);
    let end = offset.saturating_add(limit).min(rows);
    let batches = load_page(source, offset, end)?;
    let next = (end < rows).then(|| encode_page_token(end, state.page_secret));
    let accept = headers
        .get(header::ACCEPT)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("application/json");
    let (content_type, bytes) = if accept.contains("application/vnd.apache.arrow.stream") {
        (
            "application/vnd.apache.arrow.stream",
            render_arrow(&batches)?,
        )
    } else if accept.contains("text/csv") {
        ("text/csv", render_output(&batches, OutputFormat::Csv)?)
    } else if accept.contains("application/x-ndjson") {
        (
            "application/x-ndjson",
            render_output(&batches, OutputFormat::JsonLines)?,
        )
    } else {
        (
            "application/json",
            render_output(&batches, OutputFormat::Json)?,
        )
    };
    let mut response = Response::new(Body::from(bytes));
    response
        .headers_mut()
        .insert(header::CONTENT_TYPE, HeaderValue::from_static(content_type));
    if let Some(next) = next {
        response.headers_mut().insert(
            "x-qcli-next-page-token",
            HeaderValue::from_str(&next).map_err(|_| {
                ApiError::new(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "pagination",
                    "could not encode page token",
                )
            })?,
        );
    }
    Ok(response)
}

async fn get_events(
    State(state): State<AppState>,
    Path(query_id): Path<String>,
    headers: HeaderMap,
) -> Result<Sse<impl futures_util::Stream<Item = Result<Event, Infallible>>>, ApiError> {
    let record = record(&state, &query_id)?;
    let last = headers
        .get("last-event-id")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(0);
    let mut receiver = record.events.subscribe();
    let history = record
        .data
        .lock()
        .expect("query record mutex poisoned")
        .events
        .iter()
        .filter(|entry| entry.id > last)
        .cloned()
        .collect::<Vec<_>>();
    let (sender, stream) = tokio::sync::mpsc::channel(32);
    tokio::spawn(async move {
        let mut seen = last;
        for entry in history {
            seen = seen.max(entry.id);
            let terminal = entry.terminal;
            if sender.send(entry).await.is_err() || terminal {
                return;
            }
        }
        loop {
            match receiver.recv().await {
                Ok(entry) if entry.id > seen => {
                    seen = entry.id;
                    let terminal = entry.terminal;
                    if sender.send(entry).await.is_err() || terminal {
                        return;
                    }
                }
                Ok(_) | Err(broadcast::error::RecvError::Lagged(_)) => {}
                Err(broadcast::error::RecvError::Closed) => return,
            }
        }
    });
    let stream = ReceiverStream::new(stream).map(|entry| {
        Ok(Event::default()
            .id(entry.id.to_string())
            .event(entry.event)
            .json_data(entry.data)
            .expect("JSON value is serializable"))
    });
    Ok(Sse::new(stream).keep_alive(KeepAlive::default()))
}

fn query_response(record: &QueryRecord) -> QueryResponse {
    debug_assert_eq!(record.owner, LOCAL_OWNER);
    let data = record.data.lock().expect("query record mutex poisoned");
    QueryResponse {
        id: record.id.clone(),
        session_id: record.session_id.clone(),
        session_version: record.session_version,
        target: record.target.clone(),
        engine: record.engine.clone(),
        engine_query_id: data.engine_query_id.clone(),
        state: data.state.clone(),
        rows: data.rows,
        error: data.error.clone(),
    }
}

fn record(state: &AppState, id: &str) -> Result<Arc<QueryRecord>, ApiError> {
    cleanup_expired(state);
    state
        .records
        .lock()
        .expect("query registry mutex poisoned")
        .get(id)
        .cloned()
        .ok_or_else(|| ApiError::new(StatusCode::NOT_FOUND, "query_not_found", "query not found"))
}

fn target(state: &AppState, name: &str) -> Result<ResolvedTarget, ApiError> {
    state.config.target(name).cloned().ok_or_else(|| {
        ApiError::new(
            StatusCode::NOT_FOUND,
            "target_not_found",
            format!("target '{name}' does not exist"),
        )
    })
}

fn values(values: BTreeMap<String, Value>) -> Result<BTreeMap<String, String>, ApiError> {
    values
        .into_iter()
        .map(|(name, value)| {
            let value = match value {
                Value::String(value) => value,
                Value::Bool(value) => value.to_string(),
                Value::Number(value) => value.to_string(),
                Value::Null | Value::Array(_) | Value::Object(_) => {
                    return Err(ApiError::new(
                        StatusCode::BAD_REQUEST,
                        "invalid_property",
                        format!("property '{name}' must be a string, number, or boolean"),
                    ));
                }
            };
            Ok((name, value))
        })
        .collect()
}

fn core_error(error: &CoreError) -> ApiError {
    let message = error.to_string();
    let status = match error {
        CoreError::SessionNotFound(_) => StatusCode::NOT_FOUND,
        CoreError::VersionConflict { .. } => StatusCode::CONFLICT,
        CoreError::AdapterNotFound(_) => StatusCode::BAD_REQUEST,
        CoreError::Driver(_) | CoreError::Task(_) => StatusCode::BAD_GATEWAY,
    };
    ApiError::new(status, "core", message)
}

fn state_name(state: QueryState) -> &'static str {
    match state {
        QueryState::Submitted => "submitted",
        QueryState::Running => "running",
        QueryState::ProducingResults => "producing_results",
        QueryState::Completed => "completed",
        QueryState::Cancelling => "cancelling",
        QueryState::Cancelled => "cancelled",
        QueryState::Failed => "failed",
    }
}

fn progress_json(progress: &QueryProgress) -> Value {
    json!({
        "state": progress.state,
        "scheduled": progress.scheduled,
        "completed_splits": progress.completed_splits,
        "total_splits": progress.total_splits,
        "processed_rows": progress.processed_rows,
        "processed_bytes": progress.processed_bytes,
        "elapsed_millis": progress.elapsed_millis,
    })
}

fn store_batch(
    query_id: &str,
    data: &mut QueryData,
    batch: RecordBatch,
    limits: &HttpLimits,
) -> Result<(), ApiErrorBody> {
    let bytes = batch
        .columns()
        .iter()
        .map(arrow_array::Array::get_array_memory_size)
        .sum::<usize>();
    let rows = batch.num_rows();
    if data.retained_bytes.saturating_add(bytes) > limits.max_result_bytes_per_query {
        return Err(ApiErrorBody {
            code: "result_limit".into(),
            message: format!(
                "query result exceeds the local {}-byte retention limit",
                limits.max_result_bytes_per_query
            ),
        });
    }
    if matches!(data.storage, ResultStorage::Memory(_))
        && data.retained_bytes.saturating_add(bytes) > limits.memory_result_bytes_per_query
    {
        let path = std::env::temp_dir().join(format!("{query_id}-{}.arrow", Uuid::new_v4()));
        let file = std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&path)
            .map_err(storage_error)?;
        let mut writer =
            FileWriter::try_new(file, batch.schema().as_ref()).map_err(storage_error)?;
        if let ResultStorage::Memory(existing) =
            std::mem::replace(&mut data.storage, ResultStorage::Memory(Vec::new()))
        {
            for existing_batch in &existing {
                writer.write(existing_batch).map_err(storage_error)?;
            }
        }
        data.storage = ResultStorage::Spill {
            path,
            writer: Some(Box::new(writer)),
        };
    }
    match &mut data.storage {
        ResultStorage::Memory(batches) => batches.push(batch),
        ResultStorage::Spill {
            writer: Some(writer),
            ..
        } => writer.write(&batch).map_err(storage_error)?,
        ResultStorage::Spill { writer: None, .. } => {
            return Err(ApiErrorBody {
                code: "result_storage".into(),
                message: "result spill was already finalized".into(),
            });
        }
    }
    data.rows += rows;
    data.retained_bytes += bytes;
    Ok(())
}

fn finish_results(record: &QueryRecord) -> Result<(), ApiErrorBody> {
    let mut data = record.data.lock().expect("query record mutex poisoned");
    if let ResultStorage::Spill { writer, .. } = &mut data.storage
        && let Some(mut writer) = writer.take()
    {
        writer.finish().map_err(storage_error)?;
    }
    Ok(())
}

fn storage_error(error: impl std::fmt::Display) -> ApiErrorBody {
    ApiErrorBody {
        code: "result_storage".into(),
        message: error.to_string(),
    }
}

enum ResultSource {
    Memory(Vec<RecordBatch>),
    Spill(PathBuf),
}

fn load_page(source: ResultSource, start: usize, end: usize) -> Result<Vec<RecordBatch>, ApiError> {
    match source {
        ResultSource::Memory(batches) => Ok(slice_batches(&batches, start, end)),
        ResultSource::Spill(path) => {
            let file = std::fs::File::open(path).map_err(arrow_error)?;
            let reader = FileReader::try_new(file, None).map_err(arrow_error)?;
            let mut cursor = 0;
            let mut output = Vec::new();
            for batch in reader {
                let batch = batch.map_err(arrow_error)?;
                let batch_end = cursor + batch.num_rows();
                let overlap_start = start.max(cursor);
                let overlap_end = end.min(batch_end);
                if overlap_start < overlap_end {
                    output.push(batch.slice(overlap_start - cursor, overlap_end - overlap_start));
                }
                cursor = batch_end;
                if cursor >= end {
                    break;
                }
            }
            Ok(output)
        }
    }
}

fn cleanup_expired(state: &AppState) {
    let now = Instant::now();
    state
        .records
        .lock()
        .expect("query registry mutex poisoned")
        .retain(|_, record| {
            record
                .data
                .lock()
                .expect("query record mutex poisoned")
                .completed_at
                .is_none_or(|completed| now.duration_since(completed) < state.limits.result_ttl)
        });
}

fn slice_batches(batches: &[RecordBatch], start: usize, end: usize) -> Vec<RecordBatch> {
    let mut cursor = 0;
    let mut output = Vec::new();
    for batch in batches {
        let batch_end = cursor + batch.num_rows();
        let overlap_start = start.max(cursor);
        let overlap_end = end.min(batch_end);
        if overlap_start < overlap_end {
            output.push(batch.slice(overlap_start - cursor, overlap_end - overlap_start));
        }
        cursor = batch_end;
        if cursor >= end {
            break;
        }
    }
    output
}

fn render_output(batches: &[RecordBatch], format: OutputFormat) -> Result<Vec<u8>, ApiError> {
    let mut bytes = Vec::new();
    let mut output = StreamOutput::new(
        &mut bytes,
        format,
        DisplayOptions {
            decimal_places: 3,
            string_truncate: usize::MAX,
        },
    )
    .map_err(output_error)?;
    for batch in batches {
        output.write_batch(batch).map_err(output_error)?;
    }
    output.finish().map_err(output_error)?;
    Ok(bytes)
}

fn render_arrow(batches: &[RecordBatch]) -> Result<Vec<u8>, ApiError> {
    let Some(first) = batches.first() else {
        return Ok(Vec::new());
    };
    let cursor = Cursor::new(Vec::new());
    let mut writer = StreamWriter::try_new(cursor, &first.schema()).map_err(arrow_error)?;
    for batch in batches {
        writer.write(batch).map_err(arrow_error)?;
    }
    writer.finish().map_err(arrow_error)?;
    writer
        .into_inner()
        .map(Cursor::into_inner)
        .map_err(arrow_error)
}

fn output_error(error: impl std::fmt::Display) -> ApiError {
    ApiError::new(
        StatusCode::INTERNAL_SERVER_ERROR,
        "result_encoding",
        error.to_string(),
    )
}

fn arrow_error(error: impl std::fmt::Display) -> ApiError {
    output_error(error)
}

fn encode_page_token(offset: usize, secret: u64) -> String {
    let offset = u64::try_from(offset).expect("row offset fits u64");
    base64::engine::general_purpose::URL_SAFE_NO_PAD
        .encode(format!("{offset:x}:{:x}", offset.rotate_left(17) ^ secret))
}

fn decode_page_token(token: &str, secret: u64) -> Result<usize, ApiError> {
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(token)
        .map_err(|_| invalid_page_token())?;
    let value = String::from_utf8(bytes).map_err(|_| invalid_page_token())?;
    let (offset, signature) = value.split_once(':').ok_or_else(invalid_page_token)?;
    let offset = u64::from_str_radix(offset, 16).map_err(|_| invalid_page_token())?;
    let signature = u64::from_str_radix(signature, 16).map_err(|_| invalid_page_token())?;
    if signature != (offset.rotate_left(17) ^ secret) {
        return Err(invalid_page_token());
    }
    usize::try_from(offset).map_err(|_| invalid_page_token())
}

fn invalid_page_token() -> ApiError {
    ApiError::new(
        StatusCode::BAD_REQUEST,
        "invalid_page_token",
        "page token is invalid",
    )
}

/// Bind the preview service to a loopback address only.
///
/// # Errors
///
/// Returns an error when the address is not loopback or cannot be bound.
pub async fn bind_local(address: SocketAddr) -> std::io::Result<TcpListener> {
    if !address.ip().is_loopback() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "M10 preview refuses non-loopback binding",
        ));
    }
    TcpListener::bind(address).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::to_bytes;
    use axum::http::Request;
    use qcli_driver_demo::DemoAdapter;
    use std::sync::atomic::{AtomicU64, Ordering};
    use tower::ServiceExt;

    static NEXT_CONFIG: AtomicU64 = AtomicU64::new(1);

    fn service(limits: HttpLimits) -> HttpService {
        let id = NEXT_CONFIG.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!("qcli-http-{}-{id}.env", std::process::id()));
        std::fs::write(&path, "[demo]\nengine=demo\n").unwrap();
        let config = Config::load(&path).unwrap();
        std::fs::remove_file(path).ok();
        HttpService::new(
            config,
            [Arc::new(DemoAdapter) as Arc<dyn EngineAdapter>],
            limits,
        )
    }

    fn json_request(method: &str, uri: &str, value: &Value) -> Request<Body> {
        Request::builder()
            .method(method)
            .uri(uri)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(serde_json::to_vec(&value).unwrap()))
            .unwrap()
    }

    async fn json_body(response: Response) -> Value {
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    async fn create_demo_session(router: &Router) -> Value {
        let response = router
            .clone()
            .oneshot(json_request(
                "POST",
                "/v1/sessions",
                &json!({
                    "target": "demo",
                    "options": { "decimal_places": 8 }
                }),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);
        json_body(response).await
    }

    async fn wait_for_terminal(router: &Router, query_id: &str) -> Value {
        for _ in 0..100 {
            let response = router
                .clone()
                .oneshot(
                    Request::builder()
                        .uri(format!("/v1/queries/{query_id}"))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            let body = json_body(response).await;
            if matches!(
                body["state"].as_str(),
                Some("completed" | "failed" | "cancelled")
            ) {
                return body;
            }
            tokio::task::yield_now().await;
        }
        panic!("query did not reach a terminal state");
    }

    #[tokio::test]
    async fn session_query_results_pagination_and_sse_share_core() {
        let router = service(HttpLimits::default()).router();
        let session = create_demo_session(&router).await;
        assert_eq!(session["version"], 1);
        let session_id = session["id"].as_str().unwrap();
        let response = router
            .clone()
            .oneshot(json_request(
                "POST",
                &format!("/v1/sessions/{session_id}/queries"),
                &json!({ "sql": "select * from sample" }),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        let query = json_body(response).await;
        let query_id = query["id"].as_str().unwrap();
        let terminal = wait_for_terminal(&router, query_id).await;
        assert_eq!(terminal["state"], "completed");
        assert_eq!(terminal["rows"], 2);

        let first = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/queries/{query_id}/results?limit=1"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let token = first
            .headers()
            .get("x-qcli-next-page-token")
            .unwrap()
            .to_str()
            .unwrap()
            .to_owned();
        let rows = json_body(first).await;
        assert_eq!(rows.as_array().unwrap().len(), 1);

        let invalid = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/v1/queries/{query_id}/results?page_token=not-a-token"
                    ))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(invalid.status(), StatusCode::BAD_REQUEST);

        let second = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/v1/queries/{query_id}/results?limit=1&page_token={token}"
                    ))
                    .header(header::ACCEPT, "text/csv")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(second.headers()[header::CONTENT_TYPE], "text/csv");
        let csv = to_bytes(second.into_body(), usize::MAX).await.unwrap();
        assert!(String::from_utf8_lossy(&csv).contains("name"));

        let events = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/queries/{query_id}/events"))
                    .header(header::ACCEPT, "text/event-stream")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let events = to_bytes(events.into_body(), usize::MAX).await.unwrap();
        let events = String::from_utf8_lossy(&events);
        assert!(events.contains("event: state"));
        assert!(events.contains("\"completed\""));
    }

    #[tokio::test]
    async fn stale_session_mutation_conflicts_and_close_removes_session() {
        let router = service(HttpLimits::default()).router();
        let session = create_demo_session(&router).await;
        let session_id = session["id"].as_str().unwrap();
        let update = json!({
            "expected_version": 1,
            "options": { "string_truncate": 20 }
        });
        let first = router
            .clone()
            .oneshot(json_request(
                "PATCH",
                &format!("/v1/sessions/{session_id}"),
                &update,
            ))
            .await
            .unwrap();
        assert_eq!(first.status(), StatusCode::OK);
        let second = router
            .clone()
            .oneshot(json_request(
                "PATCH",
                &format!("/v1/sessions/{session_id}"),
                &update,
            ))
            .await
            .unwrap();
        assert_eq!(second.status(), StatusCode::CONFLICT);

        let deleted = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri(format!("/v1/sessions/{session_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(deleted.status(), StatusCode::NO_CONTENT);
    }

    #[tokio::test]
    async fn http_and_direct_core_machine_results_match() {
        let service = service(HttpLimits::default());
        let router = service.router();
        let response = router
            .clone()
            .oneshot(json_request(
                "POST",
                "/v1/queries",
                &json!({ "target": "demo", "sql": "select * from sample" }),
            ))
            .await
            .unwrap();
        let query = json_body(response).await;
        let query_id = query["id"].as_str().unwrap();
        wait_for_terminal(&router, query_id).await;
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/queries/{query_id}/results"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let http = to_bytes(response.into_body(), usize::MAX).await.unwrap();

        let sessions = SessionManager::default();
        let snapshot = sessions.create(service.state.config.target("demo").unwrap().clone());
        let queries = QueryService::new([Arc::new(DemoAdapter) as Arc<dyn EngineAdapter>], 8);
        let mut handle = queries
            .submit(snapshot, "select * from sample".into())
            .unwrap();
        let mut batches = Vec::new();
        while let Some(batch) = handle.next_batch().await {
            batches.push(batch);
        }
        while handle.next_event().await.is_some() {}
        handle.finish().await.unwrap();
        let direct = render_output(&batches, OutputFormat::Json).unwrap();
        assert_eq!(http.as_ref(), direct);
    }

    #[tokio::test]
    async fn engine_session_updates_are_applied_atomically() {
        let router = service(HttpLimits::default()).router();
        let session = create_demo_session(&router).await;
        let session_id = session["id"].as_str().unwrap();
        let response = router
            .clone()
            .oneshot(json_request(
                "POST",
                &format!("/v1/sessions/{session_id}/queries"),
                &json!({ "sql": "set-session catalog=analytics" }),
            ))
            .await
            .unwrap();
        let query = json_body(response).await;
        wait_for_terminal(&router, query["id"].as_str().unwrap()).await;
        let session = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/sessions/{session_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let session = json_body(session).await;
        assert_eq!(session["version"], 2);
    }

    #[tokio::test]
    async fn result_limit_fails_without_unbounded_retention() {
        let limits = HttpLimits {
            max_result_bytes_per_query: 1,
            ..HttpLimits::default()
        };
        let router = service(limits).router();
        let session = create_demo_session(&router).await;
        let session_id = session["id"].as_str().unwrap();
        let response = router
            .clone()
            .oneshot(json_request(
                "POST",
                &format!("/v1/sessions/{session_id}/queries"),
                &json!({ "sql": "select * from sample" }),
            ))
            .await
            .unwrap();
        let query = json_body(response).await;
        let terminal = wait_for_terminal(&router, query["id"].as_str().unwrap()).await;
        assert_eq!(terminal["state"], "failed");
        assert_eq!(terminal["error"]["code"], "result_limit");
    }

    #[tokio::test]
    async fn larger_results_spill_to_arrow_and_remain_pageable() {
        let limits = HttpLimits {
            memory_result_bytes_per_query: 1,
            ..HttpLimits::default()
        };
        let service = service(limits);
        let router = service.router();
        let session = create_demo_session(&router).await;
        let session_id = session["id"].as_str().unwrap();
        let response = router
            .clone()
            .oneshot(json_request(
                "POST",
                &format!("/v1/sessions/{session_id}/queries"),
                &json!({ "sql": "select * from sample" }),
            ))
            .await
            .unwrap();
        let query = json_body(response).await;
        let query_id = query["id"].as_str().unwrap();
        let terminal = wait_for_terminal(&router, query_id).await;
        assert_eq!(terminal["state"], "completed");

        let record = service
            .state
            .records
            .lock()
            .unwrap()
            .get(query_id)
            .unwrap()
            .clone();
        let spill_path = {
            let data = record.data.lock().unwrap();
            let ResultStorage::Spill { path, writer } = &data.storage else {
                panic!("result did not spill");
            };
            assert!(writer.is_none());
            path.clone()
        };
        assert!(spill_path.exists());

        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/queries/{query_id}/results"))
                    .header(header::ACCEPT, "application/vnd.apache.arrow.stream")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert!(
            !to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap()
                .is_empty()
        );

        service.state.records.lock().unwrap().remove(query_id);
        drop(record);
        assert!(!spill_path.exists());
    }

    #[tokio::test]
    async fn cancellation_is_exposed_through_http() {
        let router = service(HttpLimits::default()).router();
        let session = create_demo_session(&router).await;
        let session_id = session["id"].as_str().unwrap();
        let response = router
            .clone()
            .oneshot(json_request(
                "POST",
                &format!("/v1/sessions/{session_id}/queries"),
                &json!({ "sql": "wait-for-cancel" }),
            ))
            .await
            .unwrap();
        let query = json_body(response).await;
        let query_id = query["id"].as_str().unwrap();
        let cancelled = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/queries/{query_id}/cancel"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(cancelled.status(), StatusCode::ACCEPTED);
        let terminal = wait_for_terminal(&router, query_id).await;
        assert_eq!(terminal["state"], "cancelled");
    }

    #[tokio::test]
    async fn expired_results_are_removed_on_access() {
        let limits = HttpLimits {
            result_ttl: Duration::from_secs(60),
            ..HttpLimits::default()
        };
        let service = service(limits);
        let router = service.router();
        let response = router
            .clone()
            .oneshot(json_request(
                "POST",
                "/v1/queries",
                &json!({ "target": "demo", "sql": "select * from sample" }),
            ))
            .await
            .unwrap();
        let query = json_body(response).await;
        let query_id = query["id"].as_str().unwrap();
        wait_for_terminal(&router, query_id).await;
        let record = service
            .state
            .records
            .lock()
            .unwrap()
            .get(query_id)
            .unwrap()
            .clone();
        record.data.lock().unwrap().completed_at =
            Some(Instant::now().checked_sub(Duration::from_secs(61)).unwrap());
        drop(record);

        let expired = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/queries/{query_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(expired.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn preview_refuses_non_loopback_binding() {
        let error = bind_local("0.0.0.0:0".parse().unwrap()).await.unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::PermissionDenied);
    }
}
