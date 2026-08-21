//! Versioned localhost HTTP transport over qcli's shared session/query core.

use arrow_array::RecordBatch;
use arrow_ipc::writer::StreamWriter;
use axum::body::Body;
use axum::extract::{Extension, Path, Query, State};
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, patch, post};
use axum::{Json, Router};
use base64::Engine;
use futures_util::StreamExt;
use qcli_auth::{AuthenticatedPrincipal, AuthenticationErrorKind, Authenticator};
use qcli_config::Config;
use qcli_core::SessionSnapshot;
use qcli_driver_api::EngineAdapter;
use qcli_output::{DisplayOptions, OutputFormat, StreamOutput};
pub use qcli_service::{AuditEvent, AuditSink};
use qcli_service::{GatewayService, QueryStatus, ServiceError, ServiceErrorKind, ServiceLimits};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::convert::Infallible;
use std::future::Future;
use std::io::Cursor;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tokio_stream::wrappers::ReceiverStream;
use utoipa::openapi::security::{Http, HttpAuthScheme, SecurityScheme};
use utoipa::{IntoParams, Modify, OpenApi, ToSchema};
use utoipa_swagger_ui::SwaggerUi;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct HttpLimits {
    pub max_queries: usize,
    pub memory_result_bytes_per_query: usize,
    pub max_result_bytes_per_query: usize,
    pub result_ttl: Duration,
    pub default_page_rows: usize,
    pub max_page_rows: usize,
    pub max_sql_bytes: usize,
    pub session_ttl: Duration,
    pub cleanup_interval: Duration,
    pub shutdown_grace: Duration,
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
            session_ttl: Duration::from_secs(30 * 60),
            cleanup_interval: Duration::from_secs(30),
            shutdown_grace: Duration::from_secs(10),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct HttpOperations {
    pub trusted_proxy: bool,
    pub allowed_origins: Vec<String>,
}

#[derive(Debug)]
struct StderrAuditSink;

impl AuditSink for StderrAuditSink {
    fn record(&self, event: &AuditEvent) {
        if let Ok(value) = serde_json::to_string(event) {
            eprintln!("qcli_audit {value}");
        }
    }
}

#[must_use]
pub fn stderr_audit_sink() -> Arc<dyn AuditSink> {
    Arc::new(StderrAuditSink)
}

#[derive(Clone)]
struct AppState {
    service: GatewayService,
    limits: HttpLimits,
    page_secret: u64,
    authenticator: Option<Arc<dyn Authenticator>>,
    operations: HttpOperations,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
struct ApiErrorBody {
    code: String,
    message: String,
}

#[derive(Debug, Serialize, ToSchema)]
struct ApiErrorResponse {
    error: ApiErrorBody,
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
        let unauthorized = self.status == StatusCode::UNAUTHORIZED;
        let mut response = (self.status, Json(json!({ "error": self.body }))).into_response();
        if unauthorized {
            response
                .headers_mut()
                .insert(header::WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"));
        }
        response
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
        let service_limits = ServiceLimits {
            max_queries: limits.max_queries,
            memory_result_bytes_per_query: limits.memory_result_bytes_per_query,
            max_result_bytes_per_query: limits.max_result_bytes_per_query,
            result_ttl: limits.result_ttl,
            max_sql_bytes: limits.max_sql_bytes,
            session_ttl: limits.session_ttl,
            shutdown_grace: limits.shutdown_grace,
            ..ServiceLimits::default()
        };
        Self::from_gateway(
            GatewayService::new(config, adapters, service_limits),
            limits,
        )
    }

    #[must_use]
    pub fn from_gateway(service: GatewayService, limits: HttpLimits) -> Self {
        let uuid = Uuid::new_v4();
        let bytes = uuid.as_bytes();
        let page_secret = u64::from_le_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]);
        Self {
            state: AppState {
                service,
                limits,
                page_secret,
                authenticator: None,
                operations: HttpOperations::default(),
            },
        }
    }

    #[must_use]
    pub fn with_authenticator(mut self, authenticator: Arc<dyn Authenticator>) -> Self {
        self.state.authenticator = Some(authenticator);
        self
    }

    #[must_use]
    pub fn with_operations(mut self, operations: HttpOperations) -> Self {
        self.state.operations = operations;
        self
    }

    #[must_use]
    pub fn with_audit_sink(self, audit: Arc<dyn AuditSink>) -> Self {
        self.state.service.set_audit_sink(audit);
        self
    }

    #[must_use]
    pub fn gateway(&self) -> GatewayService {
        self.state.service.clone()
    }

    pub fn router(&self) -> Router {
        let openapi = ApiDoc::openapi();
        let api = Router::new()
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
            .route_layer(middleware::from_fn_with_state(
                self.state.clone(),
                authenticate_request,
            ))
            .route_layer(middleware::from_fn_with_state(
                self.state.clone(),
                enforce_http_operations,
            ));
        api.merge(SwaggerUi::new("/docs").url("/openapi.json", openapi))
            .with_state(self.state.clone())
    }

    /// Serve until the listener fails or the task is cancelled.
    ///
    /// # Errors
    ///
    /// Returns an I/O error from accepting or serving a connection.
    pub async fn serve(self, listener: TcpListener) -> std::io::Result<()> {
        self.serve_with_shutdown(listener, std::future::pending::<()>())
            .await
    }

    /// Serve with periodic cleanup and cancellation of active work on shutdown.
    ///
    /// # Errors
    ///
    /// Returns an I/O error from accepting or serving a connection.
    pub async fn serve_with_shutdown(
        self,
        listener: TcpListener,
        shutdown: impl Future<Output = ()> + Send + 'static,
    ) -> std::io::Result<()> {
        let state = self.state.clone();
        let cleanup_state = state.clone();
        let cleanup = tokio::spawn(async move {
            let mut interval = tokio::time::interval(cleanup_state.limits.cleanup_interval);
            loop {
                interval.tick().await;
                if cleanup_state.service.is_shutting_down() {
                    return;
                }
                cleanup_state.service.cleanup_expired();
            }
        });
        let shutdown_state = state.clone();
        let signal = async move {
            shutdown.await;
            shutdown_state.service.begin_shutdown();
        };
        let result = axum::serve(listener, self.router())
            .with_graceful_shutdown(signal)
            .await;
        cleanup.abort();
        state.service.wait_for_queries().await;
        result
    }
}

async fn enforce_http_operations(
    State(state): State<AppState>,
    request: axum::extract::Request,
    next: Next,
) -> Result<Response, ApiError> {
    if state.service.is_shutting_down() {
        return Err(ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "shutting_down",
            "server is shutting down",
        ));
    }
    let headers = request.headers();
    let forwarded = headers.contains_key("forwarded")
        || headers.contains_key("x-forwarded-for")
        || headers.contains_key("x-forwarded-host")
        || headers.contains_key("x-forwarded-proto");
    if forwarded && !state.operations.trusted_proxy {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "untrusted_forwarded_headers",
            "forwarded headers require trusted-proxy mode",
        ));
    }
    if state.operations.trusted_proxy
        && headers
            .get("x-forwarded-proto")
            .and_then(|value| value.to_str().ok())
            != Some("https")
    {
        return Err(ApiError::new(
            StatusCode::UPGRADE_REQUIRED,
            "https_required",
            "trusted proxy must report x-forwarded-proto: https",
        ));
    }
    let origin = headers
        .get(header::ORIGIN)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    if let Some(origin) = &origin
        && !state
            .operations
            .allowed_origins
            .iter()
            .any(|item| item == origin)
    {
        return Err(ApiError::new(
            StatusCode::FORBIDDEN,
            "origin_forbidden",
            "request origin is not allowed",
        ));
    }
    if request.method() == Method::OPTIONS
        && request
            .headers()
            .contains_key(header::ACCESS_CONTROL_REQUEST_METHOD)
    {
        let mut response = StatusCode::NO_CONTENT.into_response();
        if let Some(origin) = origin {
            add_cors_headers(&mut response, &origin)?;
            response.headers_mut().insert(
                header::ACCESS_CONTROL_ALLOW_METHODS,
                HeaderValue::from_static("GET, POST, PATCH, DELETE, OPTIONS"),
            );
            response.headers_mut().insert(
                header::ACCESS_CONTROL_ALLOW_HEADERS,
                HeaderValue::from_static("Authorization, Content-Type, Last-Event-ID"),
            );
        }
        return Ok(response);
    }
    let mut response = next.run(request).await;
    if let Some(origin) = origin {
        add_cors_headers(&mut response, &origin)?;
    }
    Ok(response)
}

fn add_cors_headers(response: &mut Response, origin: &str) -> Result<(), ApiError> {
    response.headers_mut().insert(
        header::ACCESS_CONTROL_ALLOW_ORIGIN,
        HeaderValue::from_str(origin).map_err(|_| {
            ApiError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                "cors",
                "configured origin is not a valid header value",
            )
        })?,
    );
    response
        .headers_mut()
        .insert(header::VARY, HeaderValue::from_static("Origin"));
    Ok(())
}

async fn authenticate_request(
    State(state): State<AppState>,
    mut request: axum::extract::Request,
    next: Next,
) -> Result<Response, ApiError> {
    let principal = if let Some(authenticator) = &state.authenticator {
        let bearer = request
            .headers()
            .get(header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("Bearer "));
        let Some(bearer) = bearer else {
            audit(
                &state,
                "request.authenticate",
                "denied",
                None,
                None,
                None,
                None,
            );
            return Err(ApiError::new(
                StatusCode::UNAUTHORIZED,
                "authentication_required",
                "a bearer credential is required",
            ));
        };
        match authenticator.authenticate(bearer).await {
            Ok(principal) => principal,
            Err(error) => {
                audit(
                    &state,
                    "request.authenticate",
                    "denied",
                    None,
                    None,
                    None,
                    None,
                );
                let status = if error.kind == AuthenticationErrorKind::Configuration {
                    StatusCode::INTERNAL_SERVER_ERROR
                } else {
                    StatusCode::UNAUTHORIZED
                };
                return Err(ApiError::new(
                    status,
                    "authentication_failed",
                    error.message,
                ));
            }
        }
    } else {
        AuthenticatedPrincipal {
            id: "local".into(),
            allowed_targets: ["*".into()].into_iter().collect(),
            max_sessions: usize::MAX,
            max_concurrent_queries: usize::MAX,
        }
    };
    audit(
        &state,
        "request.authenticate",
        "allowed",
        Some(&principal),
        None,
        None,
        None,
    );
    request.extensions_mut().insert(principal);
    Ok(next.run(request).await)
}

#[derive(Debug, Deserialize, ToSchema)]
struct CreateSessionRequest {
    target: String,
    #[serde(default)]
    context: BTreeMap<String, Value>,
    #[serde(default)]
    properties: BTreeMap<String, Value>,
    #[serde(default)]
    options: BTreeMap<String, Value>,
}

#[derive(Debug, Deserialize, ToSchema)]
struct UpdateSessionRequest {
    expected_version: u64,
    #[serde(default)]
    context: BTreeMap<String, Value>,
    #[serde(default)]
    properties: BTreeMap<String, Value>,
    #[serde(default)]
    options: BTreeMap<String, Value>,
}

#[derive(Debug, Deserialize, ToSchema)]
struct SwitchTargetRequest {
    expected_version: u64,
    target: String,
}

#[derive(Debug, Deserialize, ToSchema)]
struct QueryRequest {
    sql: String,
}

#[derive(Debug, Deserialize, ToSchema)]
struct StatelessQueryRequest {
    target: String,
    sql: String,
    #[serde(default)]
    context: BTreeMap<String, Value>,
    #[serde(default)]
    properties: BTreeMap<String, Value>,
}

#[derive(Debug, Serialize, ToSchema)]
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

#[derive(Debug, Serialize, ToSchema)]
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

impl From<QueryStatus> for QueryResponse {
    fn from(status: QueryStatus) -> Self {
        Self {
            id: status.id,
            session_id: status.session_id,
            session_version: status.session_version,
            target: status.target,
            engine: status.engine,
            engine_query_id: status.engine_query_id,
            state: status.state,
            rows: status.rows,
            error: status.error.map(|error| ApiErrorBody {
                code: error.code,
                message: error.message,
            }),
        }
    }
}

#[derive(OpenApi)]
#[openapi(
    info(
        title = "qcli Local HTTP API",
        version = "1.0.0",
        description = "Versioned, loopback-only preview API backed by qcli's shared session and query core."
    ),
    paths(
        create_session,
        get_session,
        update_session,
        update_session_properties,
        update_session_options,
        switch_session_target,
        delete_session,
        submit_session_query,
        submit_stateless_query,
        get_query,
        get_results,
        get_events,
        cancel_query
    ),
    components(schemas(
        ApiErrorBody,
        ApiErrorResponse,
        CreateSessionRequest,
        UpdateSessionRequest,
        SwitchTargetRequest,
        QueryRequest,
        StatelessQueryRequest,
        SessionResponse,
        QueryResponse
    )),
    tags(
        (name = "sessions", description = "Persistent versioned sessions"),
        (name = "queries", description = "Asynchronous query execution, events, results, and cancellation")
    ),
    modifiers(&SecurityAddon),
    security(("bearer_auth" = []))
)]
struct ApiDoc;

struct SecurityAddon;

impl Modify for SecurityAddon {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        if let Some(components) = openapi.components.as_mut() {
            components.add_security_scheme(
                "bearer_auth",
                SecurityScheme::Http(Http::new(HttpAuthScheme::Bearer)),
            );
        }
    }
}

#[must_use]
pub fn openapi() -> utoipa::openapi::OpenApi {
    ApiDoc::openapi()
}

#[utoipa::path(
    post,
    path = "/v1/sessions",
    tag = "sessions",
    request_body = CreateSessionRequest,
    responses(
        (status = 201, description = "Session created", body = SessionResponse),
        (status = 400, description = "Invalid request", body = ApiErrorResponse),
        (status = 404, description = "Target not found", body = ApiErrorResponse)
    )
)]
async fn create_session(
    State(state): State<AppState>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<CreateSessionRequest>,
) -> Result<(StatusCode, Json<SessionResponse>), ApiError> {
    let mut overrides = values(request.context)?;
    overrides.extend(values(request.properties)?);
    overrides.extend(values(request.options)?);
    let snapshot = state
        .service
        .create_session_clustered(&principal, &request.target, overrides)
        .await
        .map_err(service_error)?;
    Ok((StatusCode::CREATED, Json(snapshot.into())))
}

#[utoipa::path(
    get,
    path = "/v1/sessions/{session_id}",
    tag = "sessions",
    params(("session_id" = String, Path, description = "Logical session ID")),
    responses(
        (status = 200, description = "Current session", body = SessionResponse),
        (status = 404, description = "Session not found", body = ApiErrorResponse)
    )
)]
async fn get_session(
    State(state): State<AppState>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Path(session_id): Path<String>,
) -> Result<Json<SessionResponse>, ApiError> {
    Ok(Json(
        state
            .service
            .session_clustered(&principal, &session_id)
            .await
            .map_err(service_error)?
            .into(),
    ))
}

#[utoipa::path(
    patch,
    path = "/v1/sessions/{session_id}",
    tag = "sessions",
    params(("session_id" = String, Path, description = "Logical session ID")),
    request_body = UpdateSessionRequest,
    responses(
        (status = 200, description = "Updated session", body = SessionResponse),
        (status = 400, description = "Invalid property", body = ApiErrorResponse),
        (status = 404, description = "Session not found", body = ApiErrorResponse),
        (status = 409, description = "Stale expected version", body = ApiErrorResponse)
    )
)]
async fn update_session(
    State(state): State<AppState>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Path(session_id): Path<String>,
    Json(request): Json<UpdateSessionRequest>,
) -> Result<Json<SessionResponse>, ApiError> {
    let mut overrides = values(request.context)?;
    overrides.extend(values(request.properties)?);
    overrides.extend(values(request.options)?);
    let snapshot = state
        .service
        .update_session_clustered(&principal, &session_id, request.expected_version, overrides)
        .await
        .map_err(service_error)?;
    Ok(Json(snapshot.into()))
}

#[utoipa::path(
    patch,
    path = "/v1/sessions/{session_id}/properties",
    tag = "sessions",
    params(("session_id" = String, Path, description = "Logical session ID")),
    request_body = UpdateSessionRequest,
    responses(
        (status = 200, description = "Updated session", body = SessionResponse),
        (status = 409, description = "Stale expected version", body = ApiErrorResponse)
    )
)]
#[allow(dead_code, reason = "OpenAPI-only operation for a shared HTTP handler")]
fn update_session_properties() {}

#[utoipa::path(
    patch,
    path = "/v1/sessions/{session_id}/options",
    tag = "sessions",
    params(("session_id" = String, Path, description = "Logical session ID")),
    request_body = UpdateSessionRequest,
    responses(
        (status = 200, description = "Updated session", body = SessionResponse),
        (status = 409, description = "Stale expected version", body = ApiErrorResponse)
    )
)]
#[allow(dead_code, reason = "OpenAPI-only operation for a shared HTTP handler")]
fn update_session_options() {}

#[utoipa::path(
    post,
    path = "/v1/sessions/{session_id}/target",
    tag = "sessions",
    params(("session_id" = String, Path, description = "Logical session ID")),
    request_body = SwitchTargetRequest,
    responses(
        (status = 200, description = "Session switched atomically", body = SessionResponse),
        (status = 404, description = "Session or target not found", body = ApiErrorResponse),
        (status = 409, description = "Stale expected version", body = ApiErrorResponse)
    )
)]
async fn switch_session_target(
    State(state): State<AppState>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Path(session_id): Path<String>,
    Json(request): Json<SwitchTargetRequest>,
) -> Result<Json<SessionResponse>, ApiError> {
    let snapshot = state
        .service
        .switch_target_clustered(
            &principal,
            &session_id,
            request.expected_version,
            &request.target,
        )
        .await
        .map_err(service_error)?;
    Ok(Json(snapshot.into()))
}

#[utoipa::path(
    delete,
    path = "/v1/sessions/{session_id}",
    tag = "sessions",
    params(("session_id" = String, Path, description = "Logical session ID")),
    responses(
        (status = 204, description = "Session closed"),
        (status = 404, description = "Session not found", body = ApiErrorResponse)
    )
)]
async fn delete_session(
    State(state): State<AppState>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Path(session_id): Path<String>,
) -> Result<StatusCode, ApiError> {
    state
        .service
        .close_session_clustered(&principal, &session_id)
        .await
        .map_err(service_error)?;
    Ok(StatusCode::NO_CONTENT)
}

#[utoipa::path(
    post,
    path = "/v1/sessions/{session_id}/queries",
    tag = "queries",
    params(("session_id" = String, Path, description = "Logical session ID")),
    request_body = QueryRequest,
    responses(
        (status = 202, description = "Query accepted", body = QueryResponse),
        (status = 404, description = "Session not found", body = ApiErrorResponse),
        (status = 413, description = "SQL exceeds request limit", body = ApiErrorResponse),
        (status = 429, description = "Retained query limit reached", body = ApiErrorResponse)
    )
)]
async fn submit_session_query(
    State(state): State<AppState>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Path(session_id): Path<String>,
    Json(request): Json<QueryRequest>,
) -> Result<(StatusCode, Json<QueryResponse>), ApiError> {
    let query = state
        .service
        .submit_session_query_clustered(&principal, &session_id, request.sql)
        .await
        .map_err(service_error)?;
    Ok((StatusCode::ACCEPTED, Json(query.into())))
}

#[utoipa::path(
    post,
    path = "/v1/queries",
    tag = "queries",
    request_body = StatelessQueryRequest,
    responses(
        (status = 202, description = "Stateless query accepted", body = QueryResponse),
        (status = 404, description = "Target not found", body = ApiErrorResponse),
        (status = 413, description = "SQL exceeds request limit", body = ApiErrorResponse),
        (status = 429, description = "Retained query limit reached", body = ApiErrorResponse)
    )
)]
async fn submit_stateless_query(
    State(state): State<AppState>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<StatelessQueryRequest>,
) -> Result<(StatusCode, Json<QueryResponse>), ApiError> {
    let mut overrides = values(request.context)?;
    overrides.extend(values(request.properties)?);
    let query = state
        .service
        .submit_stateless_query_clustered(&principal, &request.target, overrides, request.sql)
        .await
        .map_err(service_error)?;
    Ok((StatusCode::ACCEPTED, Json(query.into())))
}

#[utoipa::path(
    get,
    path = "/v1/queries/{query_id}",
    tag = "queries",
    params(("query_id" = String, Path, description = "Opaque HTTP query ID")),
    responses(
        (status = 200, description = "Query status", body = QueryResponse),
        (status = 404, description = "Query not found or expired", body = ApiErrorResponse)
    )
)]
async fn get_query(
    State(state): State<AppState>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Path(query_id): Path<String>,
) -> Result<Json<QueryResponse>, ApiError> {
    Ok(Json(
        state
            .service
            .query_clustered(&principal, &query_id)
            .await
            .map_err(service_error)?
            .into(),
    ))
}

#[utoipa::path(
    post,
    path = "/v1/queries/{query_id}/cancel",
    tag = "queries",
    params(("query_id" = String, Path, description = "Opaque HTTP query ID")),
    responses(
        (status = 202, description = "Cancellation requested", body = QueryResponse),
        (status = 404, description = "Query not found or expired", body = ApiErrorResponse)
    )
)]
async fn cancel_query(
    State(state): State<AppState>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Path(query_id): Path<String>,
) -> Result<(StatusCode, Json<QueryResponse>), ApiError> {
    let status = state
        .service
        .cancel(&principal, &query_id)
        .map_err(service_error)?;
    Ok((StatusCode::ACCEPTED, Json(status.into())))
}

#[derive(Debug, Deserialize, IntoParams)]
struct ResultsQuery {
    page_token: Option<String>,
    limit: Option<usize>,
}

#[utoipa::path(
    get,
    path = "/v1/queries/{query_id}/results",
    tag = "queries",
    params(
        ("query_id" = String, Path, description = "Opaque HTTP query ID"),
        ResultsQuery
    ),
    responses(
        (status = 200, description = "Paginated result. Content negotiation supports application/json, application/x-ndjson, text/csv, and application/vnd.apache.arrow.stream"),
        (status = 400, description = "Invalid page token", body = ApiErrorResponse),
        (status = 404, description = "Query not found or expired", body = ApiErrorResponse),
        (status = 409, description = "Query is still running", body = ApiErrorResponse),
        (status = 422, description = "Query or retention failed", body = ApiErrorResponse)
    )
)]
async fn get_results(
    State(state): State<AppState>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Path(query_id): Path<String>,
    Query(query): Query<ResultsQuery>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
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
    let page = state
        .service
        .result_page_clustered(&principal, &query_id, offset, limit)
        .await
        .map_err(service_error)?;
    let next = page
        .next_offset
        .map(|offset| encode_page_token(offset, state.page_secret));
    let accept = headers
        .get(header::ACCEPT)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("application/json");
    let (content_type, bytes) = if accept.contains("application/vnd.apache.arrow.stream") {
        (
            "application/vnd.apache.arrow.stream",
            render_arrow(&page.batches)?,
        )
    } else if accept.contains("text/csv") {
        ("text/csv", render_output(&page.batches, OutputFormat::Csv)?)
    } else if accept.contains("application/x-ndjson") {
        (
            "application/x-ndjson",
            render_output(&page.batches, OutputFormat::JsonLines)?,
        )
    } else {
        (
            "application/json",
            render_output(&page.batches, OutputFormat::Json)?,
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

#[utoipa::path(
    get,
    path = "/v1/queries/{query_id}/events",
    tag = "queries",
    params(
        ("query_id" = String, Path, description = "Opaque HTTP query ID"),
        ("Last-Event-ID" = Option<u64>, Header, description = "Resume after this SSE event ID")
    ),
    responses(
        (status = 200, description = "Replayable live server-sent event stream"),
        (status = 404, description = "Query not found or expired", body = ApiErrorResponse)
    )
)]
async fn get_events(
    State(state): State<AppState>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Path(query_id): Path<String>,
    headers: HeaderMap,
) -> Result<Sse<impl futures_util::Stream<Item = Result<Event, Infallible>>>, ApiError> {
    let last = headers
        .get("last-event-id")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(0);
    let mut receiver = state
        .service
        .subscribe(&principal, &query_id)
        .map_err(service_error)?;
    let history = state
        .service
        .event_history(&principal, &query_id, last)
        .map_err(service_error)?;
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

fn service_error(error: ServiceError) -> ApiError {
    let status = match error.kind {
        ServiceErrorKind::InvalidArgument => {
            if error.code == "invalid_sql_size" {
                StatusCode::PAYLOAD_TOO_LARGE
            } else {
                StatusCode::BAD_REQUEST
            }
        }
        ServiceErrorKind::NotFound => StatusCode::NOT_FOUND,
        ServiceErrorKind::Forbidden => StatusCode::FORBIDDEN,
        ServiceErrorKind::Conflict => StatusCode::CONFLICT,
        ServiceErrorKind::ResourceExhausted => StatusCode::TOO_MANY_REQUESTS,
        ServiceErrorKind::FailedPrecondition => {
            if error.code == "shutting_down" {
                StatusCode::SERVICE_UNAVAILABLE
            } else if error.code == "query_running" {
                StatusCode::CONFLICT
            } else {
                StatusCode::UNPROCESSABLE_ENTITY
            }
        }
        ServiceErrorKind::Upstream => StatusCode::BAD_GATEWAY,
        ServiceErrorKind::Internal => StatusCode::INTERNAL_SERVER_ERROR,
    };
    ApiError::new(status, error.code, error.message)
}

fn audit(
    state: &AppState,
    action: &str,
    outcome: &str,
    principal: Option<&AuthenticatedPrincipal>,
    target: Option<&str>,
    session_id: Option<&str>,
    query_id: Option<&str>,
) {
    state
        .service
        .audit(action, outcome, principal, target, session_id, query_id);
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
    bind_http(address, false, false).await
}

/// Bind according to the production exposure policy.
///
/// Non-loopback binding requires both authenticated mode and an explicit
/// trusted TLS-terminating proxy declaration.
///
/// # Errors
///
/// Returns an error when exposure is unsafe or the address cannot be bound.
pub async fn bind_http(
    address: SocketAddr,
    trusted_proxy: bool,
    authenticated: bool,
) -> std::io::Result<TcpListener> {
    if !(address.ip().is_loopback() || trusted_proxy && authenticated) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "non-loopback binding requires --auth-file and --trusted-proxy",
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
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicU64, Ordering};
    use tower::ServiceExt;

    static NEXT_CONFIG: AtomicU64 = AtomicU64::new(1);

    #[derive(Default)]
    struct MemoryAuditSink(Mutex<Vec<AuditEvent>>);

    impl AuditSink for MemoryAuditSink {
        fn record(&self, event: &AuditEvent) {
            self.0.lock().unwrap().push(event.clone());
        }
    }

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

    fn authenticated_json_request(
        method: &str,
        uri: &str,
        value: &Value,
        key: &str,
    ) -> Request<Body> {
        let mut request = json_request(method, uri, value);
        request.headers_mut().insert(
            header::AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {key}")).unwrap(),
        );
        request
    }

    fn authenticated_service() -> (Router, String, String) {
        let id = NEXT_CONFIG.fetch_add(1, Ordering::Relaxed);
        let config_path = std::env::temp_dir().join(format!(
            "qcli-http-auth-targets-{}-{id}.env",
            std::process::id()
        ));
        std::fs::write(
            &config_path,
            "[demo]\nengine=demo\n\n[restricted]\nengine=demo\n",
        )
        .unwrap();
        let config = Config::load(&config_path).unwrap();
        std::fs::remove_file(config_path).ok();

        let (alice_key, alice_hash) = qcli_auth::generate_api_key_material("alice-key").unwrap();
        let (bob_key, bob_hash) = qcli_auth::generate_api_key_material("bob-key").unwrap();
        let auth_path =
            std::env::temp_dir().join(format!("qcli-http-auth-{}-{id}.toml", std::process::id()));
        std::fs::write(
            &auth_path,
            format!(
                "[principals.alice]\ntargets=[\"demo\"]\nmax_sessions=1\nmax_concurrent_queries=1\n\
                 \n[principals.bob]\ntargets=[\"restricted\"]\n\
                 \n[keys.alice-key]\nprincipal=\"alice\"\nsecret_hash={alice_hash:?}\n\
                 \n[keys.bob-key]\nprincipal=\"bob\"\nsecret_hash={bob_hash:?}\n"
            ),
        )
        .unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&auth_path, std::fs::Permissions::from_mode(0o600)).unwrap();
        }
        let authenticator = qcli_auth::ApiKeyAuthenticator::load(&auth_path).unwrap();
        std::fs::remove_file(auth_path).ok();
        let router = HttpService::new(
            config,
            [Arc::new(DemoAdapter) as Arc<dyn EngineAdapter>],
            HttpLimits::default(),
        )
        .with_authenticator(Arc::new(authenticator))
        .router();
        (
            router,
            alice_key.expose().to_owned(),
            bob_key.expose().to_owned(),
        )
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

    #[tokio::test]
    async fn authentication_isolates_callers_and_enforces_target_acl_and_quota() {
        let (router, alice_key, bob_key) = authenticated_service();
        let unauthenticated = router
            .clone()
            .oneshot(json_request(
                "POST",
                "/v1/sessions",
                &json!({"target": "demo"}),
            ))
            .await
            .unwrap();
        assert_eq!(unauthenticated.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(
            unauthenticated.headers()[header::WWW_AUTHENTICATE],
            "Bearer"
        );

        let created = router
            .clone()
            .oneshot(authenticated_json_request(
                "POST",
                "/v1/sessions",
                &json!({"target": "demo"}),
                &alice_key,
            ))
            .await
            .unwrap();
        assert_eq!(created.status(), StatusCode::CREATED);
        let session = json_body(created).await;
        let session_id = session["id"].as_str().unwrap();

        let hidden = router
            .clone()
            .oneshot(authenticated_json_request(
                "GET",
                &format!("/v1/sessions/{session_id}"),
                &json!({}),
                &bob_key,
            ))
            .await
            .unwrap();
        assert_eq!(hidden.status(), StatusCode::NOT_FOUND);

        let forbidden = router
            .clone()
            .oneshot(authenticated_json_request(
                "POST",
                "/v1/sessions",
                &json!({"target": "demo"}),
                &bob_key,
            ))
            .await
            .unwrap();
        assert_eq!(forbidden.status(), StatusCode::FORBIDDEN);

        let quota = router
            .oneshot(authenticated_json_request(
                "POST",
                "/v1/sessions",
                &json!({"target": "demo"}),
                &alice_key,
            ))
            .await
            .unwrap();
        assert_eq!(quota.status(), StatusCode::TOO_MANY_REQUESTS);
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
    async fn http_and_direct_service_machine_results_match() {
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

        let principal = AuthenticatedPrincipal {
            id: "local".into(),
            allowed_targets: ["*".into()].into_iter().collect(),
            max_sessions: usize::MAX,
            max_concurrent_queries: usize::MAX,
        };
        let direct_query = service
            .gateway()
            .submit_stateless_query(
                &principal,
                "demo",
                BTreeMap::new(),
                "select * from sample".into(),
            )
            .unwrap();
        loop {
            let status = service
                .gateway()
                .query(&principal, &direct_query.id)
                .unwrap();
            if matches!(status.state.as_str(), "completed" | "failed" | "cancelled") {
                break;
            }
            tokio::task::yield_now().await;
        }
        let page = service
            .gateway()
            .result_page(&principal, &direct_query.id, 0, usize::MAX)
            .unwrap();
        let direct = render_output(&page.batches, OutputFormat::Json).unwrap();
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
            result_ttl: Duration::from_millis(1),
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
        tokio::time::sleep(Duration::from_millis(5)).await;

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
        let error = bind_http("0.0.0.0:0".parse().unwrap(), true, false)
            .await
            .unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::PermissionDenied);
        let listener = bind_http("0.0.0.0:0".parse().unwrap(), true, true)
            .await
            .unwrap();
        drop(listener);
    }

    #[tokio::test]
    async fn forwarded_headers_and_cors_are_fail_closed() {
        let router = service(HttpLimits::default())
            .with_operations(HttpOperations {
                trusted_proxy: false,
                allowed_origins: vec!["https://console.example".into()],
            })
            .router();
        let forwarded = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/sessions/missing")
                    .header("x-forwarded-proto", "https")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(forwarded.status(), StatusCode::BAD_REQUEST);

        let forbidden_origin = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/sessions/missing")
                    .header(header::ORIGIN, "https://evil.example")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(forbidden_origin.status(), StatusCode::FORBIDDEN);

        let preflight = router
            .oneshot(
                Request::builder()
                    .method(Method::OPTIONS)
                    .uri("/v1/sessions")
                    .header(header::ORIGIN, "https://console.example")
                    .header(header::ACCESS_CONTROL_REQUEST_METHOD, "POST")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(preflight.status(), StatusCode::NO_CONTENT);
        assert_eq!(
            preflight.headers()[header::ACCESS_CONTROL_ALLOW_ORIGIN],
            "https://console.example"
        );
    }

    #[tokio::test]
    async fn trusted_proxy_requires_forwarded_https() {
        let router = service(HttpLimits::default())
            .with_operations(HttpOperations {
                trusted_proxy: true,
                allowed_origins: Vec::new(),
            })
            .router();
        let direct = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/sessions/missing")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(direct.status(), StatusCode::UPGRADE_REQUIRED);
        let proxied = router
            .oneshot(
                Request::builder()
                    .uri("/v1/sessions/missing")
                    .header("x-forwarded-proto", "https")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(proxied.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn cleanup_expires_sessions_and_audit_omits_sql() {
        let limits = HttpLimits {
            session_ttl: Duration::from_millis(1),
            ..HttpLimits::default()
        };
        let audit = Arc::new(MemoryAuditSink::default());
        let service = service(limits).with_audit_sink(audit.clone());
        let router = service.router();
        let session = create_demo_session(&router).await;
        let session_id = session["id"].as_str().unwrap();
        let response = router
            .clone()
            .oneshot(json_request(
                "POST",
                &format!("/v1/sessions/{session_id}/queries"),
                &json!({ "sql": "select 'never-audit-this-sql'" }),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        tokio::time::sleep(Duration::from_millis(5)).await;
        service.gateway().cleanup_expired();
        let principal = AuthenticatedPrincipal {
            id: "local".into(),
            allowed_targets: ["*".into()].into_iter().collect(),
            max_sessions: usize::MAX,
            max_concurrent_queries: usize::MAX,
        };
        assert!(service.gateway().session(&principal, session_id).is_err());
        let encoded = serde_json::to_string(&*audit.0.lock().unwrap()).unwrap();
        assert!(encoded.contains("query.submit"));
        assert!(!encoded.contains("never-audit-this-sql"));
    }

    #[tokio::test]
    async fn generated_openapi_and_swagger_ui_expose_the_contract() {
        let document = serde_json::to_value(openapi()).unwrap();
        let paths = document["paths"].as_object().unwrap();
        for path in [
            "/v1/sessions",
            "/v1/sessions/{session_id}",
            "/v1/sessions/{session_id}/target",
            "/v1/sessions/{session_id}/properties",
            "/v1/sessions/{session_id}/options",
            "/v1/sessions/{session_id}/queries",
            "/v1/queries",
            "/v1/queries/{query_id}",
            "/v1/queries/{query_id}/results",
            "/v1/queries/{query_id}/events",
            "/v1/queries/{query_id}/cancel",
        ] {
            assert!(paths.contains_key(path), "missing OpenAPI path {path}");
        }
        let schemas = document["components"]["schemas"].as_object().unwrap();
        for schema in [
            "CreateSessionRequest",
            "SessionResponse",
            "QueryRequest",
            "QueryResponse",
            "ApiErrorResponse",
        ] {
            assert!(
                schemas.contains_key(schema),
                "missing OpenAPI schema {schema}"
            );
        }

        let router = service(HttpLimits::default()).router();
        let specification = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/openapi.json")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(specification.status(), StatusCode::OK);
        let specification = json_body(specification).await;
        assert_eq!(specification["info"]["title"], "qcli Local HTTP API");

        let documentation = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/docs/")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(documentation.status(), StatusCode::OK);
        let html = to_bytes(documentation.into_body(), usize::MAX)
            .await
            .unwrap();
        assert!(String::from_utf8_lossy(&html).contains("Swagger UI"));
    }
}
