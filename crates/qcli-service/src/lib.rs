//! Protocol-neutral state and lifecycle services shared by qcli frontends.

use arrow_array::RecordBatch;
use arrow_ipc::reader::FileReader;
use arrow_ipc::writer::FileWriter;
use qcli_auth::AuthenticatedPrincipal;
use qcli_config::{Config, ResolvedTarget};
use qcli_core::{CoreError, QueryHandle, QueryItem, QueryService, SessionManager, SessionSnapshot};
use qcli_driver_api::{CancellationSignal, EngineAdapter, QueryEvent, QueryProgress, QueryState};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::broadcast;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct ServiceLimits {
    pub max_queries: usize,
    pub memory_result_bytes_per_query: usize,
    pub max_result_bytes_per_query: usize,
    pub result_ttl: Duration,
    pub max_sql_bytes: usize,
    pub session_ttl: Duration,
    pub shutdown_grace: Duration,
}

impl Default for ServiceLimits {
    fn default() -> Self {
        Self {
            max_queries: 128,
            memory_result_bytes_per_query: 1024 * 1024,
            max_result_bytes_per_query: 64 * 1024 * 1024,
            result_ttl: Duration::from_secs(15 * 60),
            max_sql_bytes: 1024 * 1024,
            session_ttl: Duration::from_secs(30 * 60),
            shutdown_grace: Duration::from_secs(10),
        }
    }
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct AuditEvent {
    pub action: String,
    pub outcome: String,
    pub principal: Option<String>,
    pub target: Option<String>,
    pub session_id: Option<String>,
    pub query_id: Option<String>,
}

pub trait AuditSink: Send + Sync {
    fn record(&self, event: &AuditEvent);
}

#[derive(Debug)]
struct NullAuditSink;

impl AuditSink for NullAuditSink {
    fn record(&self, _event: &AuditEvent) {}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServiceErrorKind {
    InvalidArgument,
    NotFound,
    Forbidden,
    Conflict,
    ResourceExhausted,
    FailedPrecondition,
    Upstream,
    Internal,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceError {
    pub kind: ServiceErrorKind,
    pub code: String,
    pub message: String,
}

impl ServiceError {
    #[must_use]
    pub fn new(
        kind: ServiceErrorKind,
        code: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            kind,
            code: code.into(),
            message: message.into(),
        }
    }
}

impl std::fmt::Display for ServiceError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for ServiceError {}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct QueryError {
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct ServiceEvent {
    pub id: u64,
    pub event: String,
    pub data: Value,
    pub terminal: bool,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct QueryStatus {
    pub id: String,
    pub session_id: String,
    pub session_version: u64,
    pub target: String,
    pub engine: String,
    pub engine_query_id: Option<String>,
    pub state: String,
    pub rows: usize,
    pub error: Option<QueryError>,
}

pub struct ResultPage {
    pub batches: Vec<RecordBatch>,
    pub total_rows: usize,
    pub next_offset: Option<usize>,
}

#[derive(Debug, Clone)]
struct SessionOwner {
    principal: String,
    last_access: Instant,
}

struct QueryRecord {
    id: String,
    owner: String,
    session_id: String,
    session_version: u64,
    target: String,
    engine: String,
    cancel: CancellationSignal,
    data: Mutex<QueryData>,
    events: broadcast::Sender<ServiceEvent>,
}

struct QueryData {
    state: String,
    engine_query_id: Option<String>,
    rows: usize,
    retained_bytes: usize,
    storage: ResultStorage,
    events: Vec<ServiceEvent>,
    next_event_id: u64,
    error: Option<QueryError>,
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

enum ResultSource {
    Memory(Vec<RecordBatch>),
    Spill(PathBuf),
}

struct ServiceState {
    config: Arc<Config>,
    sessions: Arc<SessionManager>,
    queries: Arc<QueryService>,
    session_owners: Mutex<HashMap<String, SessionOwner>>,
    records: Mutex<HashMap<String, Arc<QueryRecord>>>,
    limits: ServiceLimits,
    audit: Mutex<Arc<dyn AuditSink>>,
    shutting_down: AtomicBool,
}

#[derive(Clone)]
pub struct GatewayService {
    state: Arc<ServiceState>,
}

#[allow(
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    reason = "the internal service boundary returns structured ServiceError values; mutex poisoning is unrecoverable"
)]
impl GatewayService {
    #[must_use]
    pub fn new(
        config: Config,
        adapters: impl IntoIterator<Item = Arc<dyn EngineAdapter>>,
        limits: ServiceLimits,
    ) -> Self {
        Self {
            state: Arc::new(ServiceState {
                config: Arc::new(config),
                sessions: Arc::new(SessionManager::default()),
                queries: Arc::new(QueryService::new(adapters, 8)),
                session_owners: Mutex::new(HashMap::new()),
                records: Mutex::new(HashMap::new()),
                limits,
                audit: Mutex::new(Arc::new(NullAuditSink)),
                shutting_down: AtomicBool::new(false),
            }),
        }
    }

    #[must_use]
    pub fn with_audit_sink(self, audit: Arc<dyn AuditSink>) -> Self {
        self.set_audit_sink(audit);
        self
    }

    pub fn set_audit_sink(&self, audit: Arc<dyn AuditSink>) {
        *self.state.audit.lock().expect("audit sink mutex poisoned") = audit;
    }

    #[must_use]
    pub fn is_shutting_down(&self) -> bool {
        self.state.shutting_down.load(Ordering::Acquire)
    }

    pub fn begin_shutdown(&self) {
        self.state.shutting_down.store(true, Ordering::Release);
        self.cancel_active_queries();
    }

    #[must_use]
    pub fn shutdown_grace(&self) -> Duration {
        self.state.limits.shutdown_grace
    }

    pub async fn wait_for_queries(&self) {
        let deadline = tokio::time::Instant::now() + self.state.limits.shutdown_grace;
        loop {
            if self.active_query_count() == 0 || tokio::time::Instant::now() >= deadline {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    pub fn create_session(
        &self,
        principal: &AuthenticatedPrincipal,
        target_name: &str,
        overrides: BTreeMap<String, String>,
    ) -> Result<SessionSnapshot, ServiceError> {
        self.ensure_available()?;
        self.cleanup_expired();
        self.enforce_session_quota(principal)?;
        let target = self.authorized_target(principal, target_name)?;
        let snapshot = self.state.sessions.create_with_overrides(target, overrides);
        self.state
            .session_owners
            .lock()
            .expect("session owner mutex poisoned")
            .insert(
                snapshot.id.clone(),
                SessionOwner {
                    principal: principal.id.clone(),
                    last_access: Instant::now(),
                },
            );
        self.audit(
            "session.create",
            "allowed",
            Some(principal),
            Some(&snapshot.target),
            Some(&snapshot.id),
            None,
        );
        Ok(snapshot)
    }

    pub fn session(
        &self,
        principal: &AuthenticatedPrincipal,
        session_id: &str,
    ) -> Result<SessionSnapshot, ServiceError> {
        self.require_session_owner(principal, session_id)?;
        self.state
            .sessions
            .snapshot(session_id)
            .map_err(|error| core_error(&error))
    }

    pub fn update_session(
        &self,
        principal: &AuthenticatedPrincipal,
        session_id: &str,
        expected_version: u64,
        overrides: BTreeMap<String, String>,
    ) -> Result<SessionSnapshot, ServiceError> {
        self.ensure_available()?;
        self.require_session_owner(principal, session_id)?;
        self.state
            .sessions
            .set_options(session_id, expected_version, overrides)
            .map_err(|error| core_error(&error))
    }

    pub fn switch_target(
        &self,
        principal: &AuthenticatedPrincipal,
        session_id: &str,
        expected_version: u64,
        target_name: &str,
    ) -> Result<SessionSnapshot, ServiceError> {
        self.ensure_available()?;
        self.require_session_owner(principal, session_id)?;
        let target = self.authorized_target(principal, target_name)?;
        self.state
            .sessions
            .switch_target(session_id, expected_version, target)
            .map_err(|error| core_error(&error))
    }

    pub fn close_session(
        &self,
        principal: &AuthenticatedPrincipal,
        session_id: &str,
    ) -> Result<(), ServiceError> {
        self.require_session_owner(principal, session_id)?;
        self.state
            .sessions
            .close(session_id)
            .map_err(|error| core_error(&error))?;
        self.state
            .session_owners
            .lock()
            .expect("session owner mutex poisoned")
            .remove(session_id);
        self.audit(
            "session.delete",
            "allowed",
            Some(principal),
            None,
            Some(session_id),
            None,
        );
        Ok(())
    }

    pub fn submit_session_query(
        &self,
        principal: &AuthenticatedPrincipal,
        session_id: &str,
        sql: String,
    ) -> Result<QueryStatus, ServiceError> {
        let snapshot = self.session(principal, session_id)?;
        self.submit_query(principal, &snapshot, sql, false)
    }

    pub fn submit_stateless_query(
        &self,
        principal: &AuthenticatedPrincipal,
        target_name: &str,
        overrides: BTreeMap<String, String>,
        sql: String,
    ) -> Result<QueryStatus, ServiceError> {
        let snapshot = self.create_session(principal, target_name, overrides)?;
        match self.submit_query(principal, &snapshot, sql, true) {
            Ok(status) => Ok(status),
            Err(error) => {
                self.state.sessions.close(&snapshot.id).ok();
                self.state
                    .session_owners
                    .lock()
                    .expect("session owner mutex poisoned")
                    .remove(&snapshot.id);
                Err(error)
            }
        }
    }

    pub fn query(
        &self,
        principal: &AuthenticatedPrincipal,
        query_id: &str,
    ) -> Result<QueryStatus, ServiceError> {
        let record = self.owned_record(principal, query_id)?;
        Ok(query_status(&record))
    }

    pub fn cancel(
        &self,
        principal: &AuthenticatedPrincipal,
        query_id: &str,
    ) -> Result<QueryStatus, ServiceError> {
        let record = self.owned_record(principal, query_id)?;
        record.cancel.cancel();
        self.audit(
            "query.cancel",
            "allowed",
            Some(principal),
            Some(&record.target),
            Some(&record.session_id),
            Some(&record.id),
        );
        push_event(&record, "state", json!({ "state": "cancelling" }), false);
        Ok(query_status(&record))
    }

    pub fn result_page(
        &self,
        principal: &AuthenticatedPrincipal,
        query_id: &str,
        offset: usize,
        limit: usize,
    ) -> Result<ResultPage, ServiceError> {
        let record = self.owned_record(principal, query_id)?;
        let (state_name, error, rows, source) = {
            let data = record.data.lock().expect("query record mutex poisoned");
            let source = match &data.storage {
                ResultStorage::Memory(batches) => ResultSource::Memory(batches.clone()),
                ResultStorage::Spill { path, writer } => {
                    if writer.is_some() {
                        return Err(ServiceError::new(
                            ServiceErrorKind::FailedPrecondition,
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
            return Err(ServiceError::new(
                ServiceErrorKind::FailedPrecondition,
                error.code,
                error.message,
            ));
        }
        if !matches!(state_name.as_str(), "completed" | "cancelled" | "failed") {
            return Err(ServiceError::new(
                ServiceErrorKind::FailedPrecondition,
                "query_running",
                "results are available after query completion",
            ));
        }
        let start = offset.min(rows);
        let end = start.saturating_add(limit.max(1)).min(rows);
        Ok(ResultPage {
            batches: load_page(source, start, end)?,
            total_rows: rows,
            next_offset: (end < rows).then_some(end),
        })
    }

    pub fn event_history(
        &self,
        principal: &AuthenticatedPrincipal,
        query_id: &str,
        after: u64,
    ) -> Result<Vec<ServiceEvent>, ServiceError> {
        let record = self.owned_record(principal, query_id)?;
        let events = record
            .data
            .lock()
            .expect("query record mutex poisoned")
            .events
            .iter()
            .filter(|entry| entry.id > after)
            .cloned()
            .collect();
        Ok(events)
    }

    pub fn subscribe(
        &self,
        principal: &AuthenticatedPrincipal,
        query_id: &str,
    ) -> Result<broadcast::Receiver<ServiceEvent>, ServiceError> {
        Ok(self.owned_record(principal, query_id)?.events.subscribe())
    }

    pub fn cleanup_expired(&self) {
        let now = Instant::now();
        self.state
            .records
            .lock()
            .expect("query registry mutex poisoned")
            .retain(|_, record| {
                record
                    .data
                    .lock()
                    .expect("query record mutex poisoned")
                    .completed_at
                    .is_none_or(|completed| {
                        now.duration_since(completed) < self.state.limits.result_ttl
                    })
            });
        let expired_sessions = {
            let owners = self
                .state
                .session_owners
                .lock()
                .expect("session owner mutex poisoned");
            owners
                .iter()
                .filter(|(_, owner)| {
                    now.duration_since(owner.last_access) >= self.state.limits.session_ttl
                })
                .map(|(id, owner)| (id.clone(), owner.principal.clone()))
                .collect::<Vec<_>>()
        };
        for (session_id, principal) in expired_sessions {
            self.state
                .records
                .lock()
                .expect("query registry mutex poisoned")
                .values()
                .filter(|record| record.session_id == session_id)
                .for_each(|record| record.cancel.cancel());
            self.state.sessions.close(&session_id).ok();
            self.state
                .session_owners
                .lock()
                .expect("session owner mutex poisoned")
                .remove(&session_id);
            self.state
                .audit
                .lock()
                .expect("audit sink mutex poisoned")
                .record(&AuditEvent {
                    action: "session.expire".into(),
                    outcome: "cancelled_active_queries".into(),
                    principal: Some(principal),
                    target: None,
                    session_id: Some(session_id),
                    query_id: None,
                });
        }
    }

    fn submit_query(
        &self,
        principal: &AuthenticatedPrincipal,
        snapshot: &SessionSnapshot,
        sql: String,
        close_session: bool,
    ) -> Result<QueryStatus, ServiceError> {
        self.ensure_available()?;
        if sql.is_empty() || sql.len() > self.state.limits.max_sql_bytes {
            return Err(ServiceError::new(
                ServiceErrorKind::InvalidArgument,
                "invalid_sql_size",
                format!(
                    "SQL must contain 1..={} bytes",
                    self.state.limits.max_sql_bytes
                ),
            ));
        }
        self.cleanup_expired();
        self.enforce_query_quota(principal)?;
        if self
            .state
            .records
            .lock()
            .expect("query registry mutex poisoned")
            .len()
            >= self.state.limits.max_queries
        {
            return Err(ServiceError::new(
                ServiceErrorKind::ResourceExhausted,
                "query_limit",
                "local retained-query limit reached",
            ));
        }
        let handle = self
            .state
            .queries
            .submit(snapshot.clone(), sql)
            .map_err(|error| core_error(&error))?;
        let (events, _) = broadcast::channel(128);
        let record = Arc::new(QueryRecord {
            id: format!("qcli_{}", Uuid::new_v4().simple()),
            owner: principal.id.clone(),
            session_id: snapshot.id.clone(),
            session_version: snapshot.version,
            target: snapshot.target.clone(),
            engine: snapshot.engine.clone(),
            cancel: handle.cancellation_signal(),
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
        self.state
            .records
            .lock()
            .expect("query registry mutex poisoned")
            .insert(record.id.clone(), record.clone());
        self.audit(
            "query.submit",
            "allowed",
            Some(principal),
            Some(&snapshot.target),
            Some(&snapshot.id),
            Some(&record.id),
        );
        let response = query_status(&record);
        let state = self.state.clone();
        tokio::spawn(collect_query(state, record, handle, close_session));
        Ok(response)
    }

    fn owned_record(
        &self,
        principal: &AuthenticatedPrincipal,
        query_id: &str,
    ) -> Result<Arc<QueryRecord>, ServiceError> {
        self.cleanup_expired();
        self.state
            .records
            .lock()
            .expect("query registry mutex poisoned")
            .get(query_id)
            .filter(|record| record.owner == principal.id)
            .cloned()
            .ok_or_else(|| {
                ServiceError::new(
                    ServiceErrorKind::NotFound,
                    "query_not_found",
                    "query not found",
                )
            })
    }

    fn authorized_target(
        &self,
        principal: &AuthenticatedPrincipal,
        name: &str,
    ) -> Result<ResolvedTarget, ServiceError> {
        if !principal.can_use_target(name) {
            self.audit(
                "target.authorize",
                "denied",
                Some(principal),
                Some(name),
                None,
                None,
            );
            return Err(ServiceError::new(
                ServiceErrorKind::Forbidden,
                "target_forbidden",
                format!("principal is not authorized for target '{name}'"),
            ));
        }
        self.state.config.target(name).cloned().ok_or_else(|| {
            ServiceError::new(
                ServiceErrorKind::NotFound,
                "target_not_found",
                format!("target '{name}' does not exist"),
            )
        })
    }

    fn require_session_owner(
        &self,
        principal: &AuthenticatedPrincipal,
        session_id: &str,
    ) -> Result<(), ServiceError> {
        let owned = self
            .state
            .session_owners
            .lock()
            .expect("session owner mutex poisoned")
            .get_mut(session_id)
            .is_some_and(|owner| {
                if owner.principal == principal.id {
                    owner.last_access = Instant::now();
                    true
                } else {
                    false
                }
            });
        if owned {
            Ok(())
        } else {
            Err(ServiceError::new(
                ServiceErrorKind::NotFound,
                "session_not_found",
                "session not found",
            ))
        }
    }

    fn enforce_session_quota(
        &self,
        principal: &AuthenticatedPrincipal,
    ) -> Result<(), ServiceError> {
        let count = self
            .state
            .session_owners
            .lock()
            .expect("session owner mutex poisoned")
            .values()
            .filter(|owner| owner.principal == principal.id)
            .count();
        if count >= principal.max_sessions {
            Err(ServiceError::new(
                ServiceErrorKind::ResourceExhausted,
                "session_quota",
                "principal session quota reached",
            ))
        } else {
            Ok(())
        }
    }

    fn enforce_query_quota(&self, principal: &AuthenticatedPrincipal) -> Result<(), ServiceError> {
        let active = self
            .state
            .records
            .lock()
            .expect("query registry mutex poisoned")
            .values()
            .filter(|record| {
                record.owner == principal.id
                    && record
                        .data
                        .lock()
                        .expect("query record mutex poisoned")
                        .completed_at
                        .is_none()
            })
            .count();
        if active >= principal.max_concurrent_queries {
            Err(ServiceError::new(
                ServiceErrorKind::ResourceExhausted,
                "query_quota",
                "principal concurrent-query quota reached",
            ))
        } else {
            Ok(())
        }
    }

    fn ensure_available(&self) -> Result<(), ServiceError> {
        if self.is_shutting_down() {
            Err(ServiceError::new(
                ServiceErrorKind::FailedPrecondition,
                "shutting_down",
                "server is shutting down",
            ))
        } else {
            Ok(())
        }
    }

    fn active_query_count(&self) -> usize {
        self.state
            .records
            .lock()
            .expect("query registry mutex poisoned")
            .values()
            .filter(|record| {
                record
                    .data
                    .lock()
                    .expect("query record mutex poisoned")
                    .completed_at
                    .is_none()
            })
            .count()
    }

    fn cancel_active_queries(&self) {
        self.state
            .records
            .lock()
            .expect("query registry mutex poisoned")
            .values()
            .filter(|record| {
                record
                    .data
                    .lock()
                    .expect("query record mutex poisoned")
                    .completed_at
                    .is_none()
            })
            .for_each(|record| record.cancel.cancel());
    }

    pub fn audit(
        &self,
        action: &str,
        outcome: &str,
        principal: Option<&AuthenticatedPrincipal>,
        target: Option<&str>,
        session_id: Option<&str>,
        query_id: Option<&str>,
    ) {
        self.state
            .audit
            .lock()
            .expect("audit sink mutex poisoned")
            .record(&AuditEvent {
                action: action.into(),
                outcome: outcome.into(),
                principal: principal.map(|value| value.id.clone()),
                target: target.map(str::to_owned),
                session_id: session_id.map(str::to_owned),
                query_id: query_id.map(str::to_owned),
            });
    }
}

async fn collect_query(
    state: Arc<ServiceState>,
    record: Arc<QueryRecord>,
    mut handle: QueryHandle,
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
                if let Err(error) = store_batch(&record.id, &mut data, batch, &state.limits) {
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
        state
            .sessions
            .set_options(&record.session_id, record.session_version, session_updates)
            .ok();
    }
    if let Err(error) = finish_results(&record) {
        record
            .data
            .lock()
            .expect("query record mutex poisoned")
            .error = Some(error);
        overflow = true;
    }
    if let Err(error) = finish {
        if matches!(&error, CoreError::Driver(driver) if driver.code == "cancelled") {
            record_event(
                &record,
                terminal_event.unwrap_or(QueryEvent::State(QueryState::Cancelled)),
            );
            close_stateless_session(&state, &record, close_session);
            return;
        }
        let mut data = record.data.lock().expect("query record mutex poisoned");
        if data.error.is_none() {
            data.error = Some(QueryError {
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
    close_stateless_session(&state, &record, close_session);
}

fn close_stateless_session(state: &ServiceState, record: &QueryRecord, close_session: bool) {
    if close_session {
        state.sessions.close(&record.session_id).ok();
        state
            .session_owners
            .lock()
            .expect("session owner mutex poisoned")
            .remove(&record.session_id);
    }
}

fn query_status(record: &QueryRecord) -> QueryStatus {
    let data = record.data.lock().expect("query record mutex poisoned");
    QueryStatus {
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

fn record_event(record: &QueryRecord, event: QueryEvent) {
    match event {
        QueryEvent::State(state) => {
            let state = state_name(state);
            push_event(
                record,
                "state",
                json!({ "state": state }),
                matches!(state, "completed" | "cancelled" | "failed"),
            );
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

fn push_event(record: &QueryRecord, event: &str, value: Value, terminal: bool) {
    let entry = {
        let mut data = record.data.lock().expect("query record mutex poisoned");
        if event == "state"
            && let Some(state) = value.get("state").and_then(Value::as_str)
        {
            data.state = state.into();
            if terminal {
                data.completed_at = Some(Instant::now());
            }
        }
        let entry = ServiceEvent {
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

fn store_batch(
    query_id: &str,
    data: &mut QueryData,
    batch: RecordBatch,
    limits: &ServiceLimits,
) -> Result<(), QueryError> {
    let bytes = batch
        .columns()
        .iter()
        .map(arrow_array::Array::get_array_memory_size)
        .sum::<usize>();
    let rows = batch.num_rows();
    if data.retained_bytes.saturating_add(bytes) > limits.max_result_bytes_per_query {
        return Err(QueryError {
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
            return Err(QueryError {
                code: "result_storage".into(),
                message: "result spill was already finalized".into(),
            });
        }
    }
    data.rows += rows;
    data.retained_bytes += bytes;
    Ok(())
}

fn finish_results(record: &QueryRecord) -> Result<(), QueryError> {
    let mut data = record.data.lock().expect("query record mutex poisoned");
    if let ResultStorage::Spill { writer, .. } = &mut data.storage
        && let Some(mut writer) = writer.take()
    {
        writer.finish().map_err(storage_error)?;
    }
    Ok(())
}

fn storage_error(error: impl std::fmt::Display) -> QueryError {
    QueryError {
        code: "result_storage".into(),
        message: error.to_string(),
    }
}

fn load_page(
    source: ResultSource,
    start: usize,
    end: usize,
) -> Result<Vec<RecordBatch>, ServiceError> {
    match source {
        ResultSource::Memory(batches) => Ok(slice_batches(&batches, start, end)),
        ResultSource::Spill(path) => {
            let file = std::fs::File::open(path).map_err(storage_service_error)?;
            let reader = FileReader::try_new(file, None).map_err(storage_service_error)?;
            let mut cursor = 0;
            let mut output = Vec::new();
            for batch in reader {
                let batch = batch.map_err(storage_service_error)?;
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

fn storage_service_error(error: impl std::fmt::Display) -> ServiceError {
    ServiceError::new(
        ServiceErrorKind::Internal,
        "result_storage",
        error.to_string(),
    )
}

fn core_error(error: &CoreError) -> ServiceError {
    let kind = match error {
        CoreError::SessionNotFound(_) => ServiceErrorKind::NotFound,
        CoreError::VersionConflict { .. } => ServiceErrorKind::Conflict,
        CoreError::AdapterNotFound(_) => ServiceErrorKind::InvalidArgument,
        CoreError::Driver(_) | CoreError::Task(_) => ServiceErrorKind::Upstream,
    };
    ServiceError::new(kind, "core", error.to_string())
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

fn is_sensitive_name(name: &str) -> bool {
    let name = name.to_ascii_lowercase();
    ["password", "token", "secret", "credential", "private_key"]
        .iter()
        .any(|part| name.contains(part))
}

#[cfg(test)]
mod tests {
    use super::*;
    use qcli_driver_demo::DemoAdapter;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT_CONFIG: AtomicU64 = AtomicU64::new(1);

    fn service(limits: ServiceLimits) -> GatewayService {
        let id = NEXT_CONFIG.fetch_add(1, Ordering::Relaxed);
        let path =
            std::env::temp_dir().join(format!("qcli-service-{}-{id}.env", std::process::id()));
        std::fs::write(&path, "[demo]\nengine=demo\n").unwrap();
        let config = Config::load(&path).unwrap();
        std::fs::remove_file(path).ok();
        GatewayService::new(
            config,
            [Arc::new(DemoAdapter) as Arc<dyn EngineAdapter>],
            limits,
        )
    }

    fn principal(id: &str) -> AuthenticatedPrincipal {
        AuthenticatedPrincipal {
            id: id.into(),
            allowed_targets: ["demo".into()].into_iter().collect(),
            max_sessions: 2,
            max_concurrent_queries: 2,
        }
    }

    async fn wait_for_terminal(
        service: &GatewayService,
        principal: &AuthenticatedPrincipal,
        id: &str,
    ) -> QueryStatus {
        loop {
            let status = service.query(principal, id).unwrap();
            if matches!(status.state.as_str(), "completed" | "failed" | "cancelled") {
                return status;
            }
            tokio::task::yield_now().await;
        }
    }

    #[tokio::test]
    async fn direct_service_executes_and_pages_arrow_results() {
        let service = service(ServiceLimits::default());
        let principal = principal("analyst");
        let session = service
            .create_session(&principal, "demo", BTreeMap::new())
            .unwrap();
        let query = service
            .submit_session_query(&principal, &session.id, "select * from sample".into())
            .unwrap();
        let terminal = wait_for_terminal(&service, &principal, &query.id).await;
        assert_eq!(terminal.state, "completed");
        assert_eq!(terminal.rows, 2);

        let first = service.result_page(&principal, &query.id, 0, 1).unwrap();
        assert_eq!(
            first
                .batches
                .iter()
                .map(RecordBatch::num_rows)
                .sum::<usize>(),
            1
        );
        assert_eq!(first.next_offset, Some(1));
        let second = service
            .result_page(&principal, &query.id, first.next_offset.unwrap(), 1)
            .unwrap();
        assert_eq!(
            second
                .batches
                .iter()
                .map(RecordBatch::num_rows)
                .sum::<usize>(),
            1
        );
        assert_eq!(second.next_offset, None);
    }

    #[test]
    fn sessions_are_owned_versioned_and_target_authorized() {
        let service = service(ServiceLimits::default());
        let analyst = principal("analyst");
        let stranger = principal("stranger");
        let session = service
            .create_session(&analyst, "demo", BTreeMap::new())
            .unwrap();
        let updated = service
            .update_session(
                &analyst,
                &session.id,
                session.version,
                [("schema".into(), "analytics".into())]
                    .into_iter()
                    .collect(),
            )
            .unwrap();
        assert_eq!(updated.version, session.version + 1);
        assert_eq!(
            service.session(&stranger, &session.id).unwrap_err().kind,
            ServiceErrorKind::NotFound
        );
        assert_eq!(
            service
                .update_session(&analyst, &session.id, session.version, BTreeMap::new(),)
                .unwrap_err()
                .kind,
            ServiceErrorKind::Conflict
        );
    }

    #[tokio::test]
    async fn retention_and_shutdown_are_protocol_neutral() {
        let service = service(ServiceLimits {
            result_ttl: Duration::from_millis(1),
            ..ServiceLimits::default()
        });
        let principal = principal("analyst");
        let query = service
            .submit_stateless_query(
                &principal,
                "demo",
                BTreeMap::new(),
                "select * from sample".into(),
            )
            .unwrap();
        wait_for_terminal(&service, &principal, &query.id).await;
        tokio::time::sleep(Duration::from_millis(5)).await;
        service.cleanup_expired();
        assert_eq!(
            service.query(&principal, &query.id).unwrap_err().kind,
            ServiceErrorKind::NotFound
        );

        service.begin_shutdown();
        assert!(service.is_shutting_down());
        assert_eq!(
            service
                .create_session(&principal, "demo", BTreeMap::new())
                .unwrap_err()
                .code,
            "shutting_down"
        );
    }

    #[tokio::test]
    async fn cancellation_and_events_are_shared_service_contracts() {
        let service = service(ServiceLimits::default());
        let principal = principal("analyst");
        let session = service
            .create_session(&principal, "demo", BTreeMap::new())
            .unwrap();
        let query = service
            .submit_session_query(&principal, &session.id, "wait-for-cancel".into())
            .unwrap();
        let cancelling = service.cancel(&principal, &query.id).unwrap();
        assert_eq!(cancelling.state, "cancelling");
        let terminal = wait_for_terminal(&service, &principal, &query.id).await;
        assert_eq!(terminal.state, "cancelled");
        let events = service.event_history(&principal, &query.id, 0).unwrap();
        assert!(
            events
                .iter()
                .any(|event| event.data["state"] == "cancelling")
        );
        assert!(
            events
                .iter()
                .any(|event| event.data["state"] == "cancelled")
        );
    }
}
