//! Protocol-neutral state and lifecycle services shared by qcli frontends.

use arrow_array::RecordBatch;
use arrow_ipc::reader::FileReader;
use arrow_ipc::writer::FileWriter;
use arrow_schema::{Schema, SchemaRef};
use bytes::Bytes;
use qcli_auth::AuthenticatedPrincipal;
use qcli_cluster::{ClusterStateStore, QueryLease, ResultObjectStore, SharedResource};
use qcli_config::{Config, ResolvedTarget};
use qcli_core::{CoreError, QueryHandle, QueryItem, QueryService, SessionManager, SessionSnapshot};
use qcli_driver_api::{
    AdapterCapability, CancellationSignal, EngineAdapter, IngestRequest, IngestSource,
    MetadataRequest, QueryEvent, QueryProgress, QueryState,
};
use qcli_metadata::MetadataService;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};
use tokio::sync::{broadcast, mpsc};
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
    pub max_prepared_statements: usize,
    pub prepared_statement_ttl: Duration,
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
            max_prepared_statements: 128,
            prepared_statement_ttl: Duration::from_secs(15 * 60),
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct QueryStatus {
    pub id: String,
    pub session_id: String,
    pub session_version: u64,
    pub target: String,
    pub engine: String,
    pub engine_query_id: Option<String>,
    pub state: String,
    pub rows: usize,
    pub retained_bytes: usize,
    pub error: Option<QueryError>,
}

pub struct ResultPage {
    pub batches: Vec<RecordBatch>,
    pub total_rows: usize,
    pub next_offset: Option<usize>,
}

pub struct ResultBatchReader {
    source: ResultBatchReaderSource,
    skip: usize,
    remaining: Option<usize>,
}

enum ResultBatchReaderSource {
    Memory(std::vec::IntoIter<RecordBatch>),
    Spill(FileReader<std::fs::File>),
    Shared(FileReader<std::io::Cursor<Bytes>>),
}

impl ResultBatchReader {
    #[must_use]
    pub fn schema(&self) -> SchemaRef {
        match &self.source {
            ResultBatchReaderSource::Memory(batches) => batches
                .as_slice()
                .first()
                .map_or_else(|| Arc::new(Schema::empty()), RecordBatch::schema),
            ResultBatchReaderSource::Spill(reader) => reader.schema(),
            ResultBatchReaderSource::Shared(reader) => reader.schema(),
        }
    }

    /// Read the next retained Arrow batch without buffering later batches.
    ///
    /// # Errors
    ///
    /// Returns a structured storage error if a spilled Arrow IPC result cannot
    /// be decoded.
    pub fn next_batch(&mut self) -> Result<Option<RecordBatch>, ServiceError> {
        while self.skip > 0 {
            let skipped = match &mut self.source {
                ResultBatchReaderSource::Memory(batches) => batches.next(),
                ResultBatchReaderSource::Spill(batches) => {
                    batches.next().transpose().map_err(storage_service_error)?
                }
                ResultBatchReaderSource::Shared(batches) => {
                    batches.next().transpose().map_err(storage_service_error)?
                }
            };
            if skipped.is_none() {
                return Ok(None);
            }
            self.skip -= 1;
        }
        if self.remaining == Some(0) {
            return Ok(None);
        }
        let batch = match &mut self.source {
            ResultBatchReaderSource::Memory(batches) => batches.next(),
            ResultBatchReaderSource::Spill(batches) => {
                batches.next().transpose().map_err(storage_service_error)?
            }
            ResultBatchReaderSource::Shared(batches) => {
                batches.next().transpose().map_err(storage_service_error)?
            }
        };
        if let (Some(_), Some(remaining)) = (batch.as_ref(), self.remaining.as_mut()) {
            *remaining -= 1;
        }
        Ok(batch)
    }
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

#[derive(Clone, Debug)]
pub struct PreparedStatementSnapshot {
    pub handle: String,
    pub session_id: String,
    pub sql: String,
    pub dataset_schema: SchemaRef,
    pub parameter_schema: SchemaRef,
    pub parameters: Vec<RecordBatch>,
}

struct PreparedStatementRecord {
    owner: String,
    session_id: String,
    owns_session: bool,
    sql: String,
    dataset_schema: SchemaRef,
    parameter_schema: SchemaRef,
    parameters: Vec<RecordBatch>,
    last_access: Instant,
}

struct QueryData {
    state: String,
    engine_query_id: Option<String>,
    rows: usize,
    retained_bytes: usize,
    schema: Option<SchemaRef>,
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
    metadata: Arc<MetadataService>,
    session_owners: Mutex<HashMap<String, SessionOwner>>,
    records: Mutex<HashMap<String, Arc<QueryRecord>>>,
    prepared: Mutex<HashMap<String, PreparedStatementRecord>>,
    limits: ServiceLimits,
    audit: Mutex<Arc<dyn AuditSink>>,
    shutting_down: AtomicBool,
    cluster: Option<ClusterContext>,
}

#[derive(Clone)]
struct ClusterContext {
    node_id: String,
    state: Arc<dyn ClusterStateStore>,
    objects: Arc<dyn ResultObjectStore>,
    lease_ttl: Duration,
}

#[derive(Serialize, Deserialize)]
struct SharedSession {
    snapshot: SessionSnapshot,
    quota_permit: String,
}

#[derive(Serialize, Deserialize)]
struct SharedQuery {
    status: QueryStatus,
    result_key: Option<String>,
    batch_count: usize,
    fencing_token: i64,
}

#[derive(Serialize, Deserialize)]
struct SharedPrepared {
    session_id: String,
    sql: String,
    parameters_key: Option<String>,
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
        let adapters = adapters.into_iter().collect::<Vec<_>>();
        Self {
            state: Arc::new(ServiceState {
                config: Arc::new(config),
                sessions: Arc::new(SessionManager::default()),
                queries: Arc::new(QueryService::new(adapters.clone(), 8)),
                metadata: Arc::new(MetadataService::new(adapters, Duration::from_secs(30))),
                session_owners: Mutex::new(HashMap::new()),
                records: Mutex::new(HashMap::new()),
                prepared: Mutex::new(HashMap::new()),
                limits,
                audit: Mutex::new(Arc::new(NullAuditSink)),
                shutting_down: AtomicBool::new(false),
                cluster: None,
            }),
        }
    }

    #[must_use]
    pub fn with_cluster(
        self,
        node_id: impl Into<String>,
        state: Arc<dyn ClusterStateStore>,
        objects: Arc<dyn ResultObjectStore>,
        lease_ttl: Duration,
    ) -> Self {
        let mut service_state = Arc::try_unwrap(self.state)
            .unwrap_or_else(|_| panic!("cluster must be configured before cloning the gateway"));
        service_state.cluster = Some(ClusterContext {
            node_id: node_id.into(),
            state,
            objects,
            lease_ttl,
        });
        Self {
            state: Arc::new(service_state),
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
    pub fn metadata(&self) -> Arc<MetadataService> {
        Arc::clone(&self.state.metadata)
    }

    pub fn session_metadata_request(
        &self,
        principal: &AuthenticatedPrincipal,
        session_id: &str,
        catalog: Option<String>,
        schema: Option<String>,
        pattern: Option<String>,
    ) -> Result<MetadataRequest, ServiceError> {
        let snapshot = self.session(principal, session_id)?;
        Ok(metadata_request(
            principal,
            snapshot.target,
            snapshot.engine,
            snapshot.properties,
            catalog,
            schema,
            pattern,
        ))
    }

    pub fn target_metadata_request(
        &self,
        principal: &AuthenticatedPrincipal,
        target_name: &str,
        catalog: Option<String>,
        schema: Option<String>,
        pattern: Option<String>,
    ) -> Result<MetadataRequest, ServiceError> {
        let target = self.authorized_target(principal, target_name)?;
        let properties = target
            .properties
            .iter()
            .map(|(name, value)| (name.clone(), value.expose().to_owned()))
            .collect();
        Ok(metadata_request(
            principal,
            target.name,
            target.engine,
            properties,
            catalog,
            schema,
            pattern,
        ))
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

    pub async fn create_session_clustered(
        &self,
        principal: &AuthenticatedPrincipal,
        target_name: &str,
        overrides: BTreeMap<String, String>,
    ) -> Result<SessionSnapshot, ServiceError> {
        let Some(cluster) = &self.state.cluster else {
            return self.create_session(principal, target_name, overrides);
        };
        self.ensure_available()?;
        let permit = cluster
            .state
            .acquire_quota(
                &principal.id,
                "sessions",
                principal.max_sessions,
                self.state.limits.session_ttl,
            )
            .await
            .map_err(cluster_service_error)?;
        let snapshot = match self.create_session(principal, target_name, overrides) {
            Ok(snapshot) => snapshot,
            Err(error) => {
                cluster.state.release_quota(&permit).await.ok();
                return Err(error);
            }
        };
        let resource = SharedResource {
            resource_id: snapshot.id.clone(),
            principal_id: principal.id.clone(),
            kind: "session".into(),
            version: 0,
            payload: serde_json::to_value(SharedSession {
                snapshot: snapshot.clone(),
                quota_permit: permit.clone(),
            })
            .map_err(json_service_error)?,
            expires_at: chrono_deadline(self.state.limits.session_ttl),
        };
        if let Err(error) = cluster.state.put_resource(resource, Some(0)).await {
            self.close_session(principal, &snapshot.id).ok();
            cluster.state.release_quota(&permit).await.ok();
            return Err(cluster_service_error(error));
        }
        Ok(snapshot)
    }

    pub async fn session_clustered(
        &self,
        principal: &AuthenticatedPrincipal,
        session_id: &str,
    ) -> Result<SessionSnapshot, ServiceError> {
        if let Ok(snapshot) = self.session(principal, session_id) {
            return Ok(snapshot);
        }
        let cluster = self.state.cluster.as_ref().ok_or_else(session_not_found)?;
        let resource = cluster
            .state
            .get_resource("session", session_id, &principal.id)
            .await
            .map_err(cluster_service_error)?
            .ok_or_else(session_not_found)?;
        let shared = serde_json::from_value::<SharedSession>(resource.payload)
            .map_err(json_service_error)?;
        let target = self
            .state
            .config
            .target(&shared.snapshot.target)
            .cloned()
            .ok_or_else(session_not_found)?;
        self.state
            .sessions
            .restore(shared.snapshot.clone(), target)
            .map_err(|error| core_error(&error))?;
        self.state
            .session_owners
            .lock()
            .expect("session owner mutex poisoned")
            .insert(
                shared.snapshot.id.clone(),
                SessionOwner {
                    principal: principal.id.clone(),
                    last_access: Instant::now(),
                },
            );
        Ok(shared.snapshot)
    }

    pub async fn submit_session_query_clustered(
        &self,
        principal: &AuthenticatedPrincipal,
        session_id: &str,
        sql: String,
    ) -> Result<QueryStatus, ServiceError> {
        let snapshot = self.session_clustered(principal, session_id).await?;
        let status = self.submit_query(principal, &snapshot, sql, None, false)?;
        self.publish_cluster_query(principal, &status).await?;
        Ok(status)
    }

    pub async fn submit_stateless_query_clustered(
        &self,
        principal: &AuthenticatedPrincipal,
        target_name: &str,
        overrides: BTreeMap<String, String>,
        sql: String,
    ) -> Result<QueryStatus, ServiceError> {
        let session = self
            .create_session_clustered(principal, target_name, overrides)
            .await?;
        let status = self.submit_query(principal, &session, sql, None, true)?;
        self.publish_cluster_query(principal, &status).await?;
        Ok(status)
    }

    async fn publish_cluster_query(
        &self,
        principal: &AuthenticatedPrincipal,
        status: &QueryStatus,
    ) -> Result<(), ServiceError> {
        let Some(cluster) = &self.state.cluster else {
            return Ok(());
        };
        let lease = cluster
            .state
            .claim_query(
                &status.id,
                &principal.id,
                &cluster.node_id,
                cluster.lease_ttl,
            )
            .await
            .map_err(cluster_service_error)?;
        put_shared_query(cluster, principal, status.clone(), None, 0, &lease).await?;
        let service = self.clone();
        let principal = principal.clone();
        let query_id = status.id.clone();
        tokio::spawn(async move {
            service
                .publish_query_completion(principal, query_id, lease)
                .await;
        });
        Ok(())
    }

    async fn publish_query_completion(
        &self,
        principal: AuthenticatedPrincipal,
        query_id: String,
        mut lease: QueryLease,
    ) {
        let Some(cluster) = &self.state.cluster else {
            return;
        };
        loop {
            tokio::time::sleep(Duration::from_millis(10)).await;
            let Ok(mut status) = self.query(&principal, &query_id) else {
                return;
            };
            if matches!(status.state.as_str(), "completed" | "failed" | "cancelled") {
                let mut result_key = None;
                let mut batch_count = 0;
                let retained_result = if status.state == "completed" {
                    self.result_reader(&principal, &query_id)
                        .ok()
                        .and_then(|mut reader| encode_reader(&mut reader).ok())
                } else {
                    None
                };
                if let Some((bytes, batches)) = retained_result {
                    let key = format!(
                        "{}/{}.arrow",
                        principal_object_prefix(&principal.id),
                        query_id
                    );
                    if cluster.objects.put(&key, bytes).await.is_ok() {
                        result_key = Some(key);
                        batch_count = batches;
                    } else {
                        status.state = "failed".into();
                        status.error = Some(QueryError {
                            code: "shared_result_store".into(),
                            message: "query completed but shared result retention failed".into(),
                        });
                    }
                }
                put_shared_query(cluster, &principal, status, result_key, batch_count, &lease)
                    .await
                    .ok();
                cluster.state.release_query(&lease).await.ok();
                return;
            }
            put_shared_query(cluster, &principal, status, None, 0, &lease)
                .await
                .ok();
            if let Ok(renewed) = cluster.state.renew_query(&lease, cluster.lease_ttl).await {
                lease = renewed;
            } else {
                return;
            }
        }
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

    pub async fn update_session_clustered(
        &self,
        principal: &AuthenticatedPrincipal,
        session_id: &str,
        expected_version: u64,
        overrides: BTreeMap<String, String>,
    ) -> Result<SessionSnapshot, ServiceError> {
        self.mutate_session_clustered(
            principal,
            session_id,
            expected_version,
            None,
            overrides
                .into_iter()
                .map(|(name, value)| (name, Some(value)))
                .collect(),
        )
        .await
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

    pub async fn switch_target_clustered(
        &self,
        principal: &AuthenticatedPrincipal,
        session_id: &str,
        expected_version: u64,
        target_name: &str,
    ) -> Result<SessionSnapshot, ServiceError> {
        self.mutate_session_clustered(
            principal,
            session_id,
            expected_version,
            Some(target_name),
            BTreeMap::new(),
        )
        .await
    }

    pub fn mutate_session(
        &self,
        principal: &AuthenticatedPrincipal,
        session_id: &str,
        expected_version: u64,
        target_name: Option<&str>,
        overrides: BTreeMap<String, Option<String>>,
    ) -> Result<SessionSnapshot, ServiceError> {
        self.ensure_available()?;
        self.require_session_owner(principal, session_id)?;
        let target = target_name
            .map(|name| self.authorized_target(principal, name))
            .transpose()?;
        self.state
            .sessions
            .mutate(session_id, expected_version, target, overrides)
            .map_err(|error| core_error(&error))
    }

    pub async fn mutate_session_clustered(
        &self,
        principal: &AuthenticatedPrincipal,
        session_id: &str,
        expected_version: u64,
        target_name: Option<&str>,
        overrides: BTreeMap<String, Option<String>>,
    ) -> Result<SessionSnapshot, ServiceError> {
        self.session_clustered(principal, session_id).await?;
        let snapshot = self.mutate_session(
            principal,
            session_id,
            expected_version,
            target_name,
            overrides,
        )?;
        if let Some(cluster) = &self.state.cluster {
            let resource = cluster
                .state
                .get_resource("session", session_id, &principal.id)
                .await
                .map_err(cluster_service_error)?
                .ok_or_else(session_not_found)?;
            let mut shared: SharedSession =
                serde_json::from_value(resource.payload.clone()).map_err(json_service_error)?;
            shared.snapshot = snapshot.clone();
            cluster
                .state
                .put_resource(
                    SharedResource {
                        payload: serde_json::to_value(shared).map_err(json_service_error)?,
                        expires_at: chrono_deadline(self.state.limits.session_ttl),
                        ..resource.clone()
                    },
                    Some(resource.version),
                )
                .await
                .map_err(cluster_service_error)?;
        }
        Ok(snapshot)
    }

    pub fn close_session(
        &self,
        principal: &AuthenticatedPrincipal,
        session_id: &str,
    ) -> Result<(), ServiceError> {
        self.require_session_owner(principal, session_id)?;
        self.state
            .records
            .lock()
            .expect("query registry mutex poisoned")
            .values()
            .filter(|record| record.session_id == session_id)
            .for_each(|record| record.cancel.cancel());
        self.state
            .sessions
            .close(session_id)
            .map_err(|error| core_error(&error))?;
        self.state
            .session_owners
            .lock()
            .expect("session owner mutex poisoned")
            .remove(session_id);
        self.state
            .prepared
            .lock()
            .expect("prepared mutex poisoned")
            .retain(|_, record| record.session_id != session_id);
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

    pub async fn close_session_clustered(
        &self,
        principal: &AuthenticatedPrincipal,
        session_id: &str,
    ) -> Result<(), ServiceError> {
        self.session_clustered(principal, session_id).await?;
        let shared = if let Some(cluster) = &self.state.cluster {
            cluster
                .state
                .get_resource("session", session_id, &principal.id)
                .await
                .map_err(cluster_service_error)?
                .and_then(|resource| serde_json::from_value::<SharedSession>(resource.payload).ok())
        } else {
            None
        };
        self.close_session(principal, session_id)?;
        if let Some(cluster) = &self.state.cluster {
            cluster
                .state
                .delete_resource("session", session_id, &principal.id)
                .await
                .map_err(cluster_service_error)?;
            if let Some(shared) = shared {
                cluster.state.release_quota(&shared.quota_permit).await.ok();
            }
        }
        Ok(())
    }

    pub fn submit_session_query(
        &self,
        principal: &AuthenticatedPrincipal,
        session_id: &str,
        sql: String,
    ) -> Result<QueryStatus, ServiceError> {
        let snapshot = self.session(principal, session_id)?;
        self.submit_query(principal, &snapshot, sql, None, false)
    }

    pub fn submit_stateless_query(
        &self,
        principal: &AuthenticatedPrincipal,
        target_name: &str,
        overrides: BTreeMap<String, String>,
        sql: String,
    ) -> Result<QueryStatus, ServiceError> {
        let snapshot = self.create_session(principal, target_name, overrides)?;
        match self.submit_query(principal, &snapshot, sql, None, true) {
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

    pub async fn create_prepared_statement(
        &self,
        principal: &AuthenticatedPrincipal,
        session_id: &str,
        sql: String,
    ) -> Result<PreparedStatementSnapshot, ServiceError> {
        self.ensure_available()?;
        if sql.is_empty() || sql.len() > self.state.limits.max_sql_bytes {
            return Err(ServiceError::new(
                ServiceErrorKind::InvalidArgument,
                "invalid_sql_size",
                "prepared SQL has an invalid size",
            ));
        }
        let session = self.session(principal, session_id)?;
        let capabilities = self
            .state
            .metadata
            .capabilities(&session.engine)
            .ok_or_else(|| {
                ServiceError::new(
                    ServiceErrorKind::InvalidArgument,
                    "adapter_not_found",
                    "adapter capabilities are unavailable",
                )
            })?;
        if !capabilities.supports(AdapterCapability::PreparedStatements) {
            return Err(ServiceError::new(
                ServiceErrorKind::FailedPrecondition,
                "unsupported_capability",
                "prepared statements are not supported by this adapter",
            ));
        }
        let metadata = self
            .state
            .queries
            .prepare(session.clone(), sql.clone())
            .await
            .map_err(|error| core_error(&error))?;
        self.cleanup_expired();
        let mut prepared = self.state.prepared.lock().expect("prepared mutex poisoned");
        if prepared.len() >= self.state.limits.max_prepared_statements {
            return Err(ServiceError::new(
                ServiceErrorKind::ResourceExhausted,
                "prepared_limit",
                "prepared statement limit reached",
            ));
        }
        let handle = Uuid::new_v4().simple().to_string();
        prepared.insert(
            handle.clone(),
            PreparedStatementRecord {
                owner: principal.id.clone(),
                session_id: session_id.into(),
                owns_session: false,
                sql,
                dataset_schema: metadata.dataset_schema,
                parameter_schema: metadata.parameter_schema,
                parameters: Vec::new(),
                last_access: Instant::now(),
            },
        );
        Ok(prepared_snapshot(
            &handle,
            prepared.get(&handle).expect("new prepared handle exists"),
        ))
    }

    pub async fn create_stateless_prepared_statement(
        &self,
        principal: &AuthenticatedPrincipal,
        target_name: &str,
        sql: String,
    ) -> Result<PreparedStatementSnapshot, ServiceError> {
        let session = self.create_session(principal, target_name, BTreeMap::new())?;
        match self
            .create_prepared_statement(principal, &session.id, sql)
            .await
        {
            Ok(snapshot) => {
                self.state
                    .prepared
                    .lock()
                    .expect("prepared mutex poisoned")
                    .get_mut(&snapshot.handle)
                    .expect("new prepared handle exists")
                    .owns_session = true;
                Ok(snapshot)
            }
            Err(error) => {
                self.close_session(principal, &session.id).ok();
                Err(error)
            }
        }
    }

    pub async fn create_prepared_statement_clustered(
        &self,
        principal: &AuthenticatedPrincipal,
        session_id: &str,
        sql: String,
    ) -> Result<PreparedStatementSnapshot, ServiceError> {
        self.session_clustered(principal, session_id).await?;
        let snapshot = self
            .create_prepared_statement(principal, session_id, sql.clone())
            .await?;
        if let Some(cluster) = &self.state.cluster {
            cluster
                .state
                .put_resource(
                    SharedResource {
                        resource_id: snapshot.handle.clone(),
                        principal_id: principal.id.clone(),
                        kind: "prepared".into(),
                        version: 0,
                        payload: serde_json::to_value(SharedPrepared {
                            session_id: session_id.into(),
                            sql,
                            parameters_key: None,
                        })
                        .map_err(json_service_error)?,
                        expires_at: chrono_deadline(self.state.limits.prepared_statement_ttl),
                    },
                    Some(0),
                )
                .await
                .map_err(cluster_service_error)?;
        }
        Ok(snapshot)
    }

    pub async fn prepared_statement_clustered(
        &self,
        principal: &AuthenticatedPrincipal,
        handle: &str,
    ) -> Result<PreparedStatementSnapshot, ServiceError> {
        if let Ok(snapshot) = self.prepared_statement(principal, handle) {
            return Ok(snapshot);
        }
        let cluster = self.state.cluster.as_ref().ok_or_else(prepared_not_found)?;
        let resource = cluster
            .state
            .get_resource("prepared", handle, &principal.id)
            .await
            .map_err(cluster_service_error)?
            .ok_or_else(prepared_not_found)?;
        let shared: SharedPrepared =
            serde_json::from_value(resource.payload).map_err(json_service_error)?;
        let session = self
            .session_clustered(principal, &shared.session_id)
            .await?;
        let metadata = self
            .state
            .queries
            .prepare(session, shared.sql.clone())
            .await
            .map_err(|error| core_error(&error))?;
        let parameters = if let Some(key) = shared.parameters_key {
            let bytes = cluster
                .objects
                .get(&key)
                .await
                .map_err(cluster_service_error)?
                .ok_or_else(prepared_not_found)?;
            decode_batches(bytes)?
        } else {
            Vec::new()
        };
        let record = PreparedStatementRecord {
            owner: principal.id.clone(),
            session_id: shared.session_id,
            owns_session: false,
            sql: shared.sql,
            dataset_schema: metadata.dataset_schema,
            parameter_schema: metadata.parameter_schema,
            parameters,
            last_access: Instant::now(),
        };
        let snapshot = prepared_snapshot(handle, &record);
        self.state
            .prepared
            .lock()
            .expect("prepared mutex poisoned")
            .insert(handle.into(), record);
        Ok(snapshot)
    }

    pub async fn bind_prepared_statement_clustered(
        &self,
        principal: &AuthenticatedPrincipal,
        handle: &str,
        parameters: Vec<RecordBatch>,
    ) -> Result<PreparedStatementSnapshot, ServiceError> {
        self.prepared_statement_clustered(principal, handle).await?;
        let snapshot = self.bind_prepared_statement(principal, handle, parameters.clone())?;
        if let Some(cluster) = &self.state.cluster {
            let key = format!(
                "{}/prepared/{handle}.arrow",
                principal_object_prefix(&principal.id)
            );
            cluster
                .objects
                .put(&key, encode_batches(&parameters)?)
                .await
                .map_err(cluster_service_error)?;
            let resource = cluster
                .state
                .get_resource("prepared", handle, &principal.id)
                .await
                .map_err(cluster_service_error)?
                .ok_or_else(prepared_not_found)?;
            let mut shared: SharedPrepared =
                serde_json::from_value(resource.payload.clone()).map_err(json_service_error)?;
            shared.parameters_key = Some(key);
            cluster
                .state
                .put_resource(
                    SharedResource {
                        payload: serde_json::to_value(shared).map_err(json_service_error)?,
                        expires_at: chrono_deadline(self.state.limits.prepared_statement_ttl),
                        ..resource.clone()
                    },
                    Some(resource.version),
                )
                .await
                .map_err(cluster_service_error)?;
        }
        Ok(snapshot)
    }

    pub async fn execute_prepared_statement_clustered(
        &self,
        principal: &AuthenticatedPrincipal,
        handle: &str,
    ) -> Result<QueryStatus, ServiceError> {
        let prepared = self.prepared_statement_clustered(principal, handle).await?;
        let snapshot = self
            .session_clustered(principal, &prepared.session_id)
            .await?;
        let status = self.submit_query(
            principal,
            &snapshot,
            prepared.sql,
            Some(prepared.parameters),
            false,
        )?;
        self.publish_cluster_query(principal, &status).await?;
        Ok(status)
    }

    pub fn bind_prepared_statement(
        &self,
        principal: &AuthenticatedPrincipal,
        handle: &str,
        parameters: Vec<RecordBatch>,
    ) -> Result<PreparedStatementSnapshot, ServiceError> {
        self.cleanup_expired();
        let mut prepared = self.state.prepared.lock().expect("prepared mutex poisoned");
        let record = prepared
            .get_mut(handle)
            .filter(|record| record.owner == principal.id)
            .ok_or_else(prepared_not_found)?;
        let session = self.session(principal, &record.session_id)?;
        let capabilities = self
            .state
            .metadata
            .capabilities(&session.engine)
            .ok_or_else(prepared_not_found)?;
        if !capabilities.supports(AdapterCapability::TypedParameters) {
            return Err(ServiceError::new(
                ServiceErrorKind::FailedPrecondition,
                "unsupported_capability",
                "native typed parameter binding is not supported by this adapter",
            ));
        }
        if let Some(first) = parameters.first()
            && parameters
                .iter()
                .any(|batch| batch.schema() != first.schema())
        {
            return Err(ServiceError::new(
                ServiceErrorKind::InvalidArgument,
                "parameter_schema_mismatch",
                "all parameter batches must have the same Arrow schema",
            ));
        }
        record.parameter_schema = parameters
            .first()
            .map_or_else(|| Arc::new(Schema::empty()), RecordBatch::schema);
        record.parameters = parameters;
        record.last_access = Instant::now();
        Ok(prepared_snapshot(handle, record))
    }

    pub fn prepared_statement(
        &self,
        principal: &AuthenticatedPrincipal,
        handle: &str,
    ) -> Result<PreparedStatementSnapshot, ServiceError> {
        self.cleanup_expired();
        let mut prepared = self.state.prepared.lock().expect("prepared mutex poisoned");
        let record = prepared
            .get_mut(handle)
            .filter(|record| record.owner == principal.id)
            .ok_or_else(prepared_not_found)?;
        record.last_access = Instant::now();
        Ok(prepared_snapshot(handle, record))
    }

    pub fn execute_prepared_statement(
        &self,
        principal: &AuthenticatedPrincipal,
        handle: &str,
    ) -> Result<QueryStatus, ServiceError> {
        let prepared = self.prepared_statement(principal, handle)?;
        let snapshot = self.session(principal, &prepared.session_id)?;
        self.submit_query(
            principal,
            &snapshot,
            prepared.sql,
            Some(prepared.parameters),
            false,
        )
    }

    pub async fn execute_prepared_update(
        &self,
        principal: &AuthenticatedPrincipal,
        handle: &str,
    ) -> Result<i64, ServiceError> {
        let prepared = self.prepared_statement(principal, handle)?;
        let snapshot = self.session(principal, &prepared.session_id)?;
        self.state
            .queries
            .execute_prepared_update(snapshot, prepared.sql, prepared.parameters)
            .await
            .map_err(|error| core_error(&error))
    }

    pub async fn execute_prepared_update_clustered(
        &self,
        principal: &AuthenticatedPrincipal,
        handle: &str,
    ) -> Result<i64, ServiceError> {
        self.prepared_statement_clustered(principal, handle).await?;
        self.execute_prepared_update(principal, handle).await
    }

    pub async fn execute_session_update(
        &self,
        principal: &AuthenticatedPrincipal,
        session_id: &str,
        sql: String,
    ) -> Result<i64, ServiceError> {
        let snapshot = self.session(principal, session_id)?;
        self.state
            .queries
            .execute_update(snapshot, sql)
            .await
            .map_err(|error| core_error(&error))
    }

    pub async fn ingest(
        &self,
        principal: &AuthenticatedPrincipal,
        session_id: &str,
        request: IngestRequest,
        batches: mpsc::Receiver<RecordBatch>,
        cancellation: CancellationSignal,
    ) -> Result<i64, ServiceError> {
        self.ensure_available()?;
        if request.table.trim().is_empty() || request.table.len() > 1024 {
            return Err(ServiceError::new(
                ServiceErrorKind::InvalidArgument,
                "invalid_ingest_table",
                "ingestion table must contain between 1 and 1024 bytes",
            ));
        }
        let snapshot = self.session(principal, session_id)?;
        let capabilities = self
            .state
            .metadata
            .capabilities(&snapshot.engine)
            .ok_or_else(|| {
                ServiceError::new(
                    ServiceErrorKind::InvalidArgument,
                    "adapter_not_found",
                    "adapter capabilities are unavailable",
                )
            })?;
        if !capabilities.supports(AdapterCapability::BulkIngest) {
            return Err(ServiceError::new(
                ServiceErrorKind::FailedPrecondition,
                "unsupported_capability",
                "Arrow bulk ingestion is not supported by this adapter",
            ));
        }
        let target = snapshot.target.clone();
        self.audit(
            "ingest.start",
            "allowed",
            Some(principal),
            Some(&target),
            Some(session_id),
            None,
        );
        let result = self
            .state
            .queries
            .ingest(
                snapshot,
                request,
                IngestSource {
                    batches,
                    cancellation,
                },
            )
            .await
            .map_err(|error| core_error(&error));
        self.audit(
            "ingest.finish",
            if result.is_ok() { "allowed" } else { "failed" },
            Some(principal),
            Some(&target),
            Some(session_id),
            None,
        );
        result
    }

    pub fn close_prepared_statement(
        &self,
        principal: &AuthenticatedPrincipal,
        handle: &str,
    ) -> Result<(), ServiceError> {
        let removed = {
            let mut prepared = self.state.prepared.lock().expect("prepared mutex poisoned");
            if prepared
                .get(handle)
                .is_some_and(|record| record.owner == principal.id)
            {
                prepared.remove(handle)
            } else {
                None
            }
        };
        let Some(record) = removed else {
            return Err(prepared_not_found());
        };
        if record.owns_session {
            self.close_session(principal, &record.session_id)?;
        }
        Ok(())
    }

    pub async fn close_prepared_statement_clustered(
        &self,
        principal: &AuthenticatedPrincipal,
        handle: &str,
    ) -> Result<(), ServiceError> {
        self.prepared_statement_clustered(principal, handle).await?;
        self.close_prepared_statement(principal, handle)?;
        if let Some(cluster) = &self.state.cluster {
            cluster
                .state
                .delete_resource("prepared", handle, &principal.id)
                .await
                .map_err(cluster_service_error)?;
        }
        Ok(())
    }

    pub fn query(
        &self,
        principal: &AuthenticatedPrincipal,
        query_id: &str,
    ) -> Result<QueryStatus, ServiceError> {
        let record = self.owned_record(principal, query_id)?;
        Ok(query_status(&record))
    }

    pub async fn query_clustered(
        &self,
        principal: &AuthenticatedPrincipal,
        query_id: &str,
    ) -> Result<QueryStatus, ServiceError> {
        if let Ok(status) = self.query(principal, query_id) {
            return Ok(status);
        }
        let cluster = self.state.cluster.as_ref().ok_or_else(query_not_found)?;
        let resource = cluster
            .state
            .get_resource("query", query_id, &principal.id)
            .await
            .map_err(cluster_service_error)?
            .ok_or_else(query_not_found)?;
        serde_json::from_value::<SharedQuery>(resource.payload)
            .map(|shared| shared.status)
            .map_err(json_service_error)
    }

    pub async fn recover_query_clustered(
        &self,
        principal: &AuthenticatedPrincipal,
        query_id: &str,
    ) -> Result<QueryStatus, ServiceError> {
        let cluster = self.state.cluster.as_ref().ok_or_else(query_not_found)?;
        let mut shared_resource = cluster
            .state
            .get_resource("query", query_id, &principal.id)
            .await
            .map_err(cluster_service_error)?
            .ok_or_else(query_not_found)?;
        let mut shared: SharedQuery =
            serde_json::from_value(shared_resource.payload.clone()).map_err(json_service_error)?;
        if matches!(
            shared.status.state.as_str(),
            "completed" | "failed" | "cancelled"
        ) {
            return Ok(shared.status);
        }
        let lease = cluster
            .state
            .claim_query(query_id, &principal.id, &cluster.node_id, cluster.lease_ttl)
            .await
            .map_err(cluster_service_error)?;
        shared.status.state = "failed".into();
        shared.status.error = Some(QueryError {
            code: "orphaned_query".into(),
            message: "the owning qcli node was lost and this adapter cannot reattach safely".into(),
        });
        shared.fencing_token = lease.fencing_token;
        shared_resource.payload = serde_json::to_value(&shared).map_err(json_service_error)?;
        shared_resource.expires_at = chrono_deadline(self.state.limits.result_ttl);
        cluster
            .state
            .put_resource(shared_resource.clone(), Some(shared_resource.version))
            .await
            .map_err(cluster_service_error)?;
        cluster.state.release_query(&lease).await.ok();
        Ok(shared.status)
    }

    pub async fn result_reader_clustered(
        &self,
        principal: &AuthenticatedPrincipal,
        query_id: &str,
    ) -> Result<ResultBatchReader, ServiceError> {
        if let Ok(reader) = self.result_reader(principal, query_id) {
            return Ok(reader);
        }
        let cluster = self.state.cluster.as_ref().ok_or_else(query_not_found)?;
        let resource = cluster
            .state
            .get_resource("query", query_id, &principal.id)
            .await
            .map_err(cluster_service_error)?
            .ok_or_else(query_not_found)?;
        let shared: SharedQuery =
            serde_json::from_value(resource.payload).map_err(json_service_error)?;
        if shared.status.state != "completed" {
            return Err(ServiceError::new(
                ServiceErrorKind::FailedPrecondition,
                "query_running",
                "results are available after query completion",
            ));
        }
        let key = shared.result_key.ok_or_else(|| {
            ServiceError::new(
                ServiceErrorKind::Internal,
                "shared_result_missing",
                "completed shared result is unavailable",
            )
        })?;
        let bytes = cluster
            .objects
            .get(&key)
            .await
            .map_err(cluster_service_error)?
            .ok_or_else(|| {
                ServiceError::new(
                    ServiceErrorKind::Internal,
                    "shared_result_missing",
                    "completed shared result is unavailable",
                )
            })?;
        let reader = FileReader::try_new(std::io::Cursor::new(bytes), None)
            .map_err(storage_service_error)?;
        Ok(ResultBatchReader {
            source: ResultBatchReaderSource::Shared(reader),
            skip: 0,
            remaining: None,
        })
    }

    pub async fn result_page_clustered(
        &self,
        principal: &AuthenticatedPrincipal,
        query_id: &str,
        offset: usize,
        limit: usize,
    ) -> Result<ResultPage, ServiceError> {
        if let Ok(page) = self.result_page(principal, query_id, offset, limit) {
            return Ok(page);
        }
        let status = self.query_clustered(principal, query_id).await?;
        let mut reader = self.result_reader_clustered(principal, query_id).await?;
        let mut batches = Vec::new();
        while let Some(batch) = reader.next_batch()? {
            batches.push(batch);
        }
        let start = offset.min(status.rows);
        let end = start.saturating_add(limit.max(1)).min(status.rows);
        Ok(ResultPage {
            batches: slice_batches(&batches, start, end),
            total_rows: status.rows,
            next_offset: (end < status.rows).then_some(end),
        })
    }

    pub async fn result_partition_reader_clustered(
        &self,
        principal: &AuthenticatedPrincipal,
        query_id: &str,
        partition: usize,
        partitions: usize,
    ) -> Result<ResultBatchReader, ServiceError> {
        if let Ok(reader) = self.result_partition_reader(principal, query_id, partition, partitions)
        {
            return Ok(reader);
        }
        if partitions == 0 || partition >= partitions {
            return Err(ServiceError::new(
                ServiceErrorKind::InvalidArgument,
                "invalid_result_partition",
                "result partition is outside the advertised range",
            ));
        }
        let cluster = self.state.cluster.as_ref().ok_or_else(query_not_found)?;
        let resource = cluster
            .state
            .get_resource("query", query_id, &principal.id)
            .await
            .map_err(cluster_service_error)?
            .ok_or_else(query_not_found)?;
        let shared: SharedQuery =
            serde_json::from_value(resource.payload).map_err(json_service_error)?;
        let key = shared.result_key.ok_or_else(|| {
            ServiceError::new(
                ServiceErrorKind::FailedPrecondition,
                "query_running",
                "results are available after query completion",
            )
        })?;
        let bytes = cluster
            .objects
            .get(&key)
            .await
            .map_err(cluster_service_error)?
            .ok_or_else(query_not_found)?;
        let source = ResultBatchReaderSource::Shared(
            FileReader::try_new(std::io::Cursor::new(bytes), None)
                .map_err(storage_service_error)?,
        );
        let start = shared.batch_count.saturating_mul(partition) / partitions;
        let end = shared.batch_count.saturating_mul(partition + 1) / partitions;
        Ok(ResultBatchReader {
            source,
            skip: start,
            remaining: Some(end - start),
        })
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

    pub fn result_batch_count(
        &self,
        principal: &AuthenticatedPrincipal,
        query_id: &str,
    ) -> Result<usize, ServiceError> {
        let record = self.owned_record(principal, query_id)?;
        let data = record.data.lock().expect("query record mutex poisoned");
        if data.state != "completed" {
            return Err(ServiceError::new(
                ServiceErrorKind::FailedPrecondition,
                "query_running",
                "query must complete before partition discovery",
            ));
        }
        match &data.storage {
            ResultStorage::Memory(batches) => Ok(batches.len()),
            ResultStorage::Spill { path, writer } => {
                if writer.is_some() {
                    return Err(ServiceError::new(
                        ServiceErrorKind::FailedPrecondition,
                        "query_running",
                        "query result is still being written",
                    ));
                }
                let file = std::fs::File::open(path).map_err(storage_service_error)?;
                let reader = FileReader::try_new(file, None).map_err(storage_service_error)?;
                Ok(reader.count())
            }
        }
    }

    pub fn result_reader(
        &self,
        principal: &AuthenticatedPrincipal,
        query_id: &str,
    ) -> Result<ResultBatchReader, ServiceError> {
        self.result_reader_range(principal, query_id, 0, None)
    }

    pub fn result_partition_reader(
        &self,
        principal: &AuthenticatedPrincipal,
        query_id: &str,
        partition: usize,
        partitions: usize,
    ) -> Result<ResultBatchReader, ServiceError> {
        if partitions == 0 || partition >= partitions {
            return Err(ServiceError::new(
                ServiceErrorKind::InvalidArgument,
                "invalid_result_partition",
                "result partition is outside the advertised range",
            ));
        }
        let batches = self.result_batch_count(principal, query_id)?;
        let start = batches.saturating_mul(partition) / partitions;
        let end = batches.saturating_mul(partition + 1) / partitions;
        self.result_reader_range(principal, query_id, start, Some(end - start))
    }

    fn result_reader_range(
        &self,
        principal: &AuthenticatedPrincipal,
        query_id: &str,
        skip: usize,
        remaining: Option<usize>,
    ) -> Result<ResultBatchReader, ServiceError> {
        let record = self.owned_record(principal, query_id)?;
        let data = record.data.lock().expect("query record mutex poisoned");
        if let Some(error) = &data.error {
            return Err(ServiceError::new(
                ServiceErrorKind::FailedPrecondition,
                error.code.clone(),
                error.message.clone(),
            ));
        }
        if !matches!(data.state.as_str(), "completed" | "cancelled") {
            return Err(ServiceError::new(
                ServiceErrorKind::FailedPrecondition,
                "query_running",
                "results are available after query completion",
            ));
        }
        let source = match &data.storage {
            ResultStorage::Memory(batches) => {
                ResultBatchReaderSource::Memory(batches.clone().into_iter())
            }
            ResultStorage::Spill { path, writer: None } => {
                let file = std::fs::File::open(path).map_err(storage_service_error)?;
                ResultBatchReaderSource::Spill(
                    FileReader::try_new(file, None).map_err(storage_service_error)?,
                )
            }
            ResultStorage::Spill {
                writer: Some(_), ..
            } => {
                return Err(ServiceError::new(
                    ServiceErrorKind::FailedPrecondition,
                    "query_running",
                    "results are available after query completion",
                ));
            }
        };
        Ok(ResultBatchReader {
            source,
            skip,
            remaining,
        })
    }

    pub fn query_schema(
        &self,
        principal: &AuthenticatedPrincipal,
        query_id: &str,
    ) -> Result<SchemaRef, ServiceError> {
        let record = self.owned_record(principal, query_id)?;
        let data = record.data.lock().expect("query record mutex poisoned");
        if let Some(error) = &data.error {
            return Err(ServiceError::new(
                ServiceErrorKind::Upstream,
                error.code.clone(),
                error.message.clone(),
            ));
        }
        if let Some(schema) = &data.schema {
            return Ok(schema.clone());
        }
        if matches!(data.state.as_str(), "completed" | "cancelled" | "failed") {
            return Ok(Arc::new(Schema::empty()));
        }
        Err(ServiceError::new(
            ServiceErrorKind::FailedPrecondition,
            "query_running",
            "query schema is not available yet",
        ))
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
        let expired_prepared_sessions = {
            let mut expired = Vec::new();
            self.state
                .prepared
                .lock()
                .expect("prepared mutex poisoned")
                .retain(|_, record| {
                    let keep = now.duration_since(record.last_access)
                        < self.state.limits.prepared_statement_ttl;
                    if !keep && record.owns_session {
                        expired.push((record.session_id.clone(), record.owner.clone()));
                    }
                    keep
                });
            expired
        };
        for (session_id, principal) in expired_prepared_sessions {
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
                    outcome: "prepared_handle_expired".into(),
                    principal: Some(principal),
                    target: None,
                    session_id: Some(session_id),
                    query_id: None,
                });
        }
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
        parameters: Option<Vec<RecordBatch>>,
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
        let handle = match parameters {
            Some(parameters) => {
                self.state
                    .queries
                    .submit_prepared(snapshot.clone(), sql, parameters)
            }
            None => self.state.queries.submit(snapshot.clone(), sql),
        }
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
                schema: None,
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
        retained_bytes: data.retained_bytes,
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
        let state = (event == "state")
            .then(|| value.get("state").and_then(Value::as_str))
            .flatten();
        if let Some(state) = state {
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

async fn put_shared_query(
    cluster: &ClusterContext,
    principal: &AuthenticatedPrincipal,
    status: QueryStatus,
    result_key: Option<String>,
    batch_count: usize,
    lease: &QueryLease,
) -> Result<(), ServiceError> {
    let resource = SharedResource {
        resource_id: status.id.clone(),
        principal_id: principal.id.clone(),
        kind: "query".into(),
        version: 0,
        payload: serde_json::to_value(SharedQuery {
            status,
            result_key,
            batch_count,
            fencing_token: lease.fencing_token,
        })
        .map_err(json_service_error)?,
        expires_at: chrono_deadline(Duration::from_secs(15 * 60)),
    };
    let current = cluster
        .state
        .get_resource("query", &resource.resource_id, &principal.id)
        .await
        .map_err(cluster_service_error)?;
    cluster
        .state
        .put_resource(resource, current.map(|value| value.version).or(Some(0)))
        .await
        .map_err(cluster_service_error)?;
    Ok(())
}

fn encode_reader(reader: &mut ResultBatchReader) -> Result<(Bytes, usize), ServiceError> {
    let mut batches = Vec::new();
    while let Some(batch) = reader.next_batch()? {
        batches.push(batch);
    }
    let count = batches.len();
    Ok((encode_batches(&batches)?, count))
}

fn encode_batches(batches: &[RecordBatch]) -> Result<Bytes, ServiceError> {
    let schema = batches
        .first()
        .map_or_else(|| Arc::new(Schema::empty()), RecordBatch::schema);
    let cursor = std::io::Cursor::new(Vec::new());
    let mut writer = FileWriter::try_new(cursor, &schema).map_err(storage_service_error)?;
    for batch in batches {
        writer.write(batch).map_err(storage_service_error)?;
    }
    let cursor = writer.into_inner().map_err(storage_service_error)?;
    Ok(Bytes::from(cursor.into_inner()))
}

fn decode_batches(bytes: Bytes) -> Result<Vec<RecordBatch>, ServiceError> {
    FileReader::try_new(std::io::Cursor::new(bytes), None)
        .map_err(storage_service_error)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(storage_service_error)
}

fn principal_object_prefix(principal: &str) -> String {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    principal.hash(&mut hasher);
    format!("principals/{:016x}", hasher.finish())
}

fn chrono_deadline(ttl: Duration) -> chrono::DateTime<chrono::Utc> {
    chrono::DateTime::<chrono::Utc>::from(SystemTime::now() + ttl)
}

fn cluster_service_error(error: qcli_cluster::ClusterError) -> ServiceError {
    let kind = match error.code {
        "forbidden" => ServiceErrorKind::NotFound,
        "quota_exhausted" => ServiceErrorKind::ResourceExhausted,
        "version_conflict" | "lease_held" | "lease_lost" => ServiceErrorKind::Conflict,
        _ => ServiceErrorKind::Internal,
    };
    ServiceError::new(kind, error.code, error.message)
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "used directly as Result::map_err"
)]
fn json_service_error(error: serde_json::Error) -> ServiceError {
    ServiceError::new(
        ServiceErrorKind::Internal,
        "shared_state_json",
        error.to_string(),
    )
}

fn session_not_found() -> ServiceError {
    ServiceError::new(
        ServiceErrorKind::NotFound,
        "session_not_found",
        "session not found",
    )
}

fn query_not_found() -> ServiceError {
    ServiceError::new(
        ServiceErrorKind::NotFound,
        "query_not_found",
        "query not found",
    )
}

fn store_batch(
    query_id: &str,
    data: &mut QueryData,
    batch: RecordBatch,
    limits: &ServiceLimits,
) -> Result<(), QueryError> {
    if data.schema.is_none() {
        data.schema = Some(batch.schema());
    }
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
    let writer = match &mut data.storage {
        ResultStorage::Spill { writer, .. } => writer.take(),
        ResultStorage::Memory(_) => None,
    };
    if let Some(mut writer) = writer {
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

fn metadata_request(
    principal: &AuthenticatedPrincipal,
    target: String,
    engine: String,
    properties: BTreeMap<String, String>,
    catalog: Option<String>,
    schema: Option<String>,
    pattern: Option<String>,
) -> MetadataRequest {
    MetadataRequest {
        identity: principal.id.clone(),
        target,
        engine,
        properties,
        catalog,
        schema,
        pattern,
    }
}

fn prepared_snapshot(handle: &str, record: &PreparedStatementRecord) -> PreparedStatementSnapshot {
    PreparedStatementSnapshot {
        handle: handle.into(),
        session_id: record.session_id.clone(),
        sql: record.sql.clone(),
        dataset_schema: Arc::clone(&record.dataset_schema),
        parameter_schema: Arc::clone(&record.parameter_schema),
        parameters: record.parameters.clone(),
    }
}

fn prepared_not_found() -> ServiceError {
    ServiceError::new(
        ServiceErrorKind::NotFound,
        "prepared_not_found",
        "prepared statement not found",
    )
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
    use arrow_array::types::Int32Type;
    use arrow_array::{
        BinaryArray, Decimal128Array, ListArray, StringArray, TimestampMicrosecondArray,
    };
    use object_store::memory::InMemory;
    use qcli_cluster::{MemoryClusterStateStore, SharedObjectStore};
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

    #[tokio::test]
    async fn prepared_registry_preserves_typed_batches_and_update_counts() {
        let service = service(ServiceLimits::default());
        let analyst = principal("analyst");
        let session = service
            .create_session(&analyst, "demo", BTreeMap::new())
            .unwrap();
        let prepared = service
            .create_prepared_statement(&analyst, &session.id, "select ?".into())
            .await
            .unwrap();
        let decimal = Decimal128Array::from(vec![Some(12_345), None])
            .with_precision_and_scale(12, 2)
            .unwrap();
        let batch = RecordBatch::try_from_iter(vec![
            (
                "nullable",
                Arc::new(StringArray::from(vec![Some("x"), None])) as _,
            ),
            ("decimal", Arc::new(decimal) as _),
            (
                "timestamp",
                Arc::new(TimestampMicrosecondArray::from(vec![
                    Some(1_700_000_000_000_000),
                    None,
                ])) as _,
            ),
            (
                "binary",
                Arc::new(BinaryArray::from(vec![Some(&b"abc"[..]), None])) as _,
            ),
            (
                "nested",
                Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
                    Some(vec![Some(1), None]),
                    None,
                ])) as _,
            ),
        ])
        .unwrap();
        let expected = vec![batch.slice(0, 1), batch.slice(1, 1)];
        service
            .bind_prepared_statement(&analyst, &prepared.handle, expected.clone())
            .unwrap();
        let query = service
            .execute_prepared_statement(&analyst, &prepared.handle)
            .unwrap();
        let terminal = wait_for_terminal(&service, &analyst, &query.id).await;
        assert_eq!(terminal.state, "completed");
        let mut reader = service.result_reader(&analyst, &query.id).unwrap();
        let mut actual = Vec::new();
        while let Some(batch) = reader.next_batch().unwrap() {
            actual.push(batch);
        }
        assert_eq!(actual, expected);
        assert_eq!(
            service
                .execute_prepared_update(&analyst, &prepared.handle)
                .await
                .unwrap(),
            2
        );
    }

    #[tokio::test]
    async fn prepared_handles_are_owner_bound_closed_and_expire() {
        let service = service(ServiceLimits {
            prepared_statement_ttl: Duration::from_millis(1),
            ..ServiceLimits::default()
        });
        let analyst = principal("analyst");
        let stranger = principal("stranger");
        let session = service
            .create_session(&analyst, "demo", BTreeMap::new())
            .unwrap();
        let prepared = service
            .create_prepared_statement(&analyst, &session.id, "select ?".into())
            .await
            .unwrap();
        assert_eq!(
            service
                .prepared_statement(&stranger, &prepared.handle)
                .unwrap_err()
                .kind,
            ServiceErrorKind::NotFound
        );
        service
            .close_prepared_statement(&analyst, &prepared.handle)
            .unwrap();
        assert_eq!(
            service
                .prepared_statement(&analyst, &prepared.handle)
                .unwrap_err()
                .kind,
            ServiceErrorKind::NotFound
        );
        let expiring = service
            .create_prepared_statement(&analyst, &session.id, "select ?".into())
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(5)).await;
        assert_eq!(
            service
                .prepared_statement(&analyst, &expiring.handle)
                .unwrap_err()
                .kind,
            ServiceErrorKind::NotFound
        );

        let transient = service
            .create_stateless_prepared_statement(&analyst, "demo", "select ?".into())
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(5)).await;
        assert_eq!(
            service
                .prepared_statement(&analyst, &transient.handle)
                .unwrap_err()
                .kind,
            ServiceErrorKind::NotFound
        );
        assert_eq!(
            service
                .session(&analyst, &transient.session_id)
                .unwrap_err()
                .kind,
            ServiceErrorKind::NotFound
        );
    }

    #[tokio::test]
    #[allow(
        clippy::too_many_lines,
        reason = "one end-to-end multi-node milestone profile"
    )]
    async fn clustered_nodes_share_sessions_queries_and_arrow_results_with_isolation() {
        let cluster = Arc::new(MemoryClusterStateStore::default());
        let objects = Arc::new(SharedObjectStore::new(Arc::new(InMemory::new()), "m23"));
        let node = |node_id: &str| {
            service(ServiceLimits::default()).with_cluster(
                node_id,
                cluster.clone(),
                objects.clone(),
                Duration::from_millis(20),
            )
        };
        let node_a = node("node-a");
        let node_b = node("node-b");
        let analyst = principal("analyst");
        let stranger = principal("stranger");
        let session = node_a
            .create_session_clustered(&analyst, "demo", BTreeMap::new())
            .await
            .unwrap();
        assert_eq!(
            node_b
                .session_clustered(&analyst, &session.id)
                .await
                .unwrap(),
            session
        );
        assert_eq!(
            node_b
                .session_clustered(&stranger, &session.id)
                .await
                .unwrap_err()
                .kind,
            ServiceErrorKind::NotFound
        );
        let query = node_a
            .submit_session_query_clustered(&analyst, &session.id, "select * from sample".into())
            .await
            .unwrap();
        let terminal = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let status = node_b.query_clustered(&analyst, &query.id).await.unwrap();
                if status.state == "completed" {
                    break status;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(terminal.rows, 2);
        let mut reader = node_b
            .result_reader_clustered(&analyst, &query.id)
            .await
            .unwrap();
        assert_eq!(reader.next_batch().unwrap().unwrap().num_rows(), 2);
        assert!(reader.next_batch().unwrap().is_none());
        assert_eq!(
            node_b
                .query_clustered(&stranger, &query.id)
                .await
                .unwrap_err()
                .kind,
            ServiceErrorKind::NotFound
        );

        let prepared = node_a
            .create_prepared_statement_clustered(&analyst, &session.id, "select ?".into())
            .await
            .unwrap();
        let parameters = vec![
            RecordBatch::try_from_iter([(
                "value",
                Arc::new(StringArray::from(vec!["shared-parameter"])) as _,
            )])
            .unwrap(),
        ];
        node_a
            .bind_prepared_statement_clustered(&analyst, &prepared.handle, parameters.clone())
            .await
            .unwrap();
        let prepared_query = node_b
            .execute_prepared_statement_clustered(&analyst, &prepared.handle)
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if node_a
                    .query_clustered(&analyst, &prepared_query.id)
                    .await
                    .unwrap()
                    .state
                    == "completed"
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        let mut reader = node_a
            .result_reader_clustered(&analyst, &prepared_query.id)
            .await
            .unwrap();
        let batch = reader.next_batch().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "shared-parameter"
        );
    }
}
