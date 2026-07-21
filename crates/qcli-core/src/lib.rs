//! Shared versioned sessions and asynchronous query orchestration.

use arrow_array::RecordBatch;
use chrono::Local;
use qcli_config::ResolvedTarget;
use qcli_driver_api::{
    CancellationSignal, DriverError, EngineAdapter, QueryEvent, QueryRequest, QuerySink, QueryState,
};
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

#[derive(Debug, Clone)]
pub struct SessionSnapshot {
    pub id: String,
    pub version: u64,
    pub target: String,
    pub engine: String,
    pub properties: BTreeMap<String, String>,
}

#[derive(Debug, Clone)]
struct Session {
    id: String,
    version: u64,
    target: ResolvedTarget,
    overrides: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CoreError {
    SessionNotFound(String),
    VersionConflict { expected: u64, actual: u64 },
    AdapterNotFound(String),
    Driver(DriverError),
    Task(String),
}

impl fmt::Display for CoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SessionNotFound(id) => write!(f, "session '{id}' does not exist"),
            Self::VersionConflict { expected, actual } => write!(
                f,
                "session version conflict: expected {expected}, actual {actual}"
            ),
            Self::AdapterNotFound(engine) => {
                write!(f, "no adapter registered for engine '{engine}'")
            }
            Self::Driver(error) => write!(f, "driver error: {error}"),
            Self::Task(error) => write!(f, "query task failed: {error}"),
        }
    }
}

impl std::error::Error for CoreError {}

pub struct SessionManager {
    sessions: Mutex<HashMap<String, Session>>,
}

impl Default for SessionManager {
    fn default() -> Self {
        Self {
            sessions: Mutex::new(HashMap::new()),
        }
    }
}

impl SessionManager {
    /// Create a new version-one logical session for a resolved target.
    ///
    /// # Panics
    ///
    /// Panics if another thread poisoned the internal session lock.
    #[must_use]
    pub fn create(&self, target: ResolvedTarget) -> SessionSnapshot {
        let username = target
            .properties
            .get("user")
            .map(|value| value.expose().to_owned())
            .or_else(|| std::env::var("USER").ok())
            .or_else(|| std::env::var("USERNAME").ok())
            .unwrap_or_else(|| "qcli".into());
        let id = next_session_id(&username);
        let session = Session {
            id: id.clone(),
            version: 1,
            target,
            overrides: BTreeMap::new(),
        };
        let snapshot = snapshot(&session);
        self.sessions
            .lock()
            .expect("session mutex poisoned")
            .insert(id, session);
        snapshot
    }

    /// Capture the session's current immutable execution state.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError::SessionNotFound`] for an unknown session ID.
    ///
    /// # Panics
    ///
    /// Panics if another thread poisoned the internal session lock.
    pub fn snapshot(&self, id: &str) -> Result<SessionSnapshot, CoreError> {
        self.sessions
            .lock()
            .expect("session mutex poisoned")
            .get(id)
            .map(snapshot)
            .ok_or_else(|| CoreError::SessionNotFound(id.into()))
    }

    /// Apply a session override when the caller has the current version.
    ///
    /// # Errors
    ///
    /// Returns an error for an unknown session or stale expected version.
    ///
    /// # Panics
    ///
    /// Panics if another thread poisoned the internal session lock.
    pub fn set_option(
        &self,
        id: &str,
        expected_version: u64,
        name: String,
        value: String,
    ) -> Result<SessionSnapshot, CoreError> {
        let mut sessions = self.sessions.lock().expect("session mutex poisoned");
        let session = sessions
            .get_mut(id)
            .ok_or_else(|| CoreError::SessionNotFound(id.into()))?;
        if session.version != expected_version {
            return Err(CoreError::VersionConflict {
                expected: expected_version,
                actual: session.version,
            });
        }
        session.overrides.insert(name, value);
        session.version += 1;
        Ok(snapshot(session))
    }
}

#[derive(Default)]
struct SessionIdState {
    minute: String,
    counter: u8,
}

fn next_session_id(username: &str) -> String {
    static IDS: OnceLock<Mutex<SessionIdState>> = OnceLock::new();
    let minute = Local::now().format("%Y%m%d_%H%M").to_string();
    let mut state = IDS
        .get_or_init(|| Mutex::new(SessionIdState::default()))
        .lock()
        .expect("session ID mutex poisoned");
    if state.minute != minute {
        state.minute.clone_from(&minute);
        state.counter = 0;
    }
    state.counter = state.counter % 99 + 1;
    format!("{}_{minute}_{:02}", safe_username(username), state.counter)
}

fn safe_username(username: &str) -> String {
    let sanitized = username
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || character == '_' {
                character
            } else {
                '_'
            }
        })
        .collect::<String>();
    if sanitized.is_empty() {
        "qcli".into()
    } else {
        sanitized
    }
}

fn snapshot(session: &Session) -> SessionSnapshot {
    let mut properties = session
        .target
        .properties
        .iter()
        .map(|(name, value)| (name.clone(), value.expose().to_owned()))
        .collect::<BTreeMap<_, _>>();
    properties.extend(session.overrides.clone());
    SessionSnapshot {
        id: session.id.clone(),
        version: session.version,
        target: session.target.name.clone(),
        engine: session.target.engine.clone(),
        properties,
    }
}

pub struct QueryHandle {
    pub id: String,
    pub session_id: String,
    pub session_version: u64,
    events: mpsc::Receiver<QueryEvent>,
    batches: mpsc::Receiver<RecordBatch>,
    events_done: bool,
    batches_done: bool,
    cancellation: CancellationSignal,
    task: JoinHandle<Result<(), DriverError>>,
}

pub enum QueryItem {
    Event(QueryEvent),
    Batch(RecordBatch),
}

impl QueryHandle {
    pub async fn next_event(&mut self) -> Option<QueryEvent> {
        self.events.recv().await
    }
    pub async fn next_batch(&mut self) -> Option<RecordBatch> {
        self.batches.recv().await
    }
    /// Receive whichever query event or result batch becomes available next.
    pub async fn next_item(&mut self) -> Option<QueryItem> {
        loop {
            if self.events_done && self.batches_done {
                return None;
            }
            tokio::select! {
                event = self.events.recv(), if !self.events_done => match event {
                    Some(event) => return Some(QueryItem::Event(event)),
                    None => self.events_done = true,
                },
                batch = self.batches.recv(), if !self.batches_done => match batch {
                    Some(batch) => return Some(QueryItem::Batch(batch)),
                    None => self.batches_done = true,
                },
            }
        }
    }
    pub fn cancel(&self) {
        self.cancellation.cancel();
    }
    /// Wait for adapter execution and return its final outcome.
    ///
    /// # Errors
    ///
    /// Returns a task or structured driver error when execution did not
    /// complete successfully.
    pub async fn finish(self) -> Result<(), CoreError> {
        self.task
            .await
            .map_err(|error| CoreError::Task(error.to_string()))?
            .map_err(CoreError::Driver)
    }
}

pub struct QueryService {
    next_id: AtomicU64,
    adapters: HashMap<String, Arc<dyn EngineAdapter>>,
    channel_capacity: usize,
}

impl QueryService {
    #[must_use]
    pub fn new(
        adapters: impl IntoIterator<Item = Arc<dyn EngineAdapter>>,
        channel_capacity: usize,
    ) -> Self {
        let adapters = adapters
            .into_iter()
            .map(|adapter| (adapter.engine().to_owned(), adapter))
            .collect();
        Self {
            next_id: AtomicU64::new(1),
            adapters,
            channel_capacity: channel_capacity.max(1),
        }
    }

    /// Submit native SQL using the adapter selected by the snapshot.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError::AdapterNotFound`] if no adapter is registered for
    /// the snapshot's engine.
    pub fn submit(&self, snapshot: SessionSnapshot, sql: String) -> Result<QueryHandle, CoreError> {
        let adapter = self
            .adapters
            .get(&snapshot.engine)
            .cloned()
            .ok_or_else(|| CoreError::AdapterNotFound(snapshot.engine.clone()))?;
        let id = format!("qcli_{:016x}", self.next_id.fetch_add(1, Ordering::Relaxed));
        let request = QueryRequest {
            qcli_query_id: id.clone(),
            session_id: snapshot.id.clone(),
            session_version: snapshot.version,
            target: snapshot.target,
            engine: snapshot.engine,
            sql,
            properties: snapshot.properties,
        };
        let (event_tx, event_rx) = mpsc::channel(self.channel_capacity);
        let (batch_tx, batch_rx) = mpsc::channel(self.channel_capacity);
        let cancellation = CancellationSignal::default();
        let sink = QuerySink {
            events: event_tx.clone(),
            batches: batch_tx,
            cancellation: cancellation.clone(),
        };
        let task = tokio::spawn(async move {
            event_tx
                .send(QueryEvent::State(QueryState::Submitted))
                .await
                .ok();
            let result = adapter.execute(request, sink).await;
            let final_state = match &result {
                Ok(()) => QueryState::Completed,
                Err(error) if error.code == "cancelled" => QueryState::Cancelled,
                Err(_) => QueryState::Failed,
            };
            event_tx.send(QueryEvent::State(final_state)).await.ok();
            result
        });
        Ok(QueryHandle {
            id,
            session_id: snapshot.id,
            session_version: snapshot.version,
            events: event_rx,
            batches: batch_rx,
            events_done: false,
            batches_done: false,
            cancellation,
            task,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use qcli_config::Config;
    use qcli_driver_demo::DemoAdapter;
    use qcli_output::{DisplayOptions, OutputFormat, StreamOutput};

    static NEXT_TEST_CONFIG: AtomicU64 = AtomicU64::new(1);

    fn target() -> ResolvedTarget {
        let id = NEXT_TEST_CONFIG.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!("qcli-core-{}-{id}.env", std::process::id()));
        std::fs::write(&path, "[demo]\nengine=demo\ndecimal_places=3\n").unwrap();
        let target = Config::load(&path).unwrap().target("demo").unwrap().clone();
        std::fs::remove_file(path).ok();
        target
    }

    #[test]
    fn mutations_are_versioned_and_snapshots_are_immutable() {
        let manager = SessionManager::default();
        let first = manager.create(target());
        let username = std::env::var("USER")
            .ok()
            .or_else(|| std::env::var("USERNAME").ok())
            .unwrap_or_else(|| "qcli".into());
        let prefix = format!("{}_", safe_username(&username));
        assert!(first.id.starts_with(&prefix));
        let timestamp_and_counter = first.id.trim_start_matches(&prefix);
        assert_eq!(timestamp_and_counter.len(), 16);
        assert!(
            timestamp_and_counter
                .chars()
                .enumerate()
                .all(
                    |(index, character)| matches!(index, 8 | 13) && character == '_'
                        || !matches!(index, 8 | 13) && character.is_ascii_digit()
                )
        );
        let second = manager
            .set_option(&first.id, 1, "decimal_places".into(), "8".into())
            .unwrap();
        assert_eq!(first.version, 1);
        assert_eq!(first.properties["decimal_places"], "3");
        assert_eq!(second.version, 2);
        assert_eq!(second.properties["decimal_places"], "8");
        assert!(matches!(
            manager.set_option(&first.id, 1, "x".into(), "y".into()),
            Err(CoreError::VersionConflict { .. })
        ));
    }

    #[tokio::test]
    async fn query_flow_reports_results_and_terminal_state() {
        let manager = SessionManager::default();
        let session = manager.create(target());
        let adapters: Vec<Arc<dyn EngineAdapter>> = vec![Arc::new(DemoAdapter)];
        let service = QueryService::new(adapters, 8);
        let mut handle = service
            .submit(session, "select * from sample".into())
            .unwrap();
        assert_eq!(handle.next_batch().await.unwrap().num_rows(), 2);
        assert!(handle.next_batch().await.is_none());
        let mut states = Vec::new();
        while let Some(event) = handle.next_event().await {
            if let QueryEvent::State(state) = event {
                states.push(state);
            }
        }
        assert_eq!(
            states,
            [
                QueryState::Submitted,
                QueryState::Running,
                QueryState::ProducingResults,
                QueryState::Completed,
            ]
        );
        handle.finish().await.unwrap();
    }

    #[tokio::test]
    async fn cancellation_is_observable() {
        let manager = SessionManager::default();
        let session = manager.create(target());
        let adapters: Vec<Arc<dyn EngineAdapter>> = vec![Arc::new(DemoAdapter)];
        let service = QueryService::new(adapters, 8);
        let mut handle = service.submit(session, "wait-for-cancel".into()).unwrap();
        handle.cancel();
        while handle.next_event().await.is_some() {}
        let error = handle.finish().await.unwrap_err();
        assert!(matches!(
            error,
            CoreError::Driver(DriverError { code, .. }) if code == "cancelled"
        ));
    }

    async fn stream_generated_rows(total: usize) {
        for format in [OutputFormat::Csv, OutputFormat::JsonLines] {
            let manager = SessionManager::default();
            let session = manager.create(target());
            let adapters: Vec<Arc<dyn EngineAdapter>> = vec![Arc::new(DemoAdapter)];
            let service = QueryService::new(adapters, 8);
            let mut handle = service
                .submit(session, format!("generate {total}"))
                .unwrap();
            let mut output = StreamOutput::new(
                std::io::sink(),
                format,
                DisplayOptions {
                    decimal_places: 3,
                    string_truncate: 80,
                },
            )
            .unwrap();
            let mut largest_batch = 0;
            while let Some(batch) = handle.next_batch().await {
                largest_batch = largest_batch.max(batch.num_rows());
                output.write_batch(&batch).unwrap();
            }
            assert_eq!(output.finish().unwrap(), total);
            assert_eq!(largest_batch, total.min(1_024));
            while handle.next_event().await.is_some() {}
            handle.finish().await.unwrap();
        }
    }

    #[tokio::test]
    async fn multiple_batches_stream_to_csv_and_jsonl() {
        stream_generated_rows(10_000).await;
    }

    #[tokio::test]
    #[ignore = "explicit Milestone 3 release gate"]
    async fn million_rows_stream_in_bounded_batches_to_csv_and_jsonl() {
        stream_generated_rows(1_000_000).await;
    }
}
