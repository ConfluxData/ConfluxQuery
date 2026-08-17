//! Frontend-neutral engine adapter contracts.

use arrow_array::RecordBatch;
use async_trait::async_trait;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::mpsc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryState {
    Submitted,
    Running,
    ProducingResults,
    Completed,
    Cancelling,
    Cancelled,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryEvent {
    State(QueryState),
    EngineQueryId(String),
    RowsProduced(usize),
    Progress(QueryProgress),
    SessionProperties(BTreeMap<String, String>),
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct QueryProgress {
    pub state: Option<String>,
    pub scheduled: Option<bool>,
    pub completed_splits: Option<u64>,
    pub total_splits: Option<u64>,
    pub processed_rows: Option<u64>,
    pub processed_bytes: Option<u64>,
    pub elapsed_millis: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct QueryRequest {
    pub qcli_query_id: String,
    pub session_id: String,
    pub session_version: u64,
    pub target: String,
    pub engine: String,
    pub sql: String,
    pub properties: BTreeMap<String, String>,
}

#[derive(Clone, Default)]
pub struct CancellationSignal(Arc<AtomicBool>);

impl CancellationSignal {
    pub fn cancel(&self) {
        self.0.store(true, Ordering::Release);
    }

    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }
}

pub struct QuerySink {
    pub events: mpsc::Sender<QueryEvent>,
    pub batches: mpsc::Sender<RecordBatch>,
    pub cancellation: CancellationSignal,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DriverError {
    pub code: String,
    pub message: String,
}

impl DriverError {
    #[must_use]
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
        }
    }
}

impl fmt::Display for DriverError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for DriverError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum AdapterCapability {
    StreamResults,
    CancelQuery,
    ListCatalogs,
    ListSchemas,
    ListObjects,
    DescribeObject,
}

impl AdapterCapability {
    pub const ALL: [Self; 6] = [
        Self::StreamResults,
        Self::CancelQuery,
        Self::ListCatalogs,
        Self::ListSchemas,
        Self::ListObjects,
        Self::DescribeObject,
    ];

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::StreamResults => "stream_results",
            Self::CancelQuery => "cancel_query",
            Self::ListCatalogs => "list_catalogs",
            Self::ListSchemas => "list_schemas",
            Self::ListObjects => "list_objects",
            Self::DescribeObject => "describe_object",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdapterCapabilities {
    pub supported: BTreeSet<AdapterCapability>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdentifierCase {
    Insensitive,
    Upper,
    Lower,
    Mixed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdentifierCapabilities {
    pub unquoted: IdentifierCase,
    pub quoted: IdentifierCase,
    pub quote: String,
}

impl Default for IdentifierCapabilities {
    fn default() -> Self {
        Self {
            unquoted: IdentifierCase::Insensitive,
            quoted: IdentifierCase::Mixed,
            quote: "\"".into(),
        }
    }
}

impl AdapterCapabilities {
    #[must_use]
    pub fn from_supported(capabilities: impl IntoIterator<Item = AdapterCapability>) -> Self {
        Self {
            supported: capabilities.into_iter().collect(),
        }
    }

    #[must_use]
    pub fn supports(&self, capability: AdapterCapability) -> bool {
        self.supported.contains(&capability)
    }
}

#[derive(Debug, Clone)]
pub struct MetadataRequest {
    pub identity: String,
    pub target: String,
    pub engine: String,
    pub properties: BTreeMap<String, String>,
    pub catalog: Option<String>,
    pub schema: Option<String>,
    pub pattern: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogMetadata {
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaMetadata {
    pub catalog: Option<String>,
    pub name: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectKind {
    Table,
    View,
    Other,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectMetadata {
    pub catalog: Option<String>,
    pub schema: Option<String>,
    pub name: String,
    pub kind: ObjectKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnMetadata {
    pub name: String,
    pub data_type: String,
    pub nullable: Option<bool>,
    pub comment: Option<String>,
}

#[async_trait]
pub trait EngineAdapter: Send + Sync {
    fn engine(&self) -> &'static str;
    fn capabilities(&self) -> AdapterCapabilities;
    fn identifier_capabilities(&self) -> IdentifierCapabilities {
        IdentifierCapabilities::default()
    }
    async fn execute(&self, request: QueryRequest, sink: QuerySink) -> Result<(), DriverError>;
    async fn list_catalogs(
        &self,
        _request: MetadataRequest,
    ) -> Result<Vec<CatalogMetadata>, DriverError> {
        Err(DriverError::new(
            "unsupported_capability",
            "catalog discovery is not supported",
        ))
    }
    async fn list_schemas(
        &self,
        _request: MetadataRequest,
    ) -> Result<Vec<SchemaMetadata>, DriverError> {
        Err(DriverError::new(
            "unsupported_capability",
            "schema discovery is not supported",
        ))
    }
    async fn list_objects(
        &self,
        _request: MetadataRequest,
    ) -> Result<Vec<ObjectMetadata>, DriverError> {
        Err(DriverError::new(
            "unsupported_capability",
            "object discovery is not supported",
        ))
    }
    async fn describe_object(
        &self,
        _request: MetadataRequest,
        _object: &str,
    ) -> Result<Vec<ColumnMetadata>, DriverError> {
        Err(DriverError::new(
            "unsupported_capability",
            "object description is not supported",
        ))
    }
}
