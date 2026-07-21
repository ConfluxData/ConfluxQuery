//! Frontend-neutral engine adapter contracts.

use arrow_array::RecordBatch;
use async_trait::async_trait;
use std::collections::BTreeMap;
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdapterCapabilities {
    pub stream_results: bool,
    pub cancel_query: bool,
}

#[async_trait]
pub trait EngineAdapter: Send + Sync {
    fn engine(&self) -> &'static str;
    fn capabilities(&self) -> AdapterCapabilities;
    async fn execute(&self, request: QueryRequest, sink: QuerySink) -> Result<(), DriverError>;
}
