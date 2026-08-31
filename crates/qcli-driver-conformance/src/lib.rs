//! Reusable, frontend-neutral conformance checks for qcli engine adapters.

use arrow_array::RecordBatch;
use qcli_driver_api::{
    AdapterCapability, CancellationSignal, DriverError, EngineAdapter, QueryEvent, QueryRequest,
    QuerySink,
};
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::mpsc;

#[derive(Debug)]
pub struct QueryOutcome {
    pub events: Vec<QueryEvent>,
    pub batches: Vec<RecordBatch>,
    pub result: Result<(), DriverError>,
}

impl QueryOutcome {
    #[must_use]
    pub fn row_count(&self) -> usize {
        self.batches.iter().map(RecordBatch::num_rows).sum()
    }

    #[must_use]
    pub fn engine_query_id(&self) -> Option<&str> {
        self.events.iter().find_map(|event| match event {
            QueryEvent::EngineQueryId(id) => Some(id.as_str()),
            _ => None,
        })
    }
}

/// Execute one adapter request while concurrently draining its bounded channels.
///
/// # Errors
///
/// Returns a join error only if the adapter task panics or is aborted. Driver
/// errors are retained in [`QueryOutcome::result`] for conformance assertions.
pub async fn run_query(
    adapter: Arc<dyn EngineAdapter>,
    target: impl Into<String>,
    properties: BTreeMap<String, String>,
    sql: impl Into<String>,
) -> Result<QueryOutcome, tokio::task::JoinError> {
    let target = target.into();
    let request = QueryRequest {
        qcli_query_id: "conformance-query-1".into(),
        session_id: "conformance-session-1".into(),
        session_version: 1,
        target: target.clone(),
        engine: adapter.engine().into(),
        sql: sql.into(),
        properties,
    };
    let (event_tx, mut event_rx) = mpsc::channel(16);
    let (batch_tx, mut batch_rx) = mpsc::channel(4);
    let sink = QuerySink {
        events: event_tx,
        batches: batch_tx,
        cancellation: CancellationSignal::default(),
    };
    let task = tokio::spawn(async move { adapter.execute(request, sink).await });
    let mut events = Vec::new();
    let mut batches = Vec::new();
    let mut events_open = true;
    let mut batches_open = true;
    while events_open || batches_open {
        tokio::select! {
            event = event_rx.recv(), if events_open => match event {
                Some(event) => events.push(event),
                None => events_open = false,
            },
            batch = batch_rx.recv(), if batches_open => match batch {
                Some(batch) => batches.push(batch),
                None => batches_open = false,
            },
        }
    }
    let result = task.await?;
    Ok(QueryOutcome {
        events,
        batches,
        result,
    })
}

/// Assert the common minimum expected from every release-candidate adapter.
///
/// # Panics
///
/// Panics when streaming or normalized metadata discovery is absent.
pub fn assert_common_capabilities(adapter: &dyn EngineAdapter) {
    let capabilities = adapter.capabilities();
    for capability in [
        AdapterCapability::StreamResults,
        AdapterCapability::ListCatalogs,
        AdapterCapability::ListSchemas,
        AdapterCapability::ListObjects,
        AdapterCapability::DescribeObject,
    ] {
        assert!(
            capabilities.supports(capability),
            "{} is missing required capability {capability:?}",
            adapter.engine()
        );
    }
}

/// Assert a successful portable validation query.
///
/// # Panics
///
/// Panics when execution fails, returns no rows, or emits inconsistent row counts.
pub fn assert_portable_query(outcome: &QueryOutcome) {
    assert!(
        outcome.result.is_ok(),
        "portable query failed: {:?}",
        outcome.result
    );
    assert!(outcome.row_count() > 0, "portable query returned no rows");
    if let Some(reported) = outcome.events.iter().rev().find_map(|event| match event {
        QueryEvent::RowsProduced(rows) => Some(*rows),
        _ => None,
    }) {
        assert_eq!(reported, outcome.row_count(), "reported row count differs");
    }
}
