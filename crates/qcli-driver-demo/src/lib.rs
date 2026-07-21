//! Deterministic adapter used for demos and core conformance tests.

use arrow_array::{ArrayRef, Decimal128Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;
use qcli_driver_api::{
    AdapterCapabilities, DriverError, EngineAdapter, QueryEvent, QueryRequest, QuerySink,
    QueryState,
};
use std::sync::Arc;

#[derive(Debug, Default)]
pub struct DemoAdapter;

#[async_trait]
impl EngineAdapter for DemoAdapter {
    fn engine(&self) -> &'static str {
        "demo"
    }

    fn capabilities(&self) -> AdapterCapabilities {
        AdapterCapabilities {
            stream_results: true,
            cancel_query: true,
        }
    }

    async fn execute(&self, request: QueryRequest, sink: QuerySink) -> Result<(), DriverError> {
        sink.events
            .send(QueryEvent::State(QueryState::Running))
            .await
            .ok();
        sink.events
            .send(QueryEvent::EngineQueryId(format!(
                "demo-{}",
                request.qcli_query_id
            )))
            .await
            .ok();
        if request.sql.trim().eq_ignore_ascii_case("fail") {
            return Err(DriverError::new(
                "demo_failure",
                "requested deterministic failure",
            ));
        }
        if request.sql.trim().eq_ignore_ascii_case("wait-for-cancel") {
            for _ in 0..1_000 {
                if sink.cancellation.is_cancelled() {
                    return Err(DriverError::new("cancelled", "query was cancelled"));
                }
                tokio::task::yield_now().await;
            }
        }
        if sink.cancellation.is_cancelled() {
            return Err(DriverError::new("cancelled", "query was cancelled"));
        }
        sink.events
            .send(QueryEvent::State(QueryState::ProducingResults))
            .await
            .ok();
        let batch = sample_batch()?;
        let rows = batch.num_rows();
        sink.batches
            .send(batch)
            .await
            .map_err(|_| DriverError::new("consumer_closed", "result consumer closed"))?;
        sink.events.send(QueryEvent::RowsProduced(rows)).await.ok();
        Ok(())
    }
}

fn sample_batch() -> Result<RecordBatch, DriverError> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("amount", DataType::Decimal128(18, 6), true),
    ]));
    let amount = Decimal128Array::from(vec![Some(123_456_789_i128), None])
        .with_precision_and_scale(18, 6)
        .map_err(|error| DriverError::new("arrow", error.to_string()))?;
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(vec![1, 2])),
        Arc::new(StringArray::from(vec![
            "alpha",
            "beta-name-that-can-be-truncated",
        ])),
        Arc::new(amount),
    ];
    RecordBatch::try_new(schema, columns)
        .map_err(|error| DriverError::new("arrow", error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use qcli_driver_api::CancellationSignal;
    use std::collections::BTreeMap;
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn produces_a_bounded_arrow_batch() {
        let (events, mut event_rx) = mpsc::channel(8);
        let (batches, mut batch_rx) = mpsc::channel(1);
        let request = QueryRequest {
            qcli_query_id: "q1".into(),
            session_id: "s1".into(),
            session_version: 1,
            target: "demo".into(),
            engine: "demo".into(),
            sql: "select * from sample".into(),
            properties: BTreeMap::default(),
        };
        DemoAdapter
            .execute(
                request,
                QuerySink {
                    events,
                    batches,
                    cancellation: CancellationSignal::default(),
                },
            )
            .await
            .unwrap();
        assert_eq!(batch_rx.recv().await.unwrap().num_rows(), 2);
        assert_eq!(
            event_rx.recv().await,
            Some(QueryEvent::State(QueryState::Running))
        );
    }
}
