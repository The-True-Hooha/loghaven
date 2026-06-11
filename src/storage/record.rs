use arrow::array::{ArrayRef, Int64Array, LargeStringArray, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::{NoContext, Timestamp, Uuid};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogRecord {
    pub id: String,
    pub app: String,
    pub timestamp_ms: i64,
    pub level: String,
    pub source: String,
    pub message: String,
    pub trace_id: Option<String>,
    pub span_id: Option<String>,
    pub chain: Option<String>,
    pub tx_hash: Option<String>,
    pub tags: Option<String>,
    pub metadata: Option<String>,
}

impl LogRecord {
    pub fn new(
        app: impl Into<String>,
        level: impl Into<String>,
        source: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        let ts = Timestamp::from_unix(NoContext, now.as_secs(), now.subsec_nanos());
        Self {
            id: Uuid::new_v7(ts).to_string(),
            app: app.into(),
            timestamp_ms: now.as_millis() as i64,
            level: level.into(),
            source: source.into(),
            message: message.into(),
            trace_id: None,
            span_id: None,
            chain: None,
            tx_hash: None,
            tags: None,
            metadata: None,
        }
    }
}

pub fn log_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("app", DataType::Utf8, false),
        Field::new("timestamp_ms", DataType::Int64, false),
        Field::new("level", DataType::Utf8, false),
        Field::new("source", DataType::Utf8, false),
        Field::new("message", DataType::LargeUtf8, false),
        Field::new("trace_id", DataType::Utf8, true),
        Field::new("span_id", DataType::Utf8, true),
        Field::new("chain", DataType::Utf8, true),
        Field::new("tx_hash", DataType::Utf8, true),
        Field::new("tags", DataType::Utf8, true),
        Field::new("metadata", DataType::Utf8, true),
    ]))
}

pub fn records_to_batch(records: &[LogRecord]) -> crate::error::Result<RecordBatch> {
    let schema = log_schema();

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(records.iter().map(|r| r.id.as_str()).collect::<Vec<_>>())) as ArrayRef,
            Arc::new(StringArray::from(records.iter().map(|r| r.app.as_str()).collect::<Vec<_>>())) as ArrayRef,
            Arc::new(Int64Array::from(records.iter().map(|r| r.timestamp_ms).collect::<Vec<_>>())) as ArrayRef,
            Arc::new(StringArray::from(records.iter().map(|r| r.level.as_str()).collect::<Vec<_>>())) as ArrayRef,
            Arc::new(StringArray::from(records.iter().map(|r| r.source.as_str()).collect::<Vec<_>>())) as ArrayRef,
            Arc::new(LargeStringArray::from(records.iter().map(|r| r.message.as_str()).collect::<Vec<_>>())) as ArrayRef,
            Arc::new(StringArray::from(records.iter().map(|r| r.trace_id.as_deref()).collect::<Vec<_>>())) as ArrayRef,
            Arc::new(StringArray::from(records.iter().map(|r| r.span_id.as_deref()).collect::<Vec<_>>())) as ArrayRef,
            Arc::new(StringArray::from(records.iter().map(|r| r.chain.as_deref()).collect::<Vec<_>>())) as ArrayRef,
            Arc::new(StringArray::from(records.iter().map(|r| r.tx_hash.as_deref()).collect::<Vec<_>>())) as ArrayRef,
            Arc::new(StringArray::from(records.iter().map(|r| r.tags.as_deref()).collect::<Vec<_>>())) as ArrayRef,
            Arc::new(StringArray::from(records.iter().map(|r| r.metadata.as_deref()).collect::<Vec<_>>())) as ArrayRef,
        ],
    )
    .map_err(|e| crate::error::LogHavenError::Storage(e.to_string()))?;

    Ok(batch)
}
