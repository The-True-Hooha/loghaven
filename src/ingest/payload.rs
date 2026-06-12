use crate::storage::record::LogRecord;
use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::{NoContext, Timestamp, Uuid};

#[derive(Debug, Serialize, Deserialize)]
pub struct IngestPayload {
    pub app: String,
    pub records: Vec<IngestRecord>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct IngestRecord {
    pub level: String,
    pub source: String,
    pub message: String,
    pub trace_id: Option<String>,
    pub span_id: Option<String>,
    pub chain: Option<String>,
    pub tx_hash: Option<String>,
    pub tags: Option<serde_json::Value>,
    pub metadata: Option<serde_json::Value>,
}

impl IngestRecord {
    pub fn into_log_record(self, app: &str) -> LogRecord {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        let ts = Timestamp::from_unix(NoContext, now.as_secs(), now.subsec_nanos());
        LogRecord {
            id: Uuid::new_v7(ts).to_string(),
            app: app.to_string(),
            timestamp_ms: now.as_millis() as i64,
            level: self.level,
            source: self.source,
            message: self.message,
            trace_id: self.trace_id,
            span_id: self.span_id,
            chain: self.chain,
            tx_hash: self.tx_hash,
            tags: self.tags.map(|v| v.to_string()),
            metadata: self.metadata.map(|v| v.to_string()),
        }
    }
}
