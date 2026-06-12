use crate::ingest::payload::{IngestPayload, IngestRecord};

fn make_json_payload(app: &str) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "app": app,
        "records": [
            { "level": "info", "source": "api", "message": "test log" }
        ]
    }))
    .unwrap()
}

// --- Payload parsing ---

#[test]
fn parse_valid_json_payload() {
    let data = make_json_payload("myapp");
    let p: IngestPayload = serde_json::from_slice(&data).unwrap();
    assert_eq!(p.app, "myapp");
    assert_eq!(p.records.len(), 1);
    assert_eq!(p.records[0].level, "info");
    assert_eq!(p.records[0].source, "api");
    assert_eq!(p.records[0].message, "test log");
}

#[test]
fn parse_msgpack_payload() {
    let payload = IngestPayload {
        app: "packapp".to_string(),
        records: vec![IngestRecord {
            level: "warn".to_string(),
            source: "worker".to_string(),
            message: "packed message".to_string(),
            trace_id: None,
            span_id: None,
            chain: None,
            tx_hash: None,
            tags: None,
            metadata: None,
        }],
    };
    let data = rmp_serde::to_vec(&payload).unwrap();
    let parsed: IngestPayload = rmp_serde::from_slice(&data).unwrap();
    assert_eq!(parsed.app, "packapp");
    assert_eq!(parsed.records[0].message, "packed message");
}

#[test]
fn into_log_record_sets_app_and_fields() {
    let rec = IngestRecord {
        level: "error".to_string(),
        source: "db".to_string(),
        message: "query failed".to_string(),
        trace_id: Some("trace-abc".to_string()),
        span_id: None,
        chain: None,
        tx_hash: None,
        tags: None,
        metadata: None,
    };
    let log = rec.into_log_record("myapp");
    assert_eq!(log.app, "myapp");
    assert_eq!(log.level, "error");
    assert_eq!(log.source, "db");
    assert_eq!(log.message, "query failed");
    assert_eq!(log.trace_id.as_deref(), Some("trace-abc"));
    assert!(!log.id.is_empty());
    assert!(log.timestamp_ms > 0);
}

#[test]
fn into_log_record_generates_unique_ids() {
    let make = || IngestRecord {
        level: "info".to_string(),
        source: "svc".to_string(),
        message: "msg".to_string(),
        trace_id: None,
        span_id: None,
        chain: None,
        tx_hash: None,
        tags: None,
        metadata: None,
    };
    let a = make().into_log_record("app");
    let b = make().into_log_record("app");
    assert_ne!(a.id, b.id);
}

#[test]
fn reject_invalid_json() {
    let data = b"not json at all {{{";
    assert!(serde_json::from_slice::<IngestPayload>(data).is_err());
}

#[test]
fn reject_empty_app_field() {
    let data = serde_json::to_vec(&serde_json::json!({
        "app": "",
        "records": []
    }))
    .unwrap();
    let p: IngestPayload = serde_json::from_slice(&data).unwrap();
    assert!(p.app.is_empty()); // caller must reject
}

#[test]
fn optional_fields_round_trip_json() {
    let data = serde_json::to_vec(&serde_json::json!({
        "app": "myapp",
        "records": [{
            "level": "info",
            "source": "api",
            "message": "traced request",
            "trace_id": "trace-xyz",
            "span_id": "span-abc",
            "chain": "solana",
            "tx_hash": "0xdeadbeef",
            "tags": {"env": "staging"},
            "metadata": {"user_id": 42}
        }]
    }))
    .unwrap();

    let p: IngestPayload = serde_json::from_slice(&data).unwrap();
    let r = &p.records[0];
    assert_eq!(r.trace_id.as_deref(), Some("trace-xyz"));
    assert_eq!(r.span_id.as_deref(), Some("span-abc"));
    assert_eq!(r.chain.as_deref(), Some("solana"));
    assert_eq!(r.tx_hash.as_deref(), Some("0xdeadbeef"));
    assert!(r.tags.is_some());
    assert!(r.metadata.is_some());
}

#[test]
fn optional_fields_into_log_record_stored_as_json_strings() {
    let rec = IngestRecord {
        level: "info".to_string(),
        source: "svc".to_string(),
        message: "msg".to_string(),
        trace_id: None,
        span_id: None,
        chain: None,
        tx_hash: None,
        tags: Some(serde_json::json!({"env": "prod"})),
        metadata: Some(serde_json::json!({"deploy": "v2"})),
    };
    let log = rec.into_log_record("app");
    assert!(log.tags.as_ref().unwrap().contains("prod"));
    assert!(log.metadata.as_ref().unwrap().contains("v2"));
}

#[test]
fn empty_records_list_parses() {
    let data = serde_json::to_vec(&serde_json::json!({
        "app": "myapp",
        "records": []
    }))
    .unwrap();
    let p: IngestPayload = serde_json::from_slice(&data).unwrap();
    assert_eq!(p.records.len(), 0);
}

#[test]
fn multiple_records_in_payload() {
    let data = serde_json::to_vec(&serde_json::json!({
        "app": "myapp",
        "records": [
            {"level": "info", "source": "svc", "message": "one"},
            {"level": "warn", "source": "svc", "message": "two"},
            {"level": "error", "source": "svc", "message": "three"},
        ]
    }))
    .unwrap();
    let p: IngestPayload = serde_json::from_slice(&data).unwrap();
    assert_eq!(p.records.len(), 3);
    assert_eq!(p.records[2].level, "error");
}

#[test]
fn msgpack_optional_fields_round_trip() {
    let payload = IngestPayload {
        app: "app".to_string(),
        records: vec![IngestRecord {
            level: "info".to_string(),
            source: "svc".to_string(),
            message: "msg".to_string(),
            trace_id: Some("t1".to_string()),
            span_id: Some("s1".to_string()),
            chain: Some("eth".to_string()),
            tx_hash: Some("0x1".to_string()),
            tags: None,
            metadata: None,
        }],
    };
    let data = rmp_serde::to_vec(&payload).unwrap();
    let parsed: IngestPayload = rmp_serde::from_slice(&data).unwrap();
    assert_eq!(parsed.records[0].trace_id.as_deref(), Some("t1"));
    assert_eq!(parsed.records[0].chain.as_deref(), Some("eth"));
}
