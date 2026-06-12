use crate::config::LocalStorageConfig;
use crate::storage::local::LocalBackend;
use crate::storage::query::{QueryFilter, query};
use crate::storage::record::{LogRecord, log_schema, records_to_batch};
use crate::storage::writer::ChunkWriter;
use std::path::PathBuf;
use tempfile::TempDir;

fn temp_config(dir: &TempDir) -> LocalStorageConfig {
    LocalStorageConfig {
        path: dir.path().to_path_buf(),
        max_size_gb: 10,
        rotate_size_mb: 128,
        rotate_records: 500_000,
        flush_interval_secs: 30,
        retention_days: 30,
    }
}

fn make_record(app: &str, level: &str, source: &str, msg: &str) -> LogRecord {
    LogRecord::new(app, level, source, msg)
}

#[test]
fn chunk_writer_creates_file() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("test.parquet");
    let mut w = ChunkWriter::new(path.clone()).unwrap();
    let records = vec![make_record("app1", "info", "svc", "hello")];
    w.write_batch(&records).unwrap();
    w.finish().unwrap();
    assert!(path.exists());
    assert!(std::fs::metadata(&path).unwrap().len() > 0);
}

#[test]
fn chunk_writer_empty_batch_noop() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("empty.parquet");
    let mut w = ChunkWriter::new(path.clone()).unwrap();
    w.write_batch(&[]).unwrap();
    w.finish().unwrap();
}

#[test]
fn chunk_writer_record_count() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("count.parquet");
    let mut w = ChunkWriter::new(path).unwrap();
    let records: Vec<_> = (0..5).map(|i| make_record("app", "info", "svc", &format!("msg {}", i))).collect();
    w.write_batch(&records).unwrap();
    assert_eq!(w.record_count, 5);
    w.finish().unwrap();
}

fn write_parquet(path: PathBuf, records: &[LogRecord]) {
    let mut w = ChunkWriter::new(path).unwrap();
    w.write_batch(records).unwrap();
    w.finish().unwrap();
}

#[test]
fn query_scans_parquet_and_returns_records() {
    let dir = TempDir::new().unwrap();
    let app = "myapp";
    let date = crate::storage::local::current_date();
    let chunk_dir = dir.path().join(app).join(&date);
    std::fs::create_dir_all(&chunk_dir).unwrap();

    let records = vec![
        make_record(app, "info", "api", "request received"),
        make_record(app, "error", "db", "connection failed"),
    ];
    write_parquet(chunk_dir.join("00000.parquet"), &records);

    let filter = QueryFilter {
        app: app.to_string(),
        ..Default::default()
    };
    let results = query(&dir.path().to_path_buf(), &filter, None).unwrap();
    assert_eq!(results.len(), 2);
}

#[test]
fn query_filters_by_level() {
    let dir = TempDir::new().unwrap();
    let app = "filterapp";
    let date = crate::storage::local::current_date();
    let chunk_dir = dir.path().join(app).join(&date);
    std::fs::create_dir_all(&chunk_dir).unwrap();

    let records = vec![
        make_record(app, "info", "svc", "all good"),
        make_record(app, "error", "svc", "something broke"),
        make_record(app, "info", "svc", "still good"),
    ];
    write_parquet(chunk_dir.join("00000.parquet"), &records);

    let filter = QueryFilter {
        app: app.to_string(),
        level: Some("error".to_string()),
        ..Default::default()
    };
    let results = query(&dir.path().to_path_buf(), &filter, None).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].level, "error");
}

#[test]
fn query_filters_by_source() {
    let dir = TempDir::new().unwrap();
    let app = "srcapp";
    let date = crate::storage::local::current_date();
    let chunk_dir = dir.path().join(app).join(&date);
    std::fs::create_dir_all(&chunk_dir).unwrap();

    let records = vec![
        make_record(app, "info", "api", "from api"),
        make_record(app, "info", "worker", "from worker"),
    ];
    write_parquet(chunk_dir.join("00000.parquet"), &records);

    let filter = QueryFilter {
        app: app.to_string(),
        source: Some("worker".to_string()),
        ..Default::default()
    };
    let results = query(&dir.path().to_path_buf(), &filter, None).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].source, "worker");
}

#[test]
fn query_respects_limit() {
    let dir = TempDir::new().unwrap();
    let app = "limitapp";
    let date = crate::storage::local::current_date();
    let chunk_dir = dir.path().join(app).join(&date);
    std::fs::create_dir_all(&chunk_dir).unwrap();

    let records: Vec<_> = (0..20).map(|i| make_record(app, "info", "svc", &format!("msg {}", i))).collect();
    write_parquet(chunk_dir.join("00000.parquet"), &records);

    let filter = QueryFilter {
        app: app.to_string(),
        limit: 5,
        ..Default::default()
    };
    let results = query(&dir.path().to_path_buf(), &filter, None).unwrap();
    assert_eq!(results.len(), 5);
}

#[test]
fn query_returns_empty_for_missing_app() {
    let dir = TempDir::new().unwrap();
    let filter = QueryFilter {
        app: "nonexistent".to_string(),
        ..Default::default()
    };
    let results = query(&dir.path().to_path_buf(), &filter, None).unwrap();
    assert!(results.is_empty());
}


#[tokio::test]
async fn local_backend_write_and_read_back() {
    let dir = TempDir::new().unwrap();
    let config = temp_config(&dir);
    let backend = LocalBackend::new("testapp".to_string(), &config).unwrap();

    for i in 0..5 {
        backend.write(make_record("testapp", "info", "svc", &format!("log line {}", i))).await.unwrap();
    }
    backend.shutdown().await.unwrap();

    let filter = QueryFilter {
        app: "testapp".to_string(),
        limit: 100,
        ..Default::default()
    };
    let results = query(&dir.path().to_path_buf(), &filter, None).unwrap();
    assert_eq!(results.len(), 5);
}

#[tokio::test]
async fn local_backend_shutdown_flushes_buffer() {
    let dir = TempDir::new().unwrap();
    let config = temp_config(&dir);
    let backend = LocalBackend::new("flushapp".to_string(), &config).unwrap();

    backend.write(make_record("flushapp", "warn", "svc", "buffered")).await.unwrap();
    backend.shutdown().await.unwrap();

    let filter = QueryFilter {
        app: "flushapp".to_string(),
        ..Default::default()
    };
    let results = query(&dir.path().to_path_buf(), &filter, None).unwrap();
    assert_eq!(results.len(), 1);
}

#[test]
fn records_to_batch_preserves_all_fields() {
    let mut r = LogRecord::new("app", "error", "svc", "msg");
    r.trace_id = Some("trace-1".to_string());
    r.span_id = Some("span-1".to_string());
    r.chain = Some("ethereum".to_string());
    r.tx_hash = Some("0xabc".to_string());
    r.tags = Some(r#"{"env":"prod"}"#.to_string());
    r.metadata = Some(r#"{"version":"1"}"#.to_string());

    let batch = records_to_batch(&[r.clone()]).unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.schema(), log_schema());
}

#[test]
fn records_to_batch_handles_null_optional_fields() {
    let r = LogRecord::new("app", "info", "svc", "no optionals");
    let batch = records_to_batch(&[r]).unwrap();
    assert_eq!(batch.num_rows(), 1);
}

#[test]
fn records_to_batch_multiple_records() {
    let records: Vec<_> = (0..100)
        .map(|i| LogRecord::new("app", "info", "svc", &format!("msg {}", i)))
        .collect();
    let batch = records_to_batch(&records).unwrap();
    assert_eq!(batch.num_rows(), 100);
}

#[test]
fn query_filters_by_time_range() {
    let dir = TempDir::new().unwrap();
    let app = "timeapp";
    let date = crate::storage::local::current_date();
    let chunk_dir = dir.path().join(app).join(&date);
    std::fs::create_dir_all(&chunk_dir).unwrap();

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

    let mut old = LogRecord::new(app, "info", "svc", "old log");
    old.timestamp_ms = now - 100_000; // 100s ago
    let mut recent = LogRecord::new(app, "info", "svc", "recent log");
    recent.timestamp_ms = now - 1_000; // 1s ago

    write_parquet(chunk_dir.join("00000.parquet"), &[old, recent]);

    let filter = QueryFilter {
        app: app.to_string(),
        from_ms: Some(now - 5_000),
        to_ms: Some(now),
        ..Default::default()
    };
    let results = query(&dir.path().to_path_buf(), &filter, None).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].message, "recent log");
}

#[test]
fn query_reads_multiple_chunk_files() {
    let dir = TempDir::new().unwrap();
    let app = "multiapp";
    let date = crate::storage::local::current_date();
    let chunk_dir = dir.path().join(app).join(&date);
    std::fs::create_dir_all(&chunk_dir).unwrap();

    write_parquet(chunk_dir.join("00000.parquet"), &[make_record(app, "info", "svc", "chunk 0")]);
    write_parquet(chunk_dir.join("00001.parquet"), &[make_record(app, "info", "svc", "chunk 1")]);
    write_parquet(chunk_dir.join("00002.parquet"), &[make_record(app, "info", "svc", "chunk 2")]);

    let filter = QueryFilter { app: app.to_string(), limit: 100, ..Default::default() };
    let results = query(&dir.path().to_path_buf(), &filter, None).unwrap();
    assert_eq!(results.len(), 3);
}

#[test]
fn query_isolates_apps() {
    let dir = TempDir::new().unwrap();
    let date = crate::storage::local::current_date();

    for app in ["app_a", "app_b"] {
        let chunk_dir = dir.path().join(app).join(&date);
        std::fs::create_dir_all(&chunk_dir).unwrap();
        write_parquet(chunk_dir.join("00000.parquet"), &[make_record(app, "info", "svc", "log")]);
    }

    let filter = QueryFilter { app: "app_a".to_string(), ..Default::default() };
    let results = query(&dir.path().to_path_buf(), &filter, None).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].app, "app_a");
}

#[tokio::test]
async fn local_backend_isolates_apps() {
    let dir = TempDir::new().unwrap();
    let config = temp_config(&dir);

    let backend_a = LocalBackend::new("app_a".to_string(), &config).unwrap();
    let backend_b = LocalBackend::new("app_b".to_string(), &config).unwrap();

    backend_a.write(make_record("app_a", "info", "svc", "from a")).await.unwrap();
    backend_a.write(make_record("app_a", "info", "svc", "also from a")).await.unwrap();
    backend_b.write(make_record("app_b", "info", "svc", "from b")).await.unwrap();

    backend_a.shutdown().await.unwrap();
    backend_b.shutdown().await.unwrap();

    let filter_a = QueryFilter { app: "app_a".to_string(), limit: 100, ..Default::default() };
    let filter_b = QueryFilter { app: "app_b".to_string(), limit: 100, ..Default::default() };

    let res_a = query(&dir.path().to_path_buf(), &filter_a, None).unwrap();
    let res_b = query(&dir.path().to_path_buf(), &filter_b, None).unwrap();

    assert_eq!(res_a.len(), 2);
    assert_eq!(res_b.len(), 1);
    assert!(res_a.iter().all(|r| r.app == "app_a"));
    assert!(res_b.iter().all(|r| r.app == "app_b"));
}
