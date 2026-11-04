use crate::config::LocalStorageConfig;
use crate::error::{LogHavenError, Result};
use crate::storage::cloud::CloudSyncer;
use crate::storage::date::date_from_ms;
use crate::storage::index::AppIndex;
use crate::storage::record::LogRecord;
use crate::storage::writer::{remove_stale_tmp, write_chunk};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{Duration, interval};

const MAX_BATCH: usize = 10_000;
/// Upper bound on records held in memory while chunk writes keep failing.
const MAX_PENDING: usize = 100_000;
const MAX_PENDING_BYTES: usize = 256 * 1024 * 1024;
const SHUTDOWN_SYNC_TIMEOUT: Duration = Duration::from_secs(30);

enum WriteCommand {
    Record(Box<LogRecord>),
    Flush(oneshot::Sender<Result<()>>),
    Shutdown(oneshot::Sender<Result<()>>),
}

pub struct LocalBackend {
    sender: mpsc::Sender<WriteCommand>,
    pub index: Arc<AppIndex>,
    pub data_path: PathBuf,
    pub app: String,
}

impl LocalBackend {
    pub fn new(app: String, config: &LocalStorageConfig) -> Result<Self> {
        Self::new_with_syncer(app, config, None)
    }

    pub fn new_with_syncer(
        app: String,
        config: &LocalStorageConfig,
        cloud_syncer: Option<Arc<CloudSyncer>>,
    ) -> Result<Self> {
        crate::storage::validate_app_name(&app)?;
        let data_path = config.path.clone();
        let app_dir = data_path.join(&app);
        let index = Arc::new(AppIndex::open_or_create(&app_dir.join("index"))?);
        remove_stale_tmp(&app_dir);

        let (tx, rx) = mpsc::channel(10_000);

        let chunk_records = usize::try_from(config.rotate_records)
            .unwrap_or(usize::MAX)
            .max(1);
        let mut writer = ChunkFlusher {
            app_dir,
            chunk_records,
            batch: chunk_records.min(MAX_BATCH),
            seq: 0,
            nonce: rand::random(),
            failing: false,
            index: Arc::clone(&index),
            cloud_syncer,
            sync_lock: Arc::new(tokio::sync::Mutex::new(())),
            sync_tasks: Vec::new(),
            pending: Vec::new(),
        };
        // chunks left over from a previous run (failed or interrupted uploads)
        writer.trigger_sync();
        tokio::spawn(writer_task(
            writer,
            rx,
            Duration::from_secs(config.flush_interval_secs.max(1)),
        ));

        Ok(Self {
            sender: tx,
            index,
            data_path,
            app,
        })
    }

    pub async fn write(&self, record: LogRecord) -> Result<()> {
        self.sender
            .send(WriteCommand::Record(Box::new(record)))
            .await
            .map_err(|_| LogHavenError::Storage("write channel closed".into()))
    }

    /// Writes all buffered records to disk before returning.
    pub async fn flush(&self) -> Result<()> {
        self.request(WriteCommand::Flush).await
    }

    pub async fn shutdown(&self) -> Result<()> {
        self.request(WriteCommand::Shutdown).await
    }

    async fn request(
        &self,
        make: impl FnOnce(oneshot::Sender<Result<()>>) -> WriteCommand,
    ) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(make(tx))
            .await
            .map_err(|_| LogHavenError::Storage("write channel closed".into()))?;
        rx.await
            .map_err(|_| LogHavenError::Storage("writer response lost".into()))?
    }
}

/// Owns the in-memory buffer of one app. Every flush writes immutable,
/// complete chunk files, so nothing on disk is ever half written.
struct ChunkFlusher {
    app_dir: PathBuf,
    chunk_records: usize,
    batch: usize,
    seq: u32,
    nonce: u32,
    /// Set after a failed flush: record arrivals stop triggering flushes until
    /// the next tick or explicit flush, so a broken disk is not hammered per record.
    failing: bool,
    index: Arc<AppIndex>,
    cloud_syncer: Option<Arc<CloudSyncer>>,
    sync_lock: Arc<tokio::sync::Mutex<()>>,
    sync_tasks: Vec<tokio::task::JoinHandle<()>>,
    pending: Vec<LogRecord>,
}

impl ChunkFlusher {
    fn push(&mut self, record: LogRecord) {
        self.pending.push(record);
        if self.pending.len() >= self.batch
            && !self.failing
            && let Err(e) = self.flush()
        {
            eprintln!("Storage flush error: {}", e);
        }
    }

    /// Chunk names sort chronologically and are never reused, even after an
    /// uploaded chunk is deleted locally or the daemon restarts.
    fn next_chunk_name(&mut self) -> String {
        let ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_millis());
        self.seq = self.seq.wrapping_add(1);
        // the nonce keeps names unique across restarts even if the clock steps back
        format!("{:013}-{:08x}-{:05}.parquet", ms, self.nonce, self.seq)
    }

    /// Writes pending records grouped by the UTC date of their own timestamp.
    /// Records whose chunk could not be written stay pending for the next try.
    fn flush(&mut self) -> Result<()> {
        let result = self.flush_pending();
        self.failing = result.is_err();
        result
    }

    fn flush_pending(&mut self) -> Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }

        let mut by_date: BTreeMap<String, Vec<LogRecord>> = BTreeMap::new();
        for record in std::mem::take(&mut self.pending) {
            by_date
                .entry(date_from_ms(record.timestamp_ms))
                .or_default()
                .push(record);
        }

        let mut first_err = None;
        let mut failed: Vec<LogRecord> = Vec::new();
        let mut wrote = false;

        for (date, records) in by_date {
            let date_dir = self.app_dir.join(&date);
            for chunk in records.chunks(self.chunk_records) {
                let path = date_dir.join(self.next_chunk_name());
                match write_chunk(&path, chunk) {
                    Ok(()) => {
                        self.index_chunk(chunk);
                        wrote = true;
                    }
                    Err(e) => {
                        failed.extend_from_slice(chunk);
                        first_err.get_or_insert(e);
                    }
                }
            }
        }

        if let Err(e) = self.index.commit() {
            eprintln!("Index commit error: {}", e);
        }
        if wrote {
            self.trigger_sync();
        }

        self.pending = failed;
        let dropped = cap_pending(&mut self.pending, MAX_PENDING, MAX_PENDING_BYTES);
        if dropped > 0 {
            eprintln!("Storage: dropped {} oldest unwritable records", dropped);
        }

        first_err.map_or(Ok(()), Err)
    }

    /// The Parquet chunk is the source of truth; an index failure never fails
    /// the flush, but is reported with its size so it is not silent.
    fn index_chunk(&self, records: &[LogRecord]) {
        let failed = records
            .iter()
            .filter(|r| self.index.add_record(r).is_err())
            .count();
        if failed > 0 {
            eprintln!(
                "Index: {} of {} records in {} not indexed",
                failed,
                records.len(),
                self.app_dir.display()
            );
        }
    }

    fn warn_if_unwritten(&self) {
        if !self.pending.is_empty() {
            eprintln!(
                "Storage: {} records for {} were not written at shutdown",
                self.pending.len(),
                self.app_dir.display()
            );
        }
    }

    /// Starts a background upload of every unsynced chunk of this app. Runs are
    /// serialized, and at most one is queued behind the running one because a
    /// queued run rescans the disk anyway.
    fn trigger_sync(&mut self) {
        let Some(syncer) = &self.cloud_syncer else {
            return;
        };
        self.sync_tasks.retain(|h| !h.is_finished());
        if self.sync_tasks.len() >= 2 {
            return;
        }
        let syncer = Arc::clone(syncer);
        let lock = Arc::clone(&self.sync_lock);
        let app_dir = self.app_dir.clone();
        self.sync_tasks.push(tokio::spawn(async move {
            let _serial = lock.lock().await;
            syncer.sync_app(&app_dir).await;
        }));
    }

    /// Waits for running uploads, then uploads whatever is still unsynced.
    async fn finish_sync(&mut self) {
        let Some(syncer) = self.cloud_syncer.clone() else {
            return;
        };
        let app_dir = self.app_dir.clone();
        let lock = Arc::clone(&self.sync_lock);
        let handles = std::mem::take(&mut self.sync_tasks);
        let work = async {
            for h in handles {
                if let Err(e) = h.await {
                    eprintln!("Cloud sync task failed: {}", e);
                }
            }
            let _serial = lock.lock().await;
            syncer.sync_app(&app_dir).await;
        };
        if tokio::time::timeout(SHUTDOWN_SYNC_TIMEOUT, work)
            .await
            .is_err()
        {
            eprintln!(
                "Cloud sync for {} timed out at shutdown; chunks will upload on next start",
                app_dir.display()
            );
        }
    }
}

async fn writer_task(
    mut flusher: ChunkFlusher,
    mut rx: mpsc::Receiver<WriteCommand>,
    flush_interval: Duration,
) {
    let mut ticker = interval(flush_interval);
    ticker.tick().await;

    loop {
        tokio::select! {
            cmd = rx.recv() => match cmd {
                Some(WriteCommand::Record(r)) => flusher.push(*r),
                Some(WriteCommand::Flush(reply)) => {
                    let _ = reply.send(flusher.flush());
                }
                Some(WriteCommand::Shutdown(reply)) => {
                    let result = flusher.flush();
                    flusher.warn_if_unwritten();
                    flusher.finish_sync().await;
                    let _ = reply.send(result);
                    return;
                }
                None => {
                    let _ = flusher.flush();
                    flusher.warn_if_unwritten();
                    flusher.finish_sync().await;
                    return;
                }
            },
            _ = ticker.tick() => {
                if let Err(e) = flusher.flush() {
                    eprintln!("Storage flush error: {}", e);
                }
            }
        }
    }
}

fn approx_size(r: &LogRecord) -> usize {
    200 + r.message.len()
        + r.tags.as_ref().map_or(0, String::len)
        + r.metadata.as_ref().map_or(0, String::len)
}

/// Keeps the newest records within both limits; returns how many were dropped.
fn cap_pending(pending: &mut Vec<LogRecord>, max_records: usize, max_bytes: usize) -> usize {
    let mut keep = 0;
    let mut bytes = 0usize;
    for r in pending.iter().rev() {
        bytes = bytes.saturating_add(approx_size(r));
        if keep >= max_records || bytes > max_bytes {
            break;
        }
        keep += 1;
    }
    let excess = pending.len() - keep;
    pending.drain(..excess);
    excess
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::query::{QueryFilter, query};
    use std::path::Path;

    fn cfg(name: &str, rotate_records: u64) -> (LocalStorageConfig, PathBuf) {
        let tmp = std::env::temp_dir().join(format!("lh-l-{}-{}", name, std::process::id()));
        let _ = std::fs::remove_dir_all(&tmp);
        let cfg = LocalStorageConfig {
            path: tmp.clone(),
            rotate_records,
            flush_interval_secs: 3600,
            ..Default::default()
        };
        (cfg, tmp)
    }

    fn rec(app: &str, ts: i64, msg: &str) -> LogRecord {
        let mut r = LogRecord::new(app, "info", "src", msg);
        r.timestamp_ms = ts;
        r
    }

    fn read_all(tmp: &Path, app: &str) -> Vec<LogRecord> {
        let f = QueryFilter {
            app: app.into(),
            limit: usize::MAX,
            ..Default::default()
        };
        query(tmp, &f).unwrap()
    }

    fn chunks_in(dir: &Path) -> usize {
        std::fs::read_dir(dir)
            .unwrap()
            .flatten()
            .filter(|f| f.path().extension().is_some_and(|e| e == "parquet"))
            .count()
    }

    fn day_dir(tmp: &Path, app: &str, ts: i64) -> PathBuf {
        tmp.join(app).join(date_from_ms(ts))
    }

    fn parquet_files(tmp: &Path, app: &str) -> Vec<PathBuf> {
        let mut out = vec![];
        for d in std::fs::read_dir(tmp.join(app)).unwrap().flatten() {
            if d.file_name() == "index" {
                continue;
            }
            for f in std::fs::read_dir(d.path()).unwrap().flatten() {
                out.push(f.path());
            }
        }
        out.sort();
        out
    }

    #[tokio::test]
    async fn backend_rejects_traversal_app() {
        let (cfg, tmp) = cfg("trav", 100);
        for app in ["../evil", "..", "a/b", ""] {
            assert!(LocalBackend::new(app.into(), &cfg).is_err(), "{app:?}");
        }
        assert!(!tmp.exists());
    }

    #[tokio::test]
    async fn flushed_records_are_readable_without_shutdown() {
        let (cfg, tmp) = cfg("vis", 100);
        let b = LocalBackend::new("web".into(), &cfg).unwrap();
        b.write(rec("web", 1_700_000_000_000, "one")).await.unwrap();
        b.write(rec("web", 1_700_000_000_001, "two")).await.unwrap();
        b.flush().await.unwrap();
        let got = read_all(&tmp, "web");
        assert_eq!(got.len(), 2);
        assert!(got.iter().any(|r| r.message == "one"));
        assert!(got.iter().any(|r| r.message == "two"));
        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[tokio::test]
    async fn empty_flush_writes_nothing() {
        let (cfg, tmp) = cfg("empty", 100);
        let b = LocalBackend::new("web".into(), &cfg).unwrap();
        b.flush().await.unwrap();
        assert!(parquet_files(&tmp, "web").is_empty());
        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[tokio::test]
    async fn chunks_never_exceed_rotate_records() {
        let (cfg, tmp) = cfg("rot", 10);
        let b = LocalBackend::new("web".into(), &cfg).unwrap();
        for i in 0..25 {
            b.write(rec("web", 1_700_000_000_000 + i, &format!("m{i}")))
                .await
                .unwrap();
        }
        b.flush().await.unwrap();
        let files = parquet_files(&tmp, "web");
        let mut sizes = vec![];
        for f in &files {
            let n =
                parquet::file::reader::SerializedFileReader::new(std::fs::File::open(f).unwrap())
                    .unwrap();
            use parquet::file::reader::FileReader;
            sizes.push(n.metadata().file_metadata().num_rows());
        }
        sizes.sort();
        assert_eq!(sizes, vec![5, 10, 10]);
        assert_eq!(read_all(&tmp, "web").len(), 25);
        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[tokio::test]
    async fn retained_backlog_is_split_into_capped_chunks() {
        let (cfg, tmp) = cfg("backlog", 10);
        let b = LocalBackend::new("web".into(), &cfg).unwrap();
        let blocker = day_dir(&tmp, "web", 1_700_000_000_000);
        std::fs::write(&blocker, b"in the way").unwrap();
        for i in 0..20 {
            b.write(rec("web", 1_700_000_000_000 + i, "m"))
                .await
                .unwrap();
        }
        assert!(b.flush().await.is_err());
        std::fs::remove_file(&blocker).unwrap();
        b.flush().await.unwrap();
        assert_eq!(parquet_files(&tmp, "web").len(), 2);
        assert_eq!(read_all(&tmp, "web").len(), 20);
        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[tokio::test]
    async fn records_are_filed_under_their_own_date() {
        let (cfg, tmp) = cfg("dates", 100);
        let b = LocalBackend::new("web".into(), &cfg).unwrap();
        // 2020-01-01T23:59:59.999Z and 2020-01-02T00:00:00.000Z
        b.write(rec("web", 1_577_923_199_999, "late"))
            .await
            .unwrap();
        b.write(rec("web", 1_577_923_200_000, "early"))
            .await
            .unwrap();
        b.flush().await.unwrap();
        assert_eq!(chunks_in(&tmp.join("web/2020-01-01")), 1);
        assert_eq!(chunks_in(&tmp.join("web/2020-01-02")), 1);
        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[tokio::test]
    async fn numbering_continues_across_restart_and_flushes() {
        let (cfg, tmp) = cfg("num", 100);
        {
            let b = LocalBackend::new("web".into(), &cfg).unwrap();
            b.write(rec("web", 1_700_000_000_000, "a")).await.unwrap();
            b.flush().await.unwrap();
            b.write(rec("web", 1_700_000_000_001, "b")).await.unwrap();
            b.flush().await.unwrap();
            b.shutdown().await.unwrap();
        }
        let b = LocalBackend::new("web".into(), &cfg).unwrap_or_else(|_| panic!("reopen"));
        b.write(rec("web", 1_700_000_000_002, "c")).await.unwrap();
        b.flush().await.unwrap();
        assert_eq!(parquet_files(&tmp, "web").len(), 3);
        assert_eq!(read_all(&tmp, "web").len(), 3);
        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[tokio::test]
    async fn stale_tmp_files_are_ignored_and_removed_on_start() {
        let (cfg, tmp) = cfg("tmp", 100);
        let dir = day_dir(&tmp, "web", 1_700_000_000_000);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("0000000000001-00001.parquet.tmp"), b"half written").unwrap();
        assert!(read_all(&tmp, "web").is_empty());
        let b = LocalBackend::new("web".into(), &cfg).unwrap();
        assert!(!dir.join("0000000000001-00001.parquet.tmp").exists());
        b.write(rec("web", 1_700_000_000_000, "ok")).await.unwrap();
        b.flush().await.unwrap();
        assert_eq!(read_all(&tmp, "web").len(), 1);
        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[tokio::test]
    async fn shutdown_flushes_pending_records() {
        let (cfg, tmp) = cfg("down", 100);
        let b = LocalBackend::new("web".into(), &cfg).unwrap();
        b.write(rec("web", 1_700_000_000_000, "last"))
            .await
            .unwrap();
        b.shutdown().await.unwrap();
        assert_eq!(read_all(&tmp, "web").len(), 1);
        assert!(b.write(rec("web", 1, "x")).await.is_err());
        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[tokio::test]
    async fn write_failure_keeps_records_and_task_survives() {
        let (cfg, tmp) = cfg("fail", 100);
        let b = LocalBackend::new("web".into(), &cfg).unwrap();
        // a regular file where the date directory must go
        let blocker = day_dir(&tmp, "web", 1_700_000_000_000);
        std::fs::write(&blocker, b"in the way").unwrap();

        b.write(rec("web", 1_700_000_000_000, "kept"))
            .await
            .unwrap();
        assert!(b.flush().await.is_err());
        assert!(
            b.flush().await.is_err(),
            "records still pending, task alive"
        );

        std::fs::remove_file(&blocker).unwrap();
        b.flush().await.unwrap();
        let got = read_all(&tmp, "web");
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].message, "kept");
        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[tokio::test]
    async fn one_bad_date_does_not_block_other_dates() {
        let (cfg, tmp) = cfg("partial", 100);
        let b = LocalBackend::new("web".into(), &cfg).unwrap();
        std::fs::write(day_dir(&tmp, "web", 1_700_000_000_000), b"in the way").unwrap();
        b.write(rec("web", 1_700_000_000_000, "blocked"))
            .await
            .unwrap(); // 2023-11-14
        b.write(rec("web", 1_577_923_200_000, "fine"))
            .await
            .unwrap(); // 2020-01-02
        assert!(b.flush().await.is_err());
        let got = read_all(&tmp, "web");
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].message, "fine");
        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[tokio::test]
    async fn all_fields_round_trip() {
        let (cfg, tmp) = cfg("rt", 100);
        let b = LocalBackend::new("web".into(), &cfg).unwrap();
        let mut full = rec("web", 1_700_000_000_000, "h\u{e9}llo \u{1f980}\nline2");
        full.trace_id = Some("t".into());
        full.span_id = Some("s".into());
        full.chain = Some("ethereum".into());
        full.tx_hash = Some("0xabc".into());
        full.tags = Some(r#"["a"]"#.into());
        full.metadata = Some(r#"{"k":1}"#.into());
        let big = rec("web", 1_700_000_000_001, &"x".repeat(1_000_000));
        b.write(full.clone()).await.unwrap();
        b.write(big.clone()).await.unwrap();
        b.flush().await.unwrap();
        let mut got = read_all(&tmp, "web");
        got.sort_by_key(|r| r.timestamp_ms);
        assert_eq!(got[0].message, full.message);
        assert_eq!(got[0].trace_id, full.trace_id);
        assert_eq!(got[0].span_id, full.span_id);
        assert_eq!(got[0].chain, full.chain);
        assert_eq!(got[0].tx_hash, full.tx_hash);
        assert_eq!(got[0].tags, full.tags);
        assert_eq!(got[0].metadata, full.metadata);
        assert_eq!(got[1].message.len(), 1_000_000);
        assert_eq!(got[1].trace_id, None);
        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn cap_pending_keeps_newest_by_count() {
        let mut v: Vec<LogRecord> = (0..10).map(|i| rec("a", i, &i.to_string())).collect();
        assert_eq!(cap_pending(&mut v, 4, usize::MAX), 6);
        assert_eq!(v.first().unwrap().message, "6");
        assert_eq!(cap_pending(&mut v, 4, usize::MAX), 0);
        assert_eq!(v.len(), 4);
    }

    #[test]
    fn cap_pending_keeps_newest_by_bytes() {
        let mut v: Vec<LogRecord> = (0..5)
            .map(|i| rec("a", i, &format!("{i}{}", "x".repeat(1000))))
            .collect();
        let dropped = cap_pending(&mut v, usize::MAX, 2 * 1_201);
        assert_eq!(dropped, 3);
        assert!(v[0].message.starts_with('3'));
        let mut one = vec![rec("a", 0, &"x".repeat(10_000))];
        assert_eq!(cap_pending(&mut one, usize::MAX, 100), 1);
        assert!(one.is_empty());
    }

    #[tokio::test]
    async fn chunk_names_are_never_reused_after_local_delete() {
        let (cfg, tmp) = cfg("reuse", 100);
        let b = LocalBackend::new("web".into(), &cfg).unwrap();
        let ts = 1_700_000_000_000;
        b.write(rec("web", ts, "a")).await.unwrap();
        b.flush().await.unwrap();
        let first = parquet_files(&tmp, "web").remove(0);
        std::fs::remove_file(&first).unwrap(); // as delete_after_upload would
        b.write(rec("web", ts + 1, "b")).await.unwrap();
        b.flush().await.unwrap();
        let second = parquet_files(&tmp, "web").remove(0);
        assert_ne!(first, second);
        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[tokio::test]
    async fn failed_flush_stops_per_record_flushing_until_explicit_flush() {
        let (cfg, tmp) = cfg("backoff", 2);
        let b = LocalBackend::new("web".into(), &cfg).unwrap();
        let ts = 1_700_000_000_000;
        let blocker = day_dir(&tmp, "web", ts);
        std::fs::write(&blocker, b"x").unwrap();
        for i in 0..4 {
            b.write(rec("web", ts + i, "m")).await.unwrap();
        }
        std::fs::remove_file(&blocker).unwrap();
        // 4 records pending; push-triggered flush is suspended after the first failure,
        // so nothing was written even though the disk is fine again
        b.write(rec("web", ts + 4, "m")).await.unwrap();
        assert!(!tmp.join("web").join(date_from_ms(ts)).exists());
        b.flush().await.unwrap();
        assert_eq!(read_all(&tmp, "web").len(), 5);
        let _ = std::fs::remove_dir_all(&tmp);
    }

    fn cfg_with_syncer(
        name: &str,
        delete: bool,
        delay_ms: u64,
    ) -> (
        LocalStorageConfig,
        PathBuf,
        Arc<CloudSyncer>,
        Arc<crate::storage::cloud::testing::MockStore>,
    ) {
        let (cfg, tmp) = cfg(name, 100);
        let store = Arc::new(crate::storage::cloud::testing::MockStore {
            delay_ms,
            ..Default::default()
        });
        let syncer = CloudSyncer::with_store(Box::new(store.clone()), None, tmp.clone(), delete);
        (cfg, tmp, syncer, store)
    }

    #[tokio::test]
    async fn shutdown_waits_for_uploads() {
        let (cfg, tmp, syncer, store) = cfg_with_syncer("sd-up", false, 150);
        let b = LocalBackend::new_with_syncer("web".into(), &cfg, Some(syncer)).unwrap();
        b.write(rec("web", 1_700_000_000_000, "a")).await.unwrap();
        b.shutdown().await.unwrap();
        assert_eq!(
            store.puts.lock().unwrap().len(),
            1,
            "upload finished before shutdown returned"
        );
        assert_eq!(chunks_in(&day_dir(&tmp, "web", 1_700_000_000_000)), 1);
        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[tokio::test]
    async fn leftover_chunks_are_uploaded_on_start() {
        let (cfg, tmp, syncer, store) = cfg_with_syncer("left", false, 0);
        {
            let b = LocalBackend::new("web".into(), &cfg).unwrap();
            b.write(rec("web", 1_700_000_000_000, "a")).await.unwrap();
            b.shutdown().await.unwrap();
        }
        assert!(store.puts.lock().unwrap().is_empty());
        let b = LocalBackend::new_with_syncer("web".into(), &cfg, Some(syncer)).unwrap();
        b.shutdown().await.unwrap();
        assert_eq!(store.puts.lock().unwrap().len(), 1);
        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[tokio::test]
    async fn failed_uploads_are_retried_and_chunks_kept_meanwhile() {
        let (cfg, tmp, syncer, store) = cfg_with_syncer("retry", true, 0);
        store
            .fail_all
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let b = LocalBackend::new_with_syncer("web".into(), &cfg, Some(syncer)).unwrap();
        let ts = 1_700_000_000_000;
        for i in 0..2 {
            b.write(rec("web", ts + i, "m")).await.unwrap();
            b.flush().await.unwrap();
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            parquet_files(&tmp, "web").len(),
            2,
            "kept while uploads fail"
        );
        assert_eq!(read_all(&tmp, "web").len(), 2);

        store
            .fail_all
            .store(false, std::sync::atomic::Ordering::SeqCst);
        b.shutdown().await.unwrap();
        assert!(
            parquet_files(&tmp, "web").is_empty(),
            "uploaded then deleted"
        );
        assert_eq!(store.puts.lock().unwrap().len(), 2);
        assert_eq!(
            read_all(&tmp, "web").len(),
            0,
            "deleted chunks are not queryable"
        );
        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[tokio::test]
    async fn every_chunk_is_uploaded_despite_rapid_flushes_and_slow_store() {
        let (cfg, tmp, syncer, store) = cfg_with_syncer("rapid", false, 40);
        let b = LocalBackend::new_with_syncer("web".into(), &cfg, Some(syncer)).unwrap();
        for i in 0..8 {
            b.write(rec("web", 1_700_000_000_000 + i, "m"))
                .await
                .unwrap();
            b.flush().await.unwrap();
        }
        b.shutdown().await.unwrap();
        let mut puts = store.puts.lock().unwrap().clone();
        puts.sort();
        puts.dedup();
        assert_eq!(puts.len(), 8, "each chunk uploaded");
        assert_eq!(
            parquet_files(&tmp, "web").len(),
            8 * 2,
            "chunk + marker per upload"
        );
        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[tokio::test]
    async fn chunk_names_differ_between_backends_started_in_the_same_millisecond() {
        let mk = |tmp: &Path| ChunkFlusher {
            app_dir: tmp.to_path_buf(),
            chunk_records: 1,
            batch: 1,
            seq: 0,
            nonce: rand::random(),
            failing: false,
            index: Arc::new(AppIndex::open_or_create(&tmp.join("index")).unwrap()),
            cloud_syncer: None,
            sync_lock: Arc::new(tokio::sync::Mutex::new(())),
            sync_tasks: Vec::new(),
            pending: Vec::new(),
        };
        let (_, tmp) = cfg("nonce", 1);
        std::fs::create_dir_all(&tmp).unwrap();
        let mut a = mk(&tmp.join("a"));
        let mut b = mk(&tmp.join("b"));
        let names: std::collections::HashSet<_> = (0..50)
            .flat_map(|_| [a.next_chunk_name(), b.next_chunk_name()])
            .collect();
        assert_eq!(names.len(), 100);
        let _ = std::fs::remove_dir_all(&tmp);
    }
}
