use crate::config::LocalStorageConfig;
use crate::error::{LogHavenError, Result};
use crate::storage::cloud::CloudSyncer;
use crate::storage::index::AppIndex;
use crate::storage::record::LogRecord;
use crate::storage::writer::ChunkWriter;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, oneshot};
use tokio::time::{Duration, interval};

enum WriteCommand {
    Record(LogRecord),
    Flush,
    Shutdown(oneshot::Sender<Result<()>>),
}

pub struct LocalBackend {
    sender: mpsc::Sender<WriteCommand>,
    pub index: Arc<AppIndex>,
    pub data_path: PathBuf,
    pub app: String,
}

struct WriterState {
    writer: Option<ChunkWriter>,
    chunk_counter: u32,
    current_date: String,
    total_records: u64,
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
        let data_path = config.path.clone();
        let index_path = data_path.join(&app).join("index");
        let index = Arc::new(AppIndex::open_or_create(&index_path)?);

        let (tx, rx) = mpsc::channel(10_000);

        let rotate_size_bytes = config.rotate_size_mb * 1024 * 1024;
        let rotate_records = config.rotate_records;
        let flush_interval = Duration::from_secs(config.flush_interval_secs);

        tokio::spawn(writer_task(
            app.clone(),
            data_path.clone(),
            rx,
            Arc::clone(&index),
            rotate_size_bytes,
            rotate_records,
            flush_interval,
            cloud_syncer,
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
            .send(WriteCommand::Record(record))
            .await
            .map_err(|_| LogHavenError::Storage("write channel closed".into()))
    }

    pub async fn flush(&self) -> Result<()> {
        self.sender
            .send(WriteCommand::Flush)
            .await
            .map_err(|_| LogHavenError::Storage("write channel closed".into()))
    }

    pub async fn shutdown(&self) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(WriteCommand::Shutdown(tx))
            .await
            .map_err(|_| LogHavenError::Storage("write channel closed".into()))?;
        rx.await
            .map_err(|_| LogHavenError::Storage("shutdown response lost".into()))?
    }
}

#[allow(clippy::too_many_arguments)]
async fn writer_task(
    app: String,
    data_path: PathBuf,
    mut rx: mpsc::Receiver<WriteCommand>,
    index: Arc<AppIndex>,
    rotate_size_bytes: u64,
    rotate_records: u64,
    flush_interval: Duration,
    cloud_syncer: Option<Arc<CloudSyncer>>,
) {
    let mut buffer: Vec<LogRecord> = Vec::new();
    let mut state = WriterState {
        writer: None,
        chunk_counter: next_chunk_id(&data_path, &app, &current_date()),
        current_date: current_date(),
        total_records: 0,
    };
    let mut ticker = interval(flush_interval);
    ticker.tick().await;

    loop {
        tokio::select! {
            cmd = rx.recv() => {
                match cmd {
                    Some(WriteCommand::Record(r)) => {
                        buffer.push(r);
                        if buffer.len() >= 10_000 {
                            maybe_upload(
                                do_flush(&mut buffer, &mut state, &data_path, &app, &index, rotate_size_bytes, rotate_records),
                                &cloud_syncer,
                            );
                        }
                    }
                    Some(WriteCommand::Flush) => {
                        maybe_upload(
                            do_flush(&mut buffer, &mut state, &data_path, &app, &index, rotate_size_bytes, rotate_records),
                            &cloud_syncer,
                        );
                    }
                    Some(WriteCommand::Shutdown(reply)) => {
                        let rotated = do_flush(&mut buffer, &mut state, &data_path, &app, &index, rotate_size_bytes, rotate_records);
                        maybe_upload(rotated, &cloud_syncer);
                        if let Some(w) = state.writer.take() {
                            let path = w.path.clone();
                            let _ = w.finish();
                            maybe_upload(Some(path), &cloud_syncer);
                        }
                        let _ = index.commit();
                        let _ = reply.send(Ok(()));
                        return;
                    }
                    None => return,
                }
            }
            _ = ticker.tick() => {
                if !buffer.is_empty() {
                    maybe_upload(
                        do_flush(&mut buffer, &mut state, &data_path, &app, &index, rotate_size_bytes, rotate_records),
                        &cloud_syncer,
                    );
                }
            }
        }
    }
}

fn maybe_upload(completed_path: Option<PathBuf>, syncer: &Option<Arc<CloudSyncer>>) {
    if let (Some(path), Some(syncer)) = (completed_path, syncer) {
        let syncer = Arc::clone(syncer);
        tokio::spawn(async move {
            if let Err(e) = syncer.upload_chunk(&path).await {
                eprintln!("Cloud sync error: {}", e);
            }
        });
    }
}

fn do_flush(
    buffer: &mut Vec<LogRecord>,
    state: &mut WriterState,
    data_path: &PathBuf,
    app: &str,
    index: &AppIndex,
    rotate_size_bytes: u64,
    rotate_records: u64,
) -> Option<PathBuf> {
    if buffer.is_empty() {
        return None;
    }

    let today = current_date();
    if today != state.current_date {
        if let Some(w) = state.writer.take() {
            let path = w.path.clone();
            let _ = w.finish();
            let _ = index.commit();
            state.current_date = today;
            state.chunk_counter = 0;
            state.total_records = 0;
            // open new writer below, return old path for upload
            let _ = state.writer.insert({
                let p = chunk_path(data_path, app, &state.current_date, state.chunk_counter);
                ChunkWriter::new(p).expect("failed to create chunk writer")
            });
            write_to_writer(buffer, state, index);
            return Some(path);
        }
        state.current_date = today;
        state.chunk_counter = 0;
        state.total_records = 0;
    }

    state.writer.get_or_insert_with(|| {
        let path = chunk_path(data_path, app, &state.current_date, state.chunk_counter);
        ChunkWriter::new(path).expect("failed to create chunk writer")
    });

    write_to_writer(buffer, state, index);

    let should_rotate = state.writer.as_ref().map_or(false, |w| {
        w.size_bytes() >= rotate_size_bytes || w.record_count >= rotate_records
    });

    if should_rotate {
        let old = state.writer.take().unwrap();
        let old_path = old.path.clone();
        let _ = old.finish();
        let _ = index.commit();
        state.chunk_counter += 1;
        let path = chunk_path(data_path, app, &state.current_date, state.chunk_counter);
        state.writer = ChunkWriter::new(path).ok();
        return Some(old_path);
    }

    None
}

fn write_to_writer(buffer: &mut Vec<LogRecord>, state: &mut WriterState, index: &AppIndex) {
    for record in buffer.iter() {
        let _ = index.add_record(record);
    }
    if let Some(w) = state.writer.as_mut() {
        if let Err(e) = w.write_batch(buffer) {
            eprintln!("Storage write error: {}", e);
        }
    }
    state.total_records += buffer.len() as u64;
    buffer.clear();
}

fn chunk_path(data_path: &PathBuf, app: &str, date: &str, chunk: u32) -> PathBuf {
    data_path
        .join(app)
        .join(date)
        .join(format!("{:05}.parquet", chunk))
}

fn next_chunk_id(data_path: &PathBuf, app: &str, date: &str) -> u32 {
    let dir = data_path.join(app).join(date);
    if !dir.exists() {
        return 0;
    }
    std::fs::read_dir(&dir)
        .ok()
        .into_iter()
        .flatten()
        .flatten()
        .filter_map(|e| {
            let name = e.file_name().into_string().ok()?;
            let stem = name.strip_suffix(".parquet")?;
            stem.parse::<u32>().ok()
        })
        .max()
        .map(|n| n + 1)
        .unwrap_or(0)
}

pub fn current_date() -> String {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    date_from_epoch_secs(secs)
}

fn date_from_epoch_secs(secs: u64) -> String {
    let z = (secs / 86400) as i64 + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = (z - era * 146097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 {
        mp as i64 + 3
    } else {
        mp as i64 - 9
    };
    let y = if m <= 2 { y + 1 } else { y };
    format!("{:04}-{:02}-{:02}", y, m, d)
}
