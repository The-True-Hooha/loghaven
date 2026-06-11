use crate::error::{LogHavenError, Result};
use crate::storage::record::{log_schema, records_to_batch, LogRecord};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::fs::{File, create_dir_all};
use std::path::PathBuf;

pub struct ChunkWriter {
    writer: ArrowWriter<File>,
    pub path: PathBuf,
    pub record_count: u64,
}

impl ChunkWriter {
    pub fn new(path: PathBuf) -> Result<Self> {
        if let Some(parent) = path.parent() {
            create_dir_all(parent)?;
        }
        let file = File::create(&path)?;
        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(Default::default()))
            .build();
        let writer = ArrowWriter::try_new(file, log_schema(), Some(props))
            .map_err(|e| LogHavenError::Storage(e.to_string()))?;
        Ok(Self { writer, path, record_count: 0 })
    }

    pub fn write_batch(&mut self, records: &[LogRecord]) -> Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        let batch = records_to_batch(records)?;
        self.writer
            .write(&batch)
            .map_err(|e| LogHavenError::Storage(e.to_string()))?;
        self.record_count += records.len() as u64;
        Ok(())
    }

    pub fn finish(self) -> Result<()> {
        self.writer
            .close()
            .map_err(|e| LogHavenError::Storage(e.to_string()))?;
        Ok(())
    }

    pub fn size_bytes(&self) -> u64 {
        std::fs::metadata(&self.path).map(|m| m.len()).unwrap_or(0)
    }
}
