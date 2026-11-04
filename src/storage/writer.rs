use crate::error::{LogHavenError, Result};
use crate::storage::record::{LogRecord, log_schema, records_to_batch};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::fs::{File, create_dir_all};
use std::path::Path;

const TMP_SUFFIX: &str = "parquet.tmp";

fn storage_err(e: impl std::fmt::Display) -> LogHavenError {
    LogHavenError::Storage(e.to_string())
}

/// Writes `records` as one complete Parquet file at `path`.
/// The file is built under a `.parquet.tmp` name, synced, then renamed, so a
/// reader or a crash never sees a partial `.parquet` file.
pub fn write_chunk(path: &Path, records: &[LogRecord]) -> Result<()> {
    if let Some(parent) = path.parent() {
        create_dir_all(parent)?;
    }
    let tmp = path.with_extension(TMP_SUFFIX);

    let result =
        write_tmp(&tmp, records).and_then(|()| std::fs::rename(&tmp, path).map_err(Into::into));
    if result.is_err() {
        let _ = std::fs::remove_file(&tmp);
        return result;
    }
    // make the rename itself durable; directories cannot be opened on every platform
    if let Some(parent) = path.parent()
        && let Ok(dir) = File::open(parent)
    {
        let _ = dir.sync_all();
    }
    Ok(())
}

fn write_tmp(tmp: &Path, records: &[LogRecord]) -> Result<()> {
    let file = File::create(tmp)?;
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(Default::default()))
        .build();
    let mut writer = ArrowWriter::try_new(file, log_schema(), Some(props)).map_err(storage_err)?;
    writer
        .write(&records_to_batch(records)?)
        .map_err(storage_err)?;
    let file = writer.into_inner().map_err(storage_err)?;
    file.sync_all()?;
    Ok(())
}

/// Removes `.parquet.tmp` leftovers (from a crash mid-write) in every date
/// directory of an app.
pub fn remove_stale_tmp(app_dir: &Path) {
    let Ok(dates) = std::fs::read_dir(app_dir) else {
        return;
    };
    for date in dates.flatten() {
        let Ok(files) = std::fs::read_dir(date.path()) else {
            continue;
        };
        for f in files.flatten() {
            if f.file_name().to_string_lossy().ends_with(TMP_SUFFIX) {
                let _ = std::fs::remove_file(f.path());
            }
        }
    }
}
