use crate::error::{LogHavenError, Result};
use crate::storage::record::{LogRecord, log_schema, records_to_batch};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::metadata::KeyValue;
use parquet::file::properties::WriterProperties;
use parquet::schema::types::ColumnPath;
use std::borrow::Cow;
use std::fs::{File, create_dir_all};
use std::path::Path;

const TMP_SUFFIX: &str = "parquet.tmp";

/// File metadata key set when every EVM-style `tx_hash` in the file is stored
/// lowercase, which lets a reader test the tx_hash bloom filter for one spelling.
pub const TX_FOLDED_KEY: &str = "loghaven.tx_hash_folded";

/// Lowercases `0x`+hex transaction hashes: they are case-insensitive, and a
/// single spelling is what makes the bloom filter usable.
fn fold_tx_hashes(records: &[LogRecord]) -> Cow<'_, [LogRecord]> {
    let needs = |r: &LogRecord| {
        r.tx_hash.as_deref().is_some_and(|h| {
            crate::storage::query::is_evm_hash(h) && h.bytes().any(|b| b.is_ascii_uppercase())
        })
    };
    if !records.iter().any(needs) {
        return Cow::Borrowed(records);
    }
    Cow::Owned(
        records
            .iter()
            .cloned()
            .map(|mut r| {
                if needs(&r) {
                    r.tx_hash = r.tx_hash.map(|h| h.to_ascii_lowercase());
                }
                r
            })
            .collect(),
    )
}

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
    let records = fold_tx_hashes(records);
    let file = File::create(tmp)?;
    let tx = ColumnPath::from("tx_hash");
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(Default::default()))
        .set_column_bloom_filter_enabled(tx.clone(), true)
        .set_column_bloom_filter_max_ndv(tx.clone(), records.len().max(1) as u64)
        .set_column_bloom_filter_fpp(tx, 0.01)
        .set_key_value_metadata(Some(vec![KeyValue::new(
            TX_FOLDED_KEY.to_string(),
            "1".to_string(),
        )]))
        .build();
    let mut writer = ArrowWriter::try_new(file, log_schema(), Some(props)).map_err(storage_err)?;
    writer
        .write(&records_to_batch(&records)?)
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
