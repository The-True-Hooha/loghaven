use crate::error::{LogHavenError, Result};
use crate::storage::index::AppIndex;
use crate::storage::record::LogRecord;
use arrow::array::{Array, Int64Array, LargeStringArray, StringArray};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;

pub struct QueryFilter {
    pub app: String,
    pub source: Option<String>,
    pub level: Option<String>,
    pub from_ms: Option<i64>,
    pub to_ms: Option<i64>,
    pub text: Option<String>,
    pub limit: usize,
}

impl Default for QueryFilter {
    fn default() -> Self {
        Self {
            app: String::new(),
            source: None,
            level: None,
            from_ms: None,
            to_ms: None,
            text: None,
            limit: 1000,
        }
    }
}

pub fn query(
    data_path: &PathBuf,
    filter: &QueryFilter,
    index: Option<&Arc<AppIndex>>,
) -> Result<Vec<LogRecord>> {
    if let Some(text) = &filter.text {
        if let Some(idx) = index {
            let mut results = idx.search(text, filter.limit)?;
            results = apply_filters(results, filter);
            return Ok(results);
        }
    }

    scan_parquet(data_path, filter)
}

fn scan_parquet(data_path: &PathBuf, filter: &QueryFilter) -> Result<Vec<LogRecord>> {
    let app_dir = data_path.join(&filter.app);
    if !app_dir.exists() {
        return Ok(vec![]);
    }

    let mut date_dirs: Vec<PathBuf> = std::fs::read_dir(&app_dir)
        .map_err(|e| LogHavenError::Storage(e.to_string()))?
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.is_dir())
        .collect();

    date_dirs.sort();

    if let (Some(from), Some(to)) = (filter.from_ms, filter.to_ms) {
        let from_date = ms_to_date_str(from);
        let to_date = ms_to_date_str(to);
        date_dirs.retain(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .map(|d| d >= from_date.as_str() && d <= to_date.as_str())
                .unwrap_or(false)
        });
    }

    let mut results: Vec<LogRecord> = Vec::new();

    'outer: for date_dir in &date_dirs {
        let mut chunk_files: Vec<PathBuf> = std::fs::read_dir(date_dir)
            .map_err(|e| LogHavenError::Storage(e.to_string()))?
            .flatten()
            .map(|e| e.path())
            .filter(|p| p.extension().and_then(|e| e.to_str()) == Some("parquet"))
            .collect();

        chunk_files.sort();

        for chunk_path in &chunk_files {
            let records = read_parquet_file(chunk_path)?;
            for record in records {
                if matches_filter(&record, filter) {
                    results.push(record);
                    if results.len() >= filter.limit {
                        break 'outer;
                    }
                }
            }
        }
    }

    Ok(results)
}

fn read_parquet_file(path: &PathBuf) -> Result<Vec<LogRecord>> {
    let file = File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| LogHavenError::Storage(e.to_string()))?;
    let reader = builder.build()
        .map_err(|e| LogHavenError::Storage(e.to_string()))?;

    let mut records = Vec::new();

    for batch_result in reader {
        let batch = batch_result.map_err(|e| LogHavenError::Storage(e.to_string()))?;
        let schema = batch.schema();

        let col_idx = |name: &str| schema.index_of(name).ok();

        let ids       = col_idx("id").and_then(|i| batch.column(i).as_any().downcast_ref::<StringArray>().map(|a| a as *const _));
        let apps      = col_idx("app").and_then(|i| batch.column(i).as_any().downcast_ref::<StringArray>().map(|a| a as *const _));
        let tss       = col_idx("timestamp_ms").and_then(|i| batch.column(i).as_any().downcast_ref::<Int64Array>().map(|a| a as *const _));
        let levels    = col_idx("level").and_then(|i| batch.column(i).as_any().downcast_ref::<StringArray>().map(|a| a as *const _));
        let sources   = col_idx("source").and_then(|i| batch.column(i).as_any().downcast_ref::<StringArray>().map(|a| a as *const _));
        let messages  = col_idx("message").and_then(|i| batch.column(i).as_any().downcast_ref::<LargeStringArray>().map(|a| a as *const _));
        let trace_ids = col_idx("trace_id").and_then(|i| batch.column(i).as_any().downcast_ref::<StringArray>().map(|a| a as *const _));
        let span_ids  = col_idx("span_id").and_then(|i| batch.column(i).as_any().downcast_ref::<StringArray>().map(|a| a as *const _));
        let chains    = col_idx("chain").and_then(|i| batch.column(i).as_any().downcast_ref::<StringArray>().map(|a| a as *const _));
        let tx_hashes = col_idx("tx_hash").and_then(|i| batch.column(i).as_any().downcast_ref::<StringArray>().map(|a| a as *const _));
        let tags_col  = col_idx("tags").and_then(|i| batch.column(i).as_any().downcast_ref::<StringArray>().map(|a| a as *const _));
        let meta_col  = col_idx("metadata").and_then(|i| batch.column(i).as_any().downcast_ref::<StringArray>().map(|a| a as *const _));

        for row in 0..batch.num_rows() {
            let get_str = |ptr: Option<*const StringArray>| -> String {
                ptr.map(|p| unsafe { &*p })
                    .filter(|a| !a.is_null(row))
                    .map(|a| a.value(row).to_string())
                    .unwrap_or_default()
            };
            let get_opt = |ptr: Option<*const StringArray>| -> Option<String> {
                ptr.map(|p| unsafe { &*p })
                    .filter(|a| !a.is_null(row))
                    .map(|a| a.value(row).to_string())
                    .filter(|s| !s.is_empty())
            };
            let get_large = |ptr: Option<*const LargeStringArray>| -> String {
                ptr.map(|p| unsafe { &*p })
                    .filter(|a| !a.is_null(row))
                    .map(|a| a.value(row).to_string())
                    .unwrap_or_default()
            };
            let get_i64 = |ptr: Option<*const Int64Array>| -> i64 {
                ptr.map(|p| unsafe { &*p })
                    .filter(|a| !a.is_null(row))
                    .map(|a| a.value(row))
                    .unwrap_or(0)
            };

            records.push(LogRecord {
                id:           get_str(ids),
                app:          get_str(apps),
                timestamp_ms: get_i64(tss),
                level:        get_str(levels),
                source:       get_str(sources),
                message:      get_large(messages),
                trace_id:     get_opt(trace_ids),
                span_id:      get_opt(span_ids),
                chain:        get_opt(chains),
                tx_hash:      get_opt(tx_hashes),
                tags:         get_opt(tags_col),
                metadata:     get_opt(meta_col),
            });
        }
    }

    Ok(records)
}

fn matches_filter(record: &LogRecord, filter: &QueryFilter) -> bool {
    if let Some(source) = &filter.source {
        if &record.source != source {
            return false;
        }
    }
    if let Some(level) = &filter.level {
        if &record.level != level {
            return false;
        }
    }
    if let Some(from) = filter.from_ms {
        if record.timestamp_ms < from {
            return false;
        }
    }
    if let Some(to) = filter.to_ms {
        if record.timestamp_ms > to {
            return false;
        }
    }
    true
}

fn apply_filters(records: Vec<LogRecord>, filter: &QueryFilter) -> Vec<LogRecord> {
    records
        .into_iter()
        .filter(|r| matches_filter(r, filter))
        .take(filter.limit)
        .collect()
}

fn ms_to_date_str(ms: i64) -> String {
    let secs = (ms / 1000) as u64;
    let z = (secs / 86400) as i64 + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = (z - era * 146097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp as i64 + 3 } else { mp as i64 - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    format!("{:04}-{:02}-{:02}", y, m, d)
}
