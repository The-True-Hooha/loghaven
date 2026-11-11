use crate::error::{LogHavenError, Result};
use crate::storage::date::parse_date;
use crate::storage::record::LogRecord;
use arrow::array::{Array, Int64Array, LargeStringArray, StringArray};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder, RowSelection,
    RowSelector,
};
use parquet::file::metadata::PageIndexPolicy;
use std::fs::File;
use std::path::{Path, PathBuf};

/// A query never returns more than about this much record data.
pub const MAX_RESULT_BYTES: usize = 64 * 1024 * 1024;

pub struct QueryFilter {
    pub app: String,
    pub source: Option<String>,
    pub level: Option<String>,
    pub from_ms: Option<i64>,
    pub to_ms: Option<i64>,
    /// Case-insensitive substring match on message, tags and metadata.
    pub text: Option<String>,
    /// Exact match, except that a `0x`+hex value (EVM) ignores ASCII case.
    pub tx_hash: Option<String>,
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
            tx_hash: None,
            limit: 1000,
        }
    }
}

/// EVM hashes are hex, so `0xABC`, `0Xabc` and `0xabc` name the same transaction.
/// Other chains (base58 and similar) are case-sensitive.
pub fn is_evm_hash(h: &str) -> bool {
    h.strip_prefix("0x")
        .or_else(|| h.strip_prefix("0X"))
        .is_some_and(|x| !x.is_empty() && x.bytes().all(|b| b.is_ascii_hexdigit()))
}

/// Returns up to `filter.limit` matching records, newest first.
///
/// Relies on the writer's invariant that a chunk lives in the directory of the
/// UTC date of its records' timestamps; mis-filed chunks can be missed.
/// `text` uses `str::to_lowercase`, which is close to but not full Unicode case folding.
///
/// Parquet chunks are the source of truth. Chunks that cannot be read are
/// skipped with a warning so one bad file never fails the whole query.
pub fn query(data_path: &Path, filter: &QueryFilter) -> Result<Vec<LogRecord>> {
    crate::storage::validate_app_name(&filter.app)?;
    if filter.limit == 0 {
        return Ok(vec![]);
    }
    let app_dir = data_path.join(&filter.app);

    let text_lc = filter
        .text
        .as_deref()
        .filter(|t| !t.is_empty())
        .map(str::to_lowercase);

    let mut date_dirs = date_dirs_in_range(&app_dir, filter)?;
    date_dirs.sort_by_key(|(day, _)| std::cmp::Reverse(*day));

    let mut results: Vec<LogRecord> = Vec::new();
    // once `limit` records are held, an older record can no longer be among the newest
    let mut floor: Option<i64> = None;

    for (_, date_dir) in &date_dirs {
        // every record of a later date is newer than any record of an earlier one
        if results.len() >= filter.limit {
            break;
        }
        for chunk_path in chunk_files(date_dir)? {
            let records = match read_chunk(&chunk_path, filter, text_lc.as_deref(), floor) {
                Ok(r) => r,
                Err(e) => {
                    eprintln!(
                        "Query: skipping unreadable chunk {}: {}",
                        chunk_path.display(),
                        e
                    );
                    continue;
                }
            };
            results.extend(records);
            trim_newest(&mut results, filter.limit, false);
            if results.len() >= filter.limit {
                trim_newest(&mut results, filter.limit, true);
                if results.len() >= filter.limit {
                    floor = results.last().map(|r| r.timestamp_ms);
                }
            }
        }
    }

    trim_newest(&mut results, filter.limit, true);
    Ok(results)
}

/// Newest first; equal timestamps break ties on the (time-ordered) id so the
/// cut at `limit` is deterministic.
fn newest_first(a: &LogRecord, b: &LogRecord) -> std::cmp::Ordering {
    b.timestamp_ms
        .cmp(&a.timestamp_ms)
        .then_with(|| b.id.cmp(&a.id))
}

fn approx_bytes(r: &LogRecord) -> usize {
    200 + r.message.len()
        + r.tags.as_ref().map_or(0, String::len)
        + r.metadata.as_ref().map_or(0, String::len)
}

/// Keeps the newest records within `limit` and `MAX_RESULT_BYTES` (always at
/// least one). Unless `force`, only does the work once the buffer is well past
/// either bound, to amortize sorting.
fn trim_newest(results: &mut Vec<LogRecord>, limit: usize, force: bool) {
    let bytes: usize = results.iter().map(approx_bytes).sum();
    if !force && results.len() <= limit.saturating_mul(2) && bytes <= MAX_RESULT_BYTES * 2 {
        return;
    }
    results.sort_by(newest_first);
    results.truncate(limit);
    let mut total = 0usize;
    let mut keep = 0;
    for r in results.iter() {
        total = total.saturating_add(approx_bytes(r));
        if total > MAX_RESULT_BYTES && keep > 0 {
            break;
        }
        keep += 1;
    }
    results.truncate(keep);
}

/// `YYYY-MM-DD` directories of an app that can overlap the filter's time range.
/// Anything else in the app directory (the index, stray files) is ignored.
fn date_dirs_in_range(app_dir: &Path, filter: &QueryFilter) -> Result<Vec<(i64, PathBuf)>> {
    let from_day = filter.from_ms.map(|ms| ms.div_euclid(86_400_000));
    let to_day = filter.to_ms.map(|ms| ms.div_euclid(86_400_000));

    let entries = match std::fs::read_dir(app_dir) {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(vec![]),
        Err(e) => return Err(LogHavenError::Storage(e.to_string())),
    };
    let mut out = Vec::new();
    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let Some(day) = entry.file_name().to_str().and_then(parse_date) else {
            continue;
        };
        if from_day.is_some_and(|f| day < f) || to_day.is_some_and(|t| day > t) {
            continue;
        }
        out.push((day, path));
    }
    Ok(out)
}

/// Finished chunks of one date, newest first. `.parquet.tmp` files are not chunks.
fn chunk_files(date_dir: &Path) -> Result<Vec<PathBuf>> {
    let entries = match std::fs::read_dir(date_dir) {
        Ok(e) => e,
        // pruned or uploaded-and-deleted while the query runs
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(vec![]),
        Err(e) => return Err(LogHavenError::Storage(e.to_string())),
    };
    let mut files: Vec<PathBuf> = entries
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.extension().and_then(|e| e.to_str()) == Some("parquet"))
        .collect();
    files.sort_by(|a, b| b.cmp(a));
    Ok(files)
}

fn col<'a, T: 'static>(batch: &'a RecordBatch, name: &str) -> Option<&'a T> {
    batch.column_by_name(name)?.as_any().downcast_ref::<T>()
}

fn str_at(a: Option<&StringArray>, row: usize) -> Option<String> {
    a.filter(|a| !a.is_null(row))
        .map(|a| a.value(row).to_string())
}

/// Row groups whose `timestamp_ms` statistics cannot overlap `[from, to]` are
/// not decoded. Groups without statistics are always read.
fn overlapping_row_groups(
    meta: &parquet::file::metadata::ParquetMetaData,
    from_ms: Option<i64>,
    to_ms: Option<i64>,
) -> Vec<usize> {
    use parquet::file::statistics::Statistics;
    let ts_col = meta
        .file_metadata()
        .schema_descr()
        .columns()
        .iter()
        .position(|c| c.name() == "timestamp_ms");
    (0..meta.num_row_groups())
        .filter(|&i| {
            let Some(col) = ts_col else { return true };
            let Some(Statistics::Int64(st)) = meta.row_group(i).column(col).statistics() else {
                return true;
            };
            let (Some(&min), Some(&max)) = (st.min_opt(), st.max_opt()) else {
                return true;
            };
            !(from_ms.is_some_and(|f| max < f) || to_ms.is_some_and(|t| min > t))
        })
        .collect()
}

/// Row groups that can hold a match: inside the time range and, when a
/// `tx_hash` is wanted, with a tx_hash min/max range that can contain it.
fn row_groups_to_read(
    meta: &parquet::file::metadata::ParquetMetaData,
    filter: &QueryFilter,
) -> Vec<usize> {
    row_groups_to_read_from(meta, filter, filter.from_ms)
}

/// `row_groups_to_read` with the lower time bound given separately from the filter's.
fn row_groups_to_read_from(
    meta: &parquet::file::metadata::ParquetMetaData,
    filter: &QueryFilter,
    from_ms: Option<i64>,
) -> Vec<usize> {
    use parquet::file::statistics::Statistics;
    let mut groups = overlapping_row_groups(meta, from_ms, filter.to_ms);
    let Some(want) = filter.tx_hash.as_deref() else {
        return groups;
    };
    let Some(col) = meta
        .file_metadata()
        .schema_descr()
        .columns()
        .iter()
        .position(|c| c.name() == "tx_hash")
    else {
        return groups;
    };
    // An EVM match ignores ASCII case. For a fixed prefix every spelling sorts
    // between the all-upper and the all-lower hex part (digits are case-less,
    // 'A'..'F' < 'a'..'f'), and the prefix itself can be "0x" or "0X".
    let ranges: Vec<(String, String)> = if is_evm_hash(want) {
        let hex = &want[2..];
        ["0x", "0X"]
            .iter()
            .map(|p| {
                (
                    format!("{p}{}", hex.to_ascii_uppercase()),
                    format!("{p}{}", hex.to_ascii_lowercase()),
                )
            })
            .collect()
    } else {
        vec![(want.to_string(), want.to_string())]
    };
    groups.retain(|&i| {
        let Some(Statistics::ByteArray(st)) = meta.row_group(i).column(col).statistics() else {
            return true;
        };
        let (Some(min), Some(max)) = (st.min_opt(), st.max_opt()) else {
            return true;
        };
        ranges
            .iter()
            .any(|(low, high)| !(max.data() < low.as_bytes() || min.data() > high.as_bytes()))
    });
    groups
}

/// Drops row groups whose tx_hash bloom filter proves the wanted hash is absent.
/// An EVM hash is only tested in files that record lowercase storage; a missing
/// filter or an unreadable one keeps the group.
fn bloom_filtered(
    builder: &ParquetRecordBatchReaderBuilder<File>,
    groups: Vec<usize>,
    filter: &QueryFilter,
) -> Vec<usize> {
    let Some(want) = filter.tx_hash.as_deref() else {
        return groups;
    };
    let evm = is_evm_hash(want);
    let folded = builder
        .metadata()
        .file_metadata()
        .key_value_metadata()
        .is_some_and(|kv| {
            kv.iter().any(|k| {
                k.key == crate::storage::writer::TX_FOLDED_KEY && k.value.as_deref() == Some("1")
            })
        });
    if evm && !folded {
        return groups;
    }
    let Some(col) = builder
        .metadata()
        .file_metadata()
        .schema_descr()
        .columns()
        .iter()
        .position(|c| c.name() == "tx_hash")
    else {
        return groups;
    };
    let spelling = if evm {
        want.to_ascii_lowercase()
    } else {
        want.to_string()
    };
    groups
        .into_iter()
        .filter(
            |&i| match builder.get_row_group_column_bloom_filter(i, col) {
                Ok(Some(bloom)) => bloom.check(spelling.as_str()),
                _ => true,
            },
        )
        .collect()
}

/// Reads the records of one chunk that match `filter`.
fn read_parquet_file(
    path: &Path,
    filter: &QueryFilter,
    text_lc: Option<&str>,
) -> Result<Vec<LogRecord>> {
    read_chunk(path, filter, text_lc, None)
}

/// The columns a row needs to be judged against `filter`, so the rest (the large `message`,
/// `tags` and `metadata` among them) are decoded only for rows that match.
fn filter_columns(filter: &QueryFilter, text: bool) -> Vec<&'static str> {
    let mut cols = vec!["timestamp_ms"];
    if filter.level.is_some() {
        cols.push("level");
    }
    if filter.source.is_some() {
        cols.push("source");
    }
    if filter.tx_hash.is_some() {
        cols.push("tx_hash");
    }
    if text {
        cols.extend(["message", "tags", "metadata"]);
    }
    cols
}

struct RowTest<'a> {
    filter: &'a QueryFilter,
    text_lc: Option<&'a str>,
    evm_fold: bool,
    from_ms: Option<i64>,
}

/// The columns of one batch that `RowTest` looks at, found once instead of once per row.
struct BatchCols<'a> {
    test: &'a RowTest<'a>,
    tss: Option<&'a Int64Array>,
    levels: Option<&'a StringArray>,
    sources: Option<&'a StringArray>,
    tx_hashes: Option<&'a StringArray>,
    messages: Option<&'a LargeStringArray>,
    tags: Option<&'a StringArray>,
    metadata: Option<&'a StringArray>,
}

impl<'a> RowTest<'a> {
    fn bind(&'a self, batch: &'a RecordBatch) -> BatchCols<'a> {
        BatchCols {
            test: self,
            tss: col(batch, "timestamp_ms"),
            levels: col(batch, "level"),
            sources: col(batch, "source"),
            tx_hashes: col(batch, "tx_hash"),
            messages: col(batch, "message"),
            tags: col(batch, "tags"),
            metadata: col(batch, "metadata"),
        }
    }
}

impl BatchCols<'_> {
    fn matches(&self, row: usize) -> bool {
        let filter = self.test.filter;
        let Some(tss) = self.tss else { return false };
        if tss.is_null(row) {
            return false; // a record without a time cannot be placed or ordered
        }
        let ts = tss.value(row);
        if self.test.from_ms.is_some_and(|f| ts < f) || filter.to_ms.is_some_and(|t| ts > t) {
            return false;
        }
        // an absent value never equals the wanted one, but a wanted "" matches it
        let eq_or_empty = |a: Option<&StringArray>, want: &Option<String>| {
            let Some(want) = want else { return true };
            match a {
                Some(a) if !a.is_null(row) => a.value(row) == want,
                _ => want.is_empty(),
            }
        };
        if !eq_or_empty(self.sources, &filter.source) || !eq_or_empty(self.levels, &filter.level) {
            return false;
        }
        if let Some(want) = &filter.tx_hash {
            let hit = self.tx_hashes.is_some_and(|a| {
                !a.is_null(row)
                    && if self.test.evm_fold {
                        a.value(row).eq_ignore_ascii_case(want)
                    } else {
                        a.value(row) == want
                    }
            });
            if !hit {
                return false;
            }
        }
        if let Some(needle) = self.test.text_lc {
            let in_str = |a: Option<&StringArray>| {
                a.is_some_and(|a| !a.is_null(row) && a.value(row).to_lowercase().contains(needle))
            };
            let in_msg = self
                .messages
                .is_some_and(|a| !a.is_null(row) && a.value(row).to_lowercase().contains(needle));
            if !(in_msg || in_str(self.tags) || in_str(self.metadata)) {
                return false;
            }
        }
        true
    }
}

/// Like `read_parquet_file`, and a record older than `floor` is not wanted (the caller already
/// holds enough newer ones), so chunks and row groups that end before it are not decoded.
///
/// The chunk is read twice: first only the columns the filter looks at, to find the matching
/// rows, then every column of just those rows.
fn read_chunk(
    path: &Path,
    filter: &QueryFilter,
    text_lc: Option<&str>,
    floor: Option<i64>,
) -> Result<Vec<LogRecord>> {
    let storage_err = |e: &dyn std::fmt::Display| LogHavenError::Storage(e.to_string());
    let from_ms = match (filter.from_ms, floor) {
        (Some(a), Some(b)) => Some(a.max(b)),
        (a, b) => a.or(b),
    };
    let file = File::open(path)?;
    let meta = ArrowReaderMetadata::load(
        &file,
        ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Optional),
    )
    .map_err(|e| storage_err(&e))?;
    let probe = ParquetRecordBatchReaderBuilder::new_with_metadata(file.try_clone()?, meta.clone());
    let groups = bloom_filtered(
        &probe,
        row_groups_to_read_from(probe.metadata(), filter, from_ms),
        filter,
    );
    if groups.is_empty() {
        return Ok(Vec::new());
    }

    let schema = meta.metadata().file_metadata().schema_descr_ptr();
    let wanted = filter_columns(filter, text_lc.is_some());
    let indices: Vec<usize> = schema
        .columns()
        .iter()
        .enumerate()
        .filter(|(_, c)| wanted.contains(&c.name()))
        .map(|(i, _)| i)
        .collect();
    let test = RowTest {
        filter,
        text_lc,
        evm_fold: filter.tx_hash.as_deref().is_some_and(is_evm_hash),
        from_ms,
    };
    let scan = probe
        .with_row_groups(groups.clone())
        .with_projection(ProjectionMask::leaves(&schema, indices))
        .build()
        .map_err(|e| storage_err(&e))?;
    let mut selectors: Vec<RowSelector> = Vec::new();
    let (mut selected, mut total) = (0usize, 0usize);
    for batch in scan {
        let batch = batch.map_err(|e| storage_err(&e))?;
        let cols = test.bind(&batch);
        for row in 0..batch.num_rows() {
            let hit = cols.matches(row);
            match selectors.last_mut() {
                Some(last) if last.skip != hit => last.row_count += 1,
                _ => selectors.push(if hit {
                    RowSelector::select(1)
                } else {
                    RowSelector::skip(1)
                }),
            }
            selected += usize::from(hit);
        }
        total += batch.num_rows();
    }
    if selected == 0 {
        return Ok(Vec::new());
    }

    let mut read =
        ParquetRecordBatchReaderBuilder::new_with_metadata(file, meta).with_row_groups(groups);
    if selected < total {
        read = read.with_row_selection(RowSelection::from(selectors));
    }
    let reader = read.build().map_err(|e| storage_err(&e))?;

    let mut records = Vec::with_capacity(selected);
    for batch in reader {
        let batch = batch.map_err(|e| storage_err(&e))?;
        let missing =
            |name: &str| LogHavenError::Storage(format!("column '{name}' missing or mistyped"));
        let ids = col::<StringArray>(&batch, "id").ok_or_else(|| missing("id"))?;
        let apps = col::<StringArray>(&batch, "app").ok_or_else(|| missing("app"))?;
        let tss =
            col::<Int64Array>(&batch, "timestamp_ms").ok_or_else(|| missing("timestamp_ms"))?;
        let levels = col::<StringArray>(&batch, "level").ok_or_else(|| missing("level"))?;
        let sources = col::<StringArray>(&batch, "source").ok_or_else(|| missing("source"))?;
        let messages =
            col::<LargeStringArray>(&batch, "message").ok_or_else(|| missing("message"))?;
        let trace_ids = col::<StringArray>(&batch, "trace_id");
        let span_ids = col::<StringArray>(&batch, "span_id");
        let chains = col::<StringArray>(&batch, "chain");
        let tx_hashes = col::<StringArray>(&batch, "tx_hash");
        let tags = col::<StringArray>(&batch, "tags");
        let metadata = col::<StringArray>(&batch, "metadata");

        for row in 0..batch.num_rows() {
            let non_empty = |a| str_at(a, row).filter(|s| !s.is_empty());
            records.push(LogRecord {
                id: str_at(Some(ids), row).unwrap_or_default(),
                app: str_at(Some(apps), row).unwrap_or_default(),
                timestamp_ms: tss.value(row),
                level: str_at(Some(levels), row).unwrap_or_default(),
                source: str_at(Some(sources), row).unwrap_or_default(),
                message: if messages.is_null(row) {
                    String::new()
                } else {
                    messages.value(row).to_string()
                },
                trace_id: non_empty(trace_ids),
                span_id: non_empty(span_ids),
                chain: non_empty(chains),
                tx_hash: non_empty(tx_hashes),
                tags: non_empty(tags),
                metadata: non_empty(metadata),
            });
        }
    }

    Ok(records)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::date::date_from_ms;
    use crate::storage::writer::write_chunk;

    const DAY: i64 = 86_400_000;
    const T0: i64 = 1_700_000_000_000; // 2023-11-14

    struct Fixture {
        tmp: PathBuf,
        seq: u32,
    }

    impl Fixture {
        fn new(name: &str) -> Self {
            let tmp = std::env::temp_dir().join(format!("lh-q-{}-{}", name, std::process::id()));
            let _ = std::fs::remove_dir_all(&tmp);
            std::fs::create_dir_all(&tmp).unwrap();
            Self { tmp, seq: 0 }
        }

        /// One chunk holding `records`, filed under the date of the first record.
        fn chunk(&mut self, app: &str, records: Vec<LogRecord>) -> PathBuf {
            self.seq += 1;
            let dir = self
                .tmp
                .join(app)
                .join(date_from_ms(records[0].timestamp_ms));
            let path = dir.join(format!("{:013}-{:05}.parquet", 0, self.seq));
            write_chunk(&path, &records).unwrap();
            path
        }

        fn q(&self, f: QueryFilter) -> Vec<LogRecord> {
            query(&self.tmp, &f).unwrap()
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.tmp);
        }
    }

    fn rec(ts: i64, msg: &str) -> LogRecord {
        let mut r = LogRecord::new("web", "info", "src", msg);
        r.timestamp_ms = ts;
        r
    }

    fn filter() -> QueryFilter {
        QueryFilter {
            app: "web".into(),
            ..Default::default()
        }
    }

    fn msgs(v: &[LogRecord]) -> Vec<&str> {
        v.iter().map(|r| r.message.as_str()).collect()
    }

    #[test]
    fn tx_hash_filter_is_exact_and_skips_records_without_one() {
        let mut fx = Fixture::new("txhash");
        let with_tx = |ts, msg: &str, tx: &str| {
            let mut r = rec(ts, msg);
            r.chain = Some("ethereum".into());
            r.tx_hash = Some(tx.into());
            r
        };
        fx.chunk(
            "web",
            vec![
                with_tx(T0, "submitted", "0xabc"),
                rec(T0 + 1, "no tx"),
                with_tx(T0 + 2, "other", "0xabcd"),
                with_tx(T0 + 3, "confirmed", "0xabc"),
                with_tx(T0 + 4, "upper", "0xABC"),
                with_tx(T0 + 5, "sol", "AbC9"),
                with_tx(T0 + 6, "sol lower", "abc9"),
            ],
        );
        let got = fx.q(QueryFilter {
            tx_hash: Some("0xabc".into()),
            ..filter()
        });
        assert_eq!(msgs(&got), ["upper", "confirmed", "submitted"]);
        let sol = fx.q(QueryFilter {
            tx_hash: Some("AbC9".into()),
            ..filter()
        });
        assert_eq!(msgs(&sol), ["sol"]);
        let none = fx.q(QueryFilter {
            tx_hash: Some("0xdead".into()),
            ..filter()
        });
        assert!(none.is_empty());
    }

    #[test]
    fn tx_hash_filter_skips_row_groups_that_cannot_hold_it() {
        let fx = Fixture::new("txrg");
        // parquet truncates byte-array statistics to 64 bytes, so values must differ early
        let tx = |n: usize| format!("0x{:02x}{:062x}", n / 1000 * 0x40 + 0x10, n % 1000);
        let records: Vec<_> = (0..30)
            .map(|i| {
                let mut r = rec(T0 + i as i64, "m");
                r.tx_hash = Some(tx(i / 10 * 1000 + i % 10));
                r
            })
            .collect();
        let path = fx
            .tmp
            .join("web")
            .join(date_from_ms(T0))
            .join("1-1.parquet");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let props = parquet::file::properties::WriterProperties::builder()
            .set_max_row_group_row_count(Some(10))
            .build();
        let mut w = parquet::arrow::ArrowWriter::try_new(
            File::create(&path).unwrap(),
            crate::storage::record::log_schema(),
            Some(props),
        )
        .unwrap();
        w.write(&crate::storage::record::records_to_batch(&records).unwrap())
            .unwrap();
        w.close().unwrap();

        let b = ParquetRecordBatchReaderBuilder::try_new(File::open(&path).unwrap()).unwrap();
        assert_eq!(b.metadata().num_row_groups(), 3);
        let groups = |t: &str| {
            row_groups_to_read(
                b.metadata(),
                &QueryFilter {
                    tx_hash: Some(t.into()),
                    ..filter()
                },
            )
        };
        assert_eq!(groups(&tx(1003)), vec![1]);
        assert_eq!(
            groups(&tx(1003).to_uppercase().replace("0X", "0x")),
            vec![1]
        );
        assert_eq!(groups(&tx(2005)), vec![2]);
        assert!(groups(&tx(999_999)).is_empty());
        assert_eq!(groups("not-evm").len(), 0);
        let none = row_groups_to_read(b.metadata(), &filter());
        assert_eq!(none, vec![0, 1, 2]);
        let got = read_parquet_file(
            &path,
            &QueryFilter {
                tx_hash: Some(tx(1003)),
                ..filter()
            },
            None,
        )
        .unwrap();
        assert_eq!(got.len(), 1);
    }

    #[test]
    fn pruning_keeps_row_groups_whose_hash_uses_another_case_or_prefix() {
        let mut fx = Fixture::new("txcase");
        let mut r = rec(T0, "upper prefix");
        r.tx_hash = Some(format!("0X{}", "AB".repeat(32)));
        fx.chunk("web", vec![r]);
        let got = fx.q(QueryFilter {
            tx_hash: Some(format!("0x{}", "ab".repeat(32))),
            ..filter()
        });
        assert_eq!(msgs(&got), ["upper prefix"]);
    }

    fn hash_rec(ts: i64, tx: &str) -> LogRecord {
        let mut r = rec(ts, "m");
        r.tx_hash = Some(tx.into());
        r
    }

    fn bloom_groups(path: &Path, tx: &str) -> Vec<usize> {
        let f = QueryFilter {
            tx_hash: Some(tx.into()),
            ..filter()
        };
        let b = ParquetRecordBatchReaderBuilder::try_new(File::open(path).unwrap()).unwrap();
        bloom_filtered(&b, row_groups_to_read(b.metadata(), &f), &f)
    }

    #[test]
    fn bloom_filter_skips_a_row_group_that_min_max_cannot() {
        let mut fx = Fixture::new("bloom");
        let at = |hex: &str| format!("0x{hex}{}", "0".repeat(62));
        let path = fx.chunk(
            "web",
            vec![
                hash_rec(T0, &at("00")),
                hash_rec(T0 + 1, &at("ab")),
                hash_rec(T0 + 2, &at("ff")),
            ],
        );
        assert_eq!(bloom_groups(&path, &at("ab")), vec![0]);
        assert!(
            bloom_groups(&path, &at("40")).is_empty(),
            "inside min/max, not in bloom"
        );
        assert_eq!(
            bloom_groups(&path, &at("ab").to_uppercase().replace("0X", "0x")),
            vec![0]
        );
        let got = read_parquet_file(
            &path,
            &QueryFilter {
                tx_hash: Some(at("40")),
                ..filter()
            },
            None,
        )
        .unwrap();
        assert!(got.is_empty());
    }

    #[test]
    fn non_evm_hashes_use_the_bloom_filter_with_exact_case() {
        let mut fx = Fixture::new("bloomsol");
        let path = fx.chunk(
            "web",
            vec![
                hash_rec(T0, "5VERv8NMvz"),
                hash_rec(T0 + 1, "zzzzzzzzzz"),
                hash_rec(T0 + 2, "3aaaaaaaaa"),
            ],
        );
        assert_eq!(bloom_groups(&path, "5VERv8NMvz"), vec![0]);
        assert!(bloom_groups(&path, "5verv8nmvz").is_empty());
        assert!(bloom_groups(&path, "4mmmmmmmmm").is_empty());
    }

    #[test]
    fn evm_hashes_are_stored_lowercase_and_found_in_any_case() {
        let mut fx = Fixture::new("fold");
        let path = fx.chunk("web", vec![hash_rec(T0, &format!("0X{}", "AB".repeat(32)))]);
        let got = read_parquet_file(&path, &filter(), None).unwrap();
        assert_eq!(
            got[0].tx_hash.as_deref(),
            Some(format!("0x{}", "ab".repeat(32)).as_str())
        );
        for spelling in [
            format!("0x{}", "ab".repeat(32)),
            format!("0x{}", "AB".repeat(32)),
        ] {
            let got = fx.q(QueryFilter {
                tx_hash: Some(spelling),
                ..filter()
            });
            assert_eq!(got.len(), 1);
        }
    }

    #[test]
    fn files_without_the_fold_flag_skip_the_bloom_filter_for_evm_hashes() {
        let fx = Fixture::new("legacy");
        let path = fx
            .tmp
            .join("web")
            .join(date_from_ms(T0))
            .join("1-1.parquet");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let stored = format!("0x{}", "AB".repeat(32));
        let props = parquet::file::properties::WriterProperties::builder()
            .set_column_bloom_filter_enabled(
                parquet::schema::types::ColumnPath::from("tx_hash"),
                true,
            )
            .build();
        let mut w = parquet::arrow::ArrowWriter::try_new(
            File::create(&path).unwrap(),
            crate::storage::record::log_schema(),
            Some(props),
        )
        .unwrap();
        w.write(&crate::storage::record::records_to_batch(&[hash_rec(T0, &stored)]).unwrap())
            .unwrap();
        w.close().unwrap();
        let want = format!("0x{}", "ab".repeat(32));
        assert_eq!(bloom_groups(&path, &want), vec![0]);
        let got = read_parquet_file(
            &path,
            &QueryFilter {
                tx_hash: Some(want),
                ..filter()
            },
            None,
        )
        .unwrap();
        assert_eq!(got.len(), 1);
    }

    #[test]
    fn rejects_traversal_app() {
        let fx = Fixture::new("trav");
        std::fs::create_dir_all(fx.tmp.join("victim")).unwrap();
        for app in ["../victim", "..", "a/b", ""] {
            let f = QueryFilter {
                app: app.into(),
                ..Default::default()
            };
            assert!(query(&fx.tmp, &f).is_err(), "{app:?}");
        }
    }

    #[test]
    fn missing_app_and_zero_limit_are_empty() {
        let mut fx = Fixture::new("empty");
        assert!(fx.q(filter()).is_empty());
        fx.chunk("web", vec![rec(T0, "a")]);
        assert!(
            fx.q(QueryFilter {
                limit: 0,
                ..filter()
            })
            .is_empty()
        );
    }

    #[test]
    fn newest_first_across_dates_and_chunks_with_limit() {
        let mut fx = Fixture::new("order");
        fx.chunk("web", vec![rec(T0, "d0-a"), rec(T0 + 10, "d0-b")]);
        fx.chunk("web", vec![rec(T0 + 5, "d0-late-chunk-older-ts")]);
        fx.chunk(
            "web",
            vec![rec(T0 + DAY, "d1-a"), rec(T0 + DAY + 1, "d1-b")],
        );
        fx.chunk("web", vec![rec(T0 + 2 * DAY, "d2-a")]);

        let all = fx.q(filter());
        assert_eq!(
            msgs(&all),
            [
                "d2-a",
                "d1-b",
                "d1-a",
                "d0-b",
                "d0-late-chunk-older-ts",
                "d0-a"
            ]
        );
        let top = fx.q(QueryFilter {
            limit: 4,
            ..filter()
        });
        assert_eq!(msgs(&top), ["d2-a", "d1-b", "d1-a", "d0-b"]);
    }

    #[test]
    fn limit_is_exact_when_one_chunk_is_larger_than_limit() {
        let mut fx = Fixture::new("big");
        fx.chunk(
            "web",
            (0..50).map(|i| rec(T0 + i, &format!("m{i}"))).collect(),
        );
        let got = fx.q(QueryFilter {
            limit: 3,
            ..filter()
        });
        assert_eq!(msgs(&got), ["m49", "m48", "m47"]);
    }

    #[test]
    fn level_and_source_filters_are_exact() {
        let mut fx = Fixture::new("lvl");
        let mut a = rec(T0, "a");
        a.level = "error".into();
        let mut b = rec(T0 + 1, "b");
        b.source = "db".into();
        fx.chunk("web", vec![a, b, rec(T0 + 2, "c")]);
        let by_level = fx.q(QueryFilter {
            level: Some("error".into()),
            ..filter()
        });
        assert_eq!(msgs(&by_level), ["a"]);
        let by_source = fx.q(QueryFilter {
            source: Some("db".into()),
            ..filter()
        });
        assert_eq!(msgs(&by_source), ["b"]);
        let none = fx.q(QueryFilter {
            level: Some("ERR".into()),
            ..filter()
        });
        assert!(none.is_empty());
    }

    #[test]
    fn time_range_is_inclusive_and_open_ended_ranges_prune_dates() {
        let mut fx = Fixture::new("range");
        fx.chunk("web", vec![rec(T0, "d0")]);
        fx.chunk("web", vec![rec(T0 + DAY, "d1")]);
        fx.chunk("web", vec![rec(T0 + 2 * DAY, "d2")]);

        let r = |from, to| {
            let got = fx.q(QueryFilter {
                from_ms: from,
                to_ms: to,
                ..filter()
            });
            got.iter().map(|r| r.message.clone()).collect::<Vec<_>>()
        };
        assert_eq!(r(Some(T0 + DAY), Some(T0 + DAY)), ["d1"]);
        assert_eq!(r(Some(T0 + DAY), None), ["d2", "d1"]);
        assert_eq!(r(None, Some(T0 + DAY)), ["d1", "d0"]);
        assert_eq!(r(Some(T0 + 1), Some(T0 + DAY - 1)), Vec::<String>::new());
        assert_eq!(r(Some(T0 + DAY + 1), Some(T0 + DAY)), Vec::<String>::new());
        assert_eq!(r(None, None).len(), 3);
    }

    #[test]
    fn text_matches_message_tags_metadata_case_insensitively() {
        let mut fx = Fixture::new("text");
        let mut tagged = rec(T0 + 1, "plain");
        tagged.tags = Some(r#"["Payment","retry"]"#.into());
        let mut meta = rec(T0 + 2, "plain2");
        meta.metadata = Some(r#"{"wallet":"0xABC"}"#.into());
        fx.chunk(
            "web",
            vec![rec(T0, "Transfer FAILED: code=500 (a+b)*c"), tagged, meta],
        );
        let t = |s: &str| {
            fx.q(QueryFilter {
                text: Some(s.into()),
                ..filter()
            })
        };
        assert_eq!(
            msgs(&t("transfer failed")),
            ["Transfer FAILED: code=500 (a+b)*c"]
        );
        assert_eq!(msgs(&t("payment")), ["plain"]);
        assert_eq!(msgs(&t("0xabc")), ["plain2"]);
        assert_eq!(t("(a+b)*c").len(), 1, "query syntax is literal");
        assert_eq!(t("code=500 AND").len(), 0);
        assert_eq!(t("\"").len(), 2, "quote appears in tags and metadata only");
        assert!(t("nomatch").is_empty());
        assert_eq!(t("").len(), 3, "empty text means no text filter");
        assert_eq!(t("PLAIN").len(), 2, "message 'plain'/'plain2'");
    }

    #[test]
    fn text_filter_applies_before_limit() {
        let mut fx = Fixture::new("textlimit");
        let mut v: Vec<LogRecord> = (0..20).map(|i| rec(T0 + i, "noise")).collect();
        v[2].message = "needle".into();
        fx.chunk("web", v);
        let got = fx.q(QueryFilter {
            text: Some("needle".into()),
            limit: 1,
            ..filter()
        });
        assert_eq!(msgs(&got), ["needle"]);
    }

    #[test]
    fn corrupt_chunk_is_skipped_not_fatal() {
        let mut fx = Fixture::new("corrupt");
        let good = fx.chunk("web", vec![rec(T0, "good")]);
        std::fs::write(
            good.with_file_name("9999999999999-99999.parquet"),
            b"not parquet",
        )
        .unwrap();
        std::fs::write(good.with_file_name("0000000000000-00000.parquet"), b"").unwrap();
        assert_eq!(msgs(&fx.q(filter())), ["good"]);
    }

    #[test]
    fn tmp_files_and_non_date_dirs_are_ignored() {
        let mut fx = Fixture::new("junk");
        let good = fx.chunk("web", vec![rec(T0, "good")]);
        std::fs::copy(&good, good.with_extension("parquet.tmp")).unwrap();
        let app = fx.tmp.join("web");
        std::fs::create_dir_all(app.join("index")).unwrap();
        std::fs::copy(&good, app.join("index/1-1.parquet")).unwrap();
        std::fs::create_dir_all(app.join("2023-13-45")).unwrap();
        std::fs::copy(&good, app.join("2023-13-45/1-1.parquet")).unwrap();
        std::fs::write(app.join("stray.txt"), b"x").unwrap();
        assert_eq!(msgs(&fx.q(filter())), ["good"]);
    }

    #[test]
    fn optional_fields_round_trip() {
        let mut fx = Fixture::new("fields");
        let mut full = rec(T0, "full");
        full.trace_id = Some("t".into());
        full.span_id = Some("s".into());
        full.chain = Some("solana".into());
        full.tx_hash = Some("0x1".into());
        fx.chunk("web", vec![full, rec(T0 + 1, "bare")]);
        let got = fx.q(filter());
        let bare = &got[0];
        let full = &got[1];
        assert_eq!(bare.trace_id, None);
        assert_eq!(bare.tx_hash, None);
        assert_eq!(full.chain.as_deref(), Some("solana"));
        assert_eq!(full.span_id.as_deref(), Some("s"));
        assert_eq!(full.app, "web");
    }

    #[test]
    fn equal_timestamps_cut_deterministically_by_id() {
        let mut fx = Fixture::new("ties");
        let mut v: Vec<LogRecord> = (0..6).map(|i| rec(T0, &format!("m{i}"))).collect();
        for (i, r) in v.iter_mut().enumerate() {
            r.id = format!("id-{i}");
        }
        fx.chunk("web", v.clone());
        v.reverse();
        fx.chunk("web", v);
        for _ in 0..3 {
            let got = fx.q(QueryFilter {
                limit: 2,
                ..filter()
            });
            assert_eq!(
                got.iter().map(|r| r.id.as_str()).collect::<Vec<_>>(),
                ["id-5", "id-5"]
            );
        }
    }

    #[test]
    fn chunk_with_foreign_schema_is_skipped() {
        use arrow::array::{ArrayRef, Int32Array};
        use arrow::datatypes::{DataType, Field, Schema};
        let mut fx = Fixture::new("schema");
        let good = fx.chunk("web", vec![rec(T0, "good")]);
        let schema =
            std::sync::Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![std::sync::Arc::new(Int32Array::from(vec![1])) as ArrayRef],
        )
        .unwrap();
        let path = good.with_file_name("9999999999999-00001.parquet");
        let mut w = parquet::arrow::ArrowWriter::try_new(File::create(path).unwrap(), schema, None)
            .unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
        assert_eq!(msgs(&fx.q(filter())), ["good"]);
    }

    #[test]
    fn vanished_directories_are_not_errors() {
        let fx = Fixture::new("vanish");
        assert!(chunk_files(&fx.tmp.join("nope")).unwrap().is_empty());
        let f = filter();
        assert!(
            date_dirs_in_range(&fx.tmp.join("nope"), &f)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn byte_budget_keeps_newest_and_at_least_one() {
        let mut v: Vec<LogRecord> = (0..3)
            .map(|i| rec(T0 + i, &"x".repeat(MAX_RESULT_BYTES / 2)))
            .collect();
        trim_newest(&mut v, 10, true);
        assert_eq!(v.len(), 1, "two do not fit; newest wins");
        assert_eq!(v[0].timestamp_ms, T0 + 2);
        let mut huge = vec![rec(T0, &"x".repeat(MAX_RESULT_BYTES * 2))];
        trim_newest(&mut huge, 10, true);
        assert_eq!(huge.len(), 1);
    }

    #[test]
    fn range_inside_a_multi_row_group_file_reads_only_matching_groups() {
        let fx = Fixture::new("multirg");
        let records: Vec<LogRecord> = (0..30).map(|i| rec(T0 + i, &format!("m{i}"))).collect();
        let path = fx
            .tmp
            .join("web")
            .join(date_from_ms(T0))
            .join("1-1.parquet");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let props = parquet::file::properties::WriterProperties::builder()
            .set_max_row_group_row_count(Some(10))
            .build();
        let mut w = parquet::arrow::ArrowWriter::try_new(
            File::create(&path).unwrap(),
            crate::storage::record::log_schema(),
            Some(props),
        )
        .unwrap();
        w.write(&crate::storage::record::records_to_batch(&records).unwrap())
            .unwrap();
        w.close().unwrap();

        let b = ParquetRecordBatchReaderBuilder::try_new(File::open(&path).unwrap()).unwrap();
        assert_eq!(b.metadata().num_row_groups(), 3);
        assert_eq!(
            overlapping_row_groups(b.metadata(), Some(T0 + 12), Some(T0 + 14)),
            vec![1]
        );
        assert_eq!(
            overlapping_row_groups(b.metadata(), Some(T0 + 9), Some(T0 + 10)),
            vec![0, 1]
        );
        let f = QueryFilter {
            from_ms: Some(T0 + 12),
            to_ms: Some(T0 + 14),
            ..filter()
        };
        let got = read_parquet_file(&path, &f, None).unwrap();
        assert_eq!(got.len(), 3);
    }

    #[test]
    fn row_groups_outside_the_range_are_not_decoded() {
        let mut fx = Fixture::new("rg");
        let path = fx.chunk("web", vec![rec(T0, "a"), rec(T0 + 10, "b")]);
        let meta = |from, to| {
            let b = ParquetRecordBatchReaderBuilder::try_new(File::open(&path).unwrap()).unwrap();
            overlapping_row_groups(b.metadata(), from, to)
        };
        assert_eq!(meta(None, None), vec![0]);
        assert_eq!(
            meta(Some(T0 + 10), Some(T0 + 10)),
            vec![0],
            "bounds are inclusive"
        );
        assert!(meta(Some(T0 + 11), None).is_empty());
        assert!(meta(None, Some(T0 - 1)).is_empty());
        assert!(
            read_parquet_file(
                &path,
                &QueryFilter {
                    from_ms: Some(T0 + 11),
                    ..filter()
                },
                None
            )
            .unwrap()
            .is_empty()
        );
        assert_eq!(
            read_parquet_file(
                &path,
                &QueryFilter {
                    from_ms: Some(T0),
                    to_ms: Some(T0 + 10),
                    ..filter()
                },
                None
            )
            .unwrap()
            .len(),
            2
        );
    }

    /// The same answer as reading every chunk whole and filtering the rows with plain code.
    fn reference(fx: &Fixture, f: &QueryFilter) -> Vec<String> {
        let mut all = vec![];
        for day in std::fs::read_dir(fx.tmp.join("web")).unwrap().flatten() {
            for chunk in chunk_files(&day.path()).unwrap() {
                let whole = QueryFilter {
                    limit: usize::MAX,
                    ..filter()
                };
                all.extend(read_parquet_file(&chunk, &whole, None).unwrap());
            }
        }
        let text = f.text.as_deref().map(str::to_lowercase);
        all.retain(|r| {
            f.from_ms.is_none_or(|t| r.timestamp_ms >= t)
                && f.to_ms.is_none_or(|t| r.timestamp_ms <= t)
                && f.level.as_ref().is_none_or(|l| &r.level == l)
                && f.source.as_ref().is_none_or(|s| &r.source == s)
                && f.tx_hash
                    .as_ref()
                    .is_none_or(|h| r.tx_hash.as_ref() == Some(h))
                && text.as_ref().is_none_or(|t| {
                    [Some(&r.message), r.tags.as_ref(), r.metadata.as_ref()]
                        .into_iter()
                        .flatten()
                        .any(|s| s.to_lowercase().contains(t))
                })
        });
        all.sort_by(newest_first);
        all.truncate(f.limit);
        all.into_iter().map(|r| r.id).collect()
    }

    #[test]
    fn query_gives_the_answer_of_a_plain_scan_for_any_filter_and_limit() {
        let mut fx = Fixture::new("equiv");
        let mut x: u64 = 0x2545f4914f6cdd1d;
        let mut next = move |n: u64| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x % n
        };
        let mut n = 0u64;
        // chunks of unequal size, on three days, with timestamps that repeat across chunks;
        // the biggest one is longer than a decoding batch
        for (day, size) in [
            (0, 40),
            (0, 20_000),
            (0, 7),
            (1, 300),
            (1, 1),
            (2, 90),
            (2, 90),
        ] {
            let recs: Vec<LogRecord> = (0..size)
                .map(|_| {
                    n += 1;
                    let mut r = rec(T0 + day * DAY + next(50) as i64 * 1000, "");
                    r.id = format!("{n:08}");
                    r.message = format!(
                        "m{} {}",
                        next(30),
                        if next(9) == 0 { "NEEDLE" } else { "x" }
                    );
                    r.level = ["info", "warn", "error"][next(3) as usize].into();
                    r.source = ["a", "b"][next(2) as usize].into();
                    r.tx_hash = (next(8) == 0).then(|| format!("0x{:04x}", next(6)));
                    r.metadata = (next(5) == 0).then(|| "meta Needle".to_string());
                    r
                })
                .collect();
            fx.chunk("web", recs);
        }
        for round in 0..120 {
            let f = QueryFilter {
                level: (next(3) == 0)
                    .then(|| ["info", "warn", "error", ""][next(4) as usize].into()),
                source: (next(3) == 0).then(|| ["a", "b", "c"][next(3) as usize].into()),
                tx_hash: (next(4) == 0).then(|| format!("0x{:04x}", next(6))),
                text: (next(3) == 0).then(|| ["needle", "m7", "zzz"][next(3) as usize].into()),
                from_ms: (next(3) == 0).then(|| T0 + next(3) as i64 * DAY + next(60) as i64 * 1000),
                to_ms: (next(3) == 0).then(|| T0 + next(3) as i64 * DAY + next(60) as i64 * 1000),
                limit: [1, 2, 7, 50, 400, 100_000][next(6) as usize],
                ..filter()
            };
            if f.from_ms.zip(f.to_ms).is_some_and(|(a, b)| a > b) {
                continue;
            }
            let got: Vec<String> = fx
                .q(QueryFilter { ..clone_filter(&f) })
                .into_iter()
                .map(|r| r.id)
                .collect();
            assert_eq!(got, reference(&fx, &f), "round {round}: {:?}", describe(&f));
        }
    }

    fn clone_filter(f: &QueryFilter) -> QueryFilter {
        QueryFilter {
            app: f.app.clone(),
            source: f.source.clone(),
            level: f.level.clone(),
            from_ms: f.from_ms,
            to_ms: f.to_ms,
            text: f.text.clone(),
            tx_hash: f.tx_hash.clone(),
            limit: f.limit,
        }
    }

    fn describe(f: &QueryFilter) -> String {
        format!(
            "level={:?} source={:?} tx={:?} text={:?} from={:?} to={:?} limit={}",
            f.level, f.source, f.tx_hash, f.text, f.from_ms, f.to_ms, f.limit
        )
    }

    #[test]
    fn a_limit_cut_inside_equal_timestamps_across_chunks_keeps_the_highest_ids() {
        let mut fx = Fixture::new("ties-across");
        for chunk in 0..4u64 {
            let recs: Vec<LogRecord> = (0..5u64)
                .map(|i| {
                    let mut r = rec(T0, "same");
                    r.id = format!("{:08}", chunk * 5 + i);
                    r
                })
                .collect();
            fx.chunk("web", recs);
        }
        let got = fx.q(QueryFilter {
            limit: 7,
            ..filter()
        });
        let ids: Vec<&str> = got.iter().map(|r| r.id.as_str()).collect();
        assert_eq!(
            ids,
            [
                "00000019", "00000018", "00000017", "00000016", "00000015", "00000014", "00000013"
            ]
        );
    }

    #[test]
    fn long_runs_of_matching_and_skipped_rows_in_one_chunk_are_read_exactly() {
        let mut fx = Fixture::new("runs");
        let recs: Vec<LogRecord> = (0..30_000u64)
            .map(|i| {
                let mut r = rec(T0 + i as i64, "x");
                r.id = format!("{i:08}");
                r.level = if (9_000..21_000).contains(&i) {
                    "error"
                } else {
                    "info"
                }
                .into();
                r
            })
            .collect();
        fx.chunk("web", recs);
        let got = fx.q(QueryFilter {
            level: Some("error".into()),
            limit: 100_000,
            ..filter()
        });
        assert_eq!(got.len(), 12_000);
        assert_eq!(got[0].id, "00020999");
        assert_eq!(got[11_999].id, "00009000");
    }

    /// `cargo test --release bench_query -- --ignored --nocapture`
    #[test]
    #[ignore]
    fn bench_query() {
        let mut fx = Fixture::new("bench");
        let mut x: u64 = 0x9e3779b97f4a7c15;
        let mut noise = |n: usize| -> String {
            (0..n)
                .map(|_| {
                    x ^= x << 13;
                    x ^= x >> 7;
                    x ^= x << 17;
                    char::from(b'a' + (x % 26) as u8)
                })
                .collect()
        };
        let chunks = 100;
        let per = 2000;
        for c in 0..chunks {
            let recs: Vec<LogRecord> = (0..per)
                .map(|i| {
                    let n = c * per + i;
                    let mut r = rec(T0 + n as i64, &noise(300));
                    r.level = if n % 100 == 0 { "error" } else { "info" }.into();
                    r.metadata = Some(noise(500));
                    r.tx_hash = (n % 50 == 0).then(|| format!("0x{n:064x}"));
                    r
                })
                .collect();
            fx.chunk("web", recs);
        }
        let time = |name: &str, f: QueryFilter| {
            let t = std::time::Instant::now();
            let n = fx.q(f).len();
            println!(
                "{name:<28} {n:>6} records {:>8.1} ms",
                t.elapsed().as_secs_f64() * 1e3
            );
        };
        time(
            "newest 100",
            QueryFilter {
                limit: 100,
                ..filter()
            },
        );
        time(
            "level=error limit 100",
            QueryFilter {
                level: Some("error".into()),
                limit: 100,
                ..filter()
            },
        );
        time(
            "level=error limit 5000",
            QueryFilter {
                level: Some("error".into()),
                limit: 5000,
                ..filter()
            },
        );
        time(
            "source=nothing",
            QueryFilter {
                source: Some("nothing".into()),
                limit: 100,
                ..filter()
            },
        );
        time(
            "tx_hash (one)",
            QueryFilter {
                tx_hash: Some(format!("0x{:064x}", 150_000)),
                ..filter()
            },
        );
        time(
            "text=zzzzzzzz",
            QueryFilter {
                text: Some("zzzzzzzz".into()),
                limit: 100,
                ..filter()
            },
        );
    }
}
