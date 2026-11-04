use crate::error::{LogHavenError, Result};
use crate::storage::date::parse_date;
use crate::storage::record::LogRecord;
use arrow::array::{Array, Int64Array, LargeStringArray, StringArray};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
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

    for (_, date_dir) in &date_dirs {
        // every record of a later date is newer than any record of an earlier one
        if results.len() >= filter.limit {
            break;
        }
        for chunk_path in chunk_files(date_dir)? {
            let records = match read_parquet_file(&chunk_path, filter.from_ms, filter.to_ms) {
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
            results.extend(
                records
                    .into_iter()
                    .filter(|r| matches_filter(r, filter, text_lc.as_deref())),
            );
            trim_newest(&mut results, filter.limit, false);
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

fn read_parquet_file(
    path: &Path,
    from_ms: Option<i64>,
    to_ms: Option<i64>,
) -> Result<Vec<LogRecord>> {
    let file = File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| LogHavenError::Storage(e.to_string()))?;
    let groups = overlapping_row_groups(builder.metadata(), from_ms, to_ms);
    if groups.is_empty() {
        return Ok(Vec::new());
    }
    let reader = builder
        .with_row_groups(groups)
        .build()
        .map_err(|e| LogHavenError::Storage(e.to_string()))?;

    let mut records = Vec::new();

    for batch in reader {
        let batch = batch.map_err(|e| LogHavenError::Storage(e.to_string()))?;

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
            if tss.is_null(row) {
                continue; // a record without a time cannot be placed or ordered
            }
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

fn matches_filter(record: &LogRecord, filter: &QueryFilter, text_lc: Option<&str>) -> bool {
    if filter.source.as_ref().is_some_and(|s| &record.source != s) {
        return false;
    }
    if filter.level.as_ref().is_some_and(|l| &record.level != l) {
        return false;
    }
    if filter.from_ms.is_some_and(|f| record.timestamp_ms < f) {
        return false;
    }
    if filter.to_ms.is_some_and(|t| record.timestamp_ms > t) {
        return false;
    }
    if let Some(needle) = text_lc {
        let hit = record.message.to_lowercase().contains(needle)
            || record
                .tags
                .as_ref()
                .is_some_and(|t| t.to_lowercase().contains(needle))
            || record
                .metadata
                .as_ref()
                .is_some_and(|m| m.to_lowercase().contains(needle));
        if !hit {
            return false;
        }
    }
    true
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
            read_parquet_file(&path, Some(T0 + 11), None)
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            read_parquet_file(&path, Some(T0), Some(T0 + 10))
                .unwrap()
                .len(),
            2
        );
    }
}
