//! Read-through queries: the chunks of the days a query asks for that exist only in a storage
//! target (uploaded, then deleted locally) are fetched into a cache directory, decrypted, and
//! queried there like local chunks. The cache has a size cap and drops its least recently used
//! chunks first; one query may download only so much.

use crate::config::QueryConfig;
use crate::storage::cloud::Target;
use crate::storage::crypt::{Encryptor, OBJECT_SUFFIX};
use crate::storage::date::{epoch_days_to_date, today_epoch_days};
use crate::storage::restore::{
    RESTORE_PREFIX, RESTORE_SUFFIX, stage, sweep_stale, valid_chunk_name,
};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

const LISTED: &str = ".listed";
/// What the marker of a day holds once nothing more is expected for it: the day is over, all of
/// its chunks are in the cache, and none was left to local disk. Any other marker only keeps the
/// day from being listed again for `RELIST_AFTER`.
const FINAL: &[u8] = b"final";
/// A day whose marker is not final is listed again when its last listing is older than this.
const RELIST_AFTER: Duration = Duration::from_secs(60);
/// The cache never drops a chunk used this recently, so a query that is scanning one is not
/// pulled from under it.
const KEEP_RECENT: Duration = Duration::from_secs(300);
/// Fetching for one query stops here, and the query runs on what it has.
const FETCH_DEADLINE: Duration = Duration::from_secs(20);

pub struct RemoteReader {
    target: Target,
    keys_dir: PathBuf,
    cache: PathBuf,
    cache_cap: u64,
    fetch_cap: u64,
    max_days: u32,
    gate: tokio::sync::Mutex<()>,
}

impl RemoteReader {
    pub fn new(target: Target, keys_dir: PathBuf, cache: PathBuf, q: &QueryConfig) -> Self {
        Self {
            target,
            keys_dir,
            cache,
            cache_cap: q.remote_cache_mb.saturating_mul(1 << 20),
            fetch_cap: q.remote_fetch_mb.saturating_mul(1 << 20),
            max_days: q.remote_max_days,
            gate: tokio::sync::Mutex::new(()),
        }
    }

    pub fn cache_dir(&self) -> &Path {
        &self.cache
    }

    /// Makes the chunks of `app` for the days `from_ms..=to_ms` that are not on local disk
    /// available in the cache. Returns true when some could not be, because of a limit, a
    /// timeout or an error, so the answer may lack records.
    pub async fn prepare(&self, data: &Path, app: &str, from_ms: i64, to_ms: i64) -> bool {
        let Ok(_one_at_a_time) = tokio::time::timeout(FETCH_DEADLINE, self.gate.lock()).await
        else {
            eprintln!("Query: another query is still fetching from the remote target");
            return true;
        };
        let work = self.fetch_days(data, app, from_ms, to_ms);
        let outcome = tokio::time::timeout(FETCH_DEADLINE, work).await;
        // a timeout drops the fetch where it stood, so the cap is applied here as well
        evict(&self.cache, self.cache_cap);
        match outcome {
            Ok(complete) => !complete,
            Err(_) => {
                eprintln!("Query: fetching from the remote target timed out");
                true
            }
        }
    }

    async fn fetch_days(&self, data: &Path, app: &str, from_ms: i64, to_ms: i64) -> bool {
        let mut complete = true;
        sweep_stale(&self.cache, app);
        // only encrypted chunks need keys, so a missing key is told about when one is met
        let keys = Encryptor::all(&self.keys_dir).unwrap_or_default();
        // no data predates the epoch or is dated in the future, and an absurd range would
        // otherwise make up directories and listings for days that cannot exist
        let first = from_ms.div_euclid(86_400_000).max(0);
        let last = to_ms.div_euclid(86_400_000).min(today_epoch_days());
        if first > last {
            return true;
        }
        let wanted = (first..=last).rev().take(self.max_days as usize);
        if last - first + 1 > i64::from(self.max_days) {
            eprintln!(
                "Query: the range spans more than {} days, so only the newest are fetched",
                self.max_days
            );
            complete = false;
        }
        let mut spent = 0u64;
        for day in wanted {
            if !self.refresh_day(data, app, day, &keys, &mut spent).await {
                complete = false;
            }
            touch_day(&self.cache.join(app).join(epoch_days_to_date(day)));
        }
        complete
    }

    /// Fetches what the target holds for the day and the data directory and cache lack.
    /// Returns false when the day is not fully there.
    async fn refresh_day(
        &self,
        data: &Path,
        app: &str,
        day: i64,
        keys: &[Encryptor],
        spent: &mut u64,
    ) -> bool {
        let date = epoch_days_to_date(day);
        let cache_day = self.cache.join(app).join(&date);
        let listed = cache_day.join(LISTED);
        let age = std::fs::metadata(&listed)
            .and_then(|m| m.modified())
            .ok()
            .and_then(|t| t.elapsed().ok());
        let marker_final = std::fs::read(&listed).is_ok_and(|b| b == FINAL);
        if marker_final || age.is_some_and(|a| a < RELIST_AFTER) {
            return true;
        }
        // read at listing time: a day that was listed before midnight may still be gaining chunks
        let settled = day <= today_epoch_days() - 2;
        let objects = match self.target.list_app(&format!("{app}/{date}/")).await {
            Ok(o) => o,
            Err(e) => {
                eprintln!("Query: cannot list {app}/{date} on the remote target: {e}");
                return false;
            }
        };
        let sealed_names: std::collections::HashSet<&str> = objects
            .iter()
            .filter_map(|o| o.strip_suffix(OBJECT_SUFFIX))
            .collect();
        let mut complete = true;
        let mut left_to_disk = false;
        for object in &objects {
            let (name, sealed) = match object.strip_suffix(OBJECT_SUFFIX) {
                Some(n) => (n, true),
                None => (object.as_str(), false),
            };
            if !name.starts_with(&format!("{app}/{date}/"))
                || !valid_chunk_name(name)
                || (!sealed && sealed_names.contains(name))
                || self.cache.join(name).exists()
            {
                continue;
            }
            if data.join(name).exists() {
                // it may be pruned from disk later, and then it has to be fetched
                left_to_disk = true;
                continue;
            }
            if sealed && keys.is_empty() {
                eprintln!("Query: {name} is encrypted and no storage key was found");
                complete = false;
                continue;
            }
            if let Err(e) = std::fs::create_dir_all(&cache_day) {
                eprintln!("Query: cannot make {}: {e}", cache_day.display());
                return false;
            }
            let tmp = Tmp(cache_day.join(format!(
                "{RESTORE_PREFIX}{:016x}{RESTORE_SUFFIX}",
                rand::random::<u64>()
            )));
            let left = self.fetch_cap.saturating_sub(*spent);
            let staged = stage(&self.target, object, name, sealed, keys, &tmp.0, left).await;
            // what a failed chunk had already downloaded counts against the cap too
            *spent += match &staged {
                Ok(n) => *n,
                Err(_) => std::fs::metadata(&tmp.0).map_or(0, |m| m.len().min(left)),
            };
            let placed = staged
                .and_then(|_| std::fs::rename(&tmp.0, self.cache.join(name)).map_err(Into::into));
            if let Err(e) = placed {
                eprintln!("Query: cannot fetch {name} from the remote target: {e}");
                complete = false;
            }
        }
        if complete {
            let _ = std::fs::create_dir_all(&cache_day);
            let _ = std::fs::write(&listed, if settled && !left_to_disk { FINAL } else { b"" });
        }
        complete
    }
}

/// A temporary file that does not outlive its owner, however the fetch ends.
struct Tmp(PathBuf);

impl Drop for Tmp {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

fn touch_day(dir: &Path) {
    let now = SystemTime::now();
    for e in std::fs::read_dir(dir).into_iter().flatten().flatten() {
        if e.path().extension().is_some_and(|x| x == "parquet")
            && let Ok(f) = std::fs::OpenOptions::new().write(true).open(e.path())
        {
            let _ = f.set_modified(now);
        }
    }
}

/// Removes least recently used chunks until the cache fits `cap`. A day that loses a chunk is
/// listed again the next time it is asked for.
fn evict(cache: &Path, cap: u64) {
    let mut chunks: Vec<(SystemTime, u64, PathBuf)> = vec![];
    for app in std::fs::read_dir(cache).into_iter().flatten().flatten() {
        for day in std::fs::read_dir(app.path())
            .into_iter()
            .flatten()
            .flatten()
        {
            for f in std::fs::read_dir(day.path())
                .into_iter()
                .flatten()
                .flatten()
            {
                let p = f.path();
                if p.extension().is_some_and(|x| x == "parquet")
                    && let Ok(m) = f.metadata()
                {
                    chunks.push((m.modified().unwrap_or(SystemTime::UNIX_EPOCH), m.len(), p));
                }
            }
        }
    }
    let mut total: u64 = chunks.iter().map(|c| c.1).sum();
    chunks.sort();
    for (used, size, path) in chunks {
        if total <= cap || used.elapsed().is_ok_and(|a| a < KEEP_RECENT) {
            break;
        }
        if std::fs::remove_file(&path).is_ok() {
            total -= size;
            if let Some(day) = path.parent() {
                let _ = std::fs::remove_file(day.join(LISTED));
                let _ = std::fs::remove_dir(day);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::cloud::CloudSyncer;
    use crate::storage::crypt::Encryptor;
    use crate::storage::query::{QueryFilter, query_with_cache};
    use crate::storage::record::LogRecord;
    use crate::storage::writer::write_chunk;
    use futures_util::TryStreamExt;
    use object_store::ObjectStore as _;
    use object_store::ObjectStoreExt;
    use std::sync::Arc;

    /// 2024-01-01 12:00 UTC.
    const DAY1_NOON: i64 = 1_704_110_400_000;
    const DAY: i64 = 86_400_000;

    fn target(mem: &Arc<object_store::memory::InMemory>) -> Target {
        Target::new(
            "mem",
            Box::new(crate::storage::cloud::CrateStore(mem.clone())),
            Some("backup".into()),
        )
    }

    /// A store that records what it is asked to download and, when `loose`, lists every object
    /// of the app whatever day is asked for.
    struct Spy {
        inner: crate::storage::cloud::CrateStore,
        loose: bool,
        asked: Arc<std::sync::Mutex<Vec<u64>>>,
    }

    impl crate::storage::cloud::ObjectStore for Spy {
        fn put<'a>(
            &'a self,
            key: &'a str,
            file: &'a Path,
        ) -> crate::storage::cloud::BoxFuture<'a, crate::error::Result<()>> {
            self.inner.put(key, file)
        }

        fn list<'a>(
            &'a self,
            prefix: &'a str,
        ) -> crate::storage::cloud::BoxFuture<'a, crate::error::Result<Vec<String>>> {
            let app_level = prefix.splitn(3, '/').take(2).collect::<Vec<_>>().join("/") + "/";
            Box::pin(async move {
                self.inner
                    .list(if self.loose { &app_level } else { prefix })
                    .await
            })
        }

        fn download<'a>(
            &'a self,
            key: &'a str,
            dest: &'a Path,
            max_bytes: u64,
        ) -> crate::storage::cloud::BoxFuture<'a, crate::error::Result<u64>> {
            self.asked.lock().unwrap().push(max_bytes);
            self.inner.download(key, dest, max_bytes)
        }
    }

    fn spied(
        mem: &Arc<object_store::memory::InMemory>,
        loose: bool,
    ) -> (Target, Arc<std::sync::Mutex<Vec<u64>>>) {
        let asked = Arc::new(std::sync::Mutex::new(vec![]));
        let spy = Spy {
            inner: crate::storage::cloud::CrateStore(mem.clone()),
            loose,
            asked: asked.clone(),
        };
        (
            Target::new("mem", Box::new(spy), Some("backup".into())),
            asked,
        )
    }

    fn rec(ts: i64, msg: &str) -> LogRecord {
        let mut r = LogRecord::new("web", "info", "s", msg);
        r.timestamp_ms = ts;
        r
    }

    fn chunk(data: &Path, day: i64, msg: &str) {
        let date = epoch_days_to_date(day.div_euclid(DAY));
        let dir = data.join("web").join(&date);
        std::fs::create_dir_all(&dir).unwrap();
        static NEXT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let n = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        write_chunk(&dir.join(format!("{n}-1.parquet")), &[rec(day, msg)]).unwrap();
    }

    async fn uploaded(data: &Path, mem: &Arc<object_store::memory::InMemory>, delete: bool) {
        let syncer = CloudSyncer::with_targets(vec![target(mem)], data.to_path_buf(), delete);
        assert_eq!(syncer.sync_app(&data.join("web")).await, 0);
    }

    fn reader(root: &Path, mem: &Arc<object_store::memory::InMemory>) -> RemoteReader {
        RemoteReader::new(
            target(mem),
            root.join("keys"),
            root.join("cache"),
            &QueryConfig::default(),
        )
    }

    fn messages(data: &Path, r: &RemoteReader, from: i64, to: i64) -> Vec<String> {
        let f = QueryFilter {
            app: "web".into(),
            from_ms: Some(from),
            to_ms: Some(to),
            ..Default::default()
        };
        let mut got: Vec<String> = query_with_cache(data, Some(r.cache_dir()), &f)
            .unwrap()
            .into_iter()
            .map(|r| r.message)
            .collect();
        got.sort();
        got
    }

    fn files_under(dir: &Path) -> Vec<String> {
        let mut out = vec![];
        for e in std::fs::read_dir(dir).into_iter().flatten().flatten() {
            if e.path().is_dir() {
                out.extend(files_under(&e.path()));
            } else {
                out.push(e.file_name().to_string_lossy().into_owned());
            }
        }
        out.sort();
        out
    }

    #[tokio::test]
    async fn days_that_are_only_in_the_store_are_fetched_and_then_served_from_the_cache() {
        let root = tempfile::tempdir().unwrap();
        let data = root.path().join("data");
        let mem = Arc::new(object_store::memory::InMemory::new());
        chunk(&data, DAY1_NOON, "one");
        chunk(&data, DAY1_NOON + DAY, "two");
        uploaded(&data, &mem, true).await;
        assert!(files_under(&data).iter().all(|f| f.ends_with(".synced")));

        let r = reader(root.path(), &mem);
        let (from, to) = (DAY1_NOON, DAY1_NOON + DAY);
        assert!(!r.prepare(&data, "web", from, to).await);
        assert_eq!(messages(&data, &r, from, to), ["one", "two"]);

        for day in ["2024-01-01", "2024-01-02"] {
            let objects = mem
                .list(Some(&format!("backup/web/{day}").as_str().into()))
                .try_collect::<Vec<_>>()
                .await
                .unwrap();
            for o in objects {
                mem.delete(&o.location).await.unwrap();
            }
        }
        assert!(!r.prepare(&data, "web", from, to).await);
        assert_eq!(
            messages(&data, &r, from, to),
            ["one", "two"],
            "a past day is listed once and then served from the cache"
        );
    }

    #[tokio::test]
    async fn a_past_day_is_not_listed_again_but_today_is_once_its_listing_is_old() {
        let root = tempfile::tempdir().unwrap();
        let data = root.path().join("data");
        let mem = Arc::new(object_store::memory::InMemory::new());
        let now = today_epoch_days() * DAY + DAY / 2;
        let past = now - 3 * DAY;
        chunk(&data, past, "past one");
        chunk(&data, now, "today one");
        uploaded(&data, &mem, true).await;
        let r = reader(root.path(), &mem);
        let (from, to) = (past, now);
        assert!(!r.prepare(&data, "web", from, to).await);
        assert_eq!(messages(&data, &r, from, to), ["past one", "today one"]);

        chunk(&data, past, "past two");
        chunk(&data, now, "today two");
        uploaded(&data, &mem, true).await;
        assert!(!r.prepare(&data, "web", from, to).await);
        assert_eq!(
            messages(&data, &r, from, to),
            ["past one", "today one"],
            "the listings are young, so nothing new is looked for"
        );

        let old = SystemTime::now() - 2 * RELIST_AFTER;
        for day in [past, now] {
            let marker = r
                .cache_dir()
                .join("web")
                .join(epoch_days_to_date(day.div_euclid(DAY)))
                .join(LISTED);
            std::fs::File::options()
                .write(true)
                .open(marker)
                .unwrap()
                .set_modified(old)
                .unwrap();
        }
        assert!(!r.prepare(&data, "web", from, to).await);
        assert_eq!(
            messages(&data, &r, from, to),
            ["past one", "today one", "today two"],
            "today is listed again, a past day is not"
        );
    }

    fn marker_of(r: &RemoteReader, day: i64) -> Option<Vec<u8>> {
        std::fs::read(
            r.cache_dir()
                .join("web")
                .join(epoch_days_to_date(day.div_euclid(DAY)))
                .join(LISTED),
        )
        .ok()
    }

    #[tokio::test]
    async fn a_day_with_a_chunk_left_to_local_disk_is_not_final_until_it_is_fetched() {
        let root = tempfile::tempdir().unwrap();
        let data = root.path().join("data");
        let mem = Arc::new(object_store::memory::InMemory::new());
        let past = (today_epoch_days() - 5) * DAY + DAY / 2;
        chunk(&data, past, "kept");
        uploaded(&data, &mem, false).await;
        let r = reader(root.path(), &mem);
        assert!(!r.prepare(&data, "web", past, past).await);
        assert_eq!(marker_of(&r, past).as_deref(), Some(&b""[..]));
        assert_eq!(messages(&data, &r, past, past), ["kept"]);

        std::fs::remove_dir_all(data.join("web")).unwrap();
        assert_eq!(messages(&data, &r, past, past), Vec::<String>::new());
        let marker = r
            .cache_dir()
            .join("web")
            .join(epoch_days_to_date(past.div_euclid(DAY)))
            .join(LISTED);
        std::fs::File::options()
            .write(true)
            .open(marker)
            .unwrap()
            .set_modified(SystemTime::now() - 2 * RELIST_AFTER)
            .unwrap();
        assert!(!r.prepare(&data, "web", past, past).await);
        assert_eq!(
            messages(&data, &r, past, past),
            ["kept"],
            "fetched once pruned"
        );
        assert_eq!(marker_of(&r, past).as_deref(), Some(FINAL));
    }

    #[tokio::test]
    async fn bytes_of_a_chunk_that_fails_after_downloading_count_against_the_cap() {
        let root = tempfile::tempdir().unwrap();
        let data = root.path().join("data");
        let mem = Arc::new(object_store::memory::InMemory::new());
        for m in ["a", "b"] {
            chunk(&data, DAY1_NOON, m);
        }
        let encryptor = {
            let _g = crate::storage::env_guard();
            unsafe { std::env::remove_var("LOGHAVEN_STORAGE_KEY") };
            Encryptor::current(&root.path().join("keys")).unwrap()
        };
        let syncer =
            CloudSyncer::with_encryption(vec![target(&mem)], data.clone(), true, encryptor);
        assert_eq!(syncer.sync_app(&data.join("web")).await, 0);
        let (t, asked) = spied(&mem, false);
        let mut r = RemoteReader::new(
            t,
            root.path().join("other-keys"),
            root.path().join("cache"),
            &QueryConfig::default(),
        );
        {
            let _g = crate::storage::env_guard();
            Encryptor::current(&root.path().join("other-keys")).unwrap();
        }
        r.fetch_cap = 100_000;
        assert!(r.prepare(&data, "web", DAY1_NOON, DAY1_NOON).await);
        let asked = asked.lock().unwrap().clone();
        assert_eq!(asked.len(), 2, "{asked:?}");
        assert!(
            asked[1] < asked[0],
            "the second download is limited by what the first used: {asked:?}"
        );
    }

    #[tokio::test]
    async fn a_range_that_cannot_hold_data_makes_no_listing_and_no_directory() {
        let root = tempfile::tempdir().unwrap();
        let data = root.path().join("data");
        let mem = Arc::new(object_store::memory::InMemory::new());
        let r = reader(root.path(), &mem);
        for (from, to) in [(i64::MIN, i64::MIN), (i64::MAX, i64::MAX), (-5 * DAY, -DAY)] {
            assert!(
                !r.prepare(&data, "web", from, to).await,
                "no data, so nothing is missing"
            );
        }
        assert!(!r.cache_dir().exists() || files_under(r.cache_dir()).is_empty());
        assert!(
            !r.cache_dir().join("web").exists(),
            "no directory is made for a day that cannot exist"
        );
    }

    #[tokio::test]
    async fn encrypted_chunks_are_fetched_with_the_key_and_left_alone_without_one() {
        let root = tempfile::tempdir().unwrap();
        let data = root.path().join("data");
        let mem = Arc::new(object_store::memory::InMemory::new());
        chunk(&data, DAY1_NOON, "secret");
        let encryptor = {
            let _g = crate::storage::env_guard();
            unsafe { std::env::remove_var("LOGHAVEN_STORAGE_KEY") };
            Encryptor::current(&root.path().join("keys")).unwrap()
        };
        let syncer =
            CloudSyncer::with_encryption(vec![target(&mem)], data.clone(), true, encryptor);
        assert_eq!(syncer.sync_app(&data.join("web")).await, 0);

        let without = RemoteReader::new(
            target(&mem),
            root.path().join("no-keys"),
            root.path().join("cache"),
            &QueryConfig::default(),
        );
        assert!(without.prepare(&data, "web", DAY1_NOON, DAY1_NOON).await);
        assert!(files_under(without.cache_dir()).is_empty());
        let (t, asked) = spied(&mem, false);
        let keyless = RemoteReader::new(
            t,
            root.path().join("no-keys"),
            root.path().join("cache2"),
            &QueryConfig::default(),
        );
        assert!(keyless.prepare(&data, "web", DAY1_NOON, DAY1_NOON).await);
        assert!(
            asked.lock().unwrap().is_empty(),
            "nothing is downloaded that cannot be read"
        );

        let r = reader(root.path(), &mem);
        assert!(!r.prepare(&data, "web", DAY1_NOON, DAY1_NOON).await);
        assert_eq!(messages(&data, &r, DAY1_NOON, DAY1_NOON), ["secret"]);
    }

    #[tokio::test]
    async fn an_object_named_for_another_app_or_day_is_not_taken_for_this_day() {
        let root = tempfile::tempdir().unwrap();
        let data = root.path().join("data");
        let mem = Arc::new(object_store::memory::InMemory::new());
        chunk(&data, DAY1_NOON, "one");
        let body = std::fs::read(
            std::fs::read_dir(data.join("web/2024-01-01"))
                .unwrap()
                .next()
                .unwrap()
                .unwrap()
                .path(),
        )
        .unwrap();
        mem.put(
            &"backup/web/2024-01-01/ok.parquet".into(),
            body.clone().into(),
        )
        .await
        .unwrap();
        mem.put(&"backup/web/2024-01-02/x.parquet".into(), body.into())
            .await
            .unwrap();
        let (t, _) = spied(&mem, true);
        assert_eq!(t.list_app("web/2024-01-01/").await.unwrap().len(), 2);
        let r = RemoteReader::new(
            t,
            root.path().join("keys"),
            root.path().join("cache"),
            &QueryConfig::default(),
        );
        assert!(!r.prepare(&data, "web", DAY1_NOON, DAY1_NOON).await);
        assert!(!r.cache_dir().join("web/2024-01-02").exists());
        assert_eq!(messages(&data, &r, DAY1_NOON, DAY1_NOON), ["one"]);
    }

    #[tokio::test]
    async fn a_record_that_is_both_on_disk_and_in_the_cache_is_returned_once() {
        let root = tempfile::tempdir().unwrap();
        let data = root.path().join("data");
        chunk(&data, DAY1_NOON, "one");
        let cached = root.path().join("cache/web/2024-01-01");
        std::fs::create_dir_all(&cached).unwrap();
        std::fs::copy(
            std::fs::read_dir(data.join("web/2024-01-01"))
                .unwrap()
                .next()
                .unwrap()
                .unwrap()
                .path(),
            cached.join("copy.parquet"),
        )
        .unwrap();
        let f = QueryFilter {
            app: "web".into(),
            ..Default::default()
        };
        let got = query_with_cache(&data, Some(&root.path().join("cache")), &f).unwrap();
        assert_eq!(got.len(), 1);
    }

    #[tokio::test]
    async fn a_chunk_that_is_still_on_disk_is_not_fetched_again() {
        let root = tempfile::tempdir().unwrap();
        let data = root.path().join("data");
        let mem = Arc::new(object_store::memory::InMemory::new());
        chunk(&data, DAY1_NOON, "one");
        uploaded(&data, &mem, false).await;
        let r = reader(root.path(), &mem);
        assert!(!r.prepare(&data, "web", DAY1_NOON, DAY1_NOON).await);
        assert!(files_under(r.cache_dir()).iter().all(|f| f == LISTED));
        assert_eq!(messages(&data, &r, DAY1_NOON, DAY1_NOON), ["one"]);
    }

    #[tokio::test]
    async fn one_query_downloads_no_more_than_its_cap_and_says_the_answer_is_incomplete() {
        let root = tempfile::tempdir().unwrap();
        let data = root.path().join("data");
        let mem = Arc::new(object_store::memory::InMemory::new());
        chunk(&data, DAY1_NOON, "one");
        uploaded(&data, &mem, true).await;
        let mut r = reader(root.path(), &mem);
        r.fetch_cap = 10;
        assert!(r.prepare(&data, "web", DAY1_NOON, DAY1_NOON).await);
        assert!(
            files_under(r.cache_dir()).is_empty(),
            "{:?}",
            files_under(r.cache_dir())
        );
        r.fetch_cap = 1 << 20;
        assert!(!r.prepare(&data, "web", DAY1_NOON, DAY1_NOON).await);
        assert_eq!(messages(&data, &r, DAY1_NOON, DAY1_NOON), ["one"]);
    }

    #[tokio::test]
    async fn a_range_longer_than_the_day_limit_fetches_the_newest_days_only() {
        let root = tempfile::tempdir().unwrap();
        let data = root.path().join("data");
        let mem = Arc::new(object_store::memory::InMemory::new());
        for d in 0..4 {
            chunk(&data, DAY1_NOON + d * DAY, &format!("day{d}"));
        }
        uploaded(&data, &mem, true).await;
        let mut r = reader(root.path(), &mem);
        r.max_days = 2;
        assert!(
            r.prepare(&data, "web", DAY1_NOON, DAY1_NOON + 3 * DAY)
                .await
        );
        assert_eq!(
            messages(&data, &r, DAY1_NOON, DAY1_NOON + 3 * DAY),
            ["day2", "day3"]
        );
    }

    #[test]
    fn the_least_recently_used_chunks_go_first_and_their_day_is_listed_again() {
        let cache = tempfile::tempdir().unwrap();
        let day = cache.path().join("web/2024-01-01");
        std::fs::create_dir_all(&day).unwrap();
        std::fs::write(day.join(LISTED), b"").unwrap();
        let now = SystemTime::now();
        for (name, age_hours) in [("a.parquet", 5), ("b.parquet", 4), ("c.parquet", 3)] {
            let p = day.join(name);
            std::fs::write(&p, [0u8; 100]).unwrap();
            std::fs::File::options()
                .write(true)
                .open(&p)
                .unwrap()
                .set_modified(now - Duration::from_secs(age_hours * 3600))
                .unwrap();
        }
        evict(cache.path(), 150);
        assert_eq!(files_under(cache.path()), ["c.parquet"]);
    }

    #[test]
    fn a_chunk_used_a_moment_ago_is_not_dropped_even_over_the_cap() {
        let cache = tempfile::tempdir().unwrap();
        let day = cache.path().join("web/2024-01-01");
        std::fs::create_dir_all(&day).unwrap();
        std::fs::write(day.join("a.parquet"), [0u8; 100]).unwrap();
        evict(cache.path(), 1);
        assert_eq!(files_under(cache.path()), ["a.parquet"]);
    }
}
