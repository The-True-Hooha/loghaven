use crate::error::{LogHavenError, Result};
use crate::storage::cloud::{Target, marker_path};
use crate::storage::crypt::{Encryptor, OBJECT_SUFFIX, decrypt_with};
use crate::storage::date::parse_date;
use std::path::Path;

/// `app/date/chunk.parquet`: what a chunk is named inside a store, and what its encryption is
/// bound to. Anything else would only produce a misleading "damaged file" later.
pub fn valid_chunk_name(name: &str) -> bool {
    let parts: Vec<&str> = name.split('/').collect();
    let [app, date, file] = parts.as_slice() else {
        return false;
    };
    crate::storage::validate_app_name(app).is_ok()
        && parse_date(date).is_some()
        && file.ends_with(".parquet")
        && file.len() > ".parquet".len()
        && !file.contains(['\\', ':'])
}

/// What one restore did, chunk by chunk.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct RestoreReport {
    /// Objects found for the app, whatever became of them.
    pub listed: usize,
    pub restored: Vec<String>,
    /// Already on disk, left as they are.
    pub present: usize,
    /// Objects that are not chunks of the asked days: other names, other days, or a plain copy
    /// of a chunk that also exists encrypted.
    pub skipped: usize,
    /// Chunk name and why it could not be restored.
    pub failed: Vec<(String, String)>,
}

/// Days an app's chunks are wanted for, as `parse_date` counts them.
#[derive(Clone, Copy, Default)]
pub struct DayRange {
    pub from: Option<i64>,
    pub to: Option<i64>,
}

impl DayRange {
    fn holds(&self, day: i64) -> bool {
        self.from.is_none_or(|f| day >= f) && self.to.is_none_or(|t| day <= t)
    }
}

/// The largest chunk restored as it is. Chunks are capped by `rotate_size_mb`, so an object this
/// big is not one of ours; it is streamed to disk, so the bound is on disk space, not memory.
const MAX_PLAIN_BYTES: u64 = 16 << 30;

/// The largest encrypted chunk restored. An AES-GCM object is one message that is only
/// authenticated once all of it is read, so it is decrypted in memory, and the bound is real.
const MAX_SEALED_BYTES: u64 = 1 << 30;

const RESTORE_PREFIX: &str = ".restore-";
const RESTORE_SUFFIX: &str = ".part";
/// A temporary file idle this long is the leftover of a restore that died; one being filled is
/// written to at least every `STALL_TIMEOUT`.
const STALE_AFTER: std::time::Duration = std::time::Duration::from_secs(3600);

/// Removes leftovers of earlier restores that were killed in the middle.
fn sweep_stale(data_path: &Path, app: &str) {
    for day in std::fs::read_dir(data_path.join(app))
        .into_iter()
        .flatten()
        .flatten()
    {
        for f in std::fs::read_dir(day.path())
            .into_iter()
            .flatten()
            .flatten()
        {
            let name = f.file_name().to_string_lossy().into_owned();
            let idle = f
                .metadata()
                .and_then(|m| m.modified())
                .ok()
                .and_then(|t| t.elapsed().ok())
                .is_some_and(|age| age > STALE_AFTER);
            if name.starts_with(RESTORE_PREFIX) && name.ends_with(RESTORE_SUFFIX) && idle {
                let _ = std::fs::remove_file(f.path());
            }
        }
    }
}

const PARQUET_MAGIC: &[u8; 4] = b"PAR1";

fn looks_like_parquet(bytes: &[u8]) -> bool {
    bytes.len() >= 8 && bytes.starts_with(PARQUET_MAGIC) && bytes.ends_with(PARQUET_MAGIC)
}

fn file_looks_like_parquet(path: &Path) -> std::io::Result<bool> {
    use std::io::{Read, Seek, SeekFrom};
    let mut f = std::fs::File::open(path)?;
    if f.metadata()?.len() < 8 {
        return Ok(false);
    }
    let (mut head, mut tail) = ([0u8; 4], [0u8; 4]);
    f.read_exact(&mut head)?;
    f.seek(SeekFrom::End(-4))?;
    f.read_exact(&mut tail)?;
    Ok(&head == PARQUET_MAGIC && &tail == PARQUET_MAGIC)
}

/// Copies one app's chunks from `target` into the data directory as plain chunks, decrypting
/// those that were uploaded encrypted. Each is marked synced, so it is neither uploaded again
/// nor kept apart from the chunks the daemon wrote. A chunk already on disk is never replaced,
/// and one that cannot be restored is reported without stopping the others. When a chunk exists
/// in the store both encrypted and plain, only the encrypted object is used.
pub async fn restore_app(
    target: &Target,
    data_path: &Path,
    keys: &[Encryptor],
    app: &str,
    days: DayRange,
) -> Result<RestoreReport> {
    crate::storage::validate_app_name(app)?;
    sweep_stale(data_path, app);
    let mut report = RestoreReport::default();
    let mut listed = target.list_app(&format!("{app}/")).await?;
    listed.sort();
    report.listed = listed.len();
    let sealed_names: std::collections::HashSet<&str> = listed
        .iter()
        .filter_map(|o| o.strip_suffix(OBJECT_SUFFIX))
        .collect();
    for object in &listed {
        let (name, sealed) = match object.strip_suffix(OBJECT_SUFFIX) {
            Some(n) => (n, true),
            None => (object.as_str(), false),
        };
        let day = name.split('/').nth(1).and_then(parse_date);
        let wanted = valid_chunk_name(name) && day.is_some_and(|d| days.holds(d));
        if !wanted || (!sealed && sealed_names.contains(name)) {
            report.skipped += 1;
            continue;
        }
        let chunk = data_path.join(name);
        if chunk.exists() {
            report.present += 1;
            continue;
        }
        match fetch(target, object, name, sealed, keys, &chunk).await {
            Ok(Published::New) => report.restored.push(name.to_string()),
            Ok(Published::AlreadyThere) => report.present += 1,
            Err(e) => report.failed.push((name.to_string(), e.to_string())),
        }
    }
    Ok(report)
}

enum Published {
    New,
    AlreadyThere,
}

async fn fetch(
    target: &Target,
    object: &str,
    name: &str,
    sealed: bool,
    keys: &[Encryptor],
    chunk: &Path,
) -> Result<Published> {
    let dir = chunk
        .parent()
        .ok_or_else(|| LogHavenError::Storage("chunk has no directory".into()))?;
    let app_dir = dir.parent().unwrap_or(dir);
    for d in [app_dir, dir] {
        if std::fs::symlink_metadata(d).is_ok_and(|m| m.file_type().is_symlink()) {
            return Err(LogHavenError::Storage(format!(
                "{} is a symbolic link",
                d.display()
            )));
        }
    }
    let made_dir = !dir.exists();
    let made_app_dir = !app_dir.exists();
    std::fs::create_dir_all(dir)?;
    // not `.parquet.tmp`: the daemon's startup sweep deletes those, even one being filled now
    let tmp = dir.join(format!(
        "{RESTORE_PREFIX}{:016x}{RESTORE_SUFFIX}",
        rand::random::<u64>()
    ));
    let staged = stage(target, object, name, sealed, keys, &tmp).await;
    let published = match staged {
        Ok(()) => publish(&tmp, chunk),
        Err(e) => Err(e),
    };
    let _ = std::fs::remove_file(&tmp);
    if made_dir && !matches!(published, Ok(Published::New)) {
        let _ = std::fs::remove_dir(dir);
        if made_app_dir {
            let _ = std::fs::remove_dir(app_dir);
        }
    }
    published
}

/// Leaves the plain, checked chunk in `tmp`.
async fn stage(
    target: &Target,
    object: &str,
    name: &str,
    sealed: bool,
    keys: &[Encryptor],
    tmp: &Path,
) -> Result<()> {
    use tokio::io::AsyncWriteExt;
    let cap = if sealed {
        MAX_SEALED_BYTES
    } else {
        MAX_PLAIN_BYTES
    };
    target.download_named(object, tmp, cap).await?;
    if sealed {
        let plain = decrypt_with(keys, name, &tokio::fs::read(tmp).await?)?;
        if !looks_like_parquet(&plain) {
            return Err(LogHavenError::Storage("not a Parquet file".into()));
        }
        let mut f = tokio::fs::File::create(tmp).await?;
        f.write_all(&plain).await?;
        f.sync_all().await?;
    } else if !file_looks_like_parquet(tmp)? {
        return Err(LogHavenError::Storage("not a Parquet file".into()));
    }
    Ok(())
}

/// Puts the chunk in place so the daemon never sees it half done or unmarked: the marker goes
/// first (a chunk with no marker would be uploaded again, and deleted when `delete_after_upload`
/// is on), then a hard link makes the chunk appear only if nothing is there yet.
fn publish(tmp: &Path, chunk: &Path) -> Result<Published> {
    let marker = marker_path(chunk);
    let marker_was_there = marker.exists();
    std::fs::write(&marker, b"")?;
    match std::fs::hard_link(tmp, chunk) {
        Ok(()) => Ok(Published::New),
        Err(e) => {
            // a marker restore did not make belongs to whatever chunk it was written for
            if !marker_was_there {
                let _ = std::fs::remove_file(&marker);
            }
            if e.kind() == std::io::ErrorKind::AlreadyExists {
                Ok(Published::AlreadyThere)
            } else {
                Err(LogHavenError::Io(e))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::cloud::{CloudSyncer, CrateStore};
    use object_store::ObjectStoreExt;
    use std::sync::Arc;

    fn memory_target(mem: &Arc<object_store::memory::InMemory>, prefix: Option<&str>) -> Target {
        Target::new(
            "mem",
            Box::new(CrateStore(mem.clone())),
            prefix.map(String::from),
        )
    }

    /// Bytes that pass for a Parquet file: the magic number at both ends.
    fn parquet(body: &str) -> Vec<u8> {
        [&b"PAR1"[..], body.as_bytes(), &b"PAR1"[..]].concat()
    }

    fn chunk(dir: &Path, rel: &str, body: &str) {
        let p = dir.join(rel);
        std::fs::create_dir_all(p.parent().unwrap()).unwrap();
        std::fs::write(p, parquet(body)).unwrap();
    }

    fn encryptor(keys: &Path) -> Encryptor {
        let _g = crate::storage::env_guard();
        unsafe { std::env::remove_var("LOGHAVEN_STORAGE_KEY") };
        Encryptor::current(keys).unwrap()
    }

    async fn upload_all(syncer: &CloudSyncer, data: &Path, app: &str) {
        assert_eq!(syncer.sync_app(&data.join(app)).await, 0);
    }

    #[tokio::test]
    async fn a_deleted_local_copy_comes_back_byte_for_byte_and_is_not_uploaded_again() {
        let mem = Arc::new(object_store::memory::InMemory::new());
        let data = tempfile::tempdir().unwrap();
        chunk(data.path(), "web/2024-01-01/1-1.parquet", "first");
        chunk(data.path(), "web/2024-01-02/2-1.parquet", "second");
        let syncer = CloudSyncer::with_targets(
            vec![memory_target(&mem, Some("backup"))],
            data.path().to_path_buf(),
            true,
        );
        upload_all(&syncer, data.path(), "web").await;
        assert!(!data.path().join("web/2024-01-01/1-1.parquet").exists());

        let target = memory_target(&mem, Some("backup"));
        let report = restore_app(&target, data.path(), &[], "web", DayRange::default())
            .await
            .unwrap();
        assert_eq!(
            report.restored,
            ["web/2024-01-01/1-1.parquet", "web/2024-01-02/2-1.parquet"]
        );
        assert_eq!(
            std::fs::read(data.path().join("web/2024-01-02/2-1.parquet")).unwrap(),
            parquet("second")
        );
        assert!(
            marker_path(&data.path().join("web/2024-01-01/1-1.parquet")).exists(),
            "restored chunks are marked synced"
        );
        assert_eq!(syncer.sync_app(&data.path().join("web")).await, 0);
        let uploaded = mem.head(&"backup/web/2024-01-01/1-1.parquet".into()).await;
        assert!(uploaded.is_ok());
    }

    #[tokio::test]
    async fn encrypted_uploads_are_decrypted_with_the_key_that_made_them() {
        let mem = Arc::new(object_store::memory::InMemory::new());
        let data = tempfile::tempdir().unwrap();
        let keys = tempfile::tempdir().unwrap();
        let enc = encryptor(keys.path());
        chunk(data.path(), "web/2024-01-01/1-1.parquet", "secret rows");
        let syncer = CloudSyncer::with_encryption(
            vec![memory_target(&mem, None)],
            data.path().to_path_buf(),
            true,
            enc,
        );
        upload_all(&syncer, data.path(), "web").await;

        let target = memory_target(&mem, None);
        let all = Encryptor::all(keys.path()).unwrap();
        let ok = restore_app(&target, data.path(), &all, "web", DayRange::default())
            .await
            .unwrap();
        assert_eq!(ok.restored, ["web/2024-01-01/1-1.parquet"]);
        assert_eq!(
            std::fs::read(data.path().join("web/2024-01-01/1-1.parquet")).unwrap(),
            parquet("secret rows")
        );

        let elsewhere = tempfile::tempdir().unwrap();
        let bad = restore_app(&target, elsewhere.path(), &[], "web", DayRange::default())
            .await
            .unwrap();
        assert!(bad.restored.is_empty());
        assert_eq!(bad.failed.len(), 1, "{bad:?}");
        assert!(bad.failed[0].1.contains("not among the keys"), "{bad:?}");
        assert!(!elsewhere.path().join("web/2024-01-01/1-1.parquet").exists());
    }

    #[tokio::test]
    async fn only_the_asked_app_and_days_come_back_and_local_chunks_are_never_replaced() {
        let mem = Arc::new(object_store::memory::InMemory::new());
        for (key, body) in [
            ("web/2024-01-01/a.parquet", "jan1"),
            ("web/2024-01-05/b.parquet", "jan5"),
            ("web/2024-01-09/c.parquet", "jan9"),
            ("webapp/2024-01-05/d.parquet", "other app"),
            ("web/2024-01-05/notes.txt", "not a chunk"),
            ("web/not-a-date/e.parquet", "bad date"),
        ] {
            mem.put(&key.into(), parquet(body).into()).await.unwrap();
        }
        let data = tempfile::tempdir().unwrap();
        chunk(data.path(), "web/2024-01-05/b.parquet", "local, newer");
        let days = DayRange {
            from: parse_date("2024-01-02"),
            to: parse_date("2024-01-08"),
        };
        let report = restore_app(&memory_target(&mem, None), data.path(), &[], "web", days)
            .await
            .unwrap();
        assert!(report.restored.is_empty(), "{report:?}");
        assert_eq!(report.present, 1);
        assert_eq!(
            std::fs::read(data.path().join("web/2024-01-05/b.parquet")).unwrap(),
            parquet("local, newer")
        );

        let all = restore_app(
            &memory_target(&mem, None),
            data.path(),
            &[],
            "web",
            DayRange::default(),
        )
        .await
        .unwrap();
        assert_eq!(
            all.restored,
            ["web/2024-01-01/a.parquet", "web/2024-01-09/c.parquet"]
        );
        assert!(!data.path().join("webapp").exists());
        assert!(!data.path().join("web/not-a-date").exists());
    }

    #[tokio::test]
    async fn one_unreadable_object_does_not_stop_the_rest() {
        let mem = Arc::new(object_store::memory::InMemory::new());
        mem.put(&"web/2024-01-01/a.parquet.enc".into(), "junk".into())
            .await
            .unwrap();
        mem.put(&"web/2024-01-02/b.parquet".into(), parquet("fine").into())
            .await
            .unwrap();
        let data = tempfile::tempdir().unwrap();
        let report = restore_app(
            &memory_target(&mem, None),
            data.path(),
            &[],
            "web",
            DayRange::default(),
        )
        .await
        .unwrap();
        assert_eq!(report.restored, ["web/2024-01-02/b.parquet"]);
        assert_eq!(report.failed.len(), 1);
        assert!(report.failed[0].0.ends_with("a.parquet"));
    }

    #[tokio::test]
    async fn a_bad_app_name_is_refused_before_the_store_is_touched() {
        let mem = Arc::new(object_store::memory::InMemory::new());
        let data = tempfile::tempdir().unwrap();
        let r = restore_app(
            &memory_target(&mem, None),
            data.path(),
            &[],
            "../x",
            DayRange::default(),
        )
        .await;
        assert!(matches!(r, Err(LogHavenError::InvalidApp(_))));
    }

    #[tokio::test]
    async fn a_plain_object_cannot_stand_in_for_the_encrypted_one_of_the_same_chunk() {
        let mem = Arc::new(object_store::memory::InMemory::new());
        let data = tempfile::tempdir().unwrap();
        let keys = tempfile::tempdir().unwrap();
        let enc = encryptor(keys.path());
        let name = "web/2024-01-01/x.parquet";
        mem.put(
            &format!("{name}.enc").as_str().into(),
            enc.encrypt(name, &parquet("real")).unwrap().into(),
        )
        .await
        .unwrap();
        mem.put(&name.into(), parquet("planted").into())
            .await
            .unwrap();
        let all = Encryptor::all(keys.path()).unwrap();
        let report = restore_app(
            &memory_target(&mem, None),
            data.path(),
            &all,
            "web",
            DayRange::default(),
        )
        .await
        .unwrap();
        assert_eq!(report.restored, [name]);
        assert_eq!(report.skipped, 1);
        assert_eq!(
            std::fs::read(data.path().join(name)).unwrap(),
            parquet("real")
        );
    }

    #[tokio::test]
    async fn an_object_that_is_not_parquet_leaves_nothing_behind() {
        let mem = Arc::new(object_store::memory::InMemory::new());
        mem.put(&"web/2024-01-01/x.parquet".into(), "<html>".into())
            .await
            .unwrap();
        let data = tempfile::tempdir().unwrap();
        let report = restore_app(
            &memory_target(&mem, None),
            data.path(),
            &[],
            "web",
            DayRange::default(),
        )
        .await
        .unwrap();
        assert_eq!(report.failed.len(), 1, "{report:?}");
        let left: Vec<_> = walk(data.path());
        assert!(left.is_empty(), "{left:?}");
        assert!(!data.path().join("web/2024-01-01").exists());
    }

    #[tokio::test]
    async fn a_big_chunk_is_streamed_to_disk_whole_and_leaves_no_temporary_file() {
        let mem = Arc::new(object_store::memory::InMemory::new());
        let body = parquet(&"x".repeat(5 << 20));
        mem.put(&"web/2024-01-01/big.parquet".into(), body.clone().into())
            .await
            .unwrap();
        let data = tempfile::tempdir().unwrap();
        let report = restore_app(
            &memory_target(&mem, None),
            data.path(),
            &[],
            "web",
            DayRange::default(),
        )
        .await
        .unwrap();
        assert_eq!(
            report.restored,
            ["web/2024-01-01/big.parquet"],
            "{report:?}"
        );
        assert_eq!(
            std::fs::read(data.path().join("web/2024-01-01/big.parquet")).unwrap(),
            body
        );
        let names: Vec<_> = walk(data.path())
            .iter()
            .map(|p| p.file_name().unwrap().to_string_lossy().into_owned())
            .collect();
        assert_eq!(names.len(), 2, "{names:?}");
        assert!(
            names.iter().all(|n| !n.starts_with(RESTORE_PREFIX)),
            "{names:?}"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_name_taken_after_the_check_leaves_no_marker_and_no_app_directory_on_failure() {
        let mem = Arc::new(object_store::memory::InMemory::new());
        mem.put(&"web/2024-01-01/a.parquet".into(), parquet("rows").into())
            .await
            .unwrap();
        let data = tempfile::tempdir().unwrap();
        let day = data.path().join("web/2024-01-01");
        std::fs::create_dir_all(&day).unwrap();
        std::os::unix::fs::symlink("/nonexistent/x", day.join("a.parquet")).unwrap();
        let report = restore_app(
            &memory_target(&mem, None),
            data.path(),
            &[],
            "web",
            DayRange::default(),
        )
        .await
        .unwrap();
        assert_eq!(report.present, 1, "{report:?}");
        assert!(!day.join("a.parquet.synced").exists());

        mem.put(
            &"api/2024-01-01/b.parquet".into(),
            b"<html>".to_vec().into(),
        )
        .await
        .unwrap();
        restore_app(
            &memory_target(&mem, None),
            data.path(),
            &[],
            "api",
            DayRange::default(),
        )
        .await
        .unwrap();
        assert!(!data.path().join("api").exists());
    }

    #[tokio::test]
    async fn only_idle_leftovers_of_an_earlier_restore_are_swept() {
        let mem = Arc::new(object_store::memory::InMemory::new());
        let data = tempfile::tempdir().unwrap();
        let day = data.path().join("web/2024-01-01");
        std::fs::create_dir_all(&day).unwrap();
        for n in [
            ".restore-1.part",
            ".restore-2.part",
            "notes.part",
            "x.parquet",
        ] {
            std::fs::write(day.join(n), b"x").unwrap();
        }
        let old = std::time::SystemTime::now() - 2 * STALE_AFTER;
        for n in [".restore-1.part", "notes.part"] {
            std::fs::File::options()
                .write(true)
                .open(day.join(n))
                .unwrap()
                .set_modified(old)
                .unwrap();
        }
        let _ = restore_app(
            &memory_target(&mem, None),
            data.path(),
            &[],
            "web",
            DayRange::default(),
        )
        .await;
        let mut left: Vec<_> = walk(&day)
            .iter()
            .map(|p| p.file_name().unwrap().to_string_lossy().into_owned())
            .collect();
        left.sort();
        assert_eq!(left, [".restore-2.part", "notes.part", "x.parquet"]);
    }

    #[test]
    fn a_file_is_checked_for_the_parquet_magic_at_both_ends() {
        let d = tempfile::tempdir().unwrap();
        for (body, ok) in [
            (parquet("rows"), true),
            (b"PAR1".to_vec(), false),
            (b"PAR1rowsrows".to_vec(), false),
            (b"rowsrowsPAR1".to_vec(), false),
            (vec![], false),
        ] {
            let p = d.path().join("f");
            std::fs::write(&p, &body).unwrap();
            assert_eq!(file_looks_like_parquet(&p).unwrap(), ok, "{body:?}");
        }
    }

    fn walk(dir: &Path) -> Vec<std::path::PathBuf> {
        let mut out = vec![];
        for e in std::fs::read_dir(dir).into_iter().flatten().flatten() {
            if e.path().is_dir() {
                out.extend(walk(&e.path()));
            } else {
                out.push(e.path());
            }
        }
        out
    }

    #[tokio::test]
    async fn a_prefix_that_starts_another_prefix_is_kept_apart() {
        let mem = Arc::new(object_store::memory::InMemory::new());
        mem.put(
            &"backup/web/2024-01-01/a.parquet".into(),
            parquet("mine").into(),
        )
        .await
        .unwrap();
        mem.put(
            &"backup2/web/2024-01-01/b.parquet".into(),
            parquet("theirs").into(),
        )
        .await
        .unwrap();
        let data = tempfile::tempdir().unwrap();
        let report = restore_app(
            &memory_target(&mem, Some("backup")),
            data.path(),
            &[],
            "web",
            DayRange::default(),
        )
        .await
        .unwrap();
        assert_eq!(report.restored, ["web/2024-01-01/a.parquet"]);
    }

    #[tokio::test]
    async fn a_restored_chunk_is_never_offered_to_the_uploader() {
        use crate::storage::cloud::testing::MockStore;
        let mem = Arc::new(object_store::memory::InMemory::new());
        mem.put(&"web/2024-01-01/a.parquet".into(), parquet("rows").into())
            .await
            .unwrap();
        let data = tempfile::tempdir().unwrap();
        restore_app(
            &memory_target(&mem, None),
            data.path(),
            &[],
            "web",
            DayRange::default(),
        )
        .await
        .unwrap();
        let mock = Arc::new(MockStore::default());
        let syncer = CloudSyncer::with_store(
            Box::new(mock.clone()),
            None,
            data.path().to_path_buf(),
            true,
        );
        assert_eq!(syncer.sync_app(&data.path().join("web")).await, 0);
        assert!(mock.puts.lock().unwrap().is_empty());
        assert!(data.path().join("web/2024-01-01/a.parquet").exists());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_symlinked_day_directory_is_not_written_through() {
        let mem = Arc::new(object_store::memory::InMemory::new());
        mem.put(&"web/2024-01-01/a.parquet".into(), parquet("rows").into())
            .await
            .unwrap();
        let data = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(data.path().join("web")).unwrap();
        std::os::unix::fs::symlink(outside.path(), data.path().join("web/2024-01-01")).unwrap();
        let report = restore_app(
            &memory_target(&mem, None),
            data.path(),
            &[],
            "web",
            DayRange::default(),
        )
        .await
        .unwrap();
        assert_eq!(report.failed.len(), 1, "{report:?}");
        assert!(walk(outside.path()).is_empty());
    }
}
