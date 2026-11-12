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
    pub restored: Vec<String>,
    /// Already on disk, left as they are.
    pub present: usize,
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

/// Copies one app's chunks from `target` into the data directory as plain chunks, decrypting
/// those that were uploaded encrypted. Each is marked synced, so it is neither uploaded again
/// nor kept apart from the chunks the daemon wrote. A chunk already on disk is never replaced,
/// and one that cannot be restored is reported without stopping the others.
pub async fn restore_app(
    target: &Target,
    data_path: &Path,
    keys: &[Encryptor],
    app: &str,
    days: DayRange,
) -> Result<RestoreReport> {
    crate::storage::validate_app_name(app)?;
    let mut report = RestoreReport::default();
    let mut listed = target.list_app(&format!("{app}/")).await?;
    listed.sort();
    for object in listed {
        let (name, sealed) = match object.strip_suffix(OBJECT_SUFFIX) {
            Some(n) => (n, true),
            None => (object.as_str(), false),
        };
        let Some(day) = name
            .split('/')
            .nth(1)
            .and_then(parse_date)
            .filter(|_| valid_chunk_name(name))
        else {
            continue;
        };
        if !days.holds(day) {
            continue;
        }
        let chunk = data_path.join(name);
        if chunk.exists() || marker_path(&chunk).exists() {
            report.present += 1;
            continue;
        }
        match fetch(target, &object, name, sealed, keys, &chunk).await {
            Ok(()) => report.restored.push(name.to_string()),
            Err(e) => report.failed.push((name.to_string(), e.to_string())),
        }
    }
    Ok(report)
}

async fn fetch(
    target: &Target,
    object: &str,
    name: &str,
    sealed: bool,
    keys: &[Encryptor],
    chunk: &Path,
) -> Result<()> {
    let bytes = target.get_named(object).await?;
    let plain = if sealed {
        decrypt_with(keys, name, &bytes)?
    } else {
        bytes
    };
    if let Some(dir) = chunk.parent() {
        std::fs::create_dir_all(dir)?;
    }
    // the same temporary name the writer uses, so a crash here is swept at the next start
    let tmp = chunk.with_extension("parquet.tmp");
    let result = std::fs::write(&tmp, &plain)
        .and_then(|()| std::fs::rename(&tmp, chunk))
        .and_then(|()| std::fs::write(marker_path(chunk), b""));
    if let Err(e) = result {
        let _ = std::fs::remove_file(&tmp);
        return Err(LogHavenError::Io(e));
    }
    Ok(())
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

    fn chunk(dir: &Path, rel: &str, bytes: &[u8]) {
        let p = dir.join(rel);
        std::fs::create_dir_all(p.parent().unwrap()).unwrap();
        std::fs::write(p, bytes).unwrap();
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
        chunk(data.path(), "web/2024-01-01/1-1.parquet", b"first");
        chunk(data.path(), "web/2024-01-02/2-1.parquet", b"second");
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
            b"second"
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
        chunk(data.path(), "web/2024-01-01/1-1.parquet", b"secret rows");
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
            b"secret rows"
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
            mem.put(&key.into(), body.into()).await.unwrap();
        }
        let data = tempfile::tempdir().unwrap();
        chunk(data.path(), "web/2024-01-05/b.parquet", b"local, newer");
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
            b"local, newer"
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
        mem.put(&"web/2024-01-02/b.parquet".into(), "fine".into())
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
}
