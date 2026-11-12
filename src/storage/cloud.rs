use crate::error::{LogHavenError, Result};
use crate::storage::stores::tidy;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;

const SYNCED_SUFFIX: &str = "synced";
const PUT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(120);
type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Where chunks are uploaded. The seam of the storage layer: the upload code only ever calls
/// `put`, so the encryption, markers, retries and pruning work for every store. Implemented once
/// over the `object_store` crate (S3 and compatible stores, Azure Blob Storage, Google Cloud
/// Storage); tests use a mock. A store the crate does not cover (SFTP, WebDAV) is one more
/// implementation of this trait plus a `TargetKind` variant.
pub trait ObjectStore: Send + Sync {
    fn put<'a>(&'a self, key: &'a str, file: &'a Path) -> BoxFuture<'a, Result<()>>;
}

/// A chunk this big or smaller goes up in one request; a larger one in parts, read from the file
/// as they are sent, so memory stays at a few parts however big the chunk is.
const MULTIPART_THRESHOLD: u64 = 8 * 1024 * 1024;
const PART_SIZE: usize = 8 * 1024 * 1024;
/// The least a slow uplink is assumed to carry, which sets how long an upload may take.
const MIN_BYTES_PER_SEC: u64 = 256 * 1024;

/// An upload is given `PUT_TIMEOUT` plus the time its size needs at `MIN_BYTES_PER_SEC`.
fn upload_budget(bytes: u64) -> std::time::Duration {
    PUT_TIMEOUT + std::time::Duration::from_secs(bytes / MIN_BYTES_PER_SEC)
}

/// `ObjectStore` over the `object_store` crate.
struct CrateStore(Arc<dyn object_store::ObjectStore>);

impl CrateStore {
    async fn send(&self, path: &object_store::path::Path, file: &Path, len: u64) -> Result<()> {
        use object_store::ObjectStoreExt;
        use tokio::io::AsyncReadExt;
        let fail = |e: &dyn std::fmt::Display| {
            LogHavenError::Storage(format!("upload to cloud failed: {}", tidy(e)))
        };
        if len <= MULTIPART_THRESHOLD {
            let bytes = tokio::fs::read(file)
                .await
                .map_err(|e| LogHavenError::Storage(format!("read chunk for upload: {e}")))?;
            self.0
                .put(path, object_store::PutPayload::from(bytes))
                .await
                .map_err(|e| fail(&e))?;
            return Ok(());
        }
        let mut input = tokio::fs::File::open(file)
            .await
            .map_err(|e| LogHavenError::Storage(format!("read chunk for upload: {e}")))?;
        let upload = self.0.put_multipart(path).await.map_err(|e| fail(&e))?;
        let mut upload = object_store::WriteMultipart::new_with_chunk_size(upload, PART_SIZE);
        let mut buf = vec![0u8; PART_SIZE];
        loop {
            // at most a few parts are in flight, so a big chunk never sits in memory whole
            upload.wait_for_capacity(2).await.map_err(|e| fail(&e))?;
            let n = input
                .read(&mut buf)
                .await
                .map_err(|e| LogHavenError::Storage(format!("read chunk for upload: {e}")))?;
            if n == 0 {
                break;
            }
            upload.write(&buf[..n]);
        }
        upload.finish().await.map_err(|e| fail(&e))?;
        Ok(())
    }
}

impl ObjectStore for CrateStore {
    fn put<'a>(&'a self, key: &'a str, file: &'a Path) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let path = object_store::path::Path::parse(key)
                .map_err(|e| LogHavenError::Storage(format!("bad object key: {}", tidy(&e))))?;
            let len = std::fs::metadata(file)
                .map_err(|e| LogHavenError::Storage(format!("read chunk for upload: {e}")))?
                .len();
            // a hung upload would hold the app's sync lock and stall all later uploads
            tokio::time::timeout(upload_budget(len), self.send(&path, file, len))
                .await
                .map_err(|_| LogHavenError::Storage("upload to cloud timed out".into()))?
        })
    }
}

/// One upload destination.
pub struct Target {
    /// Names the target's partial marker while several targets are in use.
    label: String,
    store: Box<dyn ObjectStore>,
    prefix: String,
}

impl Target {
    pub fn new(label: &str, store: Box<dyn ObjectStore>, prefix: Option<String>) -> Self {
        Self {
            label: label.to_string(),
            store,
            prefix: prefix.unwrap_or_default().trim_matches('/').to_string(),
        }
    }

    /// A store set up from one `[[storage.targets]]` entry.
    pub fn from_config(t: &crate::config::StorageTarget) -> Result<Self> {
        let store = crate::storage::stores::build(t)?;
        Ok(Self::new(
            &t.name,
            Box::new(CrateStore(store)),
            t.prefix.clone(),
        ))
    }

    pub fn label(&self) -> &str {
        &self.label
    }

    fn key_for(&self, data_path: &Path, chunk_path: &Path) -> Result<String> {
        let rel = chunk_path
            .strip_prefix(data_path)
            .map_err(|_| LogHavenError::Storage("chunk path not under data_path".into()))?;
        let rel = rel.to_string_lossy().replace('\\', "/");
        Ok(if self.prefix.is_empty() {
            rel
        } else {
            format!("{}/{}", self.prefix, rel)
        })
    }
}

/// Removes the encrypted copy made for an upload however the upload ends.
struct TempCopy(PathBuf);

impl Drop for TempCopy {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

/// Uploads finished chunks to one or more stores and remembers which ones are done.
///
/// A chunk `X.parquet` is synced once `X.parquet.synced` exists next to it, or
/// once it was deleted after upload; with several targets that is only after every
/// target has it. While some targets are still missing, `X.parquet.synced-<label>`
/// records each one that is done, so a retry uploads to the missing targets only.
/// Anything else is retried on the next run.
pub struct CloudSyncer {
    targets: Vec<Target>,
    encryptor: Option<crate::storage::crypt::Encryptor>,
    data_path: PathBuf,
    pub delete_after_upload: bool,
}

impl CloudSyncer {
    pub fn with_store(
        store: Box<dyn ObjectStore>,
        prefix: Option<String>,
        data_path: PathBuf,
        delete_after_upload: bool,
    ) -> Arc<Self> {
        Self::with_targets(
            vec![Target::new("store", store, prefix)],
            data_path,
            delete_after_upload,
        )
    }

    /// At least one target.
    pub fn with_targets(
        targets: Vec<Target>,
        data_path: PathBuf,
        delete_after_upload: bool,
    ) -> Arc<Self> {
        assert!(!targets.is_empty(), "a syncer needs a target");
        Arc::new(Self {
            targets,
            encryptor: None,
            data_path,
            delete_after_upload,
        })
    }

    /// Like `with_targets`, and every upload is encrypted first.
    pub fn with_encryption(
        targets: Vec<Target>,
        data_path: PathBuf,
        delete_after_upload: bool,
        encryptor: crate::storage::crypt::Encryptor,
    ) -> Arc<Self> {
        assert!(!targets.is_empty(), "a syncer needs a target");
        Arc::new(Self {
            targets,
            encryptor: Some(encryptor),
            data_path,
            delete_after_upload,
        })
    }

    #[cfg(test)]
    pub fn target_labels(&self) -> Vec<String> {
        self.targets.iter().map(|t| t.label.clone()).collect()
    }

    /// The chunk's path below the data directory, the same for every store and prefix.
    fn relative_name(&self, chunk_path: &Path) -> Result<String> {
        let rel = chunk_path
            .strip_prefix(&self.data_path)
            .map_err(|_| LogHavenError::Storage("chunk path not under data_path".into()))?;
        Ok(rel.to_string_lossy().replace('\\', "/"))
    }

    /// Uploads one chunk to every target that does not have it yet, then marks it synced
    /// (or deletes it). A target that fails does not stop the others; the first error is
    /// returned and the chunk stays unsynced.
    pub async fn upload_chunk(&self, chunk_path: &Path) -> Result<()> {
        let several = self.targets.len() > 1;
        // encrypted once, next to the chunk (mode 0600), and removed however this ends
        let sealed = match &self.encryptor {
            Some(enc) => {
                let name = self.relative_name(chunk_path)?;
                let tmp = TempCopy(chunk_path.with_extension("parquet.enc.tmp"));
                let _ = std::fs::remove_file(&tmp.0);
                crate::storage::crypt::write_new_private(
                    &tmp.0,
                    &enc.encrypt(&name, &std::fs::read(chunk_path)?)?,
                )?;
                Some(tmp)
            }
            None => None,
        };
        let source = sealed.as_ref().map_or(chunk_path, |t| t.0.as_path());
        let mut failed = None;
        for t in &self.targets {
            if several && partial_marker(chunk_path, &t.label).exists() {
                continue;
            }
            let mut key = t.key_for(&self.data_path, chunk_path)?;
            if sealed.is_some() {
                key.push_str(crate::storage::crypt::OBJECT_SUFFIX);
            }
            match t.store.put(&key, source).await {
                Ok(()) if several => std::fs::write(partial_marker(chunk_path, &t.label), b"")?,
                Ok(()) => {}
                Err(e) => {
                    failed.get_or_insert(e);
                }
            }
        }
        drop(sealed);
        if let Some(e) = failed {
            return Err(e);
        }

        if self.delete_after_upload {
            std::fs::remove_file(chunk_path)?;
        } else {
            std::fs::write(marker_path(chunk_path), b"")?;
        }
        if several {
            for t in &self.targets {
                let _ = std::fs::remove_file(partial_marker(chunk_path, &t.label));
            }
        }
        Ok(())
    }

    /// Uploads every finished chunk of an app that is not yet synced.
    /// One failing chunk does not stop the others; returns how many failed.
    pub async fn sync_app(&self, app_dir: &Path) -> usize {
        let mut failures = 0;
        for chunk in unsynced_chunks(app_dir) {
            if let Err(e) = self.upload_chunk(&chunk).await {
                failures += 1;
                eprintln!("Cloud sync error for {}: {}", chunk.display(), e);
            }
        }
        failures
    }
}

fn partial_marker(chunk: &Path, label: &str) -> PathBuf {
    chunk.with_extension(format!("parquet.{SYNCED_SUFFIX}-{label}"))
}

pub(crate) fn marker_path(chunk: &Path) -> PathBuf {
    chunk.with_extension(format!("parquet.{SYNCED_SUFFIX}"))
}

/// Finished `.parquet` chunks in date directories that have no `.synced` marker,
/// oldest first.
fn unsynced_chunks(app_dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let Ok(dates) = std::fs::read_dir(app_dir) else {
        return out;
    };
    for date in dates.flatten() {
        let is_date = date
            .file_name()
            .to_str()
            .is_some_and(|n| crate::storage::date::parse_date(n).is_some());
        if !is_date {
            continue;
        }
        let Ok(files) = std::fs::read_dir(date.path()) else {
            continue;
        };
        for f in files.flatten() {
            let p = f.path();
            if p.extension().and_then(|e| e.to_str()) == Some("parquet")
                && !marker_path(&p).exists()
            {
                out.push(p);
            }
        }
    }
    out.sort();
    out
}

#[cfg(test)]
pub(crate) mod testing {
    use super::*;
    use std::collections::HashSet;
    use std::sync::Mutex;

    /// Records uploaded keys; keys in `fail` fail once per entry until cleared.
    #[derive(Default)]
    pub struct MockStore {
        pub puts: Mutex<Vec<String>>,
        pub fail: Mutex<HashSet<String>>,
        pub delay_ms: u64,
        pub fail_all: std::sync::atomic::AtomicBool,
    }

    impl ObjectStore for Arc<MockStore> {
        fn put<'a>(&'a self, key: &'a str, _file: &'a Path) -> BoxFuture<'a, Result<()>> {
            Box::pin(async move {
                if self.delay_ms > 0 {
                    tokio::time::sleep(std::time::Duration::from_millis(self.delay_ms)).await;
                }
                if self.fail_all.load(std::sync::atomic::Ordering::SeqCst)
                    || self.fail.lock().unwrap().contains(key)
                {
                    return Err(LogHavenError::Storage("mock failure".into()));
                }
                self.puts.lock().unwrap().push(key.to_string());
                Ok(())
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::testing::MockStore;
    use super::*;
    use std::sync::Mutex;

    fn setup(
        name: &str,
        prefix: Option<&str>,
        delete: bool,
    ) -> (Arc<CloudSyncer>, Arc<MockStore>, PathBuf) {
        let tmp = std::env::temp_dir().join(format!("lh-c-{}-{}", name, std::process::id()));
        let _ = std::fs::remove_dir_all(&tmp);
        std::fs::create_dir_all(&tmp).unwrap();
        let store = Arc::new(MockStore::default());
        let syncer = CloudSyncer::with_store(
            Box::new(store.clone()),
            prefix.map(String::from),
            tmp.clone(),
            delete,
        );
        (syncer, store, tmp)
    }

    fn chunk(tmp: &Path, app: &str, date: &str, name: &str) -> PathBuf {
        let p = tmp.join(app).join(date).join(name);
        std::fs::create_dir_all(p.parent().unwrap()).unwrap();
        std::fs::write(&p, b"data").unwrap();
        p
    }

    #[test]
    fn keys_are_relative_and_prefix_is_normalised() {
        for (prefix, expect) in [
            (None, "web/2024-01-01/a.parquet"),
            (Some(""), "web/2024-01-01/a.parquet"),
            (Some("logs"), "logs/web/2024-01-01/a.parquet"),
            (Some("logs/"), "logs/web/2024-01-01/a.parquet"),
            (Some("/a/b/"), "a/b/web/2024-01-01/a.parquet"),
        ] {
            let (s, _, tmp) = setup("keys", prefix, false);
            let p = tmp.join("web/2024-01-01/a.parquet");
            assert_eq!(
                s.targets[0].key_for(&tmp, &p).unwrap(),
                expect,
                "{prefix:?}"
            );
        }
        let (s, _, tmp) = setup("keys-out", None, false);
        assert!(
            s.targets[0]
                .key_for(&tmp, Path::new("/elsewhere/x.parquet"))
                .is_err()
        );
    }

    #[tokio::test]
    async fn uploads_each_chunk_once_and_marks_it() {
        let (s, store, tmp) = setup("once", Some("p"), false);
        let a = chunk(&tmp, "web", "2024-01-01", "1-1.parquet");
        chunk(&tmp, "web", "2024-01-02", "2-1.parquet");
        assert_eq!(s.sync_app(&tmp.join("web")).await, 0);
        assert_eq!(store.puts.lock().unwrap().len(), 2);
        assert!(a.exists() && marker_path(&a).exists());

        assert_eq!(s.sync_app(&tmp.join("web")).await, 0);
        assert_eq!(store.puts.lock().unwrap().len(), 2, "no re-upload");

        chunk(&tmp, "web", "2024-01-02", "2-2.parquet");
        s.sync_app(&tmp.join("web")).await;
        assert_eq!(
            store.puts.lock().unwrap().last().unwrap(),
            "p/web/2024-01-02/2-2.parquet"
        );
        assert_eq!(store.puts.lock().unwrap().len(), 3);
    }

    #[tokio::test]
    async fn local_copy_is_kept_unless_delete_after_upload() {
        let (keep, _, tmp) = setup("keep", None, false);
        let a = chunk(&tmp, "web", "2024-01-01", "1-1.parquet");
        keep.sync_app(&tmp.join("web")).await;
        assert!(a.exists());

        let (del, _, tmp) = setup("del", None, true);
        let a = chunk(&tmp, "web", "2024-01-01", "1-1.parquet");
        del.sync_app(&tmp.join("web")).await;
        assert!(!a.exists());
        assert!(!marker_path(&a).exists());
    }

    #[tokio::test]
    async fn failed_upload_keeps_chunk_unmarked_and_is_retried() {
        for delete in [false, true] {
            let (s, store, tmp) = setup("retry", None, delete);
            let bad = chunk(&tmp, "web", "2024-01-01", "1-1.parquet");
            chunk(&tmp, "web", "2024-01-01", "1-2.parquet");
            store
                .fail
                .lock()
                .unwrap()
                .insert("web/2024-01-01/1-1.parquet".into());

            assert_eq!(
                s.sync_app(&tmp.join("web")).await,
                1,
                "good chunk still uploaded"
            );
            assert!(bad.exists() && !marker_path(&bad).exists());
            assert_eq!(store.puts.lock().unwrap().len(), 1);

            store.fail.lock().unwrap().clear();
            assert_eq!(s.sync_app(&tmp.join("web")).await, 0);
            assert_eq!(store.puts.lock().unwrap().len(), 2);
            assert_eq!(bad.exists(), !delete);
        }
    }

    #[tokio::test]
    async fn ignores_tmp_markers_index_and_stray_files() {
        let (s, store, tmp) = setup("ignore", None, false);
        chunk(&tmp, "web", "2024-01-01", "1-1.parquet.tmp");
        chunk(&tmp, "web", "2024-01-01", "1-2.parquet.synced");
        chunk(&tmp, "web", "2024-01-01", "notes.txt");
        chunk(&tmp, "web", "index", "meta.json");
        chunk(&tmp, "web", "index", "x.parquet");
        chunk(&tmp, "web", "2024-13-99", "y.parquet");
        std::fs::write(tmp.join("web/stray.parquet"), b"x").unwrap();
        assert_eq!(s.sync_app(&tmp.join("web")).await, 0);
        assert!(store.puts.lock().unwrap().is_empty());
        assert_eq!(s.sync_app(&tmp.join("missing")).await, 0);
    }

    #[tokio::test]
    async fn chunk_outside_data_path_is_an_error() {
        let (s, store, tmp) = setup("outside", None, true);
        let other = std::env::temp_dir().join(format!("lh-c-other-{}", std::process::id()));
        std::fs::create_dir_all(&other).unwrap();
        let f = other.join("x.parquet");
        std::fs::write(&f, b"x").unwrap();
        assert!(s.upload_chunk(&f).await.is_err());
        assert!(f.exists(), "never deleted when the upload did not happen");
        assert!(store.puts.lock().unwrap().is_empty());
        let _ = std::fs::remove_dir_all(&other);
        let _ = std::fs::remove_dir_all(&tmp);
    }

    fn setup_two(
        name: &str,
        delete: bool,
    ) -> (Arc<CloudSyncer>, Arc<MockStore>, Arc<MockStore>, PathBuf) {
        let tmp = std::env::temp_dir().join(format!("lh-c2-{}-{}", name, std::process::id()));
        let _ = std::fs::remove_dir_all(&tmp);
        std::fs::create_dir_all(&tmp).unwrap();
        let (first, second) = (
            Arc::new(MockStore::default()),
            Arc::new(MockStore::default()),
        );
        let syncer = CloudSyncer::with_targets(
            vec![
                Target::new("s3", Box::new(first.clone()), Some("one".into())),
                Target::new("r2", Box::new(second.clone()), Some("two".into())),
            ],
            tmp.clone(),
            delete,
        );
        (syncer, first, second, tmp)
    }

    #[tokio::test]
    async fn every_target_gets_the_chunk_under_its_own_prefix() {
        let (s, first, second, tmp) = setup_two("both", false);
        let a = chunk(&tmp, "web", "2024-01-01", "1-1.parquet");
        assert_eq!(s.sync_app(&tmp.join("web")).await, 0);
        assert_eq!(
            *first.puts.lock().unwrap(),
            ["one/web/2024-01-01/1-1.parquet"]
        );
        assert_eq!(
            *second.puts.lock().unwrap(),
            ["two/web/2024-01-01/1-1.parquet"]
        );
        assert!(a.exists() && marker_path(&a).exists());
        assert!(!partial_marker(&a, "s3").exists() && !partial_marker(&a, "r2").exists());
        s.sync_app(&tmp.join("web")).await;
        assert_eq!(first.puts.lock().unwrap().len(), 1, "no re-upload");
    }

    #[tokio::test]
    async fn a_failing_target_does_not_stop_the_other_and_only_it_is_retried() {
        for delete in [false, true] {
            let (s, first, second, tmp) = setup_two("one-down", delete);
            let a = chunk(&tmp, "web", "2024-01-01", "1-1.parquet");
            second
                .fail_all
                .store(true, std::sync::atomic::Ordering::SeqCst);

            assert_eq!(s.sync_app(&tmp.join("web")).await, 1);
            assert_eq!(
                first.puts.lock().unwrap().len(),
                1,
                "the healthy target has it"
            );
            assert!(a.exists(), "kept until every target has it");
            assert!(!marker_path(&a).exists(), "not synced yet");
            assert!(partial_marker(&a, "s3").exists());

            second
                .fail_all
                .store(false, std::sync::atomic::Ordering::SeqCst);
            assert_eq!(s.sync_app(&tmp.join("web")).await, 0);
            assert_eq!(first.puts.lock().unwrap().len(), 1, "not uploaded again");
            assert_eq!(second.puts.lock().unwrap().len(), 1);
            assert_eq!(a.exists(), !delete);
            assert_eq!(marker_path(&a).exists(), !delete);
            assert!(!partial_marker(&a, "s3").exists());
        }
    }

    #[tokio::test]
    async fn the_first_target_failing_is_retried_while_the_second_keeps_its_copy() {
        let (s, first, second, tmp) = setup_two("first-down", false);
        let a = chunk(&tmp, "web", "2024-01-01", "1-1.parquet");
        first
            .fail_all
            .store(true, std::sync::atomic::Ordering::SeqCst);
        assert_eq!(s.sync_app(&tmp.join("web")).await, 1);
        assert_eq!(second.puts.lock().unwrap().len(), 1);
        assert!(!marker_path(&a).exists());

        first
            .fail_all
            .store(false, std::sync::atomic::Ordering::SeqCst);
        assert_eq!(s.sync_app(&tmp.join("web")).await, 0);
        assert_eq!(second.puts.lock().unwrap().len(), 1, "not uploaded again");
        assert!(marker_path(&a).exists());
    }

    #[tokio::test]
    async fn encrypted_uploads_carry_ciphertext_under_an_enc_key_and_leave_the_local_chunk() {
        let tmp = std::env::temp_dir().join(format!("lh-enc-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&tmp);
        std::fs::create_dir_all(&tmp).unwrap();
        let seen = Arc::new(CaptureStore::default());
        let enc = {
            let _g = crate::storage::crypt::env_lock();
            crate::storage::crypt::Encryptor::current(&tmp.join("keys")).unwrap()
        };
        let s = CloudSyncer::with_encryption(
            vec![Target::new("s3", Box::new(seen.clone()), Some("p".into()))],
            tmp.clone(),
            false,
            enc,
        );
        let a = chunk(&tmp, "web", "2024-01-01", "1-1.parquet");
        std::fs::write(&a, b"PAR1 secret rows").unwrap();
        assert_eq!(s.sync_app(&tmp.join("web")).await, 0);

        let (key, body) = seen.0.lock().unwrap()[0].clone();
        assert_eq!(key, "p/web/2024-01-01/1-1.parquet.enc");
        assert!(!body.windows(6).any(|w| w == b"secret"));
        let keys = {
            let _g = crate::storage::crypt::env_lock();
            crate::storage::crypt::Encryptor::all(&tmp.join("keys")).unwrap()
        };
        let name = "web/2024-01-01/1-1.parquet";
        assert_eq!(
            crate::storage::crypt::decrypt_with(&keys, name, &body).unwrap(),
            b"PAR1 secret rows"
        );
        assert!(
            crate::storage::crypt::decrypt_with(&keys, "web/2024-01-01/1-2.parquet", &body)
                .is_err(),
            "bound to the chunk's name, not the prefix or store"
        );
        assert_eq!(
            std::fs::read(&a).unwrap(),
            b"PAR1 secret rows",
            "local copy stays plain"
        );
        assert!(marker_path(&a).exists());
        let left: Vec<_> = std::fs::read_dir(a.parent().unwrap())
            .unwrap()
            .flatten()
            .collect();
        assert!(
            left.iter()
                .all(|f| !f.file_name().to_string_lossy().contains(".enc"))
        );
    }

    #[derive(Default)]
    struct CaptureStore(Mutex<Vec<(String, Vec<u8>)>>);

    impl ObjectStore for Arc<CaptureStore> {
        fn put<'a>(&'a self, key: &'a str, file: &'a Path) -> BoxFuture<'a, Result<()>> {
            Box::pin(async move {
                self.0
                    .lock()
                    .unwrap()
                    .push((key.to_string(), std::fs::read(file)?));
                Ok(())
            })
        }
    }

    #[tokio::test]
    async fn the_temporary_encrypted_file_is_removed_after_a_failed_upload_too() {
        let tmp = std::env::temp_dir().join(format!("lh-enc2-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&tmp);
        std::fs::create_dir_all(&tmp).unwrap();
        let store = Arc::new(MockStore::default());
        store
            .fail_all
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let enc = {
            let _g = crate::storage::crypt::env_lock();
            crate::storage::crypt::Encryptor::current(&tmp.join("keys")).unwrap()
        };
        let s = CloudSyncer::with_encryption(
            vec![Target::new("s3", Box::new(store), None)],
            tmp.clone(),
            false,
            enc,
        );
        let a = chunk(&tmp, "web", "2024-01-01", "1-1.parquet");
        assert_eq!(s.sync_app(&tmp.join("web")).await, 1);
        assert!(!marker_path(&a).exists());
        assert_eq!(std::fs::read_dir(a.parent().unwrap()).unwrap().count(), 1);
    }

    #[tokio::test]
    async fn a_stale_encrypted_copy_is_replaced_and_the_temp_file_is_private_and_gone_after() {
        let tmp = std::env::temp_dir().join(format!("lh-enc3-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&tmp);
        std::fs::create_dir_all(&tmp).unwrap();
        let seen = Arc::new(CaptureStore::default());
        let enc = {
            let _g = crate::storage::crypt::env_lock();
            crate::storage::crypt::Encryptor::current(&tmp.join("keys")).unwrap()
        };
        let s = CloudSyncer::with_encryption(
            vec![Target::new("s3", Box::new(seen.clone()), None)],
            tmp.clone(),
            false,
            enc,
        );
        let a = chunk(&tmp, "web", "2024-01-01", "1-1.parquet");
        // left by a crash, and a symlink planted where the copy will be written
        let stale = a.with_extension("parquet.enc.tmp");
        std::fs::write(&stale, b"old").unwrap();
        assert_eq!(s.sync_app(&tmp.join("web")).await, 0);
        assert!(!stale.exists());
        assert_eq!(seen.0.lock().unwrap().len(), 1);

        #[cfg(unix)]
        {
            let b = chunk(&tmp, "web", "2024-01-01", "1-2.parquet");
            let victim = tmp.join("victim");
            std::fs::write(&victim, b"keep").unwrap();
            std::os::unix::fs::symlink(&victim, b.with_extension("parquet.enc.tmp")).unwrap();
            assert_eq!(s.sync_app(&tmp.join("web")).await, 0);
            assert_eq!(
                std::fs::read(&victim).unwrap(),
                b"keep",
                "a planted link is not written through"
            );
        }
    }

    #[tokio::test]
    async fn the_temp_file_is_removed_when_the_upload_future_is_dropped_mid_put() {
        struct Hang;
        impl ObjectStore for Hang {
            fn put<'a>(&'a self, _k: &'a str, _f: &'a Path) -> BoxFuture<'a, Result<()>> {
                Box::pin(std::future::pending())
            }
        }
        let tmp = std::env::temp_dir().join(format!("lh-enc4-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&tmp);
        std::fs::create_dir_all(&tmp).unwrap();
        let enc = {
            let _g = crate::storage::crypt::env_lock();
            crate::storage::crypt::Encryptor::current(&tmp.join("keys")).unwrap()
        };
        let s = CloudSyncer::with_encryption(
            vec![Target::new("s3", Box::new(Hang), None)],
            tmp.clone(),
            false,
            enc,
        );
        let a = chunk(&tmp, "web", "2024-01-01", "1-1.parquet");
        let timed_out =
            tokio::time::timeout(std::time::Duration::from_millis(200), s.upload_chunk(&a)).await;
        assert!(timed_out.is_err());
        assert!(!a.with_extension("parquet.enc.tmp").exists());
    }

    /// What a fake store server saw.
    #[derive(Clone, Debug)]
    struct Seen {
        method: String,
        path: String,
        query: String,
        host: String,
        headers: Vec<(String, String)>,
        body: usize,
    }

    async fn fake_store() -> (String, Arc<Mutex<Vec<Seen>>>) {
        use axum::{Router, body::Bytes, extract::Request};
        let seen = Arc::new(Mutex::new(Vec::new()));
        let log = seen.clone();
        let app = Router::new().fallback(move |req: Request| {
            let log = log.clone();
            async move {
                let (parts, body) = req.into_parts();
                let bytes: Bytes = axum::body::to_bytes(body, usize::MAX).await.unwrap();
                log.lock().unwrap().push(Seen {
                    method: parts.method.to_string(),
                    path: parts.uri.path().to_string(),
                    query: parts.uri.query().unwrap_or("").to_string(),
                    host: parts
                        .headers
                        .get("host")
                        .and_then(|h| h.to_str().ok())
                        .unwrap_or("")
                        .to_string(),
                    headers: parts
                        .headers
                        .iter()
                        .map(|(k, v)| (k.to_string(), v.to_str().unwrap_or("").to_string()))
                        .collect(),
                    body: bytes.len(),
                });
                // the three answers an S3 multipart upload needs; anything else is a plain 200
                let (method, query) = (parts.method.as_str(), parts.uri.query().unwrap_or(""));
                let body = if method == "POST" && query.contains("uploads") {
                    "<InitiateMultipartUploadResult><UploadId>u1</UploadId></InitiateMultipartUploadResult>"
                } else if method == "POST" && query.contains("uploadId") {
                    "<CompleteMultipartUploadResult><ETag>\"done\"</ETag></CompleteMultipartUploadResult>"
                } else {
                    ""
                };
                axum::http::Response::builder()
                    .status(200)
                    .header("etag", "\"abc\"")
                    .body(axum::body::Body::from(body))
                    .unwrap()
            }
        });
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap().to_string();
        tokio::spawn(async move { axum::serve(l, app).await.unwrap() });
        (addr, seen)
    }

    fn from_toml(toml: &str) -> crate::config::StorageTarget {
        let cfg: crate::config::Config =
            toml::from_str(&format!("[[storage.targets]]\n{toml}")).unwrap();
        cfg.storage.targets.into_iter().next().unwrap()
    }

    async fn upload_one(t: &crate::config::StorageTarget, tmp: &Path) -> Result<()> {
        // the client reads the environment when it is built, and only then
        let target = {
            let _g = crate::storage::env_guard();
            Target::from_config(t).unwrap()
        };
        let file = tmp.join("web/2024-01-01/1-1.parquet");
        std::fs::create_dir_all(file.parent().unwrap()).unwrap();
        std::fs::write(&file, vec![7u8; 1234]).unwrap();
        let key = target.key_for(tmp, &file).unwrap();
        target.store.put(&key, &file).await
    }

    #[tokio::test]
    async fn s3_path_style_puts_the_object_under_the_bucket_with_a_signed_request() {
        let tmp = tempfile::tempdir().unwrap();
        let (addr, seen) = fake_store().await;
        let t = from_toml(&format!(
            "name = \"b2\"\ntype = \"s3\"\nbucket = \"logs\"\nprefix = \"p\"\nendpoint = \"http://{addr}\"\npath_style = true\naccess_key = \"k\"\nsecret_key = \"s\"\n"
        ));
        upload_one(&t, tmp.path()).await.unwrap();
        let s = seen.lock().unwrap()[0].clone();
        assert_eq!(
            (s.method.as_str(), s.path.as_str()),
            ("PUT", "/logs/p/web/2024-01-01/1-1.parquet")
        );
        assert_eq!(s.body, 1234);
        assert!(
            s.headers
                .iter()
                .any(|(k, v)| k == "authorization" && v.starts_with("AWS4-HMAC-SHA256"))
        );
    }

    #[test]
    fn s3_virtual_hosted_style_needs_a_host_name_and_path_style_is_the_way_for_an_address() {
        let t = from_toml(
            "name = \"w\"\ntype = \"s3\"\nbucket = \"logs\"\nendpoint = \"https://s3.example.com\"\naccess_key = \"k\"\nsecret_key = \"s\"\n",
        );
        let _g = crate::storage::env_guard();
        assert!(Target::from_config(&t).is_ok());
        let t = from_toml(
            "name = \"w\"\ntype = \"s3\"\nbucket = \"logs\"\nendpoint = \"http://192.168.1.5:9000\"\naccess_key = \"k\"\nsecret_key = \"s\"\n",
        );
        let e = Target::from_config(&t).err().unwrap().to_string();
        assert!(e.contains("path_style = true") && e.contains("'w'"), "{e}");
    }

    #[tokio::test]
    async fn azure_puts_a_block_blob_with_the_shared_key_or_the_sas_token() {
        let tmp = tempfile::tempdir().unwrap();
        let (addr, seen) = fake_store().await;
        let key = "c2VjcmV0LWtleS1mb3ItdGVzdHM=";
        let t = from_toml(&format!(
            "name = \"az\"\ntype = \"azure\"\naccount = \"acct\"\ncontainer = \"logs\"\nendpoint = \"http://{addr}\"\naccess_key = \"{key}\"\n"
        ));
        upload_one(&t, tmp.path()).await.unwrap();
        let s = seen.lock().unwrap().last().unwrap().clone();
        assert_eq!(s.method, "PUT");
        assert!(
            s.path.ends_with("/logs/web/2024-01-01/1-1.parquet"),
            "{}",
            s.path
        );
        assert!(
            s.headers
                .iter()
                .any(|(k, v)| k == "x-ms-blob-type" && v == "BlockBlob")
        );
        assert!(
            s.headers
                .iter()
                .any(|(k, v)| k == "authorization" && v.starts_with("SharedKey acct:"))
        );

        let t = from_toml(&format!(
            "name = \"az2\"\ntype = \"azure\"\naccount = \"acct\"\ncontainer = \"logs\"\nendpoint = \"http://{addr}\"\nsas_token = \"?sv=2022&sig=abc\"\n"
        ));
        upload_one(&t, tmp.path()).await.unwrap();
        let log = seen.lock().unwrap();
        let s = log.last().unwrap();
        assert!(
            s.headers.iter().all(|(k, _)| k != "authorization"),
            "a SAS token rides in the query"
        );
    }

    #[tokio::test]
    async fn a_failing_store_gives_an_error_without_the_query_string() {
        let tmp = tempfile::tempdir().unwrap();
        let t = from_toml(
            "name = \"az\"\ntype = \"azure\"\naccount = \"acct\"\ncontainer = \"logs\"\nendpoint = \"http://127.0.0.1:1\"\nsas_token = \"sv=1&sig=TOPSECRETSIG\"\n",
        );
        let e = upload_one(&t, tmp.path()).await.unwrap_err().to_string();
        assert!(
            e.contains("upload to cloud failed") && !e.contains("TOPSECRETSIG"),
            "{e}"
        );
    }

    #[tokio::test]
    async fn a_portal_made_sas_token_keeps_its_percent_encoding_on_the_wire() {
        let tmp = tempfile::tempdir().unwrap();
        let (addr, seen) = fake_store().await;
        let t = from_toml(&format!(
            "name = \"az\"\ntype = \"azure\"\naccount = \"acct\"\ncontainer = \"logs\"\nendpoint = \"http://{addr}\"\nsas_token = \"?sv=2022-11-02&se=2030-01-01T00%3A00%3A00Z&sig=abc%2Bdef%3D\"\n"
        ));
        upload_one(&t, tmp.path()).await.unwrap();
        let q = seen.lock().unwrap().last().unwrap().query.clone();
        assert!(q.contains("sig=abc%2Bdef%3D"), "{q}");
        assert!(q.contains("se=2030-01-01T00%3A00%3A00Z"), "{q}");
        assert!(!q.contains("%25"), "encoded twice: {q}");
    }

    #[tokio::test]
    async fn explicit_settings_are_not_changed_by_stray_environment_variables() {
        let tmp = tempfile::tempdir().unwrap();
        let (addr, seen) = fake_store().await;
        let t = from_toml(&format!(
            "name = \"b2\"\ntype = \"s3\"\nbucket = \"logs\"\nendpoint = \"http://{addr}\"\npath_style = true\naccess_key = \"k\"\nsecret_key = \"s\"\n"
        ));
        let target = {
            let _g = crate::storage::env_guard();
            unsafe {
                std::env::set_var("AWS_ENDPOINT_URL_S3", "http://127.0.0.1:1");
                std::env::set_var("AWS_ENDPOINT", "http://127.0.0.1:1");
                std::env::set_var("AWS_SESSION_TOKEN", "stale-token");
            }
            let built = Target::from_config(&t).unwrap();
            unsafe {
                std::env::remove_var("AWS_ENDPOINT_URL_S3");
                std::env::remove_var("AWS_ENDPOINT");
                std::env::remove_var("AWS_SESSION_TOKEN");
            }
            built
        };
        let file = tmp.path().join("web/2024-01-01/1-1.parquet");
        std::fs::create_dir_all(file.parent().unwrap()).unwrap();
        std::fs::write(&file, b"rows").unwrap();
        target.store.put("web/x.parquet", &file).await.unwrap();
        let s = seen.lock().unwrap().last().unwrap().clone();
        assert_eq!(s.path, "/logs/web/x.parquet");
        assert!(
            s.headers.iter().all(|(k, _)| k != "x-amz-security-token"),
            "a stale session token was signed in"
        );
    }

    #[tokio::test]
    async fn a_chunk_above_the_threshold_goes_up_in_parts_and_arrives_whole() {
        use object_store::ObjectStoreExt;
        let mem = Arc::new(object_store::memory::InMemory::new());
        let store = CrateStore(mem.clone());
        let tmp = tempfile::tempdir().unwrap();
        let big = tmp.path().join("big.parquet");
        let len = MULTIPART_THRESHOLD as usize * 2 + 12_345;
        let data: Vec<u8> = (0..len).map(|i| (i * 31 % 251) as u8).collect();
        std::fs::write(&big, &data).unwrap();
        store.put("p/big.parquet", &big).await.unwrap();
        let got = mem
            .get(&object_store::path::Path::from("p/big.parquet"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(got.len(), len);
        assert!(got.as_ref() == data.as_slice(), "content differs");

        let small = tmp.path().join("small.parquet");
        std::fs::write(&small, b"tiny").unwrap();
        store.put("p/small.parquet", &small).await.unwrap();
        let got = mem
            .get(&object_store::path::Path::from("p/small.parquet"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(got.as_ref(), b"tiny");
    }

    #[test]
    fn a_big_upload_is_given_more_time_than_a_small_one() {
        let small = upload_budget(1024);
        let big = upload_budget(512 * 1024 * 1024);
        assert_eq!(small, PUT_TIMEOUT);
        assert!(
            big >= PUT_TIMEOUT + std::time::Duration::from_secs(2000),
            "{big:?}"
        );
    }

    #[tokio::test]
    async fn a_big_chunk_to_an_s3_store_is_sent_as_a_multipart_upload_and_a_small_one_as_one_put() {
        let tmp = tempfile::tempdir().unwrap();
        let (addr, seen) = fake_store().await;
        let t = from_toml(&format!(
            "name = \"b2\"\ntype = \"s3\"\nbucket = \"logs\"\nendpoint = \"http://{addr}\"\npath_style = true\naccess_key = \"k\"\nsecret_key = \"s\"\n"
        ));
        let target = {
            let _g = crate::storage::env_guard();
            Target::from_config(&t).unwrap()
        };
        let small = tmp.path().join("small.parquet");
        std::fs::write(&small, vec![1u8; 1000]).unwrap();
        target.store.put("web/small.parquet", &small).await.unwrap();
        {
            let log = seen.lock().unwrap();
            assert_eq!(log.len(), 1);
            assert_eq!((log[0].method.as_str(), log[0].body), ("PUT", 1000));
        }
        seen.lock().unwrap().clear();

        let big = tmp.path().join("big.parquet");
        let len = MULTIPART_THRESHOLD as usize * 2 + 100;
        std::fs::write(&big, vec![2u8; len]).unwrap();
        target.store.put("web/big.parquet", &big).await.unwrap();
        let log = seen.lock().unwrap();
        let parts: Vec<_> = log
            .iter()
            .filter(|s| s.query.contains("partNumber"))
            .collect();
        assert_eq!(parts.len(), 3, "{log:#?}");
        assert_eq!(parts.iter().map(|p| p.body).sum::<usize>(), len);
        assert!(parts.iter().all(|p| p.body <= PART_SIZE));
        assert!(
            log.iter()
                .any(|s| s.method == "POST" && s.query.contains("uploads"))
        );
        assert!(
            log.iter()
                .any(|s| s.method == "POST" && s.query.contains("uploadId"))
        );
        assert!(
            log.iter()
                .all(|s| !(s.method == "PUT" && !s.query.contains("partNumber"))),
            "no single PUT of the whole chunk"
        );
    }
}
