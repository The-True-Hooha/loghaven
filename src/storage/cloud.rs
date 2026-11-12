use crate::error::{LogHavenError, Result};
use aws_sdk_s3::Client;
use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
use aws_sdk_s3::primitives::ByteStream;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;

const SYNCED_SUFFIX: &str = "synced";
const PUT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(120);

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Where chunks are uploaded. Implemented for S3-compatible stores; tests use a mock.
pub trait ObjectStore: Send + Sync {
    fn put<'a>(&'a self, key: &'a str, file: &'a Path) -> BoxFuture<'a, Result<()>>;
}

struct S3Store {
    client: Client,
    bucket: String,
}

impl ObjectStore for S3Store {
    fn put<'a>(&'a self, key: &'a str, file: &'a Path) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let body = ByteStream::from_path(file)
                .await
                .map_err(|e| LogHavenError::Storage(format!("read chunk for upload: {}", e)))?;
            let send = self
                .client
                .put_object()
                .bucket(&self.bucket)
                .key(key)
                .body(body)
                .send();
            // a hung PUT would hold the app's sync lock and stall all later uploads
            tokio::time::timeout(PUT_TIMEOUT, send)
                .await
                .map_err(|_| LogHavenError::Storage("upload to cloud timed out".into()))?
                .map_err(|e| LogHavenError::Storage(format!("upload to cloud failed: {}", e)))?;
            Ok(())
        })
    }
}

/// Everything the S3 adapter needs to reach one store.
pub struct S3Settings {
    /// Names the store in partial-upload markers, so keep it stable.
    pub label: String,
    pub bucket: String,
    pub region: String,
    pub prefix: Option<String>,
    /// `None` is AWS.
    pub endpoint: Option<String>,
    pub path_style: bool,
    pub access_key: String,
    pub secret_key: String,
}

/// One upload destination. `ObjectStore` is the seam for a store that does not speak the S3 API
/// (Azure Blob Storage, Google Cloud Storage's native API, SFTP): implement it and add a
/// constructor and a config section next to `s3_compatible`.
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

    /// Any store that speaks the S3 API. AWS S3, Cloudflare R2 and MinIO are the same adapter
    /// with different settings, as are Backblaze B2, Wasabi, DigitalOcean Spaces and Ceph.
    pub fn s3_compatible(s: S3Settings) -> Self {
        let client = build_client(
            &s.access_key,
            &s.secret_key,
            s.region,
            s.endpoint,
            s.path_style,
        );
        Self::new(
            &s.label,
            Box::new(S3Store {
                client,
                bucket: s.bucket,
            }),
            s.prefix,
        )
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

fn build_client(
    access_key: &str,
    secret_key: &str,
    region: String,
    endpoint_url: Option<String>,
    force_path_style: bool,
) -> Client {
    let creds = Credentials::new(access_key, secret_key, None, None, "loghaven");
    let mut builder = aws_sdk_s3::Config::builder()
        .credentials_provider(creds)
        .region(Region::new(region))
        .behavior_version(BehaviorVersion::latest());

    if let Some(url) = endpoint_url {
        builder = builder.endpoint_url(url);
    }

    if force_path_style {
        builder = builder.force_path_style(true);
    }

    Client::from_conf(builder.build())
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
}
