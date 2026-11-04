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

/// Uploads finished chunks and remembers which ones are done.
///
/// A chunk `X.parquet` is synced once `X.parquet.synced` exists next to it, or
/// once it was deleted after upload. Anything else is retried on the next run.
pub struct CloudSyncer {
    store: Box<dyn ObjectStore>,
    prefix: String,
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
        Arc::new(Self {
            store,
            prefix: prefix.unwrap_or_default().trim_matches('/').to_string(),
            data_path,
            delete_after_upload,
        })
    }

    pub fn new_s3(
        bucket: String,
        region: String,
        prefix: Option<String>,
        access_key: String,
        secret_key: String,
        data_path: PathBuf,
        delete_after_upload: bool,
    ) -> Arc<Self> {
        let client = build_client(&access_key, &secret_key, region, None, false);
        Self::with_store(
            Box::new(S3Store { client, bucket }),
            prefix,
            data_path,
            delete_after_upload,
        )
    }

    pub fn new_r2(
        bucket: String,
        account_id: String,
        prefix: Option<String>,
        access_key: String,
        secret_key: String,
        data_path: PathBuf,
        delete_after_upload: bool,
    ) -> Arc<Self> {
        let endpoint = format!("https://{}.r2.cloudflarestorage.com", account_id);
        let client = build_client(
            &access_key,
            &secret_key,
            "auto".into(),
            Some(endpoint),
            false,
        );
        Self::with_store(
            Box::new(S3Store { client, bucket }),
            prefix,
            data_path,
            delete_after_upload,
        )
    }

    pub fn new_minio(
        bucket: String,
        endpoint: String,
        access_key: String,
        secret_key: String,
        data_path: PathBuf,
        delete_after_upload: bool,
    ) -> Arc<Self> {
        let client = build_client(
            &access_key,
            &secret_key,
            "us-east-1".into(),
            Some(endpoint),
            true,
        );
        Self::with_store(
            Box::new(S3Store { client, bucket }),
            None,
            data_path,
            delete_after_upload,
        )
    }

    fn key_for(&self, chunk_path: &Path) -> Result<String> {
        let rel = chunk_path
            .strip_prefix(&self.data_path)
            .map_err(|_| LogHavenError::Storage("chunk path not under data_path".into()))?;
        let rel = rel.to_string_lossy().replace('\\', "/");
        Ok(if self.prefix.is_empty() {
            rel
        } else {
            format!("{}/{}", self.prefix, rel)
        })
    }

    /// Uploads one chunk, then marks it synced (or deletes it).
    pub async fn upload_chunk(&self, chunk_path: &Path) -> Result<()> {
        let key = self.key_for(chunk_path)?;
        self.store.put(&key, chunk_path).await?;

        if self.delete_after_upload {
            std::fs::remove_file(chunk_path)?;
        } else {
            std::fs::write(marker_path(chunk_path), b"")?;
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

fn marker_path(chunk: &Path) -> PathBuf {
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
            assert_eq!(s.key_for(&p).unwrap(), expect, "{prefix:?}");
        }
        let (s, _, _) = setup("keys-out", None, false);
        assert!(s.key_for(Path::new("/elsewhere/x.parquet")).is_err());
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
}
