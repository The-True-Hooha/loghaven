mod app_name;
pub mod cloud;
pub mod crypt;
pub mod date;
pub mod local;
pub mod pruner;
pub mod query;
pub mod record;
#[cfg(test)]
mod storage_tests;
pub(crate) mod writer;

pub use app_name::validate_app_name;
pub use local::LocalBackend;
pub use record::LogRecord;

use crate::config::Config;
use crate::error::{LogHavenError, Result};
use cloud::{CloudSyncer, Target};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

pub fn create_storage(app: String, config: &Config) -> Result<LocalBackend> {
    let syncer = resolve_syncer(config)?;
    LocalBackend::new_with_syncer(app, &config.storage.local, syncer)
}

pub struct StorageRouter {
    backends: Mutex<HashMap<String, Arc<LocalBackend>>>,
    local_config: crate::config::LocalStorageConfig,
    cloud_syncer: Option<Arc<CloudSyncer>>,
}

impl StorageRouter {
    pub fn new(config: &Config) -> Result<Self> {
        let cloud_syncer = resolve_syncer(config)?;
        Ok(Self {
            backends: Mutex::new(HashMap::new()),
            local_config: config.storage.local.clone(),
            cloud_syncer,
        })
    }

    pub async fn write(&self, app: &str, record: LogRecord) -> Result<()> {
        validate_app_name(app)?;
        let backend = self.get_or_create(app)?;
        backend.write(record).await
    }

    fn get_or_create(&self, app: &str) -> Result<Arc<LocalBackend>> {
        let mut map = self.backends.lock().unwrap();
        if let Some(b) = map.get(app) {
            return Ok(Arc::clone(b));
        }
        let b = Arc::new(LocalBackend::new_with_syncer(
            app.to_string(),
            &self.local_config,
            self.cloud_syncer.clone(),
        )?);
        map.insert(app.to_string(), Arc::clone(&b));
        Ok(b)
    }

    pub fn has_cloud_sync(&self) -> bool {
        self.cloud_syncer.is_some()
    }

    /// Writes every app's buffered records to disk.
    pub async fn flush_all(&self) -> Result<()> {
        let backends: Vec<Arc<LocalBackend>> =
            { self.backends.lock().unwrap().values().cloned().collect() };
        for b in backends {
            b.flush().await?;
        }
        Ok(())
    }

    pub async fn shutdown_all(&self) -> Result<()> {
        let backends: Vec<Arc<LocalBackend>> =
            { self.backends.lock().unwrap().values().cloned().collect() };
        // concurrently, so a slow cloud sync of one app does not multiply the wait
        let mut set = tokio::task::JoinSet::new();
        for b in backends {
            set.spawn(async move {
                if let Err(e) = b.shutdown().await {
                    eprintln!("Storage shutdown error ({}): {}", b.app, e);
                }
            });
        }
        while set.join_next().await.is_some() {}
        Ok(())
    }
}

fn resolve_syncer(config: &Config) -> Result<Option<std::sync::Arc<CloudSyncer>>> {
    let backend = config.storage.backend.as_str();
    let mut targets = vec![];
    match backend {
        "local" => {}
        "s3" | "r2" | "minio" => targets.push(required_target(config, backend)?),
        "auto" => targets.extend(auto_detect_target(config)),
        _ => {
            return Err(LogHavenError::Config(format!(
                "unknown backend: {}",
                backend
            )));
        }
    }

    if let Some(second) = config.storage.secondary.as_deref() {
        if !matches!(second, "s3" | "r2" | "minio") {
            return Err(LogHavenError::Config(format!(
                "unknown secondary: {second} (use s3, r2 or minio)"
            )));
        }
        let Some(first) = targets.first() else {
            return Err(LogHavenError::Config(
                "secondary needs a cloud backend: set backend to s3, r2, minio or auto".into(),
            ));
        };
        if first.label() == second {
            return Err(LogHavenError::Config(format!(
                "secondary is the same store as the backend ({second}); pick a different one"
            )));
        }
        targets.push(required_target(config, second)?);
    }

    if targets.is_empty() {
        return Ok(None);
    }
    let (data_path, delete) = (
        config.storage.local.path.clone(),
        config.storage.delete_after_upload,
    );
    if config.storage.encrypt_uploads {
        let encryptor = crypt::Encryptor::current(&config.auth.keys_dir)?;
        return Ok(Some(CloudSyncer::with_encryption(
            targets, data_path, delete, encryptor,
        )));
    }
    Ok(Some(CloudSyncer::with_targets(targets, data_path, delete)))
}

/// The named store, which must be fully configured.
fn required_target(config: &Config, kind: &str) -> Result<Target> {
    match kind {
        "s3" => {
            let s3 = config.storage.s3.as_ref().ok_or_else(|| {
                LogHavenError::Config(format!(
                    "{kind} is used but there is no [storage.s3] config"
                ))
            })?;
            Ok(Target::s3(
                s3.bucket.clone(),
                s3.region.clone(),
                s3.prefix.clone(),
                &required_key(&s3.access_key, "s3.access_key")?,
                &required_key(&s3.secret_key, "s3.secret_key")?,
            ))
        }
        "r2" => {
            let r2 = config.storage.r2.as_ref().ok_or_else(|| {
                LogHavenError::Config(format!(
                    "{kind} is used but there is no [storage.r2] config"
                ))
            })?;
            Ok(Target::r2(
                r2.bucket.clone(),
                r2.account_id.clone(),
                r2.prefix.clone(),
                &required_key(&r2.access_key, "r2.access_key")?,
                &required_key(&r2.secret_key, "r2.secret_key")?,
            ))
        }
        _ => {
            let m = config.storage.minio.as_ref().ok_or_else(|| {
                LogHavenError::Config(format!(
                    "{kind} is used but there is no [storage.minio] config"
                ))
            })?;
            Ok(Target::minio(
                m.bucket.clone(),
                m.endpoint.clone(),
                &m.access_key,
                &m.secret_key,
            ))
        }
    }
}

/// The first of S3, R2 and MinIO that has credentials.
fn auto_detect_target(config: &Config) -> Option<Target> {
    if let Some(s3) = &config.storage.s3
        && let (Some(key), Some(secret)) = (&s3.access_key, &s3.secret_key)
    {
        return Some(Target::s3(
            s3.bucket.clone(),
            s3.region.clone(),
            s3.prefix.clone(),
            key,
            secret,
        ));
    }
    if let Some(r2) = &config.storage.r2
        && let (Some(key), Some(secret)) = (&r2.access_key, &r2.secret_key)
    {
        return Some(Target::r2(
            r2.bucket.clone(),
            r2.account_id.clone(),
            r2.prefix.clone(),
            key,
            secret,
        ));
    }
    if let Some(m) = &config.storage.minio
        && !m.access_key.is_empty()
        && !m.secret_key.is_empty()
    {
        return Some(Target::minio(
            m.bucket.clone(),
            m.endpoint.clone(),
            &m.access_key,
            &m.secret_key,
        ));
    }
    None
}

fn required_key(val: &Option<String>, field: &str) -> Result<String> {
    val.clone()
        .ok_or_else(|| LogHavenError::Config(format!("missing required field: {}", field)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn router_rejects_invalid_app_without_creating_dirs() {
        let tmp = std::env::temp_dir().join(format!("lh-r-{}", std::process::id()));
        let mut cfg = crate::config::default_config();
        cfg.storage.backend = "local".into();
        cfg.storage.local.path = tmp.clone();
        let router = StorageRouter::new(&cfg).unwrap();
        for app in ["../evil", "..", "a/b", "", "Web"] {
            let r = router
                .write(app, LogRecord::new(app, "info", "t", "m"))
                .await;
            assert!(matches!(r, Err(LogHavenError::InvalidApp(_))), "{app:?}");
        }
        assert!(!tmp.exists());
    }

    fn s3_config(delete: Option<bool>) -> Config {
        let mut cfg = crate::config::default_config();
        cfg.storage.backend = "s3".into();
        cfg.storage.s3 = Some(crate::config::S3StorageConfig {
            bucket: "b".into(),
            region: "us-east-1".into(),
            prefix: None,
            access_key: Some("k".into()),
            secret_key: Some("s".into()),
        });
        if let Some(d) = delete {
            cfg.storage.delete_after_upload = d;
        }
        cfg
    }

    #[test]
    fn delete_after_upload_defaults_off_and_is_honoured_for_every_backend() {
        assert!(!crate::config::default_config().storage.delete_after_upload);
        let parsed: Config = toml::from_str("[storage]\nbackend = \"local\"\n").unwrap();
        assert!(!parsed.storage.delete_after_upload);

        for delete in [false, true] {
            let syncer = resolve_syncer(&s3_config(Some(delete))).unwrap().unwrap();
            assert_eq!(syncer.delete_after_upload, delete);

            let mut auto = s3_config(Some(delete));
            auto.storage.backend = "auto".into();
            let syncer = resolve_syncer(&auto).unwrap().unwrap();
            assert_eq!(syncer.delete_after_upload, delete);

            let mut r2 = crate::config::default_config();
            r2.storage.backend = "r2".into();
            r2.storage.delete_after_upload = delete;
            r2.storage.r2 = Some(crate::config::R2StorageConfig {
                account_id: "a".into(),
                bucket: "b".into(),
                prefix: None,
                access_key: Some("k".into()),
                secret_key: Some("s".into()),
            });
            assert_eq!(
                resolve_syncer(&r2).unwrap().unwrap().delete_after_upload,
                delete
            );

            let mut minio = crate::config::default_config();
            minio.storage.backend = "minio".into();
            minio.storage.delete_after_upload = delete;
            minio.storage.minio = Some(crate::config::MinioStorageConfig {
                endpoint: "http://localhost:9000".into(),
                bucket: "b".into(),
                access_key: "k".into(),
                secret_key: "s".into(),
            });
            assert_eq!(
                resolve_syncer(&minio).unwrap().unwrap().delete_after_upload,
                delete
            );
        }
        let mut local = crate::config::default_config();
        local.storage.backend = "local".into();
        assert!(resolve_syncer(&local).unwrap().is_none());
    }

    fn two_stores(backend: &str, secondary: Option<&str>) -> Config {
        let mut cfg = s3_config(None);
        cfg.storage.backend = backend.into();
        cfg.storage.secondary = secondary.map(String::from);
        cfg.storage.minio = Some(crate::config::MinioStorageConfig {
            endpoint: "http://localhost:9000".into(),
            bucket: "b".into(),
            access_key: "k".into(),
            secret_key: "s".into(),
        });
        cfg
    }

    fn labels(cfg: &Config) -> Result<Vec<String>> {
        Ok(resolve_syncer(cfg)?
            .map(|s| s.target_labels())
            .unwrap_or_default())
    }

    #[test]
    fn a_secondary_adds_a_second_upload_target() {
        assert_eq!(labels(&two_stores("s3", None)).unwrap(), ["s3"]);
        assert_eq!(
            labels(&two_stores("s3", Some("minio"))).unwrap(),
            ["s3", "minio"]
        );
        assert_eq!(
            labels(&two_stores("auto", Some("minio"))).unwrap(),
            ["s3", "minio"]
        );
        assert!(labels(&two_stores("local", None)).unwrap().is_empty());
    }

    #[test]
    fn a_secondary_that_cannot_work_is_a_startup_error() {
        let err = |b: &str, s: &str| {
            labels(&two_stores(b, Some(s)))
                .err()
                .map(|e| e.to_string())
                .unwrap_or_default()
        };
        assert!(err("local", "minio").contains("needs a cloud backend"));
        assert!(err("s3", "s3").contains("same store"));
        assert!(err("auto", "s3").contains("same store"));
        assert!(err("s3", "gcs").contains("unknown secondary"));
        let mut cfg = two_stores("s3", Some("r2"));
        cfg.storage.r2 = None;
        assert!(
            labels(&cfg)
                .err()
                .unwrap()
                .to_string()
                .contains("[storage.r2]")
        );
        // an auto backend with nothing configured has no first target for a secondary to follow
        let mut cfg = two_stores("auto", Some("minio"));
        cfg.storage.s3 = None;
        cfg.storage.minio = None;
        assert!(
            labels(&cfg)
                .err()
                .unwrap()
                .to_string()
                .contains("needs a cloud backend")
        );
    }
}
