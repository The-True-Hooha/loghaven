mod app_name;
pub mod cloud;
pub mod date;
mod index;
pub mod local;
pub mod pruner;
pub mod query;
pub mod record;
pub(crate) mod writer;

pub use app_name::validate_app_name;
pub use local::LocalBackend;
pub use record::LogRecord;

use crate::config::Config;
use crate::error::{LogHavenError, Result};
use cloud::CloudSyncer;
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
    let data_path = config.storage.local.path.clone();

    match backend {
        "local" => Ok(None),

        "s3" => {
            let s3 = config.storage.s3.as_ref().ok_or_else(|| {
                LogHavenError::Config("backend=s3 but no [storage.s3] config".into())
            })?;
            let key = required_key(&s3.access_key, "s3.access_key")?;
            let secret = required_key(&s3.secret_key, "s3.secret_key")?;
            Ok(Some(CloudSyncer::new_s3(
                s3.bucket.clone(),
                s3.region.clone(),
                s3.prefix.clone(),
                key,
                secret,
                data_path,
                config.storage.delete_after_upload,
            )))
        }

        "r2" => {
            let r2 = config.storage.r2.as_ref().ok_or_else(|| {
                LogHavenError::Config("backend=r2 but no [storage.r2] config".into())
            })?;
            let key = required_key(&r2.access_key, "r2.access_key")?;
            let secret = required_key(&r2.secret_key, "r2.secret_key")?;
            Ok(Some(CloudSyncer::new_r2(
                r2.bucket.clone(),
                r2.account_id.clone(),
                r2.prefix.clone(),
                key,
                secret,
                data_path,
                config.storage.delete_after_upload,
            )))
        }

        "minio" => {
            let m = config.storage.minio.as_ref().ok_or_else(|| {
                LogHavenError::Config("backend=minio but no [storage.minio] config".into())
            })?;
            Ok(Some(CloudSyncer::new_minio(
                m.bucket.clone(),
                m.endpoint.clone(),
                m.access_key.clone(),
                m.secret_key.clone(),
                data_path,
                config.storage.delete_after_upload,
            )))
        }

        "auto" => Ok(auto_detect_syncer(config, data_path)),

        _ => Err(LogHavenError::Config(format!(
            "unknown backend: {}",
            backend
        ))),
    }
}

fn auto_detect_syncer(
    config: &Config,
    data_path: std::path::PathBuf,
) -> Option<std::sync::Arc<CloudSyncer>> {
    // S3 keys present?
    if let Some(s3) = &config.storage.s3
        && let (Some(key), Some(secret)) = (&s3.access_key, &s3.secret_key)
    {
        return Some(CloudSyncer::new_s3(
            s3.bucket.clone(),
            s3.region.clone(),
            s3.prefix.clone(),
            key.clone(),
            secret.clone(),
            data_path,
            config.storage.delete_after_upload,
        ));
    }

    // R2 keys present?
    if let Some(r2) = &config.storage.r2
        && let (Some(key), Some(secret)) = (&r2.access_key, &r2.secret_key)
    {
        return Some(CloudSyncer::new_r2(
            r2.bucket.clone(),
            r2.account_id.clone(),
            r2.prefix.clone(),
            key.clone(),
            secret.clone(),
            data_path,
            config.storage.delete_after_upload,
        ));
    }

    // MinIO configured?
    if let Some(m) = &config.storage.minio
        && !m.access_key.is_empty()
        && !m.secret_key.is_empty()
    {
        return Some(CloudSyncer::new_minio(
            m.bucket.clone(),
            m.endpoint.clone(),
            m.access_key.clone(),
            m.secret_key.clone(),
            data_path,
            config.storage.delete_after_upload,
        ));
    }

    // No cloud config found — fall back to local only
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
}
