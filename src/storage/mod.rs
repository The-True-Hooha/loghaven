pub mod cloud;
mod index;
pub mod local;
pub mod pruner;
pub mod query;
pub mod record;
mod writer;

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
        for b in backends {
            let _ = b.shutdown().await;
        }
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
                true,
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
                true,
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
                true,
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
    if let Some(s3) = &config.storage.s3 {
        if let (Some(key), Some(secret)) = (&s3.access_key, &s3.secret_key) {
            return Some(CloudSyncer::new_s3(
                s3.bucket.clone(),
                s3.region.clone(),
                s3.prefix.clone(),
                key.clone(),
                secret.clone(),
                data_path,
                true,
            ));
        }
    }

    // R2 keys present?
    if let Some(r2) = &config.storage.r2 {
        if let (Some(key), Some(secret)) = (&r2.access_key, &r2.secret_key) {
            return Some(CloudSyncer::new_r2(
                r2.bucket.clone(),
                r2.account_id.clone(),
                r2.prefix.clone(),
                key.clone(),
                secret.clone(),
                data_path,
                true,
            ));
        }
    }

    // MinIO configured?
    if let Some(m) = &config.storage.minio {
        if !m.access_key.is_empty() && !m.secret_key.is_empty() {
            return Some(CloudSyncer::new_minio(
                m.bucket.clone(),
                m.endpoint.clone(),
                m.access_key.clone(),
                m.secret_key.clone(),
                data_path,
                true,
            ));
        }
    }

    // No cloud config found — fall back to local only
    None
}

fn required_key(val: &Option<String>, field: &str) -> Result<String> {
    val.clone()
        .ok_or_else(|| LogHavenError::Config(format!("missing required field: {}", field)))
}
