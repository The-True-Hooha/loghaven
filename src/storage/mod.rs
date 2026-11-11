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
use cloud::{CloudSyncer, S3Settings, Target};
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
    Ok(Target::s3_compatible(s3_settings(config, kind)?))
}

/// `s3`, `r2` and `minio` are three ways to fill in the same adapter.
fn s3_settings(config: &Config, kind: &str) -> Result<S3Settings> {
    let missing = |section: &str| {
        LogHavenError::Config(format!(
            "{kind} is used but there is no [storage.{section}] config"
        ))
    };
    match kind {
        "s3" => {
            let s3 = config.storage.s3.as_ref().ok_or_else(|| missing("s3"))?;
            Ok(S3Settings {
                label: "s3".into(),
                bucket: s3.bucket.clone(),
                region: s3.region.clone(),
                prefix: s3.prefix.clone(),
                endpoint: s3.endpoint.clone(),
                path_style: s3.path_style,
                access_key: required_key(&s3.access_key, "s3.access_key")?,
                secret_key: required_key(&s3.secret_key, "s3.secret_key")?,
            })
        }
        "r2" => {
            let r2 = config.storage.r2.as_ref().ok_or_else(|| missing("r2"))?;
            Ok(S3Settings {
                label: "r2".into(),
                bucket: r2.bucket.clone(),
                region: "auto".into(),
                prefix: r2.prefix.clone(),
                endpoint: Some(format!(
                    "https://{}.r2.cloudflarestorage.com",
                    r2.account_id
                )),
                path_style: false,
                access_key: required_key(&r2.access_key, "r2.access_key")?,
                secret_key: required_key(&r2.secret_key, "r2.secret_key")?,
            })
        }
        "minio" => {
            let m = config
                .storage
                .minio
                .as_ref()
                .ok_or_else(|| missing("minio"))?;
            Ok(S3Settings {
                label: "minio".into(),
                bucket: m.bucket.clone(),
                region: m.region.clone().unwrap_or_else(|| "us-east-1".into()),
                prefix: m.prefix.clone(),
                endpoint: Some(m.endpoint.clone()),
                path_style: m.path_style.unwrap_or(true),
                access_key: m.access_key.clone(),
                secret_key: m.secret_key.clone(),
            })
        }
        other => Err(LogHavenError::Config(format!("unknown store: {other}"))),
    }
}

/// The first of S3, R2 and MinIO that has credentials.
fn auto_detect_target(config: &Config) -> Option<Target> {
    let has_keys = |a: &Option<String>, b: &Option<String>| a.is_some() && b.is_some();
    let kind = if config
        .storage
        .s3
        .as_ref()
        .is_some_and(|s| has_keys(&s.access_key, &s.secret_key))
    {
        "s3"
    } else if config
        .storage
        .r2
        .as_ref()
        .is_some_and(|r| has_keys(&r.access_key, &r.secret_key))
    {
        "r2"
    } else if config
        .storage
        .minio
        .as_ref()
        .is_some_and(|m| !m.access_key.is_empty() && !m.secret_key.is_empty())
    {
        "minio"
    } else {
        return None;
    };
    required_target(config, kind).ok()
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
            endpoint: None,
            path_style: false,
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
                region: None,
                prefix: None,
                path_style: None,
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
            region: None,
            prefix: None,
            path_style: None,
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

    #[test]
    fn the_three_store_kinds_are_the_same_adapter_with_different_settings() {
        let mut cfg = two_stores("s3", None);
        cfg.storage.r2 = Some(crate::config::R2StorageConfig {
            account_id: "acct".into(),
            bucket: "rb".into(),
            prefix: Some("p".into()),
            access_key: Some("k".into()),
            secret_key: Some("s".into()),
        });

        let aws = s3_settings(&cfg, "s3").unwrap();
        assert_eq!(
            (aws.label.as_str(), aws.endpoint.as_deref(), aws.path_style),
            ("s3", None, false)
        );

        let r2 = s3_settings(&cfg, "r2").unwrap();
        assert_eq!(r2.label, "r2");
        assert_eq!(
            r2.endpoint.as_deref(),
            Some("https://acct.r2.cloudflarestorage.com")
        );
        assert_eq!(
            (r2.region.as_str(), r2.path_style, r2.prefix.as_deref()),
            ("auto", false, Some("p"))
        );

        let minio = s3_settings(&cfg, "minio").unwrap();
        assert_eq!(minio.label, "minio");
        assert_eq!(minio.endpoint.as_deref(), Some("http://localhost:9000"));
        assert_eq!(
            (minio.region.as_str(), minio.path_style),
            ("us-east-1", true)
        );
        assert!(s3_settings(&cfg, "gcs").is_err());
    }

    #[test]
    fn any_s3_compatible_service_is_an_s3_section_with_an_endpoint() {
        let mut cfg = two_stores("s3", None);
        let s3 = cfg.storage.s3.as_mut().unwrap();
        s3.endpoint = Some("https://s3.us-west-004.backblazeb2.com".into());
        s3.region = "us-west-004".into();
        s3.path_style = true;
        let got = s3_settings(&cfg, "s3").unwrap();
        assert_eq!(
            got.endpoint.as_deref(),
            Some("https://s3.us-west-004.backblazeb2.com")
        );
        assert_eq!((got.region.as_str(), got.path_style), ("us-west-004", true));
        assert_eq!(labels(&cfg).unwrap(), ["s3"]);
    }

    #[test]
    fn minio_settings_can_be_overridden() {
        let mut cfg = two_stores("minio", None);
        let m = cfg.storage.minio.as_mut().unwrap();
        m.region = Some("eu-west-1".into());
        m.path_style = Some(false);
        m.prefix = Some("logs".into());
        let got = s3_settings(&cfg, "minio").unwrap();
        assert_eq!(
            (got.region.as_str(), got.path_style, got.prefix.as_deref()),
            ("eu-west-1", false, Some("logs"))
        );
    }

    #[test]
    fn an_s3_section_from_a_config_file_without_the_new_keys_still_parses() {
        let cfg: Config = toml::from_str(
            "[storage]\nbackend = \"s3\"\n[storage.s3]\nbucket = \"b\"\nregion = \"us-east-1\"\naccess_key = \"k\"\nsecret_key = \"s\"\n[storage.minio]\nendpoint = \"http://h:9000\"\nbucket = \"b\"\naccess_key = \"k\"\nsecret_key = \"s\"\n",
        )
        .unwrap();
        let s3 = cfg.storage.s3.unwrap();
        assert!(s3.endpoint.is_none() && !s3.path_style);
        assert!(cfg.storage.minio.unwrap().path_style.is_none());
    }

    #[test]
    fn a_bad_endpoint_is_a_config_error_that_never_prints_the_url() {
        for bad in [
            "",
            "localhost:9000",
            "ftp://user:SECRETKEY@h",
            "http://",
            "https://",
        ] {
            let mut cfg = two_stores("s3", None);
            cfg.storage.s3.as_mut().unwrap().endpoint = Some(bad.into());
            let e = cfg.validate().unwrap_err().to_string();
            assert!(
                e.contains("storage.s3.endpoint") && !e.contains("SECRETKEY"),
                "{bad:?}: {e}"
            );
            let mut cfg = two_stores("minio", None);
            cfg.storage.minio.as_mut().unwrap().endpoint = bad.into();
            let e = cfg.validate().unwrap_err().to_string();
            assert!(
                e.contains("storage.minio.endpoint") && !e.contains("SECRETKEY"),
                "{bad:?}: {e}"
            );
        }
        let mut cfg = two_stores("s3", None);
        cfg.storage.s3.as_mut().unwrap().endpoint = Some("https://s3.example.com".into());
        cfg.validate().unwrap();
    }
}
