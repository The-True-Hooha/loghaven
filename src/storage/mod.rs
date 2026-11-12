mod app_name;
pub mod cloud;
pub mod crypt;
pub mod date;
pub mod local;
pub mod pruner;
pub mod query;
pub mod record;
pub mod restore;
#[cfg(test)]
mod storage_tests;
mod stores;
pub(crate) use stores::host_takes_a_bucket_prefix;

/// Tests that set environment variables, or build a store client (which reads them), hold this:
/// the process environment is shared by every test thread.
#[cfg(test)]
pub(crate) fn env_guard() -> std::sync::MutexGuard<'static, ()> {
    static ENV: std::sync::Mutex<()> = std::sync::Mutex::new(());
    ENV.lock().unwrap_or_else(|e| e.into_inner())
}
pub(crate) mod writer;

pub use app_name::validate_app_name;
pub use local::LocalBackend;
pub use record::LogRecord;

use crate::config::Config;
use crate::config::{StorageTarget, TargetKind};
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

    /// Opens every app already on disk so chunks left by a failed or interrupted upload are
    /// retried now, not whenever that app next receives a record.
    pub fn resume_uploads(&self) {
        if self.cloud_syncer.is_none() {
            return;
        }
        let Ok(apps) = std::fs::read_dir(&self.local_config.path) else {
            return;
        };
        for entry in apps.flatten() {
            let name = entry.file_name().to_string_lossy().into_owned();
            if entry.path().is_dir()
                && validate_app_name(&name).is_ok()
                && let Err(e) = self.get_or_create(&name)
            {
                eprintln!("Cannot resume uploads for {name}: {e}");
            }
        }
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

/// The upload targets: `[[storage.targets]]` as written, or the older `backend`, `secondary` and
/// `[storage.s3|r2|minio]` settings expanded into the same list, named `s3`, `r2` and `minio`.
pub fn resolve_targets(config: &Config) -> Result<Vec<StorageTarget>> {
    if !config.storage.targets.is_empty() {
        // `local` (also set with LOGHAVEN_STORAGE_BACKEND) means never upload, targets or not
        if config.storage.backend == "local" {
            return Ok(vec![]);
        }
        return Ok(config.storage.targets.clone());
    }
    let backend = config.storage.backend.as_str();
    let mut targets = vec![];
    match backend {
        "local" => {}
        "s3" | "r2" | "minio" => targets.push(legacy_target(config, backend)?),
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
        if first.name == second {
            return Err(LogHavenError::Config(format!(
                "secondary is the same store as the backend ({second}); pick a different one"
            )));
        }
        targets.push(legacy_target(config, second)?);
    }
    Ok(targets)
}

fn resolve_syncer(config: &Config) -> Result<Option<std::sync::Arc<CloudSyncer>>> {
    let targets = resolve_targets(config)?
        .iter()
        .map(Target::from_config)
        .collect::<Result<Vec<_>>>()?;
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

/// One of the older fixed sections as a target. `s3`, `r2` and `minio` are the same S3-API store
/// with different settings.
fn legacy_target(config: &Config, kind: &str) -> Result<StorageTarget> {
    let missing = |section: &str| {
        LogHavenError::Config(format!(
            "{kind} is used but there is no [storage.{section}] config"
        ))
    };
    let (prefix, s3) = match kind {
        "s3" => {
            let s3 = config.storage.s3.as_ref().ok_or_else(|| missing("s3"))?;
            (
                s3.prefix.clone(),
                TargetKind::S3 {
                    bucket: s3.bucket.clone(),
                    region: Some(s3.region.clone()),
                    endpoint: s3.endpoint.clone(),
                    // an address or bare host name cannot carry the bucket in the host; the
                    // previous client addressed such a server by path on its own
                    path_style: s3.path_style
                        || s3
                            .endpoint
                            .as_deref()
                            .is_some_and(|e| !host_takes_a_bucket_prefix(e)),
                    access_key: Some(required_key(&s3.access_key, "s3.access_key")?),
                    access_key_env: None,
                    secret_key: Some(required_key(&s3.secret_key, "s3.secret_key")?),
                    secret_key_env: None,
                },
            )
        }
        "r2" => {
            let r2 = config.storage.r2.as_ref().ok_or_else(|| missing("r2"))?;
            (
                r2.prefix.clone(),
                TargetKind::S3 {
                    bucket: r2.bucket.clone(),
                    region: Some("auto".into()),
                    endpoint: Some(format!(
                        "https://{}.r2.cloudflarestorage.com",
                        r2.account_id
                    )),
                    path_style: true,
                    access_key: Some(required_key(&r2.access_key, "r2.access_key")?),
                    access_key_env: None,
                    secret_key: Some(required_key(&r2.secret_key, "r2.secret_key")?),
                    secret_key_env: None,
                },
            )
        }
        "minio" => {
            let m = config
                .storage
                .minio
                .as_ref()
                .ok_or_else(|| missing("minio"))?;
            (
                m.prefix.clone(),
                TargetKind::S3 {
                    bucket: m.bucket.clone(),
                    region: Some(m.region.clone().unwrap_or_else(|| "us-east-1".into())),
                    endpoint: Some(m.endpoint.clone()),
                    path_style: m.path_style.unwrap_or(true),
                    access_key: Some(m.access_key.clone()),
                    access_key_env: None,
                    secret_key: Some(m.secret_key.clone()),
                    secret_key_env: None,
                },
            )
        }
        other => {
            return Err(LogHavenError::Config(format!("unknown store: {other}")));
        }
    };
    Ok(StorageTarget {
        name: kind.to_string(),
        prefix,
        kind: s3,
    })
}

/// The first of S3, R2 and MinIO that has credentials.
fn auto_detect_target(config: &Config) -> Option<StorageTarget> {
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
    legacy_target(config, kind).ok()
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

    #[tokio::test]
    async fn resume_opens_the_apps_on_disk_only_when_there_is_somewhere_to_upload() {
        let tmp = tempfile::tempdir().unwrap();
        for app in ["web", "api"] {
            std::fs::create_dir_all(tmp.path().join(app)).unwrap();
        }
        std::fs::create_dir_all(tmp.path().join("Not An App")).unwrap();

        let mut cfg = s3_config(None);
        cfg.storage.local.path = tmp.path().to_path_buf();
        let router = StorageRouter::new(&cfg).unwrap();
        router.resume_uploads();
        let mut open: Vec<String> = router.backends.lock().unwrap().keys().cloned().collect();
        open.sort();
        assert_eq!(open, ["api", "web"]);

        cfg.storage.backend = "local".into();
        let router = StorageRouter::new(&cfg).unwrap();
        router.resume_uploads();
        assert!(router.backends.lock().unwrap().is_empty());
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

    /// The S3 settings of the target `name` after the older settings were expanded.
    fn s3_of(cfg: &Config, name: &str) -> (StorageTarget, String, Option<String>, bool, String) {
        let t = resolve_targets(cfg)
            .unwrap()
            .into_iter()
            .find(|t| t.name == name)
            .unwrap();
        let TargetKind::S3 {
            region,
            endpoint,
            path_style,
            bucket,
            ..
        } = t.kind.clone()
        else {
            panic!("not s3");
        };
        (t, region.unwrap(), endpoint, path_style, bucket)
    }

    #[test]
    fn the_older_settings_expand_into_the_same_target_list() {
        let mut cfg = two_stores("auto", None);
        cfg.storage.r2 = Some(crate::config::R2StorageConfig {
            account_id: "acct".into(),
            bucket: "rb".into(),
            prefix: Some("p".into()),
            access_key: Some("k".into()),
            secret_key: Some("s".into()),
        });

        let (t, region, endpoint, path_style, _) = s3_of(&cfg, "s3");
        assert_eq!(t.name, "s3");
        assert_eq!(
            (endpoint, path_style, region.as_str()),
            (None, false, "us-east-1")
        );

        let (t, region, endpoint, path_style, bucket) = s3_of(
            &{
                let mut c = cfg.clone();
                c.storage.s3 = None;
                c
            },
            "r2",
        );
        assert_eq!(
            endpoint.as_deref(),
            Some("https://acct.r2.cloudflarestorage.com")
        );
        assert_eq!(
            (region.as_str(), path_style, bucket.as_str()),
            ("auto", true, "rb")
        );
        assert_eq!(t.prefix.as_deref(), Some("p"));

        let (_, region, endpoint, path_style, _) = s3_of(
            &{
                let mut c = cfg.clone();
                c.storage.s3 = None;
                c.storage.r2 = None;
                c
            },
            "minio",
        );
        assert_eq!(endpoint.as_deref(), Some("http://localhost:9000"));
        assert_eq!((region.as_str(), path_style), ("us-east-1", true));
        assert!(legacy_target(&cfg, "gcs").is_err());
    }

    #[test]
    fn any_s3_compatible_service_is_an_s3_section_with_an_endpoint() {
        let mut cfg = two_stores("s3", None);
        let s3 = cfg.storage.s3.as_mut().unwrap();
        s3.endpoint = Some("https://s3.us-west-004.backblazeb2.com".into());
        s3.region = "us-west-004".into();
        s3.path_style = true;
        let (_, region, endpoint, path_style, _) = s3_of(&cfg, "s3");
        assert_eq!(
            endpoint.as_deref(),
            Some("https://s3.us-west-004.backblazeb2.com")
        );
        assert_eq!((region.as_str(), path_style), ("us-west-004", true));
        assert_eq!(labels(&cfg).unwrap(), ["s3"]);
    }

    #[test]
    fn minio_settings_can_be_overridden() {
        let mut cfg = two_stores("minio", None);
        let m = cfg.storage.minio.as_mut().unwrap();
        m.region = Some("eu-west-1".into());
        m.path_style = Some(false);
        m.prefix = Some("logs".into());
        let (t, region, _, path_style, _) = s3_of(&cfg, "minio");
        assert_eq!(
            (region.as_str(), path_style, t.prefix.as_deref()),
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

    fn with_targets(toml: &str) -> Result<Config> {
        let cfg: Config = toml::from_str(toml).map_err(|e| LogHavenError::Config(e.to_string()))?;
        cfg.validate()?;
        Ok(cfg)
    }

    const THREE: &str = r#"
[[storage.targets]]
name = "archive"
type = "s3"
bucket = "b"
region = "eu-west-1"
access_key_env = "LH_T_AK"
secret_key_env = "LH_T_SK"

[[storage.targets]]
name = "backup"
type = "azure"
account = "acct"
container = "logs"
prefix = "loghaven"
sas_token_env = "LH_T_SAS"

[[storage.targets]]
name = "gcs-eu"
type = "gcs"
bucket = "g"
credentials_file = "/nonexistent/sa.json"
"#;

    /// A service-account file with a real private key, which is what the store client checks for.
    fn service_account(dir: &std::path::Path) -> String {
        let (private, public) = (dir.join("private.pem"), dir.join("public.pem"));
        crate::auth::generate_keypair(&private, &public).unwrap();
        let pem = std::fs::read_to_string(&private).unwrap();
        let path = dir.join("sa.json");
        std::fs::write(
            &path,
            serde_json::json!({
                "type": "service_account",
                "project_id": "p",
                "private_key_id": "k1",
                "private_key": pem,
                "client_email": "sa@p.iam.gserviceaccount.com",
                "token_uri": "http://127.0.0.1:1/token"
            })
            .to_string(),
        )
        .unwrap();
        path.to_string_lossy().into_owned()
    }

    #[test]
    fn a_target_list_of_mixed_kinds_parses_and_keeps_its_names_and_order() {
        let cfg = with_targets(THREE).unwrap();
        let t = resolve_targets(&cfg).unwrap();
        let names: Vec<_> = t.iter().map(|t| t.name.as_str()).collect();
        assert_eq!(names, ["archive", "backup", "gcs-eu"]);
        let kinds: Vec<_> = t.iter().map(|t| t.kind.type_name()).collect();
        assert_eq!(kinds, ["s3", "azure", "gcs"]);
        assert_eq!(t[1].prefix.as_deref(), Some("loghaven"));
        assert!(
            matches!(&t[0].kind, TargetKind::S3 { region: Some(r), path_style: false, .. } if r == "eu-west-1")
        );
    }

    #[test]
    fn target_lists_that_cannot_work_are_config_errors() {
        let err = |extra: &str| {
            with_targets(extra)
                .err()
                .map(|e| e.to_string())
                .unwrap_or_default()
        };
        let one = |body: &str| format!("[[storage.targets]]\n{body}\n");
        assert!(
            err(&one("name = \"Bad Name\"\ntype = \"gcs\"\nbucket = \"b\""))
                .contains("name must be")
        );
        assert!(err(&one("name = \"\"\ntype = \"gcs\"\nbucket = \"b\"")).contains("name must be"));
        assert!(
            err(&one(&format!(
                "name = \"{}\"\ntype = \"gcs\"\nbucket = \"b\"",
                "a".repeat(33)
            )))
            .contains("name must be")
        );
        let dup = format!(
            "{0}{0}",
            one("name = \"x\"\ntype = \"gcs\"\nbucket = \"b\"")
        );
        assert!(err(&dup).contains("used twice"));
        assert!(
            err(&one("name = \"x\"\ntype = \"s3\"\nbucket = \"\"")).contains("bucket is required")
        );
        assert!(
            err(&one(
                "name = \"x\"\ntype = \"s3\"\nbucket = \"b\"\nendpoint = \"ftp://h\""
            ))
            .contains("endpoint")
        );
        assert!(err(&one("name = \"x\"\ntype = \"s3\"\nbucket = \"b\"\naccess_key = \"a\"\naccess_key_env = \"A\"\nsecret_key = \"s\"")).contains("not both"));
        assert!(
            err(&one(
                "name = \"x\"\ntype = \"s3\"\nbucket = \"b\"\naccess_key = \"a\""
            ))
            .contains("together")
        );
        assert!(err(&one("name = \"x\"\ntype = \"azure\"\naccount = \"a\"\ncontainer = \"c\"\naccess_key = \"k\"\nsas_token = \"s\"")).contains("not both"));
        assert!(
            err(&one(
                "name = \"x\"\ntype = \"gcs\"\nbucket = \"b\"\nprefix = \"../up\""
            ))
            .contains("prefix")
        );
        assert!(
            err(&one("name = \"x\"\ntype = \"nope\"")).contains("nope")
                || !err(&one("name = \"x\"\ntype = \"nope\"")).is_empty()
        );
    }

    #[test]
    fn the_list_and_the_older_settings_cannot_be_mixed() {
        let target = "[[storage.targets]]\nname = \"x\"\ntype = \"gcs\"\nbucket = \"b\"\n";
        for older in [
            "[storage]\nbackend = \"s3\"\n",
            "[storage]\nsecondary = \"minio\"\n",
        ] {
            // the list has to come after a [storage] header that holds the older keys
            let text = format!("{older}{target}");
            let e = with_targets(&text)
                .err()
                .map(|e| e.to_string())
                .unwrap_or_default();
            assert!(e.contains("cannot be combined"), "{older:?}: {e}");
        }
        let with_section = format!("{target}[storage.s3]\nbucket = \"b\"\nregion = \"r\"\n");
        assert!(
            with_targets(&with_section)
                .err()
                .unwrap()
                .to_string()
                .contains("cannot be combined")
        );
        assert!(with_targets(target).is_ok());
        assert!(with_targets("[storage]\nbackend = \"local\"\n").is_ok());
    }

    #[test]
    fn every_kind_builds_and_a_missing_secret_variable_is_named() {
        let _g = env_guard();
        unsafe {
            std::env::set_var("LH_T_AK", "ak");
            std::env::set_var("LH_T_SK", "sk");
            std::env::set_var("LH_T_SAS", "sv=1&sig=x");
        }
        let dir = tempfile::tempdir().unwrap();
        let sa = service_account(dir.path());
        let cfg = with_targets(&THREE.replace("/nonexistent/sa.json", &sa)).unwrap();
        let built: Vec<_> = resolve_targets(&cfg)
            .unwrap()
            .iter()
            .map(|t| Target::from_config(t).map(|t| t.label().to_string()))
            .collect();
        assert_eq!(built[0].as_ref().unwrap(), "archive");
        assert_eq!(built[1].as_ref().unwrap(), "backup");
        assert_eq!(built[2].as_ref().unwrap(), "gcs-eu");
        let missing = with_targets(THREE).unwrap();
        let e = resolve_syncer(&missing).err().unwrap().to_string();
        assert!(
            e.contains("'gcs-eu'") && e.contains("sa.json"),
            "a missing service-account file is a startup error: {e}"
        );
        unsafe { std::env::remove_var("LH_T_SK") };
        let e = resolve_syncer(&cfg).err().unwrap().to_string();
        assert!(
            e.contains("LH_T_SK") && e.contains("'archive'") && !e.contains("ak"),
            "{e}"
        );
        unsafe {
            std::env::remove_var("LH_T_AK");
            std::env::remove_var("LH_T_SAS");
        }
    }

    #[test]
    fn several_targets_make_one_syncer_with_a_marker_name_each() {
        let _g = env_guard();
        unsafe {
            std::env::set_var("LH_T2_AK", "ak");
            std::env::set_var("LH_T2_SK", "sk");
        }
        let dir = tempfile::tempdir().unwrap();
        let sa = service_account(dir.path());
        let cfg = with_targets(
            &THREE
                .replace("/nonexistent/sa.json", &sa)
                .replace("LH_T_AK", "LH_T2_AK")
                .replace("LH_T_SK", "LH_T2_SK")
                .replace("sas_token_env = \"LH_T_SAS\"", "sas_token = \"sv=1&sig=x\""),
        )
        .unwrap();
        assert_eq!(labels(&cfg).unwrap(), ["archive", "backup", "gcs-eu"]);
    }

    #[test]
    fn a_default_config_file_can_be_extended_with_a_targets_list() {
        let written = toml::to_string(&crate::config::default_config()).unwrap();
        assert!(!written.contains("targets"), "{written}");
        let extended = format!(
            "{written}\n[[storage.targets]]\nname = \"x\"\ntype = \"gcs\"\nbucket = \"b\"\n"
        );
        let cfg: Config = toml::from_str(&extended).unwrap();
        assert_eq!(cfg.storage.targets.len(), 1);
        // and a config that has targets writes them back
        let again: Config = toml::from_str(&toml::to_string(&cfg).unwrap()).unwrap();
        assert_eq!(again.storage.targets[0].name, "x");
    }

    #[test]
    fn a_misspelt_or_misplaced_key_in_a_target_is_an_error_naming_the_target() {
        let err = |body: &str| {
            with_targets(&format!("[[storage.targets]]\n{body}\n"))
                .err()
                .map(|e| e.to_string())
                .unwrap_or_default()
        };
        let s3 = "name = \"b2\"\ntype = \"s3\"\nbucket = \"b\"\n";
        for (extra, field) in [
            ("path-style = true", "path-style"),
            ("rgion = \"x\"", "rgion"),
            ("acess_key_env = \"A\"", "acess_key_env"),
            ("sas_token = \"s\"", "sas_token"),
        ] {
            let e = err(&format!("{s3}{extra}"));
            assert!(e.contains("'b2'") && e.contains(field), "{extra}: {e}");
        }
        let e = err("name = \"g\"\ntype = \"gcs\"\nbucket = \"b\"\naccess_key = \"zz\"");
        assert!(e.contains("'g'") && e.contains("access_key"), "{e}");
        assert!(err("type = \"gcs\"\nbucket = \"b\"").contains("name is required"));
        assert!(err("name = 5\ntype = \"gcs\"\nbucket = \"b\"").contains("name must be a string"));
        assert!(with_targets(&format!("[[storage.targets]]\n{s3}")).is_ok());
    }

    #[test]
    fn a_prefix_must_be_plain_segments_for_targets_and_for_the_older_sections() {
        let target = |prefix: &str| {
            with_targets(&format!(
                "[[storage.targets]]\nname = \"x\"\ntype = \"gcs\"\nbucket = \"b\"\nprefix = {prefix:?}\n"
            ))
        };
        for bad in ["a//b", "./x", "x/./y", "../up", "a/../b", "a\\b", "a\u{1}b"] {
            assert!(target(bad).is_err(), "{bad:?}");
        }
        for ok in ["loghaven", "a/b", "/a/b/", "v1..2", ""] {
            assert!(target(ok).is_ok(), "{ok:?}");
        }
        let legacy = |section: &str| {
            with_targets(&format!(
                "[storage]\nbackend = \"s3\"\n[storage.s3]\nbucket = \"b\"\nregion = \"r\"\naccess_key = \"k\"\nsecret_key = \"s\"\nprefix = {section:?}\n"
            ))
        };
        assert!(legacy("a//b").is_err());
        assert!(legacy("logs/x").is_ok());
    }

    #[test]
    fn backend_local_means_never_upload_even_with_targets() {
        let _g = env_guard();
        let target = "[[storage.targets]]\nname = \"x\"\ntype = \"gcs\"\nbucket = \"b\"\n";
        let mut cfg = with_targets(target).unwrap();
        assert_eq!(resolve_targets(&cfg).unwrap().len(), 1);
        cfg.storage.backend = "local".into();
        cfg.validate().unwrap();
        assert!(resolve_targets(&cfg).unwrap().is_empty());
        assert!(resolve_syncer(&cfg).unwrap().is_none());
    }

    #[test]
    fn an_older_s3_section_with_an_address_endpoint_still_works_by_path() {
        let _g = env_guard();
        for endpoint in [
            "http://192.168.1.5:9000",
            "http://minio:9000",
            "http://localhost:9000",
        ] {
            let mut cfg = two_stores("s3", None);
            cfg.storage.s3.as_mut().unwrap().endpoint = Some(endpoint.into());
            cfg.validate().unwrap();
            let (_, _, _, path_style, _) = s3_of(&cfg, "s3");
            assert!(path_style, "{endpoint}");
            assert!(resolve_syncer(&cfg).is_ok(), "{endpoint}");
        }
        let mut cfg = two_stores("s3", None);
        cfg.storage.s3.as_mut().unwrap().endpoint = Some("https://s3.example.com".into());
        assert!(!s3_of(&cfg, "s3").3);
    }

    #[test]
    fn a_target_with_an_address_endpoint_and_no_path_style_fails_validation_not_only_startup() {
        let e = with_targets(
            "[[storage.targets]]\nname = \"x\"\ntype = \"s3\"\nbucket = \"b\"\nendpoint = \"http://10.0.0.5:9000\"\n",
        )
        .err()
        .unwrap()
        .to_string();
        assert!(e.contains("path_style = true"), "{e}");
    }

    #[test]
    fn empty_secrets_in_a_target_are_rejected_by_validation() {
        for body in [
            "type = \"s3\"\nbucket = \"b\"\naccess_key = \"\"\nsecret_key = \"s\"",
            "type = \"s3\"\nbucket = \"b\"\naccess_key = \"a\"\nsecret_key = \"  \"",
            "type = \"azure\"\naccount = \"a\"\ncontainer = \"c\"\nsas_token = \"\"",
        ] {
            let e = with_targets(&format!("[[storage.targets]]\nname = \"x\"\n{body}\n"))
                .err()
                .map(|e| e.to_string())
                .unwrap_or_default();
            assert!(e.contains("is empty"), "{body}: {e}");
        }
    }
}
