//! Builds a store client from one `[[storage.targets]]` entry.

use crate::config::{StorageTarget, TargetKind};
use crate::error::{LogHavenError, Result};
use object_store::ClientOptions;
use object_store::ObjectStore;
use object_store::aws::AmazonS3Builder;
use object_store::azure::{AzureConfigKey, MicrosoftAzureBuilder};
use object_store::gcp::GoogleCloudStorageBuilder;
use std::sync::Arc;

fn config_err(at: &str, what: String) -> LogHavenError {
    LogHavenError::Config(format!("storage target '{at}': {what}"))
}

/// Provider error text that is safe to print: query strings removed (a SAS token or signature
/// rides in one), control characters dropped, cut to a sane length.
pub(crate) fn tidy(e: &dyn std::fmt::Display) -> String {
    e.to_string()
        .split_whitespace()
        .map(|w| match w.split_once('?') {
            // keep the closing punctuation of "(https://host/x?sig=...)"
            Some((url, rest)) => {
                let tail: String = rest
                    .chars()
                    .rev()
                    .take_while(|c| ")]}>\"'.,;".contains(*c))
                    .collect::<Vec<_>>()
                    .into_iter()
                    .rev()
                    .collect();
                format!("{url}{tail}")
            }
            None => w.to_string(),
        })
        .collect::<Vec<_>>()
        .join(" ")
        .chars()
        .filter(|c| !c.is_control())
        .take(MAX_ERROR_CHARS)
        .collect()
}

/// Most of a provider's error text that is kept.
const MAX_ERROR_CHARS: usize = 300;

/// The secret from the config, or from the environment variable that names it. Neither is
/// `None` (ambient credentials). Errors name the variable, never a value. Surrounding whitespace
/// is removed, as a value copied from a file usually ends in a newline.
fn secret(
    target: &str,
    what: &str,
    plain: &Option<String>,
    env: &Option<String>,
) -> Result<Option<String>> {
    if let Some(name) = env {
        return match std::env::var(name).map(|v| v.trim().to_string()) {
            Ok(v) if !v.is_empty() => Ok(Some(v)),
            _ => Err(config_err(
                target,
                format!("{what}_env names {name}, which is not set"),
            )),
        };
    }
    match plain.as_deref().map(str::trim) {
        Some("") => Err(config_err(target, format!("{what} is empty"))),
        other => Ok(other.map(String::from)),
    }
}

/// `https://host:port` -> `https://bucket.host:port`, which is what virtual-hosted style needs.
fn with_bucket_host(endpoint: &str, bucket: &str) -> String {
    match endpoint.split_once("://") {
        Some((scheme, rest)) => format!("{scheme}://{bucket}.{}", rest.trim_end_matches('/')),
        None => endpoint.to_string(),
    }
}

/// A bucket cannot be put in front of an IP address or a bare name, and the crate panics
/// when asked to; such an endpoint needs path-style addressing.
pub(crate) fn host_takes_a_bucket_prefix(endpoint: &str) -> bool {
    let host = endpoint
        .split_once("://")
        .map_or(endpoint, |(_, rest)| rest)
        .split(['/', ':'])
        .next()
        .unwrap_or("");
    let bracketed = host.starts_with('[');
    !bracketed && host.parse::<std::net::IpAddr>().is_err() && host.contains('.')
}

fn is_http(url: &str) -> bool {
    url.starts_with("http://")
}

/// No request timeout: a chunk can be large and the uplink slow. The upload as a whole is
/// bounded by the caller, in proportion to the chunk's size.
fn client_options() -> ClientOptions {
    ClientOptions::new().with_timeout_disabled()
}

pub fn build(t: &StorageTarget) -> Result<Arc<dyn ObjectStore>> {
    let name = t.name.as_str();
    let built =
        |e: object_store::Error| config_err(name, format!("cannot set up the store: {}", tidy(&e)));
    match &t.kind {
        TargetKind::S3 {
            bucket,
            region,
            endpoint,
            path_style,
            access_key,
            access_key_env,
            secret_key,
            secret_key_env,
        } => {
            let access = secret(name, "access_key", access_key, access_key_env)?;
            let secret_key = secret(name, "secret_key", secret_key, secret_key_env)?;
            // With keys in the config, nothing in the environment (AWS_ENDPOINT_URL_S3,
            // AWS_SESSION_TOKEN, ...) may change what was written; with none, the provider's
            // usual sources are wanted.
            let explicit = access.is_some() || secret_key.is_some();
            let mut b = if explicit {
                AmazonS3Builder::new()
            } else {
                AmazonS3Builder::from_env()
            }
            .with_bucket_name(bucket)
            .with_region(region.as_deref().unwrap_or("us-east-1"))
            .with_client_options(client_options());
            if let Some(k) = access {
                b = b.with_access_key_id(k);
            }
            if let Some(k) = secret_key {
                b = b.with_secret_access_key(k);
            }
            match endpoint {
                // AWS: the crate derives the bucket host from the region
                None => b = b.with_virtual_hosted_style_request(!path_style),
                Some(e) if *path_style => {
                    b = b
                        .with_endpoint(e.trim_end_matches('/'))
                        .with_virtual_hosted_style_request(false)
                        .with_allow_http(is_http(e));
                }
                Some(e) => {
                    if !host_takes_a_bucket_prefix(e) {
                        return Err(config_err(
                            name,
                            "this endpoint is an IP address or a bare host name, which cannot carry the bucket in the host: set path_style = true".into(),
                        ));
                    }
                    b = b
                        .with_endpoint(with_bucket_host(e, bucket))
                        .with_virtual_hosted_style_request(true)
                        .with_allow_http(is_http(e));
                }
            }
            Ok(Arc::new(b.build().map_err(built)?))
        }
        TargetKind::Azure {
            account,
            container,
            endpoint,
            access_key,
            access_key_env,
            sas_token,
            sas_token_env,
        } => {
            let key = secret(name, "access_key", access_key, access_key_env)?;
            let sas = secret(name, "sas_token", sas_token, sas_token_env)?;
            let explicit = key.is_some() || sas.is_some();
            let mut b = if explicit {
                MicrosoftAzureBuilder::new()
            } else {
                MicrosoftAzureBuilder::from_env()
            }
            .with_account(account)
            .with_container_name(container)
            .with_client_options(client_options());
            if let Some(k) = key {
                b = b.with_access_key(k);
            }
            if let Some(sas) = sas {
                // the crate decodes the percent-encoding a portal-made token carries
                b = b.with_config(AzureConfigKey::SasKey, sas);
            }
            if let Some(e) = endpoint {
                b = b
                    .with_endpoint(e.trim_end_matches('/').to_string())
                    .with_allow_http(is_http(e));
            }
            Ok(Arc::new(b.build().map_err(built)?))
        }
        TargetKind::Gcs {
            bucket,
            credentials_file,
        } => {
            let mut b = if credentials_file.is_some() {
                GoogleCloudStorageBuilder::new()
            } else {
                GoogleCloudStorageBuilder::from_env()
            }
            .with_bucket_name(bucket)
            // only a service-account file can name a non-Google `gcs_base_url` (an emulator)
            .with_client_options(client_options().with_allow_http(true));
            if let Some(path) = credentials_file {
                b = b.with_service_account_path(path);
            }
            Ok(Arc::new(b.build().map_err(built)?))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_bucket_goes_in_front_of_the_host_for_virtual_hosted_style() {
        assert_eq!(
            with_bucket_host("https://s3.us-west-004.backblazeb2.com", "logs"),
            "https://logs.s3.us-west-004.backblazeb2.com"
        );
        assert_eq!(
            with_bucket_host("http://localhost:9000/", "b"),
            "http://b.localhost:9000"
        );
    }

    #[test]
    fn only_a_dotted_host_name_can_carry_the_bucket() {
        for ok in ["https://s3.example.com", "http://minio.internal:9000/"] {
            assert!(host_takes_a_bucket_prefix(ok), "{ok}");
        }
        for no in [
            "http://127.0.0.1:9000",
            "http://10.0.0.5",
            "http://localhost:9000",
            "http://minio:9000",
            "http://[::1]:9000",
        ] {
            assert!(!host_takes_a_bucket_prefix(no), "{no}");
        }
    }

    #[test]
    fn tidy_removes_query_strings_keeps_closing_punctuation_and_cuts_long_text() {
        assert_eq!(
            tidy(&"PUT http://h/x?sig=SECRET&a=b failed"),
            "PUT http://h/x failed"
        );
        assert_eq!(
            tidy(&"error (https://h/x?sig=SECRET).").as_str(),
            "error (https://h/x)."
        );
        assert_eq!(tidy(&"x".repeat(5_000)).len(), MAX_ERROR_CHARS);
        assert!(!tidy(&"a\u{1b}[31mb").contains('\u{1b}'));
    }

    #[test]
    fn plain_secrets_must_not_be_empty_and_values_from_the_environment_are_trimmed() {
        let _g = crate::storage::env_guard();
        for blank in ["", "   ", "\n"] {
            let e = secret("t", "access_key", &Some(blank.into()), &None)
                .unwrap_err()
                .to_string();
            assert!(e.contains("access_key is empty"), "{blank:?}: {e}");
        }
        let name = "LH_TEST_STORE_SECRET_B";
        unsafe { std::env::set_var(name, "value-from-a-file\n") };
        assert_eq!(
            secret("t", "k", &None, &Some(name.into()))
                .unwrap()
                .as_deref(),
            Some("value-from-a-file")
        );
        unsafe { std::env::set_var(name, " \n") };
        assert!(secret("t", "k", &None, &Some(name.into())).is_err());
        unsafe { std::env::remove_var(name) };
    }

    #[test]
    fn a_secret_comes_from_the_named_variable_and_a_missing_one_is_named_not_shown() {
        let _g = crate::storage::env_guard();
        let plain = Some("from-config".to_string());
        assert_eq!(
            secret("t", "k", &plain, &None).unwrap().as_deref(),
            Some("from-config")
        );
        assert_eq!(secret("t", "k", &None, &None).unwrap(), None);
        let name = "LH_TEST_STORE_SECRET_A";
        unsafe { std::env::set_var(name, "from-env") };
        assert_eq!(
            secret("t", "k", &plain, &Some(name.into()))
                .unwrap()
                .as_deref(),
            Some("from-env")
        );
        unsafe { std::env::remove_var(name) };
        let e = secret("t", "k", &None, &Some(name.into()))
            .unwrap_err()
            .to_string();
        assert!(e.contains(name) && e.contains("'t'"), "{e}");
        unsafe { std::env::set_var(name, "") };
        assert!(secret("t", "k", &None, &Some(name.into())).is_err());
        unsafe { std::env::remove_var(name) };
    }

    #[test]
    fn clients_are_built_without_a_request_timeout() {
        let shown = format!("{:?}", client_options());
        // `None` is "no overall timeout"; an unset client gets the crate's 30 seconds
        assert!(shown.contains(", timeout: None,"), "{shown}");
        assert!(format!("{:?}", ClientOptions::new()).contains(", timeout: Some("));
    }
}
