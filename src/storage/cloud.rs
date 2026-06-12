use crate::error::{LogHavenError, Result};
use aws_sdk_s3::Client;
use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
use aws_sdk_s3::primitives::ByteStream;
use std::path::PathBuf;
use std::sync::Arc;

pub struct CloudSyncer {
    client: Client,
    bucket: String,
    prefix: String,
    data_path: PathBuf,
    pub delete_after_upload: bool,
}

impl CloudSyncer {
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
        Arc::new(Self {
            client,
            bucket,
            prefix: prefix.unwrap_or_default(),
            data_path,
            delete_after_upload,
        })
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
        Arc::new(Self {
            client,
            bucket,
            prefix: prefix.unwrap_or_default(),
            data_path,
            delete_after_upload,
        })
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
        Arc::new(Self {
            client,
            bucket,
            prefix: String::new(),
            data_path,
            delete_after_upload,
        })
    }

    pub async fn upload_chunk(&self, chunk_path: &PathBuf) -> Result<()> {
        let rel = chunk_path
            .strip_prefix(&self.data_path)
            .map_err(|_| LogHavenError::Storage("chunk path not under data_path".into()))?;

        let key = if self.prefix.is_empty() {
            rel.to_string_lossy().replace('\\', "/")
        } else {
            format!(
                "{}/{}",
                self.prefix,
                rel.to_string_lossy().replace('\\', "/")
            )
        };

        let body = ByteStream::from_path(chunk_path)
            .await
            .map_err(|e| LogHavenError::Storage(format!("read chunk for upload: {}", e)))?;

        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(&key)
            .body(body)
            .send()
            .await
            .map_err(|e| LogHavenError::Storage(format!("upload to cloud failed: {}", e)))?;

        if self.delete_after_upload {
            let _ = std::fs::remove_file(chunk_path);
        }

        Ok(())
    }
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
