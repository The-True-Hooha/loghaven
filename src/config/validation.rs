use super::Config;
use crate::error::{LogHavenError, Result};

pub fn validate(config: &Config) -> Result<()> {
    match config.storage.backend.as_str() {
        "local" => validate_local_storage(config)?,
        "s3" => validate_s3_storage(config)?,
        "r2" => validate_r2_storage(config)?,
        "minio" => validate_minio_storage(config)?,
        backend => {
            return Err(LogHavenError::Config(format!(
                "Unknown storage backend: {}",
                backend
            )));
        }
    }

    match config.agent.log_level.as_str() {
        "trace" | "debug" | "info" | "warn" | "error" => {}
        level => {
            return Err(LogHavenError::Config(format!(
                "Invalid log level: {}. Must be one of: trace, debug, info, warn, error",
                level
            )));
        }
    }

    for chain in &config.chains.enabled {
        match chain.as_str() {
            "ethereum" => {
                if config.chains.ethereum.is_none() {
                    return Err(LogHavenError::Config(
                        "Ethereum enabled but no config provided".to_string(),
                    ));
                }
            }
            "solana" => {
                if config.chains.solana.is_none() {
                    return Err(LogHavenError::Config(
                        "Solana enabled but no config provided".to_string(),
                    ));
                }
            }
            "stellar" => {
                if config.chains.stellar.is_none() {
                    return Err(LogHavenError::Config(
                        "Stellar enabled but no config provided".to_string(),
                    ));
                }
            }
            unknown => {
                return Err(LogHavenError::Config(format!("Unknown chain: {}", unknown)));
            }
        }
    }

    Ok(())
}

fn validate_local_storage(config: &Config) -> Result<()> {
    let path = &config.storage.local.path;

    if let Some(parent) = path.parent()
        && !parent.exists()
    {
        std::fs::create_dir_all(parent)?;
    }

    Ok(())
}

fn validate_s3_storage(config: &Config) -> Result<()> {
    if config.storage.s3.is_none() {
        return Err(LogHavenError::Config(
            "S3 backend selected but no S3 config provided".to_string(),
        ));
    }

    // TODO: Check AWS credentials
    Ok(())
}

fn validate_r2_storage(config: &Config) -> Result<()> {
    if config.storage.r2.is_none() {
        return Err(LogHavenError::Config(
            "R2 backend selected but no R2 config provided".to_string(),
        ));
    }

    // TODO: Check R2 credentials

    Ok(())
}

fn validate_minio_storage(config: &Config) -> Result<()> {
    if config.storage.minio.is_none() {
        return Err(LogHavenError::Config(
            "MinIO backend selected but no MinIO config provided".to_string(),
        ));
    }
    // TODO: Check MINIO credentials

    Ok(())
}
