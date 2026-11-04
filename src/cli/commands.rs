use colored::Colorize;
use std::path::PathBuf;

use super::style;
use crate::auth::keys::generate_keypair;
use crate::config::{self, Config};
use crate::error::Result;

use crate::daemon::{Daemon, DaemonProcess};

pub fn init(
    force: bool,
    storage: Option<String>,
    storage_path: Option<String>,
    profile: Option<&str>,
) -> Result<()> {
    style::print_banner();

    if profile.is_none()
        && let Some(p) = config::migrate_legacy_config()?
    {
        style::info(&format!("Renamed legacy config_toml to {}", p.display()));
    }

    let config_path = config::get_config_path(profile);

    if config_path.exists() && !force {
        style::error(&format!(
            "Configuration already exists at {}",
            config_path.display()
        ));
        style::info("Use --force to overwrite");
        return Ok(());
    }

    style::step("Creating LogHaven configuration...");

    let mut cfg = config::default_config();

    if let Some(backend) = storage {
        cfg.storage.backend = backend.clone();
        style::info(&format!("Storage backend: {}", backend));
    }

    if let Some(path) = storage_path {
        cfg.storage.local.path = PathBuf::from(&path);
        style::info(&format!("Storage path: {}", path));
    }

    if let Some(p) = profile {
        style::info(&format!("Profile: {}", p));
    }

    cfg.save(&config_path)?;

    style::success(&format!("Configuration saved to {}", config_path.display()));

    println!("\n{}", "Next steps:".bright_black());
    println!("  1. Edit config: {}", config_path.display());
    println!("  2. Start agent: loghaven run");

    Ok(())
}

pub fn run(foreground: bool, profile: Option<&str>) -> Result<()> {
    style::print_banner();

    if let Some(pid) = DaemonProcess::is_running(profile)? {
        style::warning(&format!("Daemon already running (PID: {})", pid));
        style::info("use 'loghaven stop' top stop it first");
        return Ok(());
    }

    let config_path = config::get_config_path(profile);

    if !config_path.exists() {
        style::error("No configuration found");
        style::info("Run 'loghaven init' first");
        return Ok(());
    }

    style::step("Loading configuration...");
    let cfg = Config::load(&config_path)?;

    style::step("Validating configuration...");
    cfg.validate()?;

    if !foreground {
        style::step("starting daemon in background...");
        DaemonProcess::daemonize()?;
    } else {
        style::info("Running in foreground mode");
    }

    DaemonProcess::write_pid(profile)?;

    style::success(&format!("Using storage backend: {}", cfg.storage.backend));
    style::success(&format!("Log level: {}", cfg.agent.log_level));

    let rt = tokio::runtime::Runtime::new()?;
    let result = rt.block_on(async {
        let mut daemon = Daemon::new(cfg);
        daemon.run().await
    });
    // signal and IPC shutdowns end here too, so the PID file never outlives the daemon
    DaemonProcess::remove_pid_if_owner(profile);
    result?;

    style::success("Agent started");

    Ok(())
}

pub fn status(profile: Option<&str>) -> Result<()> {
    style::step("Checking daemon status...");

    match DaemonProcess::is_running(profile)? {
        Some(_) => {
            let config_path = config::get_config_path(profile);
            let cfg = Config::load(&config_path)?;

            let rt = tokio::runtime::Runtime::new()?;
            let response = rt.block_on(async {
                let client =
                    crate::ipc::IpcClient::new(cfg.daemon.socket_path.clone(), cfg.daemon.tcp_port);
                client.send(crate::ipc::Request::Status).await
            })?;

            if response.success {
                if let Some(crate::ipc::protocol::ResponseData::Status(data)) = response.data {
                    style::success(&format!("Daemon is running (PID: {})", data.pid));
                    println!("  Version: {}", data.version);
                    println!("  Uptime: {}s", data.uptime);
                    println!("  Storage: {}", data.storage_backend);
                    println!("  Log Level: {}", data.log_level);
                }
            } else {
                style::error(&format!("Error: {}", response.error.unwrap_or_default()));
            }
        }
        None => {
            style::info("Daemon is not running");
        }
    }

    Ok(())
}

pub fn config(key: String, value: Option<String>, profile: Option<&str>) -> Result<()> {
    let config_path = config::get_config_path(profile);

    if !config_path.exists() {
        style::error("No configuration found");
        style::info("Run 'loghaven init' first");
        return Ok(());
    }

    let mut cfg = Config::load(&config_path)?;

    match value {
        None => {
            let val = match key.as_str() {
                "storage.backend" => cfg.storage.backend.clone(),
                "storage.local.path" => cfg.storage.local.path.display().to_string(),
                "storage.local.max_size_gb" => cfg.storage.local.max_size_gb.to_string(),
                "storage.local.rotate_size_mb" => cfg.storage.local.rotate_size_mb.to_string(),
                "storage.local.rotate_records" => cfg.storage.local.rotate_records.to_string(),
                "storage.local.flush_interval_secs" => {
                    cfg.storage.local.flush_interval_secs.to_string()
                }
                "storage.local.retention_days" => cfg.storage.local.retention_days.to_string(),
                "agent.log_level" => cfg.agent.log_level.clone(),
                "agent.name" => cfg.agent.name.clone(),
                "daemon.tcp_port" => cfg.daemon.tcp_port.to_string(),
                "daemon.socket_path" => cfg.daemon.socket_path.display().to_string(),
                _ => {
                    style::error(&format!("Unknown key: {}", key));
                    return Ok(());
                }
            };
            println!("{} = {}", key, val);
        }
        Some(val) => {
            match key.as_str() {
                "storage.backend" => cfg.storage.backend = val.clone(),
                "storage.local.path" => cfg.storage.local.path = PathBuf::from(&val),
                "storage.local.max_size_gb" => {
                    cfg.storage.local.max_size_gb = val.parse().map_err(|_| {
                        crate::error::LogHavenError::Config("max_size_gb must be a number".into())
                    })?;
                }
                "storage.local.rotate_size_mb" => {
                    cfg.storage.local.rotate_size_mb = val.parse().map_err(|_| {
                        crate::error::LogHavenError::Config(
                            "rotate_size_mb must be a number".into(),
                        )
                    })?;
                }
                "storage.local.rotate_records" => {
                    cfg.storage.local.rotate_records = val.parse().map_err(|_| {
                        crate::error::LogHavenError::Config(
                            "rotate_records must be a number".into(),
                        )
                    })?;
                }
                "storage.local.flush_interval_secs" => {
                    cfg.storage.local.flush_interval_secs = val.parse().map_err(|_| {
                        crate::error::LogHavenError::Config(
                            "flush_interval_secs must be a number".into(),
                        )
                    })?;
                }
                "storage.local.retention_days" => {
                    cfg.storage.local.retention_days = val.parse().map_err(|_| {
                        crate::error::LogHavenError::Config(
                            "retention_days must be a number".into(),
                        )
                    })?;
                }
                "agent.log_level" => cfg.agent.log_level = val.clone(),
                "agent.name" => cfg.agent.name = val.clone(),
                "daemon.tcp_port" => {
                    cfg.daemon.tcp_port = val.parse().map_err(|_| {
                        crate::error::LogHavenError::Config("tcp_port must be a number".into())
                    })?;
                }
                "daemon.socket_path" => cfg.daemon.socket_path = PathBuf::from(&val),
                _ => {
                    style::error(&format!("Unknown key: {}", key));
                    return Ok(());
                }
            }
            cfg.save(&config_path)?;
            style::success(&format!("Set {} = {}", key, val));
        }
    }

    Ok(())
}

pub fn stop(force: bool, profile: Option<&str>) -> Result<()> {
    style::step("Stopping daemon...");

    match DaemonProcess::is_running(profile)? {
        Some(_) => {
            if force {
                style::warning("Force stopping daemon");
                DaemonProcess::kill(profile)?;
            } else {
                // Send graceful stop via IPC
                let config_path = config::get_config_path(profile);
                let cfg = Config::load(&config_path)?;

                let rt = tokio::runtime::Runtime::new()?;
                rt.block_on(async {
                    let client = crate::ipc::IpcClient::new(
                        cfg.daemon.socket_path.clone(),
                        cfg.daemon.tcp_port,
                    );
                    client.send(crate::ipc::Request::Stop).await
                })?;

                // Wait a bit for graceful shutdown
                std::thread::sleep(std::time::Duration::from_secs(1));

                // Clean up PID file
                DaemonProcess::remove_pid(profile)?;
            }

            style::success("Daemon stopped");
        }
        None => {
            style::info("Daemon is not running");
        }
    }

    Ok(())
}

fn logs_params(
    app: &str,
    limit: usize,
    level: &Option<String>,
    source: &Option<String>,
    from_ms: Option<i64>,
    to_ms: Option<i64>,
    text: &Option<String>,
) -> Vec<(&'static str, String)> {
    let mut p = vec![("app", app.to_string()), ("limit", limit.to_string())];
    if let Some(v) = level {
        p.push(("level", v.clone()));
    }
    if let Some(v) = source {
        p.push(("source", v.clone()));
    }
    if let Some(v) = from_ms {
        p.push(("from_ms", v.to_string()));
    }
    if let Some(v) = to_ms {
        p.push(("to_ms", v.to_string()));
    }
    if let Some(v) = text {
        p.push(("text", v.clone()));
    }
    p
}

#[allow(clippy::too_many_arguments)]
pub fn logs(
    app: String,
    level: Option<String>,
    source: Option<String>,
    from_ms: Option<i64>,
    to_ms: Option<i64>,
    text: Option<String>,
    limit: usize,
    token: Option<String>,
    profile: Option<&str>,
) -> Result<()> {
    let config_path = config::get_config_path(profile);
    let cfg = Config::load(&config_path)?;

    let url = format!("http://127.0.0.1:{}/logs", cfg.auth.query_port);
    let params = logs_params(&app, limit, &level, &source, from_ms, to_ms, &text);

    let rt = tokio::runtime::Runtime::new()?;
    let records = rt.block_on(async {
        let client = reqwest::Client::new();
        let mut req = client.get(&url).query(&params);
        if let Some(tok) = &token {
            req = req.header("authorization", format!("Bearer {}", tok));
        }
        let resp = req
            .send()
            .await
            .map_err(|e| crate::error::LogHavenError::Daemon(e.to_string()))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(crate::error::LogHavenError::Daemon(format!(
                "query failed {}: {}",
                status, body
            )));
        }

        let body: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| crate::error::LogHavenError::Daemon(e.to_string()))?;
        Ok(body)
    })?;

    let count = records["count"].as_u64().unwrap_or(0);
    style::success(&format!("{} records", count));

    if let Some(arr) = records["records"].as_array() {
        for r in arr {
            let ts = r["timestamp_ms"].as_i64().unwrap_or(0);
            let level = r["level"].as_str().unwrap_or("?");
            let source = r["source"].as_str().unwrap_or("?");
            let msg = r["message"].as_str().unwrap_or("");
            println!("[{}] {} {} | {}", ts, level, source, msg);
        }
    }

    Ok(())
}

pub fn auth_keygen(force: bool, profile: Option<&str>) -> Result<()> {
    let config_path = config::get_config_path(profile);

    let keys_dir = if config_path.exists() {
        Config::load(&config_path)?.auth.keys_dir
    } else {
        crate::config::AuthConfig::default().keys_dir
    };

    let private_path = keys_dir.join("private.pem");
    let public_path = keys_dir.join("public.pem");

    if private_path.exists() && !force {
        style::error("Keypair already exists");
        style::info(&format!("Keys dir: {}", keys_dir.display()));
        style::info("Use --force to regenerate (this will invalidate existing sessions)");
        return Ok(());
    }

    style::step("Generating RSA-2048 keypair...");
    generate_keypair(&private_path, &public_path)?;

    style::success(&format!("Private key: {}", private_path.display()));
    style::success(&format!("Public key:  {}", public_path.display()));
    println!(
        "\n{}",
        "Share the public key with SDK callers. Keep the private key secret.".bright_black()
    );

    Ok(())
}

pub fn auth_public_key(profile: Option<&str>) -> Result<()> {
    let config_path = config::get_config_path(profile);

    let keys_dir = if config_path.exists() {
        Config::load(&config_path)?.auth.keys_dir
    } else {
        crate::config::AuthConfig::default().keys_dir
    };

    let public_path = keys_dir.join("public.pem");

    if !public_path.exists() {
        style::error("No public key found");
        style::info("Run 'loghaven auth keygen' first");
        return Ok(());
    }

    println!("{}", public_path.display());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn logs_params_are_structured_not_string_spliced() {
        let text = Some("a&app=evil #x=y".to_string());
        let p = logs_params("web", 5, &None, &None, Some(1), None, &text);
        assert_eq!(p[0], ("app", "web".to_string()));
        assert_eq!(p[1], ("limit", "5".to_string()));
        assert!(p.contains(&("from_ms", "1".to_string())));
        assert!(p.contains(&("text", "a&app=evil #x=y".to_string())));
        assert!(!p.iter().any(|(k, _)| *k == "to_ms" || *k == "level"));
        let req = reqwest::Client::new()
            .get("http://127.0.0.1:1/logs")
            .query(&p)
            .build()
            .unwrap();
        let pairs: Vec<_> = req.url().query_pairs().collect();
        assert_eq!(pairs.iter().filter(|(k, _)| k == "app").count(), 1);
        assert!(
            pairs
                .iter()
                .any(|(k, v)| k == "text" && v == "a&app=evil #x=y")
        );
    }
}
