use colored::Colorize;

use super::style;
use crate::config::{self, Config};
use crate::error::Result;

use crate::daemon::{Daemon, DaemonProcess};

pub fn init(force: bool, storage: Option<String>, profile: Option<&str>) -> Result<()> {
    style::print_banner();

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
    rt.block_on(async {
        let mut daemon = Daemon::new(cfg);
        daemon.run().await
    })?;

    style::success("Agent started");

    Ok(())
}

pub fn status(profile: Option<&str>) -> Result<()> {
    let config_path = config::get_config_path(profile);

    style::step("Checking agent status...");

    if !config_path.exists() {
        style::warning("No configuration found");
        return Ok(());
    }

    match DaemonProcess::is_running(profile)? {
        Some(pid) => {
            style::success(&format!("Daemon is running (PID: {})", pid));
            // TODO - get the daemon active status
        }
        None => {
            style::info("Agent is not running");
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
            }
            
            DaemonProcess::kill(profile)?;
            style::success("Daemon stopped");
        }
        None => {
            style::info("Daemon is not running");
        }
    }
    
    Ok(())
}
