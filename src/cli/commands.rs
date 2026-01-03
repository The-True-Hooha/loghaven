use colored::Colorize;

use super::style;
use crate::config::{self, Config};
use crate::error::Result;

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
    
    if foreground {
        style::info("Running in foreground mode");
    }
    
    style::success(&format!("Using storage backend: {}", cfg.storage.backend));
    style::success(&format!("Log level: {}", cfg.agent.log_level));
    
    // TODO: Actual daemon startup
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
    
    // TODO: Actual status check
    style::info("Agent is not running");
    
    Ok(())
}

pub fn stop(force: bool, _profile: Option<&str>) -> Result<()> {
    style::step("Stopping LogHaven agent...");
    
    if force {
        style::warning("Force stopping agent");
    }
    
    // TODO: Actual daemon stop
    style::success("Agent stopped");
    
    Ok(())
}