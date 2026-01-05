use crate::error::{LogHavenError, Result};
use std::fs;
use std::path::PathBuf;

pub struct DaemonProcess;

impl DaemonProcess {
    pub fn get_pid_file(profile: Option<&str>) -> PathBuf {
        let base = dirs::data_local_dir()
            .unwrap_or_else(|| PathBuf::from("."))
            .join("loghaven");

        match profile {
            Some(p) => base.join(format!("{}.pid", p)),
            None => base.join("loghaven.pid"),
        }
    }

    pub fn is_running(profile: Option<&str>) -> Result<Option<u32>> {
        let pid_file = Self::get_pid_file(profile);

        if !pid_file.exists() {
            return Ok(None);
        }

        let pid_str = fs::read_to_string(&pid_file)?;
        let pid: u32 = pid_str
            .trim()
            .parse()
            .map_err(|_| LogHavenError::Daemon("Invalid PID file".to_string()))?;

        // Check if process is actually running
        #[cfg(unix)]
        {
            use nix::sys::signal::{Signal, kill};
            use nix::unistd::Pid;

            match kill(Pid::from_raw(pid as i32), None) {
                Ok(_) => Ok(Some(pid)),
                Err(_) => {
                    let _ = fs::remove_file(&pid_file);
                    Ok(None)
                }
            }
        }

        #[cfg(windows)]
        {
            use std::process::Command;

            // Check if process exists using tasklist
            let output = Command::new("tasklist")
                .args(&["/FI", &format!("PID eq {}", pid), "/NH"])
                .output();

            match output {
                Ok(out) => {
                    let stdout = String::from_utf8_lossy(&out.stdout);
                    if stdout.contains(&pid.to_string()) {
                        Ok(Some(pid))
                    } else {
                        let _ = fs::remove_file(&pid_file);
                        Ok(None)
                    }
                }
                Err(_) => {
                    let _ = fs::remove_file(&pid_file);
                    Ok(None)
                }
            }
        }
    }

    pub fn kill(profile: Option<&str>) -> Result<()> {
        if let Some(pid) = Self::is_running(profile)? {
            #[cfg(unix)]
            {
                use nix::sys::signal::{Signal, kill};
                use nix::unistd::Pid;

                kill(Pid::from_raw(pid as i32), Signal::SIGTERM)
                    .map_err(|e| LogHavenError::Daemon(format!("Failed to kill process: {}", e)))?;
            }

            #[cfg(windows)]
            {
                use std::process::Command;

                Command::new("taskkill")
                    .args(&["/PID", &pid.to_string(), "/F"])
                    .output()
                    .map_err(|e| LogHavenError::Daemon(format!("Failed to kill process: {}", e)))?;
            }

            Self::remove_pid(profile)?;
        }

        Ok(())
    }

    pub fn write_pid(profile: Option<&str>) -> Result<()> {
        let pid_file = Self::get_pid_file(profile);

        if let Some(parent) = pid_file.parent() {
            fs::create_dir_all(parent)?;
        }

        let pid = std::process::id();
        fs::write(&pid_file, pid.to_string())?;

        Ok(())
    }

    pub fn remove_pid(profile: Option<&str>) -> Result<()> {
        let pid_file = Self::get_pid_file(profile);

        if pid_file.exists() {
            fs::remove_file(&pid_file)?;
        }

        Ok(())
    }

    #[cfg(unix)]
    pub fn daemonize() -> Result<()> {
        use nix::unistd::{ForkResult, fork, setsid};

        match unsafe { fork() } {
            Ok(ForkResult::Parent { .. }) => {
                // Parent process exits
                std::process::exit(0);
            }
            Ok(ForkResult::Child) => {
                // Child becomes session leader
                setsid().map_err(|e| LogHavenError::Daemon(format!("setsid failed: {}", e)))?;
                Ok(())
            }
            Err(e) => Err(LogHavenError::Daemon(format!("Fork failed: {}", e))),
        }
    }

    #[cfg(windows)]
    pub fn daemonize() -> Result<()> {
        // Windows doesn't fork, just continue running, lol
        Ok(())
    }
}
