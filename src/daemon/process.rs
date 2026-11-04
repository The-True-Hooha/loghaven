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
            use nix::sys::signal::kill;
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
                .args(["/FI", &format!("PID eq {}", pid), "/NH"])
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
                    .args(["/PID", &pid.to_string(), "/F"])
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

        // create_new: two instances starting together cannot both claim the file
        let mut f = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&pid_file)
            .map_err(|e| match e.kind() {
                std::io::ErrorKind::AlreadyExists => {
                    LogHavenError::Daemon("another instance owns the PID file".into())
                }
                _ => e.into(),
            })?;
        std::io::Write::write_all(&mut f, std::process::id().to_string().as_bytes())?;

        Ok(())
    }

    /// Removes the PID file only if it still names this process, so a failed
    /// second instance or a late exit never deletes another daemon's file.
    pub fn remove_pid_if_owner(profile: Option<&str>) {
        let file = Self::get_pid_file(profile);
        if let Ok(s) = fs::read_to_string(&file)
            && s.trim() == std::process::id().to_string()
        {
            let _ = fs::remove_file(&file);
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pid_file_is_only_removed_by_its_owner() {
        let profile = format!("test-owner-{}", std::process::id());
        let file = DaemonProcess::get_pid_file(Some(&profile));
        fs::create_dir_all(file.parent().unwrap()).unwrap();
        fs::write(&file, "999999999").unwrap();
        DaemonProcess::remove_pid_if_owner(Some(&profile));
        assert!(file.exists(), "foreign pid kept");
        fs::remove_file(&file).unwrap();
        DaemonProcess::write_pid(Some(&profile)).unwrap();
        assert!(
            DaemonProcess::write_pid(Some(&profile)).is_err(),
            "second claim fails"
        );
        DaemonProcess::remove_pid_if_owner(Some(&profile));
        assert!(!file.exists());
        DaemonProcess::remove_pid_if_owner(Some(&profile)); // missing file is fine
    }
}
