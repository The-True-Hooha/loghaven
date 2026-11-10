//! The loop behind `loghaven watch <chain>`: poll, send, save the cursor, wait.
//! A chain supplies a `Watcher`; the cursor is an opaque text file it owns.

use crate::cli::style;
use crate::client::{ClientRecord, IngestClient};
use crate::config::Config;
use crate::error::{LogHavenError, Result};
use std::path::Path;
use std::time::Duration;

pub struct Step {
    pub records: Vec<ClientRecord>,
    /// Saved once `records` are sent.
    pub cursor: String,
    /// True when the poll stopped short of the chain head, so the next one should not wait.
    pub more: bool,
    pub notice: Option<String>,
}

pub trait Watcher {
    fn app(&self) -> &str;
    fn poll_interval_secs(&self) -> u64;
    fn describe(&self) -> String;
    /// Reads from `cursor` (the saved text, `None` on the first run). Stateless apart from caches.
    async fn step(&mut self, cursor: Option<&str>) -> Result<Step>;
}

/// Settings and cursor-file problems need a person; network problems may pass.
fn is_fatal(e: &LogHavenError) -> bool {
    matches!(e, LogHavenError::Config(_))
}

/// `Ok(None)` only when the file does not exist.
fn read_cursor_text(path: &Path) -> Result<Option<String>> {
    match std::fs::read_to_string(path) {
        Ok(text) => Ok(Some(text)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e.into()),
    }
}

fn write_cursor_text(path: &Path, text: &str) -> Result<()> {
    use std::io::Write;
    let tmp = path.with_extension("cursor.tmp");
    let mut f = std::fs::File::create(&tmp)?;
    f.write_all(text.as_bytes())?;
    f.sync_all()?;
    std::fs::rename(&tmp, path)?;
    Ok(())
}

pub async fn run<W: Watcher>(
    watcher: &mut W,
    cfg: &Config,
    cursor_path: &Path,
    once: bool,
    session_ttl_secs: u64,
) -> Result<()> {
    let app = watcher.app().to_string();
    let interval = Duration::from_secs(watcher.poll_interval_secs());
    style::info(&format!("Watching {}; Ctrl+C to stop", watcher.describe()));
    let mut last_warning = String::new();
    loop {
        let step_fut = async {
            let saved = read_cursor_text(cursor_path)?;
            let step = tokio::select! {
                _ = tokio::signal::ctrl_c() => return Ok(None),
                s = watcher.step(saved.as_deref()) => s?,
            };
            if let Some(notice) = &step.notice {
                style::warning(notice);
            }
            // not cancellable: the cursor must follow a send that has started
            if !step.records.is_empty() {
                let mut client = IngestClient::connect(cfg, &app, session_ttl_secs).await?;
                client.send(&step.records).await?;
                style::info(&format!("Recorded {} event(s)", step.records.len()));
            }
            write_cursor_text(cursor_path, &step.cursor)?;
            Ok::<_, LogHavenError>(Some(step.more))
        };
        let more = match step_fut.await {
            Ok(None) => return Ok(()),
            Ok(Some(more)) => {
                last_warning.clear();
                more
            }
            // the node or daemon may be down for a while; retry, saying each problem once
            Err(e) => {
                if once || is_fatal(&e) {
                    return Err(e);
                }
                let msg = e.to_string();
                if msg != last_warning {
                    style::warning(&msg);
                    last_warning = msg;
                }
                false
            }
        };
        if once && !more {
            return Ok(());
        }
        if !more {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => return Ok(()),
                _ = tokio::time::sleep(interval) => {}
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_errors_stop_the_watch_but_network_errors_retry() {
        assert!(is_fatal(&LogHavenError::Config("x".into())));
        assert!(!is_fatal(&LogHavenError::Daemon("x".into())));
    }

    #[test]
    fn cursor_text_round_trips_and_a_missing_file_is_none() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("x.cursor");
        assert!(read_cursor_text(&p).unwrap().is_none());
        write_cursor_text(&p, "42\nabc\n").unwrap();
        assert_eq!(read_cursor_text(&p).unwrap().as_deref(), Some("42\nabc\n"));
        write_cursor_text(&p, "43\n").unwrap();
        assert_eq!(read_cursor_text(&p).unwrap().as_deref(), Some("43\n"));
        assert!(!p.with_extension("cursor.tmp").exists());
    }

    #[test]
    fn an_unreadable_cursor_path_is_an_error_not_a_first_run() {
        let dir = tempfile::tempdir().unwrap();
        assert!(read_cursor_text(dir.path()).is_err());
    }
}
