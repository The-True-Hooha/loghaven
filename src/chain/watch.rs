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

/// How many entries of each backlog to take, for chains that list per address. At
/// most `max` each, and never past the slot (or ledger) where the busiest capped
/// address stops, so a transaction that touches several addresses is taken together
/// in one poll. A slot split by the cap is left for the next poll. When the cap falls
/// inside one slot that fills the whole page, that address takes its page and the
/// others wait until it has finished the slot.
pub fn take_counts<T>(pendings: &[&[T]], max: usize, slot: impl Fn(&T) -> u64) -> Vec<usize> {
    if pendings.len() < 2 {
        return pendings.iter().map(|p| p.len().min(max)).collect();
    }
    // per capped address: where it stops, and whether its whole page is one slot
    let stops: Vec<Option<(u64, bool)>> = pendings
        .iter()
        .map(|p| {
            if p.len() <= max {
                return None;
            }
            let last = slot(&p[max - 1]);
            Some(match p[..max].iter().rposition(|i| slot(i) != last) {
                Some(k) => (slot(&p[k]), false),
                None => (last.saturating_sub(1), true),
            })
        })
        .collect();
    let horizon = stops.iter().flatten().map(|(h, _)| *h).min();
    pendings
        .iter()
        .zip(&stops)
        .map(|(p, stop)| match (horizon, stop) {
            (None, _) => p.len(),
            (Some(h), Some((own, true))) if *own == h => max,
            (Some(h), _) => p.partition_point(|i| slot(i) <= h).min(max),
        })
        .collect()
}

pub trait Watcher {
    fn app(&self) -> &str;
    fn poll_interval_secs(&self) -> u64;
    fn describe(&self) -> String;
    /// Reads from `cursor` (the saved text, `None` on the first run). Stateless apart from caches.
    async fn step(&mut self, cursor: Option<&str>) -> Result<Step>;
    /// Waits before the next poll. A chain may end the wait early when it learns of new
    /// data, but must still return within `interval` so polling never depends on a push.
    async fn wait(&mut self, interval: Duration) {
        tokio::time::sleep(interval).await;
    }
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
    style::info(&format!("Watching {}; Ctrl+C to stop", watcher.describe()));
    let interval = Duration::from_secs(watcher.poll_interval_secs());
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
                _ = watcher.wait(interval) => {}
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

    #[test]
    fn take_counts_stops_at_the_busiest_capped_slot_and_always_progresses() {
        let slot = |s: &u64| *s;
        let a = [1u64, 2, 3, 4, 5];
        assert_eq!(take_counts(&[&a[..], &[5, 6]], 3, slot), [2, 0]);
        assert_eq!(take_counts(&[&a[..], &[1]], 3, slot), [2, 1]);
        assert_eq!(take_counts(&[&[1u64][..]], 3, slot), [1]);
        assert_eq!(take_counts(&[&[7u64; 5][..]], 3, slot), [3]);
        assert_eq!(take_counts(&[&[][..], &[1u64]], 3, slot), [0, 1]);
    }

    #[test]
    fn a_slot_that_fills_a_whole_page_is_finished_before_other_addresses_take_it() {
        let slot = |s: &u64| *s;
        // A's page is all slot 7: A takes its page, B (which has slot 7 too) waits
        assert_eq!(take_counts(&[&[7u64; 5][..], &[7, 8]], 3, slot), [3, 0]);
        // B has older entries: those go now, slot 7 waits
        assert_eq!(take_counts(&[&[7u64; 5][..], &[5, 6, 7]], 3, slot), [3, 2]);
        // two full pages in different slots: the lower one goes, the other waits
        assert_eq!(
            take_counts(&[&[9u64; 5][..], &[7u64; 5][..]], 3, slot),
            [0, 3]
        );
    }
}
