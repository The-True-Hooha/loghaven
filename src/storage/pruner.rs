use crate::storage::date::{parse_date, today_epoch_days};
use std::path::{Path, PathBuf};
use tokio::time::{Duration, interval};

const PRUNE_INTERVAL: Duration = Duration::from_secs(3600);
/// A day directory touched more recently than this is left alone: a writer may still be flushing into it.
const MIN_AGE: Duration = Duration::from_secs(3600);

#[derive(Debug, Default, PartialEq)]
pub struct PruneReport {
    pub removed: Vec<PathBuf>,
    /// Bytes still above the size limit after evicting everything evictable.
    pub bytes_over_limit: u64,
}

/// Prunes once at startup and then hourly. A limit of 0 disables that limit.
pub async fn run_pruner(
    data_path: PathBuf,
    retention_days: u32,
    max_size_gb: u64,
    require_synced: bool,
) {
    let max_bytes = max_size_gb.saturating_mul(1024 * 1024 * 1024);
    let mut ticker = interval(PRUNE_INTERVAL);

    loop {
        ticker.tick().await;
        let path = data_path.clone();
        let report = tokio::task::spawn_blocking(move || {
            prune_once(
                &path,
                today_epoch_days(),
                retention_days,
                max_bytes,
                require_synced,
                MIN_AGE,
            )
        })
        .await;
        match report {
            Ok(r) => {
                for dir in &r.removed {
                    println!("Pruned {}", dir.display());
                }
                if r.bytes_over_limit > 0 {
                    eprintln!(
                        "Pruner: {} bytes over max_size_gb, only today's data is left to evict",
                        r.bytes_over_limit
                    );
                }
            }
            Err(e) => eprintln!("Pruner task failed: {}", e),
        }
    }
}

/// Removes `<data>/<app>/<YYYY-MM-DD>` directories.
///
/// 1. Anything older than `retention_days` days before `today` (0 = keep forever).
/// 2. If the data directory is still above `max_bytes` (0 = no limit), the
///    oldest remaining date directories, across all apps.
///
/// Today's and future date directories are never removed, and neither is
/// anything that is not a real directory named like a date (the search index,
/// stray files, symlinks).
pub fn prune_once(
    data_path: &Path,
    today: i64,
    retention_days: u32,
    max_bytes: u64,
    require_synced: bool,
    min_age: Duration,
) -> PruneReport {
    let mut report = PruneReport::default();
    let mut dirs = date_dirs(data_path);
    // With a cloud backend, a day holding chunks that were never uploaded is the only copy.
    dirs.retain(|(_, p)| !is_recent(p, min_age) && !(require_synced && has_unsynced(p)));

    if retention_days > 0 {
        let cutoff = today.saturating_sub(retention_days as i64);
        dirs.retain(|(day, path)| {
            if *day < cutoff {
                remove(path, &mut report);
                false
            } else {
                true
            }
        });
    }

    if max_bytes > 0 {
        let mut total = dir_size(data_path);
        dirs.sort();
        for (day, path) in &dirs {
            if total <= max_bytes {
                break;
            }
            if *day >= today {
                break; // sorted: everything after is today or later
            }
            let size = dir_size(path);
            if remove(path, &mut report) {
                total = total.saturating_sub(size);
            }
        }
        report.bytes_over_limit = total.saturating_sub(max_bytes);
    }

    report
}

fn is_recent(path: &Path, min_age: Duration) -> bool {
    std::fs::metadata(path)
        .and_then(|m| m.modified())
        .ok()
        .and_then(|t| t.elapsed().ok())
        .is_some_and(|age| age < min_age)
}

fn has_unsynced(day_dir: &Path) -> bool {
    let Ok(files) = std::fs::read_dir(day_dir) else {
        return true; // unreadable: do not risk deleting it
    };
    files.flatten().any(|f| {
        let p = f.path();
        p.extension().and_then(|e| e.to_str()) == Some("parquet")
            && !crate::storage::cloud::marker_path(&p).exists()
    })
}

fn remove(path: &Path, report: &mut PruneReport) -> bool {
    match std::fs::remove_dir_all(path) {
        Ok(()) => {
            report.removed.push(path.to_path_buf());
            true
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => true,
        Err(e) => {
            eprintln!("Pruner: cannot remove {}: {}", path.display(), e);
            false
        }
    }
}

/// `(epoch day, path)` of every real `YYYY-MM-DD` directory under an app directory.
fn date_dirs(data_path: &Path) -> Vec<(i64, PathBuf)> {
    let mut out = Vec::new();
    let Ok(apps) = std::fs::read_dir(data_path) else {
        return out;
    };
    for app in apps.flatten() {
        if !app.file_type().is_ok_and(|t| t.is_dir()) {
            continue;
        }
        let Ok(dates) = std::fs::read_dir(app.path()) else {
            continue;
        };
        for d in dates.flatten() {
            if !d.file_type().is_ok_and(|t| t.is_dir()) {
                continue;
            }
            if let Some(day) = d.file_name().to_str().and_then(parse_date) {
                out.push((day, d.path()));
            }
        }
    }
    out
}

/// Total size of regular files below `path`; symlinks are not followed.
fn dir_size(path: &Path) -> u64 {
    let mut total = 0u64;
    let Ok(entries) = std::fs::read_dir(path) else {
        return 0;
    };
    for entry in entries.flatten() {
        let Ok(kind) = entry.file_type() else {
            continue;
        };
        if kind.is_dir() {
            total += dir_size(&entry.path());
        } else if kind.is_file() {
            total += entry.metadata().map(|m| m.len()).unwrap_or(0);
        }
    }
    total
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::date::date_to_epoch_days;

    const TODAY: i64 = 20_000; // 2024-10-04

    fn day(offset: i64) -> String {
        crate::storage::date::epoch_days_to_date(TODAY + offset)
    }

    struct Fixture(PathBuf);

    impl Fixture {
        fn new(name: &str) -> Self {
            let p = std::env::temp_dir().join(format!("lh-p-{}-{}", name, std::process::id()));
            let _ = std::fs::remove_dir_all(&p);
            std::fs::create_dir_all(&p).unwrap();
            Self(p)
        }

        fn file(&self, rel: &str, bytes: usize) -> PathBuf {
            let p = self.0.join(rel);
            std::fs::create_dir_all(p.parent().unwrap()).unwrap();
            std::fs::write(&p, vec![0u8; bytes]).unwrap();
            p
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn retention_removes_only_older_date_dirs() {
        let fx = Fixture::new("ret");
        let old = fx.file(&format!("web/{}/a.parquet", day(-31)), 10);
        let edge = fx.file(&format!("web/{}/a.parquet", day(-30)), 10);
        let today = fx.file(&format!("web/{}/a.parquet", day(0)), 10);
        let other = fx.file(&format!("api/{}/a.parquet", day(-100)), 10);
        let r = prune_once(&fx.0, TODAY, 30, 0, false, Duration::ZERO);
        assert!(!old.exists() && !other.exists());
        assert!(edge.exists() && today.exists(), "cutoff day itself is kept");
        assert_eq!(r.removed.len(), 2);
    }

    #[test]
    fn non_date_entries_are_never_touched() {
        let fx = Fixture::new("junk");
        let index = fx.file("web/index/meta.json", 100);
        let stray = fx.file("web/notes.txt", 100);
        let bad = fx.file("web/2000-13-45/a.parquet", 100);
        let top = fx.file("loose.txt", 100);
        let oldname = fx.file("web/1999-01-01x/a", 100);
        prune_once(&fx.0, TODAY, 1, 1, false, Duration::ZERO);
        for p in [index, stray, bad, top, oldname] {
            assert!(p.exists(), "{}", p.display());
        }
    }

    #[test]
    fn zero_disables_each_limit() {
        let fx = Fixture::new("zero");
        let old = fx.file(&format!("web/{}/a.parquet", day(-5000)), 1000);
        let r = prune_once(&fx.0, TODAY, 0, 0, false, Duration::ZERO);
        assert!(old.exists());
        assert_eq!(r, PruneReport::default());
    }

    #[test]
    fn size_limit_evicts_oldest_first_across_apps_and_stops_when_under() {
        let fx = Fixture::new("size");
        let a = fx.file(&format!("web/{}/a", day(-3)), 100);
        let b = fx.file(&format!("api/{}/a", day(-2)), 100);
        let c = fx.file(&format!("web/{}/a", day(-1)), 100);
        let t = fx.file(&format!("web/{}/a", day(0)), 100);
        let r = prune_once(&fx.0, TODAY, 0, 250, false, Duration::ZERO);
        assert!(!a.exists() && !b.exists(), "two oldest evicted");
        assert!(c.exists() && t.exists());
        assert_eq!(r.bytes_over_limit, 0);
    }

    #[test]
    fn today_and_future_are_protected_and_overage_is_reported() {
        let fx = Fixture::new("protect");
        let t = fx.file(&format!("web/{}/a", day(0)), 500);
        let f = fx.file(&format!("web/{}/a", day(3)), 500);
        let old = fx.file(&format!("web/{}/a", day(-1)), 100);
        let r = prune_once(&fx.0, TODAY, 0, 200, false, Duration::ZERO);
        assert!(t.exists() && f.exists());
        assert!(!old.exists());
        assert_eq!(r.bytes_over_limit, 1000 - 200);
    }

    #[test]
    fn index_counts_toward_size_but_is_never_evicted() {
        let fx = Fixture::new("index");
        let index = fx.file("web/index/segment", 1000);
        let old = fx.file(&format!("web/{}/a", day(-1)), 100);
        let r = prune_once(&fx.0, TODAY, 0, 500, false, Duration::ZERO);
        assert!(index.exists());
        assert!(!old.exists());
        assert_eq!(r.bytes_over_limit, 500);
    }

    #[test]
    fn missing_data_dir_is_a_noop() {
        let fx = Fixture::new("missing");
        let r = prune_once(&fx.0.join("nope"), TODAY, 30, 1, false, Duration::ZERO);
        assert_eq!(r, PruneReport::default());
    }

    #[cfg(unix)]
    #[test]
    fn symlinked_date_dirs_are_not_followed_or_removed() {
        let fx = Fixture::new("link");
        let outside = Fixture::new("link-outside");
        let victim = outside.file("keep.txt", 10);
        std::fs::create_dir_all(fx.0.join("web")).unwrap();
        let link = fx.0.join("web").join(day(-500));
        std::os::unix::fs::symlink(&outside.0, &link).unwrap();
        prune_once(&fx.0, TODAY, 1, 1, false, Duration::ZERO);
        assert!(victim.exists());
        assert!(link.symlink_metadata().is_ok());
    }

    #[test]
    fn works_with_real_date_math() {
        let today = date_to_epoch_days(2024, 3, 1);
        let fx = Fixture::new("math");
        let kept = fx.file("web/2024-02-01/a", 1); // exactly 29 days before: kept at 29
        let gone = fx.file("web/2024-01-31/a", 1);
        prune_once(&fx.0, today, 29, 0, false, Duration::ZERO);
        assert!(kept.exists() && !gone.exists());
    }

    #[test]
    fn unsynced_days_are_kept_when_cloud_sync_is_configured() {
        let fx = Fixture::new("unsynced");
        let synced = fx.file(&format!("web/{}/a.parquet", day(-50)), 10);
        fx.file(&format!("web/{}/a.parquet.synced", day(-50)), 0);
        let pending = fx.file(&format!("web/{}/a.parquet", day(-60)), 10);
        let mixed = fx.file(&format!("web/{}/a.parquet", day(-70)), 10);
        fx.file(&format!("web/{}/b.parquet", day(-70)), 10);
        fx.file(&format!("web/{}/b.parquet.synced", day(-70)), 0);
        prune_once(&fx.0, TODAY, 30, 1, true, Duration::ZERO);
        assert!(!synced.exists(), "fully synced day is pruned");
        assert!(pending.exists() && mixed.exists(), "unsynced data survives");
        prune_once(&fx.0, TODAY, 30, 0, false, Duration::ZERO);
        assert!(!pending.exists(), "no cloud: nothing to wait for");
    }

    #[test]
    fn recently_modified_days_are_left_alone() {
        let fx = Fixture::new("recent");
        let p = fx.file(&format!("web/{}/a.parquet", day(-60)), 10);
        prune_once(&fx.0, TODAY, 30, 1, false, Duration::from_secs(3600));
        assert!(p.exists());
    }
}
