use crate::storage::date::today as current_date;
use std::path::PathBuf;
use tokio::time::{Duration, interval};

pub async fn run_pruner(data_path: PathBuf, retention_days: u32, max_size_gb: u64) {
    let mut ticker = interval(Duration::from_secs(86400));
    ticker.tick().await;

    loop {
        ticker.tick().await;
        let path = data_path.clone();
        tokio::task::spawn_blocking(move || {
            prune_sync(&path, retention_days, max_size_gb);
        })
        .await
        .ok();
    }
}

fn prune_sync(data_path: &PathBuf, retention_days: u32, max_size_gb: u64) {
    let today = current_date();
    let cutoff = date_subtract_days(&today, retention_days);

    let app_dirs = match std::fs::read_dir(data_path) {
        Ok(d) => d,
        Err(_) => return,
    };

    let mut all_date_dirs: Vec<(String, PathBuf)> = Vec::new();

    for app_entry in app_dirs.flatten() {
        let app_path = app_entry.path();
        if !app_path.is_dir() {
            continue;
        }
        if let Ok(date_dirs) = std::fs::read_dir(&app_path) {
            for date_entry in date_dirs.flatten() {
                let date_path = date_entry.path();
                if !date_path.is_dir() {
                    continue;
                }
                if let Some(date_str) = date_entry.file_name().to_str().map(|s| s.to_string()) {
                    if date_str < cutoff {
                        let _ = std::fs::remove_dir_all(&date_path);
                    } else {
                        all_date_dirs.push((date_str, date_path));
                    }
                }
            }
        }
    }

    enforce_size_limit(data_path, max_size_gb, &mut all_date_dirs);
}

fn enforce_size_limit(data_path: &PathBuf, max_size_gb: u64, date_dirs: &mut [(String, PathBuf)]) {
    let max_bytes = max_size_gb * 1024 * 1024 * 1024;
    let total = dir_size(data_path);

    if total <= max_bytes {
        return;
    }

    date_dirs.sort_by(|a, b| a.0.cmp(&b.0));

    let mut current = total;
    for (_, path) in date_dirs.iter() {
        if current <= max_bytes {
            break;
        }
        let size = dir_size(path);
        if std::fs::remove_dir_all(path).is_ok() {
            current = current.saturating_sub(size);
        }
    }
}

fn dir_size(path: &PathBuf) -> u64 {
    walkdir(path)
}

fn walkdir(path: &PathBuf) -> u64 {
    let mut total = 0u64;
    if let Ok(entries) = std::fs::read_dir(path) {
        for entry in entries.flatten() {
            let p = entry.path();
            if p.is_file() {
                total += std::fs::metadata(&p).map(|m| m.len()).unwrap_or(0);
            } else if p.is_dir() {
                total += walkdir(&p);
            }
        }
    }
    total
}

fn date_subtract_days(date: &str, days: u32) -> String {
    use crate::storage::date::{epoch_days_to_date, parse_date};
    match parse_date(date) {
        Some(d) => epoch_days_to_date(d.saturating_sub(days as i64)),
        None => date.to_string(),
    }
}
