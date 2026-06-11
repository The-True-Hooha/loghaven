use crate::storage::local::current_date;
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

fn enforce_size_limit(
    data_path: &PathBuf,
    max_size_gb: u64,
    date_dirs: &mut Vec<(String, PathBuf)>,
) {
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
    let parts: Vec<&str> = date.split('-').collect();
    if parts.len() != 3 {
        return date.to_string();
    }
    let (y, m, d) = match (
        parts[0].parse::<i64>(),
        parts[1].parse::<i64>(),
        parts[2].parse::<i64>(),
    ) {
        (Ok(y), Ok(m), Ok(d)) => (y, m, d),
        _ => return date.to_string(),
    };

    let epoch_days = date_to_epoch_days(y, m, d);
    let new_days = epoch_days.saturating_sub(days as i64);
    epoch_days_to_date(new_days)
}

fn date_to_epoch_days(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let m = if m <= 2 { m + 9 } else { m - 3 };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let doy = (153 * m + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

fn epoch_days_to_date(days: i64) -> String {
    let z = days + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = (z - era * 146097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp as i64 + 3 } else { mp as i64 - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    format!("{:04}-{:02}-{:02}", y, m, d)
}
