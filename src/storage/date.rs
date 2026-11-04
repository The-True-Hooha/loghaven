use std::time::{SystemTime, UNIX_EPOCH};

/// Days since 1970-01-01 to `YYYY-MM-DD` (proleptic Gregorian, UTC).
pub fn epoch_days_to_date(days: i64) -> String {
    let z = days + 719468;
    let era = z.div_euclid(146097);
    let doe = z.rem_euclid(146097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    format!("{:04}-{:02}-{:02}", y, m, d)
}

pub fn date_to_epoch_days(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let m = if m <= 2 { m + 9 } else { m - 3 };
    let era = y.div_euclid(400);
    let yoe = y.rem_euclid(400);
    let doy = (153 * m + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

pub fn date_from_ms(ms: i64) -> String {
    epoch_days_to_date(ms.div_euclid(86_400_000))
}

pub fn today_epoch_days() -> i64 {
    let ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);
    ms.div_euclid(86_400_000)
}

pub fn today() -> String {
    let ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);
    date_from_ms(ms)
}

/// Parses a strict `YYYY-MM-DD` that round-trips to itself, else None.
/// Only such names are treated as date directories.
pub fn parse_date(s: &str) -> Option<i64> {
    let b = s.as_bytes();
    if b.len() != 10 || b[4] != b'-' || b[7] != b'-' {
        return None;
    }
    if !b
        .iter()
        .enumerate()
        .all(|(i, c)| i == 4 || i == 7 || c.is_ascii_digit())
    {
        return None;
    }
    let (y, m, d) = (
        s[0..4].parse::<i64>().ok()?,
        s[5..7].parse::<i64>().ok()?,
        s[8..10].parse::<i64>().ok()?,
    );
    let days = date_to_epoch_days(y, m, d);
    (epoch_days_to_date(days) == s).then_some(days)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_dates() {
        assert_eq!(date_from_ms(0), "1970-01-01");
        assert_eq!(date_from_ms(86_399_999), "1970-01-01");
        assert_eq!(date_from_ms(86_400_000), "1970-01-02");
        assert_eq!(date_from_ms(951_782_400_000), "2000-02-29");
        assert_eq!(date_from_ms(1_582_934_400_000), "2020-02-29");
        assert_eq!(date_from_ms(1_583_020_800_000), "2020-03-01");
        assert_eq!(date_from_ms(1_793_836_800_000), "2026-11-05");
        assert_eq!(date_from_ms(-1), "1969-12-31");
    }

    #[test]
    fn century_leap_rules() {
        assert_eq!(
            epoch_days_to_date(date_to_epoch_days(1900, 2, 28) + 1),
            "1900-03-01"
        );
        assert_eq!(
            epoch_days_to_date(date_to_epoch_days(2100, 2, 28) + 1),
            "2100-03-01"
        );
        assert_eq!(
            epoch_days_to_date(date_to_epoch_days(2000, 2, 28) + 1),
            "2000-02-29"
        );
    }

    #[test]
    fn round_trip_over_many_days() {
        for days in (-800_000..800_000).step_by(37) {
            let s = epoch_days_to_date(days);
            if days >= -719_000 {
                assert_eq!(parse_date(&s), Some(days), "{s}");
            }
        }
    }

    #[test]
    fn parse_date_is_strict() {
        assert!(parse_date("2024-02-29").is_some());
        for bad in [
            "2023-02-29",
            "2024-13-01",
            "2024-00-10",
            "2024-01-00",
            "2024-1-01",
            "index",
            "",
            "2024-01-011",
            " 2024-01-01",
            "2024/01/01",
            "+024-01-01",
            "2024-01-0a",
            "20240101",
        ] {
            assert!(parse_date(bad).is_none(), "{bad:?}");
        }
    }

    #[test]
    fn ordering_matches_string_ordering() {
        let a = parse_date("2024-12-31").unwrap();
        let b = parse_date("2025-01-01").unwrap();
        assert_eq!(b - a, 1);
        assert!("2024-12-31" < "2025-01-01");
    }
}
