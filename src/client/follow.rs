use crate::storage::record::LogRecord;
use std::collections::HashSet;

/// Turns repeated "newest first" query results into a stream of new records.
///
/// Records get their timestamp when the daemon receives them but only become
/// queryable after the next flush, so a record can show up with a timestamp
/// older than ones already seen. Each poll therefore re-queries a lookback
/// window and drops records already printed (by id).
pub struct FollowState {
    lookback_ms: i64,
    newest_ts: i64,
    seen: HashSet<String>,
    seen_ts: Vec<(String, i64)>,
}

impl FollowState {
    pub fn new(start_ms: i64, lookback_ms: i64) -> Self {
        Self {
            lookback_ms,
            newest_ts: start_ms,
            seen: HashSet::new(),
            seen_ts: Vec::new(),
        }
    }

    /// `from_ms` to use for the next poll.
    pub fn next_from_ms(&self) -> i64 {
        self.newest_ts.saturating_sub(self.lookback_ms)
    }

    /// Returns the records not seen before, oldest first.
    pub fn accept(&mut self, newest_first: Vec<LogRecord>) -> Vec<LogRecord> {
        let mut fresh: Vec<LogRecord> = newest_first
            .into_iter()
            .filter(|r| self.seen.insert(r.id.clone()))
            .collect();
        fresh.reverse();
        for r in &fresh {
            self.newest_ts = self.newest_ts.max(r.timestamp_ms);
            self.seen_ts.push((r.id.clone(), r.timestamp_ms));
        }
        // ids older than the window can never be returned again
        let floor = self.next_from_ms();
        let seen = &mut self.seen;
        self.seen_ts.retain(|(id, ts)| {
            let keep = *ts >= floor;
            if !keep {
                seen.remove(id);
            }
            keep
        });
        fresh
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn r(id: &str, ts: i64) -> LogRecord {
        let mut r = LogRecord::new("web", "info", "s", id);
        r.id = id.into();
        r.timestamp_ms = ts;
        r
    }

    fn ids(v: &[LogRecord]) -> Vec<&str> {
        v.iter().map(|r| r.id.as_str()).collect()
    }

    #[test]
    fn new_records_come_out_oldest_first_exactly_once() {
        let mut f = FollowState::new(0, 1000);
        assert_eq!(
            ids(&f.accept(vec![r("c", 30), r("b", 20), r("a", 10)])),
            ["a", "b", "c"]
        );
        assert!(f.accept(vec![r("c", 30), r("b", 20)]).is_empty());
        assert_eq!(ids(&f.accept(vec![r("d", 40), r("c", 30)])), ["d"]);
    }

    #[test]
    fn late_flushed_record_with_an_older_timestamp_is_still_shown() {
        let mut f = FollowState::new(0, 1000);
        f.accept(vec![r("b", 500)]);
        assert_eq!(f.next_from_ms(), -500, "lookback keeps the window open");
        assert_eq!(ids(&f.accept(vec![r("b", 500), r("a", 400)])), ["a"]);
    }

    #[test]
    fn window_advances_and_memory_is_bounded() {
        let mut f = FollowState::new(0, 100);
        for i in 0..1000 {
            f.accept(vec![r(&format!("id{i}"), i * 10)]);
        }
        assert_eq!(f.next_from_ms(), 9990 - 100);
        assert!(f.seen.len() <= 12, "{}", f.seen.len());
        assert_eq!(f.seen.len(), f.seen_ts.len());
    }

    #[test]
    fn empty_poll_changes_nothing() {
        let mut f = FollowState::new(5_000, 1000);
        assert!(f.accept(vec![]).is_empty());
        assert_eq!(f.next_from_ms(), 4_000);
    }
}
