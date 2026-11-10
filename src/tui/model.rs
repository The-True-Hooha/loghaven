//! The terminal UI's state and how it reacts to keys and fetched data. No I/O here,
//! so every behavior is testable without a terminal.

use crate::storage::record::LogRecord;
use ratatui::crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

/// Records kept in memory; the oldest are dropped past this.
pub const MAX_RECORDS: usize = 5_000;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum MinLevel {
    All,
    Error,
    Warn,
    Info,
}

impl MinLevel {
    fn next(self) -> Self {
        match self {
            Self::All => Self::Error,
            Self::Error => Self::Warn,
            Self::Warn => Self::Info,
            Self::Info => Self::All,
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::All => "all",
            Self::Error => "error",
            Self::Warn => "warn+",
            Self::Info => "info+",
        }
    }

    fn admits(self, level: &str) -> bool {
        let rank = match level.to_ascii_lowercase().as_str() {
            "error" | "err" | "fatal" | "critical" | "crit" | "panic" | "emerg" | "alert" => 0,
            "warn" | "warning" => 1,
            "debug" | "trace" => 3,
            // "info", "notice" and anything unknown: shown unless only errors or warnings are wanted
            _ => 2,
        };
        match self {
            Self::All => true,
            Self::Error => rank == 0,
            Self::Warn => rank <= 1,
            Self::Info => rank <= 2,
        }
    }
}

/// All records that carry one transaction hash, fetched from the daemon.
pub struct Trace {
    pub id: u64,
    pub hash: String,
    pub records: Vec<LogRecord>,
    pub loading: bool,
    pub error: Option<String>,
    selected: Option<Pos>,
}

/// Where the highlight sits: the record's id, plus its sort key so the highlight can
/// settle on a neighbour when that record is dropped or filtered out.
#[derive(Clone, Debug)]
struct Pos {
    ts: i64,
    id: String,
}

impl Pos {
    fn of(r: &LogRecord) -> Self {
        Self {
            ts: r.timestamp_ms,
            id: r.id.clone(),
        }
    }
}

/// Records and fetch notes from one trace request.
pub type TraceResult = Result<(Vec<LogRecord>, Option<String>), String>;

pub enum Msg {
    Key(KeyEvent),
    Records(Vec<LogRecord>),
    Trace(u64, TraceResult),
    /// Problem with polling; empty text means the last poll worked.
    Status(String),
}

/// Something the fetcher must do on the model's behalf.
#[derive(Debug, PartialEq)]
pub enum Request {
    Trace(u64, String),
}

pub struct Model {
    pub apps: Vec<String>,
    /// Oldest first, by (timestamp, id).
    pub records: Vec<LogRecord>,
    pub min_level: MinLevel,
    pub text: String,
    /// `Some` while the text filter is being typed.
    pub input: Option<String>,
    pub trace: Option<Trace>,
    selected: Option<Pos>,
    pub follow: bool,
    pub detail: bool,
    /// Polling problem reported by the fetcher; stays until it changes.
    pub poll_error: String,
    /// Answer to the last key press; cleared by the next one.
    pub note: String,
    next_trace: u64,
    pub quit: bool,
}

fn key(r: &LogRecord) -> (i64, &str) {
    (r.timestamp_ms, r.id.as_str())
}

impl Model {
    pub fn new(apps: Vec<String>) -> Self {
        Self {
            apps,
            records: vec![],
            min_level: MinLevel::All,
            text: String::new(),
            input: None,
            trace: None,
            selected: None,
            follow: true,
            detail: false,
            poll_error: String::new(),
            note: String::new(),
            next_trace: 0,
            quit: false,
        }
    }

    fn matches(&self, r: &LogRecord) -> bool {
        if !self.min_level.admits(&r.level) {
            return false;
        }
        if self.text.is_empty() {
            return true;
        }
        let needle = self.text.to_lowercase();
        [
            Some(r.message.as_str()),
            Some(r.source.as_str()),
            Some(r.app.as_str()),
            r.tx_hash.as_deref(),
            r.trace_id.as_deref(),
            r.chain.as_deref(),
            r.tags.as_deref(),
        ]
        .into_iter()
        .flatten()
        .any(|f| f.to_lowercase().contains(&needle))
    }

    /// What the list shows: the trace when one is open, else the live buffer, filtered.
    pub fn visible(&self) -> Vec<&LogRecord> {
        let source = match &self.trace {
            Some(t) => &t.records,
            None => &self.records,
        };
        source.iter().filter(|r| self.matches(r)).collect()
    }

    fn highlight(&self) -> Option<&Pos> {
        match &self.trace {
            Some(t) => t.selected.as_ref(),
            None => self.selected.as_ref(),
        }
    }

    fn set_highlight(&mut self, pos: Option<Pos>) {
        match self.trace.as_mut() {
            Some(t) => t.selected = pos,
            None => self.selected = pos,
        }
    }

    /// Following only applies to the live list; a trace is read in place.
    pub fn following(&self) -> bool {
        self.follow && self.trace.is_none()
    }

    /// Index of the highlighted row in `visible`, if any row exists. A highlight whose
    /// record is gone (dropped by the cap, hidden by a filter) settles on the nearest
    /// row that follows it in time.
    pub fn cursor(&self, visible: &[&LogRecord]) -> Option<usize> {
        if visible.is_empty() {
            return None;
        }
        if self.following() {
            return Some(visible.len() - 1);
        }
        let Some(pos) = self.highlight() else {
            return Some(visible.len() - 1);
        };
        Some(
            visible
                .iter()
                .position(|r| r.id == pos.id)
                .unwrap_or_else(|| {
                    visible
                        .partition_point(|r| key(r) < (pos.ts, pos.id.as_str()))
                        .min(visible.len() - 1)
                }),
        )
    }

    /// Remembers the row at `index` (or, when `None`, the current row) as the highlight.
    fn select(&mut self, index: Option<usize>) {
        let visible = self.visible();
        let pos = match index {
            Some(i) => visible.get(i).map(|r| Pos::of(r)),
            None => self.cursor(&visible).map(|i| Pos::of(visible[i])),
        };
        if pos.is_some() {
            self.set_highlight(pos);
        }
    }

    fn select_last(&mut self) {
        let n = self.visible().len();
        self.select(n.checked_sub(1));
    }

    fn step(&mut self, delta: isize) {
        let visible = self.visible();
        let Some(cur) = self.cursor(&visible) else {
            return;
        };
        let last = visible.len() - 1;
        let next = cur.saturating_add_signed(delta).min(last);
        let pos = Pos::of(visible[next]);
        self.set_highlight(Some(pos));
        // reaching the newest row again resumes following; moving away stops it
        if self.trace.is_none() {
            self.follow = delta > 0 && next == last;
        }
    }

    pub fn selected(&self) -> Option<&LogRecord> {
        let visible = self.visible();
        let i = self.cursor(&visible)?;
        let id = visible[i].id.clone();
        self.trace
            .as_ref()
            .map_or(&self.records, |t| &t.records)
            .iter()
            .find(|r| r.id == id)
    }

    fn insert(&mut self, rec: LogRecord) {
        let at = self.records.partition_point(|r| key(r) < key(&rec));
        if self.records.get(at).is_some_and(|r| r.id == rec.id) {
            return;
        }
        self.records.insert(at, rec);
        if self.records.len() > MAX_RECORDS {
            let drop = self.records.len() - MAX_RECORDS;
            self.records.drain(..drop);
        }
    }

    pub fn update(&mut self, msg: Msg) -> Option<Request> {
        match msg {
            Msg::Key(k) => self.on_key(k),
            Msg::Records(batch) => {
                for r in batch {
                    self.insert(r);
                }
                None
            }
            Msg::Trace(id, result) => {
                if let Some(t) = self.trace.as_mut().filter(|t| t.id == id) {
                    t.loading = false;
                    match result {
                        Ok((mut recs, note)) => {
                            recs.sort_by(|a, b| key(a).cmp(&key(b)));
                            recs.dedup_by(|a, b| a.id == b.id);
                            t.selected = recs.first().map(Pos::of);
                            t.records = recs;
                            if let Some(n) = note {
                                self.note = n;
                            }
                        }
                        Err(e) => t.error = Some(e),
                    }
                }
                None
            }
            Msg::Status(s) => {
                self.poll_error = s;
                None
            }
        }
    }

    fn on_key(&mut self, k: KeyEvent) -> Option<Request> {
        self.note.clear();
        let ctrl = k.modifiers.contains(KeyModifiers::CONTROL);
        if ctrl && k.code == KeyCode::Char('c') {
            self.quit = true;
            return None;
        }
        if let Some(input) = self.input.as_mut() {
            match k.code {
                KeyCode::Enter => {
                    self.text = self.input.take().unwrap_or_default();
                }
                KeyCode::Esc => self.input = None,
                KeyCode::Backspace => {
                    input.pop();
                }
                KeyCode::Char('u') if ctrl => input.clear(),
                KeyCode::Char(c) if !ctrl && !c.is_control() => input.push(c),
                _ => {}
            }
            return None;
        }
        match k.code {
            KeyCode::Char('q') => self.quit = true,
            KeyCode::Down | KeyCode::Char('j') => self.step(1),
            KeyCode::Up | KeyCode::Char('k') => self.step(-1),
            KeyCode::PageDown => self.step(10),
            KeyCode::PageUp => self.step(-10),
            KeyCode::Char('g') | KeyCode::Home => {
                self.select(Some(0));
                if self.trace.is_none() {
                    self.follow = false;
                }
            }
            KeyCode::Char('G') | KeyCode::End => {
                self.select_last();
                if self.trace.is_none() {
                    self.follow = true;
                }
            }
            KeyCode::Char('p') => {
                if self.trace.is_none() {
                    self.select(None);
                    self.follow = !self.follow;
                }
            }
            KeyCode::Char('/') => self.input = Some(self.text.clone()),
            KeyCode::Char('l') => self.min_level = self.min_level.next(),
            KeyCode::Enter => self.detail = !self.detail,
            KeyCode::Esc => {
                if self.trace.is_some() {
                    self.trace = None;
                } else {
                    self.text.clear();
                }
            }
            KeyCode::Char('t') => {
                if self.trace.as_ref().is_some_and(|t| t.loading) {
                    self.note = "a trace is still loading".into();
                    return None;
                }
                let hash = self
                    .selected()
                    .and_then(|r| r.tx_hash.clone())
                    .filter(|h| !h.is_empty());
                match hash {
                    Some(hash) => {
                        self.next_trace += 1;
                        self.trace = Some(Trace {
                            id: self.next_trace,
                            hash: hash.clone(),
                            records: vec![],
                            loading: true,
                            error: None,
                            selected: None,
                        });
                        return Some(Request::Trace(self.next_trace, hash));
                    }
                    None => self.note = "selected record has no tx_hash".into(),
                }
            }
            _ => {}
        }
        None
    }
}

#[cfg(test)]
mod tests;
