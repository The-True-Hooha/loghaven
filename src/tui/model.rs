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
            "error" | "fatal" => 0,
            "warn" | "warning" => 1,
            "info" => 2,
            _ => 3,
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
    pub hash: String,
    pub records: Vec<LogRecord>,
    pub loading: bool,
}

pub enum Msg {
    Key(KeyEvent),
    Records(Vec<LogRecord>),
    Trace(String, Result<Vec<LogRecord>, String>),
    /// Empty text means the last fetch worked.
    Status(String),
}

/// Something the fetcher must do on the model's behalf.
#[derive(Debug, PartialEq)]
pub enum Request {
    Trace(String),
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
    selected_id: Option<String>,
    pub follow: bool,
    pub detail: bool,
    pub status: String,
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
            selected_id: None,
            follow: true,
            detail: false,
            status: String::new(),
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

    /// Index of the highlighted row in `visible`, if any row exists.
    pub fn cursor(&self, visible: &[&LogRecord]) -> Option<usize> {
        if visible.is_empty() {
            return None;
        }
        if self.follow && self.trace.is_none() {
            return Some(visible.len() - 1);
        }
        self.selected_id
            .as_ref()
            .and_then(|id| visible.iter().position(|r| &r.id == id))
            .or(Some(visible.len() - 1))
    }

    /// Remembers the row at `index` (or, when `None`, the current row) as the highlight.
    fn select(&mut self, index: Option<usize>) {
        let visible = self.visible();
        let id = match index {
            Some(i) => visible.get(i).map(|r| r.id.clone()),
            None => self.cursor(&visible).map(|i| visible[i].id.clone()),
        };
        if id.is_some() {
            self.selected_id = id;
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
        let id = visible[next].id.clone();
        self.selected_id = Some(id);
        // reaching the newest row again resumes following; moving away stops it
        self.follow = delta > 0 && next == last;
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
            Msg::Trace(hash, result) => {
                if let Some(t) = self.trace.as_mut().filter(|t| t.hash == hash) {
                    t.loading = false;
                    match result {
                        Ok(mut recs) => {
                            recs.sort_by(|a, b| key(a).cmp(&key(b)));
                            recs.dedup_by(|a, b| a.id == b.id);
                            t.records = recs;
                            self.selected_id = t.records.first().map(|r| r.id.clone());
                        }
                        Err(e) => self.status = format!("trace failed: {e}"),
                    }
                }
                None
            }
            Msg::Status(s) => {
                self.status = s;
                None
            }
        }
    }

    fn on_key(&mut self, k: KeyEvent) -> Option<Request> {
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
            KeyCode::Up | KeyCode::Char('k') => {
                self.step(-1);
                self.follow = false;
            }
            KeyCode::PageDown => self.step(10),
            KeyCode::PageUp => {
                self.step(-10);
                self.follow = false;
            }
            KeyCode::Char('g') | KeyCode::Home => {
                self.select(Some(0));
                self.follow = false;
            }
            KeyCode::Char('G') | KeyCode::End => {
                self.select_last();
                self.follow = true;
            }
            KeyCode::Char('p') => {
                self.select(None);
                self.follow = !self.follow;
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
                let hash = self.selected().and_then(|r| r.tx_hash.clone());
                match hash {
                    Some(hash) => {
                        self.trace = Some(Trace {
                            hash: hash.clone(),
                            records: vec![],
                            loading: true,
                        });
                        self.status.clear();
                        return Some(Request::Trace(hash));
                    }
                    None => self.status = "selected record has no tx_hash".into(),
                }
            }
            _ => {}
        }
        None
    }
}

#[cfg(test)]
mod tests;
