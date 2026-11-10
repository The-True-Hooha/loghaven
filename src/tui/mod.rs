//! `loghaven tui`: a live, filterable view of one or more apps' logs.

pub mod fetch;
pub mod model;
pub mod view;

use crate::config::{self, Config};
use crate::error::{LogHavenError, Result};
use model::{Model, Msg};
use ratatui::crossterm::event::{self, Event, KeyEventKind};
use std::io::IsTerminal;
use std::time::Duration;

/// Puts the terminal back however `run` ends, including a panic in the loop.
struct Restore;

impl Drop for Restore {
    fn drop(&mut self) {
        ratatui::restore();
    }
}

pub fn run(apps: Vec<String>, token: Option<String>, profile: Option<&str>) -> Result<()> {
    let mut seen = std::collections::HashSet::new();
    let apps: Vec<String> = apps
        .into_iter()
        .filter(|a| seen.insert(a.clone()))
        .collect();
    if token.is_some() && apps.len() > 1 {
        return Err(LogHavenError::Config(
            "--token is valid for one app; omit it to watch several apps".into(),
        ));
    }
    for app in &apps {
        crate::storage::validate_app_name(app)?;
    }
    if !std::io::stdout().is_terminal() || !std::io::stdin().is_terminal() {
        return Err(LogHavenError::Config(
            "the terminal UI needs an interactive terminal; use 'loghaven logs --follow' in scripts"
                .into(),
        ));
    }
    let cfg = Config::load(&config::get_config_path(profile))?;

    let rt = tokio::runtime::Runtime::new()?;
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let (req_tx, req_rx) = tokio::sync::mpsc::unbounded_channel();
    rt.spawn(fetch::run(cfg, apps.clone(), token, tx, req_rx));

    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        // a kill signal must still put the terminal back, so it ends the loop like `q`
        let _guard = rt.enter();
        for kind in [SignalKind::terminate(), SignalKind::hangup()] {
            if let Ok(mut sig) = signal(kind) {
                let stop = stop.clone();
                rt.spawn(async move {
                    sig.recv().await;
                    stop.store(true, std::sync::atomic::Ordering::SeqCst);
                });
            }
        }
    }

    let mut terminal = ratatui::init();
    let _restore = Restore;
    let mut model = Model::new(apps);
    while !model.quit && !stop.load(std::sync::atomic::Ordering::SeqCst) {
        terminal.draw(|f| view::draw(f, &model))?;
        if event::poll(Duration::from_millis(100))?
            && let Event::Key(key) = event::read()?
            && key.kind == KeyEventKind::Press
            && let Some(request) = model.update(Msg::Key(key))
            && req_tx.send(request).is_err()
        {
            model.note = "background fetching stopped; restart the UI".into();
        }
        loop {
            match rx.try_recv() {
                Ok(msg) => {
                    model.update(msg);
                }
                Err(tokio::sync::mpsc::error::TryRecvError::Empty) => break,
                Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                    model.update(Msg::Status(
                        "background fetching stopped; restart the UI".into(),
                    ));
                    break;
                }
            }
        }
    }
    Ok(())
}
