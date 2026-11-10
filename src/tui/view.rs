//! Draws the model. Log content is untrusted, so every string is cleaned of
//! control and bidirectional-override characters before it reaches the terminal.

use super::model::Model;
use crate::storage::record::LogRecord;
use ratatui::Frame;
use ratatui::layout::{Constraint, Layout};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph, Wrap};

const DETAIL_HEIGHT: u16 = 12;

/// Characters that move the cursor, reorder text, break lines or have no width.
fn is_unsafe(c: char) -> bool {
    c.is_control()
        || matches!(
            c,
            '\u{200b}'..='\u{200f}'
                | '\u{2028}'..='\u{202e}'
                | '\u{2060}'..='\u{2069}'
                | '\u{061c}'
                | '\u{feff}'
                | '\u{fe00}'..='\u{fe0f}'
        )
}

fn is_combining(c: char) -> bool {
    matches!(c, '\u{0300}'..='\u{036f}' | '\u{1ab0}'..='\u{1aff}' | '\u{1dc0}'..='\u{1dff}' | '\u{20d0}'..='\u{20ff}' | '\u{fe20}'..='\u{fe2f}')
}

/// Control characters would let a log line move the cursor or rewrite the screen;
/// bidirectional overrides would let it reorder what the operator reads; long runs of
/// combining marks would smear over neighbouring rows.
pub fn clean(s: &str) -> String {
    let mut run = 0;
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        if is_unsafe(c) {
            out.push('·');
            run = 0;
        } else if is_combining(c) {
            run += 1;
            if run <= 2 {
                out.push(c);
            }
        } else {
            run = 0;
            out.push(c);
        }
    }
    out
}

/// `HH:MM:SS.mmm` in UTC.
pub fn clock(ms: i64) -> String {
    let day = ms.rem_euclid(86_400_000);
    format!(
        "{:02}:{:02}:{:02}.{:03}",
        day / 3_600_000,
        day / 60_000 % 60,
        day / 1000 % 60,
        day % 1000
    )
}

fn level_style(level: &str) -> Style {
    match level.to_ascii_lowercase().as_str() {
        "error" | "fatal" => Style::new().fg(Color::Red),
        "warn" | "warning" => Style::new().fg(Color::Yellow),
        "debug" | "trace" => Style::new().fg(Color::DarkGray),
        _ => Style::new(),
    }
}

fn row(r: &LogRecord, show_app: bool) -> Line<'static> {
    let mut spans = vec![
        Span::styled(clock(r.timestamp_ms), Style::new().fg(Color::DarkGray)),
        Span::raw(" "),
        Span::styled(format!("{:<5}", clean(&r.level)), level_style(&r.level)),
        Span::raw(" "),
    ];
    if show_app {
        spans.push(Span::styled(
            format!("{} ", clean(&r.app)),
            Style::new().fg(Color::Cyan),
        ));
    }
    spans.push(Span::styled(
        format!("{} ", clean(&r.source)),
        Style::new().fg(Color::Blue),
    ));
    spans.push(Span::raw(clean(&r.message)));
    if let Some(tx) = &r.tx_hash {
        spans.push(Span::styled(
            format!("  tx={}", clean(tx)),
            Style::new().fg(Color::Magenta),
        ));
    }
    Line::from(spans)
}

fn detail(r: &LogRecord) -> Vec<Line<'static>> {
    let mut out = vec![];
    let mut field = |name: &str, value: &str| {
        out.push(Line::from(vec![
            Span::styled(format!("{name:<9}"), Style::new().fg(Color::DarkGray)),
            Span::raw(clean(value)),
        ]));
    };
    field("id", &r.id);
    field(
        "time",
        &format!("{} ({} ms)", clock(r.timestamp_ms), r.timestamp_ms),
    );
    field("app", &r.app);
    field("level", &r.level);
    field("source", &r.source);
    field("message", &r.message);
    for (name, v) in [
        ("chain", &r.chain),
        ("tx_hash", &r.tx_hash),
        ("trace_id", &r.trace_id),
        ("span_id", &r.span_id),
        ("tags", &r.tags),
        ("metadata", &r.metadata),
    ] {
        if let Some(v) = v {
            field(name, v);
        }
    }
    out
}

fn header(m: &Model) -> Line<'static> {
    let mut spans = vec![Span::styled(
        " loghaven ",
        Style::new().add_modifier(Modifier::REVERSED),
    )];
    spans.push(Span::raw(format!(" {} ", m.apps.join(","))));
    spans.push(Span::styled(
        format!("level:{} ", m.min_level.label()),
        Style::new().fg(Color::Yellow),
    ));
    if !m.text.is_empty() {
        spans.push(Span::styled(
            format!("text:{} ", clean(&m.text)),
            Style::new().fg(Color::Yellow),
        ));
    }
    match &m.trace {
        Some(t) if t.loading => spans.push(Span::styled(
            format!("TRACE {} (loading) ", clean(&t.hash)),
            Style::new().fg(Color::Magenta),
        )),
        Some(t) => spans.push(Span::styled(
            format!("TRACE {} ", clean(&t.hash)),
            Style::new().fg(Color::Magenta),
        )),
        None if m.following() => {
            spans.push(Span::styled("following ", Style::new().fg(Color::Green)))
        }
        None => spans.push(Span::styled("paused ", Style::new().fg(Color::Yellow))),
    }
    Line::from(spans)
}

fn footer(m: &Model) -> Line<'static> {
    if let Some(input) = &m.input {
        return Line::from(vec![
            Span::styled("filter: ", Style::new().fg(Color::Yellow)),
            Span::raw(clean(input)),
            Span::styled("▏", Style::new().add_modifier(Modifier::SLOW_BLINK)),
            Span::styled(
                "  Enter apply  Esc cancel",
                Style::new().fg(Color::DarkGray),
            ),
        ]);
    }
    if !m.note.is_empty() {
        return Line::from(Span::styled(clean(&m.note), Style::new().fg(Color::Yellow)));
    }
    if !m.poll_error.is_empty() {
        return Line::from(Span::styled(
            clean(&m.poll_error),
            Style::new().fg(Color::Red),
        ));
    }
    Line::from(Span::styled(
        "q quit  j/k move  g/G top/newest  p pause  / filter  l level  t trace tx  Enter detail  Esc back",
        Style::new().fg(Color::DarkGray),
    ))
}

pub fn draw(frame: &mut Frame, m: &Model) {
    let area = frame.area();
    let detail_h = if m.detail { DETAIL_HEIGHT } else { 0 };
    let [head, list, det, foot] = Layout::vertical([
        Constraint::Length(1),
        Constraint::Min(1),
        Constraint::Length(detail_h.min(area.height.saturating_sub(3))),
        Constraint::Length(1),
    ])
    .areas(area);
    frame.render_widget(Paragraph::new(header(m)), head);

    let visible = m.visible();
    let cursor = m.cursor(&visible);
    let show_app = m.apps.len() > 1;
    let rows = list.height as usize;
    let start = match cursor {
        Some(c) if rows > 0 => (c + 1)
            .saturating_sub(rows)
            .min(visible.len().saturating_sub(rows)),
        _ => 0,
    };
    let lines: Vec<Line> = visible
        .iter()
        .enumerate()
        .skip(start)
        .take(rows)
        .map(|(i, r)| {
            let mut line = row(r, show_app);
            if Some(i) == cursor {
                line = line.style(Style::new().add_modifier(Modifier::REVERSED));
            }
            line
        })
        .collect();
    if lines.is_empty() {
        let failed;
        let msg = match &m.trace {
            Some(t) if t.loading => "loading trace…",
            Some(t) if t.error.is_some() => {
                failed = format!("trace failed: {}", clean(t.error.as_deref().unwrap_or("")));
                failed.as_str()
            }
            Some(_) if m.text.is_empty() && m.min_level == super::model::MinLevel::All => {
                "no records carry this transaction hash"
            }
            Some(_) => "no trace records match the current filters (Esc leaves the trace)",
            None => "no records yet (new ones can take up to the flush interval to appear)",
        };
        frame.render_widget(
            Paragraph::new(Span::styled(msg, Style::new().fg(Color::DarkGray))),
            list,
        );
    } else {
        frame.render_widget(Paragraph::new(lines), list);
    }

    if det.height > 0 {
        let body = m.selected().map(detail).unwrap_or_default();
        frame.render_widget(
            Paragraph::new(body)
                .wrap(Wrap { trim: false })
                .block(Block::default().borders(Borders::TOP)),
            det,
        );
    }
    frame.render_widget(Paragraph::new(footer(m)), foot);
}

#[cfg(test)]
mod tests;
