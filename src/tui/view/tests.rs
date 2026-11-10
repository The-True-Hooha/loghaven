use super::*;
use crate::tui::model::{Msg, Request};
use ratatui::Terminal;
use ratatui::backend::TestBackend;
use ratatui::crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

fn rec(id: &str, ts: i64, level: &str, msg: &str) -> LogRecord {
    let mut r = LogRecord::new("web", level, "src", msg);
    r.id = id.into();
    r.timestamp_ms = ts;
    r
}

fn screen(m: &Model, w: u16, h: u16) -> Vec<String> {
    let mut t = Terminal::new(TestBackend::new(w, h)).unwrap();
    t.draw(|f| draw(f, m)).unwrap();
    let buf = t.backend().buffer().clone();
    (0..h)
        .map(|y| {
            (0..w)
                .map(|x| buf[(x, y)].symbol().to_string())
                .collect::<String>()
        })
        .collect()
}

fn text(m: &Model, w: u16, h: u16) -> String {
    screen(m, w, h).join("\n")
}

fn key(m: &mut Model, c: char) -> Option<Request> {
    m.update(Msg::Key(KeyEvent::new(
        KeyCode::Char(c),
        KeyModifiers::NONE,
    )))
}

#[test]
fn clock_is_utc_and_handles_negative_and_day_wrap() {
    assert_eq!(clock(0), "00:00:00.000");
    assert_eq!(clock(3_723_004), "01:02:03.004");
    assert_eq!(clock(86_400_000 + 1), "00:00:00.001");
    assert_eq!(clock(-1), "23:59:59.999");
}

#[test]
fn control_and_bidi_characters_never_reach_the_screen() {
    let nasty = "ok\u{1b}[2J\u{1b}]0;pwned\u{7}\r\n\u{202e}txt\u{2066}";
    let c = clean(nasty);
    assert!(!c.chars().any(|ch| ch.is_control()));
    assert!(!c.contains('\u{202e}') && !c.contains('\u{2066}'));
    let mut m = Model::new(vec!["web".into()]);
    let mut r = rec("a", 1, "info", nasty);
    r.source = "s\u{1b}[31m".into();
    r.tx_hash = Some("0x\u{1b}[0m".into());
    m.update(Msg::Records(vec![r]));
    key(&mut m, '/');
    key(&mut m, 'a');
    let with_prompt = text(&m, 100, 8);
    assert!(!with_prompt.contains('\u{1b}'));
    m.input = None;
    m.update(Msg::Status("bad\u{1b}[1m".into()));
    let screen = text(&m, 100, 8);
    assert!(!screen.contains('\u{1b}') && !screen.contains('\u{202e}'));
    assert!(screen.contains("ok·[2J"));
}

#[test]
fn the_list_shows_time_level_source_message_and_marks_the_highlight() {
    let mut m = Model::new(vec!["web".into()]);
    m.update(Msg::Records(vec![
        rec("a", 1_000, "error", "first"),
        rec("b", 2_000, "info", "second"),
    ]));
    let s = screen(&m, 80, 6);
    assert!(s[0].contains("loghaven") && s[0].contains("web") && s[0].contains("following"));
    assert!(s[1].contains("00:00:01.000") && s[1].contains("error") && s[1].contains("first"));
    assert!(s[2].contains("second"));
    assert!(s[5].contains("q quit"));
}

#[test]
fn the_app_column_appears_only_with_several_apps() {
    let mut one = Model::new(vec!["web".into()]);
    let mut many = Model::new(vec!["web".into(), "api".into()]);
    for m in [&mut one, &mut many] {
        m.update(Msg::Records(vec![rec("a", 1, "info", "hello")]));
    }
    assert!(!screen(&one, 80, 4)[1].contains("web "));
    assert!(screen(&many, 80, 4)[1].contains("web "));
}

#[test]
fn a_long_list_scrolls_so_the_highlighted_row_is_always_on_screen() {
    let mut m = Model::new(vec!["web".into()]);
    m.update(Msg::Records(
        (0..50)
            .map(|i| rec(&format!("{i:03}"), i, "info", &format!("line-{i:03}")))
            .collect(),
    ));
    assert!(text(&m, 60, 8).contains("line-049"));
    for _ in 0..30 {
        key(&mut m, 'k');
    }
    let s = text(&m, 60, 8);
    assert!(s.contains("line-019"), "{s}");
    assert!(!s.contains("line-049"));
    key(&mut m, 'g');
    assert!(text(&m, 60, 8).contains("line-000"));
}

#[test]
fn the_detail_pane_shows_every_field_of_the_highlighted_record() {
    let mut m = Model::new(vec!["web".into()]);
    let mut r = rec("rec-id-1", 5, "warn", "details please");
    r.chain = Some("ethereum".into());
    r.tx_hash = Some("0xabc123".into());
    r.trace_id = Some("trace-9".into());
    r.metadata = Some("{\"k\":1}".into());
    m.update(Msg::Records(vec![r]));
    m.update(Msg::Key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE)));
    let s = text(&m, 80, 20);
    for needle in [
        "rec-id-1",
        "details please",
        "ethereum",
        "0xabc123",
        "trace-9",
        "{\"k\":1}",
    ] {
        assert!(s.contains(needle), "missing {needle}\n{s}");
    }
}

#[test]
fn filters_and_trace_are_named_in_the_header_and_empty_states_explain_themselves() {
    let mut m = Model::new(vec!["web".into()]);
    assert!(text(&m, 100, 5).contains("no records yet"));
    let mut a = rec("a", 1, "info", "x");
    a.tx_hash = Some("0xabc".into());
    m.update(Msg::Records(vec![a]));
    key(&mut m, 'l');
    key(&mut m, '/');
    for c in "zzz".chars() {
        key(&mut m, c);
    }
    m.update(Msg::Key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE)));
    let s = screen(&m, 100, 5);
    assert!(
        s[0].contains("level:error") && s[0].contains("text:zzz"),
        "{}",
        s[0]
    );
    m.text.clear();
    m.min_level = crate::tui::model::MinLevel::All;
    assert_eq!(key(&mut m, 't'), Some(Request::Trace(1, "0xabc".into())));
    assert!(text(&m, 100, 5).contains("loading trace"));
    m.update(Msg::Trace(1, Ok((vec![], None))));
    let s = text(&m, 100, 5);
    assert!(
        s.contains("TRACE 0xabc") && s.contains("no records carry"),
        "{s}"
    );
}

#[test]
fn typing_a_filter_shows_the_prompt_and_errors_replace_the_key_help() {
    let mut m = Model::new(vec!["web".into()]);
    key(&mut m, '/');
    key(&mut m, 'a');
    assert!(screen(&m, 80, 4)[3].contains("filter: a"));
    m.input = None;
    m.update(Msg::Status("cannot reach query server".into()));
    let last = &screen(&m, 80, 4)[3];
    assert!(last.contains("cannot reach") && !last.contains("q quit"));
    key(&mut m, 't');
    assert!(
        screen(&m, 80, 4)[3].contains("no tx_hash"),
        "a note shows above the poll error"
    );
}

#[test]
fn tiny_terminals_do_not_panic() {
    let mut m = Model::new(vec!["web".into(), "api".into()]);
    m.update(Msg::Records(vec![rec("a", 1, "info", "hello world")]));
    m.detail = true;
    m.input = Some("x".into());
    for (w, h) in [(0, 0), (1, 1), (5, 2), (10, 3), (3, 20), (200, 1)] {
        screen(&m, w, h);
    }
}

#[test]
fn clean_removes_line_separators_zero_width_and_combining_floods() {
    for c in [
        '\u{2028}', '\u{2029}', '\u{200b}', '\u{200e}', '\u{061c}', '\u{2060}', '\u{feff}',
        '\u{fe0f}', '\u{85}', '\u{7f}',
    ] {
        let out = clean(&format!("a{c}b"));
        assert_eq!(out, "a·b", "U+{:04X}", c as u32);
    }
    let flood = format!("e{}", "\u{0301}".repeat(50));
    assert_eq!(clean(&flood).chars().count(), 3);
    assert_eq!(clean("é ünï ✓ 日本"), "é ünï ✓ 日本");
}

#[test]
fn a_failed_trace_says_why_in_the_list_and_filters_that_hide_trace_rows_are_named() {
    let mut m = Model::new(vec!["web".into()]);
    let mut a = rec("a", 100, "info", "x");
    a.tx_hash = Some("0xabc".into());
    m.update(Msg::Records(vec![a]));
    key(&mut m, 't');
    m.update(Msg::Trace(1, Err("query failed 500".into())));
    assert!(text(&m, 100, 5).contains("trace failed: query failed 500"));
    m.update(Msg::Key(KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE)));
    key(&mut m, 't');
    m.update(Msg::Trace(2, Ok((vec![rec("z", 1, "info", "z")], None))));
    key(&mut m, 'l');
    assert!(text(&m, 100, 5).contains("no trace records match the current filters"));
}

#[test]
fn the_header_says_following_only_for_the_live_list() {
    let mut m = Model::new(vec!["web".into()]);
    let mut a = rec("a", 100, "info", "x");
    a.tx_hash = Some("0xabc".into());
    m.update(Msg::Records(vec![a]));
    assert!(screen(&m, 100, 4)[0].contains("following"));
    key(&mut m, 't');
    let h = &screen(&m, 100, 4)[0];
    assert!(h.contains("TRACE") && !h.contains("following"));
}
