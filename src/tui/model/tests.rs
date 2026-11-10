use super::*;

fn rec(id: &str, ts: i64, level: &str, msg: &str) -> LogRecord {
    let mut r = LogRecord::new("web", level, "src", msg);
    r.id = id.into();
    r.timestamp_ms = ts;
    r
}

fn press(m: &mut Model, code: KeyCode) -> Option<Request> {
    m.update(Msg::Key(KeyEvent::new(code, KeyModifiers::NONE)))
}

fn ch(m: &mut Model, c: char) -> Option<Request> {
    press(m, KeyCode::Char(c))
}

fn ids(m: &Model) -> Vec<String> {
    m.visible().iter().map(|r| r.id.clone()).collect()
}

fn model(n: i64) -> Model {
    let mut m = Model::new(vec!["web".into()]);
    m.update(Msg::Records(
        (1..=n)
            .map(|i| rec(&format!("r{i}"), i * 10, "info", &format!("msg {i}")))
            .collect(),
    ));
    m
}

fn sel(m: &Model) -> String {
    m.selected().unwrap().id.clone()
}

#[test]
fn records_are_kept_oldest_first_and_late_arrivals_slot_in_by_time() {
    let mut m = Model::new(vec![]);
    m.update(Msg::Records(vec![
        rec("c", 30, "info", ""),
        rec("a", 10, "info", ""),
    ]));
    m.update(Msg::Records(vec![rec("b", 20, "info", "")]));
    assert_eq!(ids(&m), ["a", "b", "c"]);
}

#[test]
fn the_same_record_twice_is_kept_once() {
    let mut m = Model::new(vec![]);
    m.update(Msg::Records(vec![rec("a", 10, "info", "")]));
    m.update(Msg::Records(vec![rec("a", 10, "info", "")]));
    assert_eq!(m.records.len(), 1);
}

#[test]
fn the_buffer_is_bounded_and_drops_the_oldest() {
    let mut m = Model::new(vec![]);
    let n = MAX_RECORDS as i64 + 10;
    m.update(Msg::Records(
        (0..n)
            .map(|i| rec(&format!("{i:06}"), i, "info", ""))
            .collect(),
    ));
    assert_eq!(m.records.len(), MAX_RECORDS);
    assert_eq!(m.records[0].id, "000010");
}

#[test]
fn following_highlights_the_newest_record_and_stays_with_it() {
    let mut m = model(3);
    assert_eq!(sel(&m), "r3");
    m.update(Msg::Records(vec![rec("r4", 40, "info", "")]));
    assert_eq!(sel(&m), "r4");
}

#[test]
fn moving_up_stops_following_and_the_highlight_stays_on_its_record() {
    let mut m = model(5);
    ch(&mut m, 'k');
    assert!(!m.follow);
    assert_eq!(sel(&m), "r4");
    m.update(Msg::Records(vec![
        rec("r6", 60, "info", ""),
        rec("r0", 5, "info", ""),
    ]));
    assert_eq!(
        sel(&m),
        "r4",
        "new and late records do not move the highlight"
    );
}

#[test]
fn moving_down_to_the_newest_resumes_following_and_capital_g_jumps_there() {
    let mut m = model(5);
    ch(&mut m, 'k');
    ch(&mut m, 'k');
    ch(&mut m, 'j');
    assert!(!m.follow);
    ch(&mut m, 'j');
    assert!(m.follow);
    ch(&mut m, 'g');
    assert_eq!((sel(&m).as_str(), m.follow), ("r1", false));
    ch(&mut m, 'G');
    assert_eq!((sel(&m).as_str(), m.follow), ("r5", true));
}

#[test]
fn movement_is_clamped_at_both_ends_and_safe_on_an_empty_list() {
    let mut m = model(3);
    for _ in 0..10 {
        ch(&mut m, 'k');
    }
    assert_eq!(sel(&m), "r1");
    for _ in 0..10 {
        press(&mut m, KeyCode::PageDown);
    }
    assert_eq!(sel(&m), "r3");
    let mut empty = Model::new(vec![]);
    for c in ['j', 'k', 'g', 'G', 'p', 't', 'l'] {
        ch(&mut empty, c);
    }
    assert!(empty.selected().is_none());
}

#[test]
fn p_pauses_and_resumes_following() {
    let mut m = model(3);
    ch(&mut m, 'p');
    assert!(!m.follow);
    m.update(Msg::Records(vec![rec("r4", 40, "info", "")]));
    assert_eq!(sel(&m), "r3");
    ch(&mut m, 'p');
    assert_eq!(sel(&m), "r4");
}

#[test]
fn level_filter_cycles_and_admits_by_severity() {
    let mut m = Model::new(vec![]);
    m.update(Msg::Records(vec![
        rec("e", 1, "error", ""),
        rec("w", 2, "warn", ""),
        rec("i", 3, "info", ""),
        rec("d", 4, "debug", ""),
        rec("x", 5, "weird", ""),
    ]));
    assert_eq!(ids(&m).len(), 5);
    ch(&mut m, 'l');
    assert_eq!(ids(&m), ["e"]);
    ch(&mut m, 'l');
    assert_eq!(ids(&m), ["e", "w"]);
    ch(&mut m, 'l');
    assert_eq!(ids(&m), ["e", "w", "i"]);
    ch(&mut m, 'l');
    assert_eq!(m.min_level, MinLevel::All);
}

#[test]
fn text_filter_matches_message_source_tx_and_tags_ignoring_case_and_esc_clears_it() {
    let mut m = Model::new(vec![]);
    let mut a = rec("a", 1, "info", "Payment Confirmed");
    a.tx_hash = Some("0xdeadbeef".into());
    let mut b = rec("b", 2, "info", "other");
    b.tags = Some("[\"billing\"]".into());
    m.update(Msg::Records(vec![a, b, rec("c", 3, "info", "nothing")]));
    let filter = |m: &mut Model, text: &str| {
        ch(m, '/');
        m.update(Msg::Key(KeyEvent::new(
            KeyCode::Char('u'),
            KeyModifiers::CONTROL,
        )));
        for c in text.chars() {
            ch(m, c);
        }
        press(m, KeyCode::Enter);
    };
    filter(&mut m, "payment");
    assert_eq!(ids(&m), ["a"]);
    filter(&mut m, "DEADBEEF");
    assert_eq!(ids(&m), ["a"]);
    filter(&mut m, "billing");
    assert_eq!(ids(&m), ["b"]);
    press(&mut m, KeyCode::Esc);
    assert_eq!(ids(&m).len(), 3);
}

#[test]
fn typing_a_filter_does_not_trigger_commands_and_esc_cancels_the_edit() {
    let mut m = model(3);
    ch(&mut m, '/');
    for c in "qtl".chars() {
        ch(&mut m, c);
    }
    assert!(!m.quit && m.trace.is_none() && m.min_level == MinLevel::All);
    assert_eq!(m.input.as_deref(), Some("qtl"));
    press(&mut m, KeyCode::Backspace);
    assert_eq!(m.input.as_deref(), Some("qt"));
    press(&mut m, KeyCode::Esc);
    assert!(m.input.is_none() && m.text.is_empty());
    ch(&mut m, '/');
    for c in "abc".chars() {
        ch(&mut m, c);
    }
    m.update(Msg::Key(KeyEvent::new(
        KeyCode::Char('u'),
        KeyModifiers::CONTROL,
    )));
    assert_eq!(m.input.as_deref(), Some(""));
}

#[test]
fn control_characters_are_not_typed_into_the_filter() {
    let mut m = model(1);
    ch(&mut m, '/');
    ch(&mut m, '\u{1b}');
    ch(&mut m, 'a');
    assert_eq!(m.input.as_deref(), Some("a"));
}

#[test]
fn q_and_ctrl_c_quit_but_ctrl_c_also_quits_while_typing() {
    let mut m = model(1);
    ch(&mut m, 'q');
    assert!(m.quit);
    let mut m = model(1);
    ch(&mut m, '/');
    m.update(Msg::Key(KeyEvent::new(
        KeyCode::Char('c'),
        KeyModifiers::CONTROL,
    )));
    assert!(m.quit);
    let mut m = model(1);
    ch(&mut m, '/');
    ch(&mut m, 'q');
    assert!(!m.quit);
}

#[test]
fn t_asks_for_the_trace_of_the_selected_transaction() {
    let mut m = Model::new(vec!["web".into(), "api".into()]);
    let mut a = rec("a", 1, "info", "x");
    a.tx_hash = Some("0xabc".into());
    m.update(Msg::Records(vec![a, rec("b", 2, "info", "no tx")]));
    ch(&mut m, 'k');
    assert_eq!(ch(&mut m, 't'), Some(Request::Trace("0xabc".into())));
    assert!(m.trace.as_ref().unwrap().loading);
}

#[test]
fn t_on_a_record_without_a_hash_says_so_and_asks_for_nothing() {
    let mut m = model(2);
    assert_eq!(ch(&mut m, 't'), None);
    assert!(m.trace.is_none());
    assert!(m.status.contains("no tx_hash"));
}

#[test]
fn a_trace_result_replaces_the_list_oldest_first_and_esc_returns_to_the_live_view() {
    let mut m = Model::new(vec![]);
    let mut a = rec("a", 5, "info", "live");
    a.tx_hash = Some("0xabc".into());
    m.update(Msg::Records(vec![a]));
    ch(&mut m, 't');
    let mut web = rec("w", 20, "info", "web side");
    web.app = "web".into();
    let mut api = rec("p", 10, "info", "api side");
    api.app = "api".into();
    m.update(Msg::Trace("0xabc".into(), Ok(vec![web.clone(), api, web])));
    assert_eq!(ids(&m), ["p", "w"], "sorted, deduped");
    assert_eq!(sel(&m), "p");
    press(&mut m, KeyCode::Esc);
    assert!(m.trace.is_none());
    assert_eq!(ids(&m), ["a"]);
}

#[test]
fn a_trace_result_for_a_closed_or_different_trace_is_ignored() {
    let mut m = model(1);
    m.update(Msg::Trace(
        "0xabc".into(),
        Ok(vec![rec("z", 1, "info", "")]),
    ));
    assert!(m.trace.is_none() && ids(&m) == ["r1"]);
    let mut a = rec("a", 100, "info", "");
    a.tx_hash = Some("0xabc".into());
    m.update(Msg::Records(vec![a]));
    ch(&mut m, 't');
    m.update(Msg::Trace(
        "0xother".into(),
        Ok(vec![rec("z", 1, "info", "")]),
    ));
    assert!(m.trace.as_ref().unwrap().loading);
}

#[test]
fn a_failed_trace_shows_the_error_and_leaves_an_empty_trace_open() {
    let mut m = Model::new(vec![]);
    let mut a = rec("a", 1, "info", "");
    a.tx_hash = Some("0xabc".into());
    m.update(Msg::Records(vec![a]));
    ch(&mut m, 't');
    m.update(Msg::Trace("0xabc".into(), Err("boom".into())));
    assert!(m.status.contains("boom"));
    let t = m.trace.as_ref().unwrap();
    assert!(!t.loading && t.records.is_empty());
}

#[test]
fn enter_toggles_the_detail_pane_and_status_is_replaced() {
    let mut m = model(1);
    press(&mut m, KeyCode::Enter);
    assert!(m.detail);
    press(&mut m, KeyCode::Enter);
    assert!(!m.detail);
    m.update(Msg::Status("cannot reach".into()));
    assert_eq!(m.status, "cannot reach");
    m.update(Msg::Status(String::new()));
    assert!(m.status.is_empty());
}

#[test]
fn the_highlight_survives_a_filter_that_hides_it() {
    let mut m = Model::new(vec![]);
    m.update(Msg::Records(vec![
        rec("e", 1, "error", ""),
        rec("i", 2, "info", ""),
    ]));
    ch(&mut m, 'k');
    assert_eq!(sel(&m), "e");
    ch(&mut m, 'l');
    ch(&mut m, 'l');
    ch(&mut m, 'l');
    assert!(m.selected().is_some());
    ch(&mut m, 'l');
    assert_eq!(sel(&m), "e");
}
