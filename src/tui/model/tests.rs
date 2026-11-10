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
    assert_eq!(
        ids(&m),
        ["e", "w", "i", "x"],
        "unknown levels count as info"
    );
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
    assert_eq!(ch(&mut m, 't'), Some(Request::Trace(1, "0xabc".into())));
    assert!(m.trace.as_ref().unwrap().loading);
}

#[test]
fn t_on_a_record_without_a_hash_says_so_and_asks_for_nothing() {
    let mut m = model(2);
    assert_eq!(ch(&mut m, 't'), None);
    assert!(m.trace.is_none());
    assert!(m.note.contains("no tx_hash"));
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
    m.update(Msg::Trace(1, Ok((vec![web.clone(), api, web], None))));
    assert_eq!(ids(&m), ["p", "w"], "sorted, deduped");
    assert_eq!(sel(&m), "p");
    press(&mut m, KeyCode::Esc);
    assert!(m.trace.is_none());
    assert_eq!(ids(&m), ["a"]);
}

#[test]
fn a_trace_result_for_a_closed_or_different_trace_is_ignored() {
    let mut m = model(1);
    m.update(Msg::Trace(1, Ok((vec![rec("z", 1, "info", "")], None))));
    assert!(m.trace.is_none() && ids(&m) == ["r1"]);
    let mut a = rec("a", 100, "info", "");
    a.tx_hash = Some("0xabc".into());
    m.update(Msg::Records(vec![a]));
    ch(&mut m, 't');
    m.update(Msg::Trace(99, Ok((vec![rec("z", 1, "info", "")], None))));
    assert!(m.trace.as_ref().unwrap().loading);
}

#[test]
fn a_failed_trace_shows_the_error_and_leaves_an_empty_trace_open() {
    let mut m = Model::new(vec![]);
    let mut a = rec("a", 1, "info", "");
    a.tx_hash = Some("0xabc".into());
    m.update(Msg::Records(vec![a]));
    ch(&mut m, 't');
    m.update(Msg::Trace(1, Err("boom".into())));
    let t = m.trace.as_ref().unwrap();
    assert!(!t.loading && t.records.is_empty());
    assert_eq!(t.error.as_deref(), Some("boom"));
}

#[test]
fn enter_toggles_the_detail_pane_and_status_is_replaced() {
    let mut m = model(1);
    press(&mut m, KeyCode::Enter);
    assert!(m.detail);
    press(&mut m, KeyCode::Enter);
    assert!(!m.detail);
    m.update(Msg::Status("cannot reach".into()));
    assert_eq!(m.poll_error, "cannot reach");
    m.update(Msg::Status(String::new()));
    assert!(m.poll_error.is_empty());
}

#[test]
fn a_highlight_hidden_by_a_filter_settles_on_the_next_row_in_time_not_the_newest() {
    let mut m = Model::new(vec![]);
    m.update(Msg::Records(vec![
        rec("a", 1, "error", ""),
        rec("b", 2, "info", ""),
        rec("c", 3, "error", ""),
        rec("d", 4, "error", ""),
    ]));
    ch(&mut m, 'g');
    ch(&mut m, 'j');
    assert_eq!(sel(&m), "b");
    ch(&mut m, 'l');
    assert_eq!(
        sel(&m),
        "c",
        "b is hidden; the next visible row is c, not the newest d"
    );
    assert!(!m.follow);
    ch(&mut m, 'l');
    ch(&mut m, 'l');
    ch(&mut m, 'l');
    assert_eq!(
        sel(&m),
        "b",
        "and b is highlighted again when it is visible"
    );
}

#[test]
fn a_paused_highlight_dropped_by_the_cap_settles_on_the_oldest_kept_row() {
    let mut m = Model::new(vec![]);
    m.update(Msg::Records(vec![rec("a", 0, "info", "")]));
    ch(&mut m, 'p');
    let n = MAX_RECORDS as i64 + 5;
    m.update(Msg::Records(
        (1..=n)
            .map(|i| rec(&format!("{i:06}"), i, "info", ""))
            .collect(),
    ));
    assert!(!m.follow);
    assert_eq!(sel(&m), format!("{:06}", n - MAX_RECORDS as i64 + 1));
}

#[test]
fn movement_inside_a_trace_leaves_the_live_view_exactly_as_it_was() {
    let mut m = Model::new(vec![]);
    let mut a = rec("a", 100, "info", "");
    a.tx_hash = Some("0xabc".into());
    m.update(Msg::Records(vec![rec("r0", 1, "info", ""), a]));
    assert!(m.follow);
    ch(&mut m, 't');
    m.update(Msg::Trace(
        1,
        Ok((
            vec![rec("t1", 5, "info", ""), rec("t2", 6, "info", "")],
            None,
        )),
    ));
    for c in ['j', 'k', 'g', 'G', 'p', 'k'] {
        ch(&mut m, c);
    }
    press(&mut m, KeyCode::Esc);
    assert!(m.follow, "following state is untouched by the trace");
    assert_eq!(sel(&m), "a");
}

#[test]
fn the_trace_keeps_its_own_highlight() {
    let mut m = Model::new(vec![]);
    let mut a = rec("a", 100, "info", "");
    a.tx_hash = Some("0xabc".into());
    m.update(Msg::Records(vec![a]));
    ch(&mut m, 't');
    m.update(Msg::Trace(
        1,
        Ok((
            vec![rec("t1", 5, "info", ""), rec("t2", 6, "info", "")],
            None,
        )),
    ));
    assert_eq!(sel(&m), "t1");
    ch(&mut m, 'j');
    assert_eq!(sel(&m), "t2");
    ch(&mut m, 'j');
    assert_eq!(sel(&m), "t2");
    ch(&mut m, 'k');
    assert_eq!(sel(&m), "t1");
}

#[test]
fn a_reply_to_an_earlier_trace_of_the_same_hash_is_ignored() {
    let mut m = Model::new(vec![]);
    let mut a = rec("a", 100, "info", "");
    a.tx_hash = Some("0xabc".into());
    m.update(Msg::Records(vec![a]));
    ch(&mut m, 't');
    press(&mut m, KeyCode::Esc);
    assert_eq!(ch(&mut m, 't'), Some(Request::Trace(2, "0xabc".into())));
    m.update(Msg::Trace(1, Err("stale".into())));
    let t = m.trace.as_ref().unwrap();
    assert!(t.loading && t.error.is_none());
}

#[test]
fn t_while_a_trace_is_loading_does_not_queue_another() {
    let mut m = Model::new(vec![]);
    let mut a = rec("a", 100, "info", "");
    a.tx_hash = Some("0xabc".into());
    m.update(Msg::Records(vec![a]));
    assert!(ch(&mut m, 't').is_some());
    assert_eq!(ch(&mut m, 't'), None);
    assert!(m.note.contains("still loading"));
}

#[test]
fn an_empty_tx_hash_is_not_traced() {
    let mut m = Model::new(vec![]);
    let mut a = rec("a", 1, "info", "");
    a.tx_hash = Some(String::new());
    m.update(Msg::Records(vec![a]));
    assert_eq!(ch(&mut m, 't'), None);
    assert!(m.note.contains("no tx_hash"));
}

#[test]
fn a_note_lasts_until_the_next_key_and_does_not_hide_the_poll_error() {
    let mut m = model(1);
    m.update(Msg::Status("daemon down".into()));
    ch(&mut m, 't');
    assert!(!m.note.is_empty());
    ch(&mut m, 'j');
    assert!(m.note.is_empty());
    assert_eq!(m.poll_error, "daemon down");
}

#[test]
fn trace_notes_about_truncation_are_shown() {
    let mut m = Model::new(vec![]);
    let mut a = rec("a", 100, "info", "");
    a.tx_hash = Some("0xabc".into());
    m.update(Msg::Records(vec![a]));
    ch(&mut m, 't');
    m.update(Msg::Trace(
        1,
        Ok((vec![], Some("showing only the newest 1000".into()))),
    ));
    assert!(m.note.contains("newest 1000"));
}

#[test]
fn unusual_level_names_are_not_hidden_by_the_error_and_warn_filters() {
    let mut m = Model::new(vec![]);
    m.update(Msg::Records(vec![
        rec("c", 1, "critical", ""),
        rec("n", 2, "notice", ""),
        rec("e", 3, "ERR", ""),
        rec("u", 4, "banana", ""),
    ]));
    ch(&mut m, 'l');
    assert_eq!(ids(&m), ["c", "e"]);
    ch(&mut m, 'l');
    assert_eq!(ids(&m), ["c", "e"]);
    ch(&mut m, 'l');
    assert_eq!(ids(&m), ["c", "n", "e", "u"]);
}
