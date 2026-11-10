'use strict';
// The token arrives in the URL fragment, which the browser never sends to a server. It is
// removed from the address bar at once and lives only in this variable.
const MAX_RECORDS = 5000;
const SHOWN = 1000;
const INITIAL = 200;
const PAGE = 1000;
const MAX_PAGE = 10000;
const TRACE_LIMIT = 1000;

const EXPIRED = 'This session has expired or is not allowed. Run "loghaven dashboard" again to open a new one.';
const $ = (id) => document.getElementById(id);
const params = new URLSearchParams(location.hash.slice(1));
const app = params.get('app') || '';
const token = params.get('token');
const lookback = Number(params.get('lookback')) || 15000;
history.replaceState(null, '', location.pathname);

const st = {
  records: new Map(),
  newest: 0,
  started: false,
  follow: true,
  level: 'all',
  text: '',
  selected: null,
  trace: null,
  stopped: false,
  floor: -1,
};

function rank(level) {
  switch (String(level).toLowerCase()) {
    case 'error': case 'err': case 'fatal': case 'critical': case 'crit': case 'panic': case 'emerg': case 'alert': return 0;
    case 'warn': case 'warning': return 1;
    case 'debug': case 'trace': return 3;
    default: return 2;
  }
}

const MAX_RANK = { all: 3, error: 0, warn: 1, info: 2 };

function admits(r) {
  if (rank(r.level) > MAX_RANK[st.level]) return false;
  if (!st.text) return true;
  const t = st.text.toLowerCase();
  return [r.message, r.source, r.tx_hash || ''].some((s) => s.toLowerCase().includes(t));
}

function clock(ms) {
  return new Date(ms).toISOString().slice(11, 23);
}

function cell(tag, cls, text) {
  const el = document.createElement(tag);
  if (cls) el.className = cls;
  el.textContent = text;
  return el;
}

function setStatus(text) {
  $('status').textContent = text;
}

function banner(text) {
  $('banner').hidden = !text;
  $('banner').textContent = text || '';
}

function visible() {
  const source = st.trace ? st.trace.records : [...st.records.values()];
  return source
    .filter((r) => st.trace || admits(r))
    .sort((a, b) => b.timestamp_ms - a.timestamp_ms || (a.id < b.id ? 1 : -1))
    .slice(0, SHOWN);
}

function render() {
  const rows = visible();
  const body = $('rows');
  const frag = document.createDocumentFragment();
  for (const r of rows) {
    const tr = document.createElement('tr');
    tr.className = 'row' + (r.id === st.selected ? ' sel' : '');
    tr.dataset.id = r.id;
    const k = rank(r.level);
    frag.append(tr);
    tr.append(
      cell('td', 'time', clock(r.timestamp_ms)),
      cell('td', k === 0 ? 'lvl-error' : k === 1 ? 'lvl-warn' : '', r.level),
      cell('td', 'src', r.source),
      cell('td', 'msg', r.message),
    );
  }
  body.replaceChildren(frag);
  $('empty').hidden = rows.length > 0;
  $('follow').hidden = !!st.trace;
  $('title').textContent = st.trace ? `LogHaven · ${app} · trace ${st.trace.hash}` : `LogHaven · ${app}`;
  renderDetail();
}

function find(id) {
  return (st.trace ? st.trace.records : [...st.records.values()]).find((r) => r.id === id);
}

function renderDetail() {
  const r = st.selected && find(st.selected);
  $('detail').hidden = !r;
  if (!r) return;
  const fields = [
    ['time', new Date(r.timestamp_ms).toISOString()], ['id', r.id], ['app', r.app], ['level', r.level],
    ['source', r.source], ['message', r.message], ['trace_id', r.trace_id], ['span_id', r.span_id],
    ['chain', r.chain], ['tx_hash', r.tx_hash], ['tags', r.tags], ['metadata', r.metadata],
  ].filter(([, v]) => v !== null && v !== undefined && v !== '');
  const list = $('fields');
  list.replaceChildren();
  for (const [k, v] of fields) list.append(cell('dt', '', k), cell('dd', '', v));
  $('trace').hidden = !r.tx_hash || !!st.trace;
}

async function fetchLogs(params) {
  const q = new URLSearchParams({ app, ...params });
  const headers = token ? { Authorization: `Bearer ${token}` } : {};
  const resp = await fetch(`/logs?${q}`, { headers, cache: 'no-store', signal: AbortSignal.timeout(10000) });
  if (!resp.ok) {
    const e = new Error(`HTTP ${resp.status}`);
    e.status = resp.status;
    throw e;
  }
  return (await resp.json()).records;
}

function accept(batch) {
  let added = false;
  for (const r of batch) {
    // a far-future timestamp must not push the next from_ms past real new records
    st.newest = Math.max(st.newest, Math.min(r.timestamp_ms, Date.now()));
    if (r.timestamp_ms <= st.floor) continue;
    if (!st.records.has(r.id)) {
      st.records.set(r.id, r);
      added = true;
    }
  }
  if (st.records.size > MAX_RECORDS) {
    const oldest = [...st.records.values()]
      .sort((a, b) => a.timestamp_ms - b.timestamp_ms)
      .slice(0, st.records.size - MAX_RECORDS);
    for (const r of oldest) st.records.delete(r.id);
    st.floor = Math.max(st.floor, oldest[oldest.length - 1].timestamp_ms);
  }
  return added;
}

async function poll() {
  if (st.stopped) return;
  try {
    let batch;
    if (!st.started) {
      batch = await fetchLogs({ limit: INITIAL });
    } else {
      const from = String(Math.max(0, st.newest - lookback));
      batch = await fetchLogs({ limit: PAGE, from_ms: from });
      if (batch.length >= PAGE) {
        batch = await fetchLogs({ limit: MAX_PAGE, from_ms: from });
        if (batch.length >= MAX_PAGE) setStatus(`more than ${MAX_PAGE} records in one poll; some may be missing`);
      }
    }
    st.started = true;
    // paused: keep fetching so nothing is lost, but do not move the view
    if (accept(batch) && st.follow && !st.trace) render();
    if (batch.length < MAX_PAGE) setStatus(st.follow ? 'live' : 'paused');
  } catch (e) {
    if (e.status === 401 || e.status === 403) {
      st.stopped = true;
      banner(EXPIRED);
      setStatus('stopped');
      return;
    }
    setStatus(`error: ${e.message}`);
  }
  setTimeout(poll, 1000);
}

async function openTrace() {
  const r = st.selected && find(st.selected);
  if (!r || !r.tx_hash) return;
  const hash = r.tx_hash;
  st.trace = { hash, records: [] };
  st.selected = null;
  render();
  try {
    const records = await fetchLogs({ tx_hash: hash, limit: TRACE_LIMIT });
    if (st.trace && st.trace.hash === hash) {
      st.trace.records = records;
      banner(records.length >= TRACE_LIMIT ? `Showing only the newest ${TRACE_LIMIT} records for this hash.` : '');
      render();
    }
  } catch (e) {
    banner(`Trace failed: ${e.message}`);
  }
}

function closeTrace() {
  st.trace = null;
  st.selected = null;
  banner(st.stopped ? EXPIRED : '');
  render();
}

$('rows').addEventListener('click', (ev) => {
  const tr = ev.target.closest('tr');
  if (!tr) return;
  st.selected = tr.dataset.id;
  render();
});
$('close').addEventListener('click', () => {
  if (st.trace) closeTrace();
  else { st.selected = null; render(); }
});
$('trace').addEventListener('click', openTrace);
$('level').addEventListener('change', (ev) => { st.level = ev.target.value; render(); });
$('text').addEventListener('input', (ev) => { st.text = ev.target.value; render(); });
$('follow').addEventListener('click', () => {
  st.follow = !st.follow;
  $('follow').textContent = st.follow ? 'Pause' : 'Resume';
  if (st.follow) render();
  setStatus(st.follow ? 'live' : 'paused');
});
document.addEventListener('keydown', (ev) => {
  if (ev.key === 'Escape') { if (st.trace) closeTrace(); else { st.selected = null; render(); } }
});

if (!app) {
  st.stopped = true;
  banner('Open this page with "loghaven dashboard --app <name>".');
} else {
  render();
  poll();
}
