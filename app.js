const CONFIG = {
  BASE_URL: 'https://benchmarkoor-api.core.ethpandaops.io/api/v1/index/query/test_stats',
  SUITE_HASH: 'bc398819d1ebc628',
  PAGE_SIZE: 1000,
};

const API_KEY_STORAGE = 'benchmarkoor_api_key';

function getApiKey() {
  return localStorage.getItem(API_KEY_STORAGE) || '';
}

function setApiKey(key) {
  if (key) localStorage.setItem(API_KEY_STORAGE, key);
  else localStorage.removeItem(API_KEY_STORAGE);
}

function maskKey(key) {
  if (!key) return '';
  if (key.length <= 10) return `${key.slice(0, 2)}…`;
  return `${key.slice(0, 6)}…${key.slice(-4)}`;
}

const MODES = ['sequential', 'nobatchio', 'full'];
const MODE_LABELS = {
  sequential: 'Sequential',
  nobatchio: 'No Batch I/O',
  full: 'Full',
};
function modeLabel(mode) {
  return MODE_LABELS[mode] || mode;
}
const CLIENTS = ['besu', 'geth', 'nethermind', 'erigon', 'reth'];

const MODE_COLORS = {
  sequential: '#f87171',
  nobatchio: '#60a5fa',
  full: '#34d399',
};

const CLIENT_COLORS = {
  besu: '#a78bfa',
  geth: '#fbbf24',
  nethermind: '#22d3ee',
  erigon: '#f472b6',
  reth: '#fb923c',
};

const COMPARISONS = {
  'full-vs-sequential': { baseline: 'sequential', target: 'full' },
  'full-vs-nobatchio': { baseline: 'nobatchio', target: 'full' },
};

const state = {
  rowsByClient: {},
  rows: [],
  runs: [],
  client: null,
  tab: 'all',
  disabledGasLimits: new Set(),
  clientSort: { column: null, direction: null },
};

function branchFromImage(image) {
  if (!image) return '';
  const idx = image.lastIndexOf(':');
  return idx > 0 ? image.slice(idx + 1) : image;
}

function formatIndexedAt(iso) {
  if (!iso) return '—';
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso;
  return d.toLocaleString(undefined, {
    year: 'numeric',
    month: 'short',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    timeZoneName: 'short',
  });
}

function computeClientVersions() {
  const map = new Map();
  for (const run of state.runs || []) {
    const client = run.client;
    const image = run.image;
    if (!client || !image) continue;
    const key = `${client}|${image}`;
    const iso = run.indexed_at || '';
    const existing = map.get(key);
    if (!existing || iso > existing.indexed_at) {
      map.set(key, { client, image, indexed_at: iso });
    }
  }
  return [...map.values()].sort(
    (a, b) => a.client.localeCompare(b.client) || a.image.localeCompare(b.image),
  );
}

function openVersionsModal() {
  const body = document.getElementById('versions-body');
  body.replaceChildren();
  const rows = computeClientVersions();
  if (!rows.length) {
    const msg = document.createElement('div');
    msg.className = 'text-xs text-gray-400';
    msg.textContent = state.runs && state.runs.length
      ? 'No run metadata for this suite.'
      : 'Runs not loaded yet — try again in a moment (or check the API key).';
    body.appendChild(msg);
  } else {
    const table = document.createElement('table');
    table.className = 'w-full text-xs';
    const thead = document.createElement('thead');
    thead.className = 'text-gray-400 uppercase';
    const htr = document.createElement('tr');
    for (const label of ['Client', 'Branch / tag', 'Image', 'Indexed at']) {
      const th = document.createElement('th');
      th.className = 'border-b border-gray-800 px-2 py-1.5 text-left font-medium';
      th.textContent = label;
      htr.appendChild(th);
    }
    thead.appendChild(htr);
    table.appendChild(thead);
    const tbody = document.createElement('tbody');
    for (const r of rows) {
      const tr = document.createElement('tr');
      tr.className = 'border-b border-gray-900/80';
      const addCell = (content, extra = '') => {
        const td = document.createElement('td');
        td.className = `px-2 py-1.5 align-top ${extra}`;
        if (typeof content === 'string') td.textContent = content;
        else td.appendChild(content);
        tr.appendChild(td);
      };
      const clientPill = document.createElement('span');
      clientPill.className = 'rounded-xs border px-1.5 py-0.5 font-semibold ' + (
        PILL_TONES.slate
      );
      clientPill.style.color = CLIENT_COLORS[r.client] || '#d1d5db';
      clientPill.textContent = r.client;
      addCell(clientPill);
      addCell(branchFromImage(r.image), 'font-mono text-emerald-300');
      addCell(r.image, 'font-mono break-all text-gray-400');
      addCell(formatIndexedAt(r.indexed_at), 'whitespace-nowrap text-gray-400');
      tbody.appendChild(tr);
    }
    table.appendChild(tbody);
    body.appendChild(table);
  }
  document.getElementById('versions-modal').showModal();
}

function extractGasLimit(testName) {
  const m = testName.match(/benchmark_(\d+)M/);
  return m ? parseInt(m[1], 10) : null;
}

function gasLimitKey(gas) {
  return gas == null ? 'other' : String(gas);
}

function extractSortKey(testName) {
  const m = testName.match(/__([^[]+)/);
  return m ? m[1] : testName;
}

function extractGbSize(testName) {
  const m = testName.match(/\[(\d+)GB/);
  return m ? parseInt(m[1], 10) : null;
}

function extractFamilyKey(testName) {
  return testName.replace(/benchmark_\d+M/, 'benchmark_*M');
}

function displayTestName(name) {
  return name.endsWith('.txt') ? name.slice(0, -4) : name;
}

function parseTestName(testName) {
  const s = displayTestName(testName);
  const m = s.match(/^(.+?\.py)__([^\[]+)(?:\[(.+)\])?$/);
  if (!m) return { raw: s, params: [] };
  const [, file, func, paramsRaw = ''] = m;
  const tokens = paramsRaw ? paramsRaw.split('-') : [];
  const params = [];
  for (const token of tokens) {
    if (token === 'benchmark_test') continue;
    let mm;
    if ((mm = token.match(/^benchmark_(\d+)M$/))) {
      params.push({ label: `${mm[1]}M gas`, tone: 'emerald' });
    } else if ((mm = token.match(/^fork_(.+)$/))) {
      params.push({ label: mm[1], tone: 'slate' });
    } else if ((mm = token.match(/^(\d+)GB$/))) {
      params.push({ label: `${mm[1]} GB state`, tone: 'sky' });
    } else if ((mm = token.match(/^compute_(\d+)pct$/))) {
      params.push({ label: `compute ${mm[1]}%`, tone: 'amber' });
    } else if (token === 'forward' || token === 'reverse') {
      params.push({ label: token, tone: 'violet' });
    } else if ((mm = token.match(/^existing_slots_(True|False)$/))) {
      params.push({ label: mm[1] === 'True' ? 'existing slots' : 'fresh slots', tone: 'gray' });
    } else if ((mm = token.match(/^distinct_senders_(True|False)$/))) {
      params.push({ label: mm[1] === 'True' ? 'distinct senders' : 'same sender', tone: 'gray' });
    } else {
      params.push({ label: token, tone: 'gray' });
    }
  }
  return {
    file,
    func,
    fileLabel: file.replace(/^test_/, '').replace(/\.py$/, ''),
    funcLabel: func.replace(/^test_/, ''),
    params,
  };
}

const PILL_TONES = {
  emerald: 'border-emerald-800 bg-emerald-950/50 text-emerald-300',
  sky: 'border-sky-800 bg-sky-950/50 text-sky-300',
  amber: 'border-amber-800 bg-amber-950/50 text-amber-300',
  violet: 'border-violet-800 bg-violet-950/50 text-violet-300',
  slate: 'border-slate-700 bg-slate-900/60 text-slate-300',
  gray: 'border-gray-700 bg-gray-800/50 text-gray-400',
};

function pillClassForTone(tone) {
  const base = PILL_TONES[tone] || PILL_TONES.gray;
  return `rounded-xs border px-1.5 py-0.5 text-xs font-medium ${base}`;
}

function makeTestCellContent(testName, showRaw) {
  const icon = makeChartIcon();
  if (showRaw) {
    const inner = document.createElement('div');
    inner.className = 'flex items-start gap-1.5';
    const nameSpan = document.createElement('span');
    nameSpan.className = 'break-all font-mono';
    nameSpan.textContent = displayTestName(testName);
    inner.append(icon, nameSpan);
    return inner;
  }
  const parsed = parseTestName(testName);
  const wrap = document.createElement('div');
  wrap.className = 'flex flex-col gap-1';
  const line1 = document.createElement('div');
  line1.className = 'flex items-center gap-2';
  line1.appendChild(icon);
  const funcSpan = document.createElement('span');
  funcSpan.className = 'font-mono text-sm font-semibold text-gray-100';
  funcSpan.textContent = parsed.funcLabel || parsed.raw || displayTestName(testName);
  line1.appendChild(funcSpan);
  if (parsed.fileLabel) {
    const fileSpan = document.createElement('span');
    fileSpan.className = 'font-mono text-xs text-gray-500';
    fileSpan.textContent = parsed.fileLabel;
    line1.appendChild(fileSpan);
  }
  wrap.appendChild(line1);
  if (parsed.params && parsed.params.length) {
    const paramsRow = document.createElement('div');
    paramsRow.className = 'flex flex-wrap gap-1';
    for (const p of parsed.params) {
      const pill = document.createElement('span');
      pill.className = pillClassForTone(p.tone);
      pill.textContent = p.label;
      paramsRow.appendChild(pill);
    }
    wrap.appendChild(paramsRow);
  }
  return wrap;
}

function makeChartIcon() {
  const svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
  svg.setAttribute('viewBox', '0 0 20 20');
  svg.setAttribute('fill', 'currentColor');
  svg.setAttribute('aria-hidden', 'true');
  svg.classList.add('mt-0.5', 'size-3.5', 'shrink-0', 'text-gray-500', 'group-hover:text-emerald-400');
  const path = document.createElementNS('http://www.w3.org/2000/svg', 'path');
  path.setAttribute('d', 'M3 17h2v-6H3v6zm4 0h2V9H7v8zm4 0h2V5h-2v12zm4 0h2v-9h-2v9z');
  svg.appendChild(path);
  return svg;
}

async function fetchAll(client) {
  const all = [];
  let offset = 0;
  while (true) {
    const url = `${CONFIG.BASE_URL}?client=eq.${client}&suite_hash=eq.${CONFIG.SUITE_HASH}&limit=${CONFIG.PAGE_SIZE}&offset=${offset}`;
    const res = await fetch(url, {
      headers: { Authorization: `Bearer ${getApiKey()}` },
    });
    if (res.status === 401 || res.status === 403) {
      const err = new Error('Unauthorized — API key missing or invalid');
      err.code = 'UNAUTHORIZED';
      throw err;
    }
    if (!res.ok) throw new Error(`HTTP ${res.status} ${res.statusText}`);
    const json = await res.json();
    const rows = json.data || [];
    all.push(...rows);
    if (rows.length < CONFIG.PAGE_SIZE) break;
    offset += CONFIG.PAGE_SIZE;
  }
  return all;
}

async function fetchRuns() {
  const all = [];
  let offset = 0;
  const base = CONFIG.BASE_URL.replace(/\/test_stats$/, '/runs');
  while (true) {
    const url = `${base}?suite_hash=eq.${CONFIG.SUITE_HASH}&select=client,instance_id,image,indexed_at,timestamp,status,tests_total,tests_passed,tests_failed&limit=${CONFIG.PAGE_SIZE}&offset=${offset}`;
    const res = await fetch(url, {
      headers: { Authorization: `Bearer ${getApiKey()}` },
    });
    if (!res.ok) throw new Error(`HTTP ${res.status} ${res.statusText}`);
    const json = await res.json();
    const rows = json.data || [];
    all.push(...rows);
    if (rows.length < CONFIG.PAGE_SIZE) break;
    offset += CONFIG.PAGE_SIZE;
  }
  return all;
}

function modeFromRunId(runId) {
  const m = runId.match(/-bal-(sequential|nobatchio|full)$/);
  return m ? m[1] : null;
}

function groupByTest(rows) {
  const groups = new Map();
  for (const row of rows) {
    const mode = modeFromRunId(row.run_id);
    if (!mode) continue;
    if (row.test_mgas_s == null || row.test_mgas_s <= 0) continue;
    if (!groups.has(row.test_name)) {
      groups.set(row.test_name, { sequential: [], nobatchio: [], full: [] });
    }
    groups.get(row.test_name)[mode].push(row.test_mgas_s);
  }
  return groups;
}

function aggregate(values, method) {
  if (!values.length) return null;
  if (method === 'mean') {
    return values.reduce((a, b) => a + b, 0) / values.length;
  }
  const sorted = [...values].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2;
}

function fmtPct(x) {
  const sign = x >= 0 ? '+' : '';
  return `${sign}${(x * 100).toFixed(1)}%`;
}

function buildEntries(method) {
  const groups = groupByTest(state.rows);
  const entries = [];
  for (const [test, g] of groups.entries()) {
    const aggs = {};
    const counts = {};
    for (const mode of MODES) {
      aggs[mode] = aggregate(g[mode], method);
      counts[mode] = g[mode].length;
    }
    entries.push({ test, aggs, counts });
  }
  return entries;
}

function buildClientFullEntries(method) {
  const byTest = new Map();
  for (const client of CLIENTS) {
    const rows = state.rowsByClient[client] || [];
    for (const row of rows) {
      if (modeFromRunId(row.run_id) !== 'full') continue;
      if (row.test_mgas_s == null || row.test_mgas_s <= 0) continue;
      let e = byTest.get(row.test_name);
      if (!e) {
        e = { test: row.test_name, aggs: {}, counts: {}, _raw: {} };
        for (const c of CLIENTS) {
          e._raw[c] = [];
          e.counts[c] = 0;
        }
        byTest.set(row.test_name, e);
      }
      e._raw[client].push(row.test_mgas_s);
      e.counts[client] += 1;
    }
  }
  const entries = [];
  for (const e of byTest.values()) {
    for (const c of CLIENTS) e.aggs[c] = aggregate(e._raw[c], method);
    delete e._raw;
    entries.push(e);
  }
  return entries;
}

function tabInfo() {
  if (state.tab === 'clients-full') {
    return {
      kind: 'clients',
      columns: CLIENTS,
      slowCols: CLIENTS,
      showGain: false,
      colorMap: CLIENT_COLORS,
    };
  }
  const match = state.tab.match(/^slowest-(\d+)$/);
  return {
    kind: 'modes',
    columns: MODES,
    slowCols: ['nobatchio', 'full'],
    showGain: true,
    colorMap: MODE_COLORS,
    slowestN: match ? parseInt(match[1], 10) : null,
  };
}

function rowSlowness(entry, cols) {
  const vals = cols.map((c) => entry.aggs[c]).filter((v) => v != null);
  return vals.length ? Math.min(...vals) : null;
}

function findSlowestEntry(entries, cols) {
  let slowest = null;
  let slowestVal = Infinity;
  for (const entry of entries) {
    const v = rowSlowness(entry, cols);
    if (v != null && v < slowestVal) {
      slowestVal = v;
      slowest = entry;
    }
  }
  return slowest;
}

function makeRow(entry, { info, comparison, isSlowest }) {
  const tr = document.createElement('tr');
  tr.className = 'group cursor-pointer border-b border-gray-800 hover:bg-gray-900/60';
  tr.addEventListener('click', () => showChart(entry.test));
  const slowLabel = info.slowCols.map((c) => (info.kind === 'modes' ? modeLabel(c) : c)).join(' + ');
  tr.title = isSlowest
    ? `Slowest row across ${slowLabel} — click for chart`
    : 'Click to chart MGas/s vs gas limit';
  if (isSlowest) {
    tr.classList.add('bg-rose-950/40', 'border-l-2', 'border-l-rose-500');
  }

  const tdTest = document.createElement('td');
  tdTest.className = 'px-3 py-2 text-xs text-gray-300';
  const showRaw = document.getElementById('raw-names-toggle').checked;
  tdTest.appendChild(makeTestCellContent(entry.test, showRaw));
  tr.appendChild(tdTest);

  const present = info.columns.map((c) => entry.aggs[c]).filter((v) => v != null);
  const best = present.length > 1 ? Math.max(...present) : null;

  for (const col of info.columns) {
    const td = document.createElement('td');
    td.className = 'px-3 py-2 text-right tabular-nums';
    const v = entry.aggs[col];
    if (v == null) {
      td.textContent = '—';
      td.classList.add('text-gray-600');
    } else {
      td.textContent = v.toFixed(2);
      td.title = `n=${entry.counts[col]}`;
      if (best != null && v === best) {
        td.classList.add('bg-emerald-900/40', 'text-emerald-300', 'font-semibold');
      }
    }
    if (comparison && (col === comparison.baseline || col === comparison.target)) {
      td.classList.add('ring-1', 'ring-inset', 'ring-gray-700');
    }
    tr.appendChild(td);
  }

  if (info.showGain && comparison) {
    const tdGain = document.createElement('td');
    tdGain.className = 'px-3 py-2 text-right tabular-nums';
    const b = entry.aggs[comparison.baseline];
    const t = entry.aggs[comparison.target];
    if (b != null && t != null && b > 0) {
      const gain = (t - b) / b;
      tdGain.textContent = fmtPct(gain);
      if (gain > 0.02) tdGain.classList.add('text-emerald-400');
      else if (gain < -0.02) tdGain.classList.add('text-rose-400');
      else tdGain.classList.add('text-gray-400');
    } else {
      tdGain.textContent = '—';
      tdGain.classList.add('text-gray-600');
    }
    tr.appendChild(tdGain);
  }

  return tr;
}

function cycleClientSort(col) {
  const s = state.clientSort;
  if (s.column !== col) {
    s.column = col;
    s.direction = 'asc';
  } else if (s.direction === 'asc') {
    s.direction = 'desc';
  } else {
    s.column = null;
    s.direction = null;
  }
  render();
}

function renderHead(info) {
  const tr = document.getElementById('head-row');
  tr.replaceChildren();
  const stickyBase =
    'sticky top-0 z-10 bg-gray-900 border-b border-gray-800 px-3 py-2 font-medium shadow-sm/30';
  const testTh = document.createElement('th');
  testTh.className = `${stickyBase} text-left`;
  testTh.textContent = 'Test';
  tr.appendChild(testTh);
  const sortable = info.kind === 'clients';
  for (const col of info.columns) {
    const th = document.createElement('th');
    th.className = `${stickyBase} text-right`;
    let label = info.kind === 'modes' ? modeLabel(col) : col;
    if (sortable && state.clientSort.column === col) {
      label += state.clientSort.direction === 'asc' ? ' ▲' : ' ▼';
    }
    th.textContent = label;
    if (sortable) {
      th.classList.add('cursor-pointer', 'select-none', 'hover:text-gray-200');
      th.title = 'Click to sort ascending · click again for descending · once more to clear';
      th.addEventListener('click', () => cycleClientSort(col));
    }
    tr.appendChild(th);
  }
  if (info.showGain) {
    const gainTh = document.createElement('th');
    gainTh.id = 'gain-header';
    gainTh.className = `${stickyBase} text-right`;
    tr.appendChild(gainTh);
  }
}

function renderSpreadSummary(entries, info) {
  const el = document.getElementById('gain-summary');
  const ratios = [];
  const gaps = [];
  for (const entry of entries) {
    const vals = info.columns.map((c) => entry.aggs[c]).filter((v) => v != null && v > 0);
    if (vals.length < 2) continue;
    const mx = Math.max(...vals);
    const mn = Math.min(...vals);
    ratios.push(mx / mn);
    gaps.push((mx - mn) / mn);
  }
  el.className = 'mt-4 rounded-xs border border-gray-800 bg-gray-900/40 px-4 py-3 text-sm';
  el.replaceChildren();
  if (!ratios.length) {
    el.textContent = 'No tests have full-mode data from 2+ clients.';
    el.classList.add('text-gray-400');
    return;
  }
  const meanRatio = ratios.reduce((a, b) => a + b, 0) / ratios.length;
  const medianRatio = aggregate(ratios, 'median');
  const maxRatio = Math.max(...ratios);
  const meanGap = gaps.reduce((a, b) => a + b, 0) / gaps.length;
  const line1 = document.createElement('div');
  line1.className = 'text-gray-400';
  line1.textContent = `Full-mode client spread across ${ratios.length} tests:`;
  const line2 = document.createElement('div');
  line2.className = 'mt-1 text-lg font-semibold tabular-nums text-gray-100';
  line2.textContent = `${meanRatio.toFixed(2)}× (fastest / slowest, mean)`;
  const line3 = document.createElement('div');
  line3.className = 'mt-1 text-xs text-gray-500 tabular-nums';
  line3.textContent = `median ${medianRatio.toFixed(2)}× · max ${maxRatio.toFixed(2)}× · avg gap ${fmtPct(meanGap)}`;
  el.append(line1, line2, line3);
}

function renderGainSummary(entries, comparison) {
  const el = document.getElementById('gain-summary');
  const gains = [];
  const ratios = [];
  for (const entry of entries) {
    const b = entry.aggs[comparison.baseline];
    const t = entry.aggs[comparison.target];
    if (b != null && t != null && b > 0) {
      gains.push((t - b) / b);
      ratios.push(t / b);
    }
  }
  if (!gains.length) {
    el.textContent = `No tests have both ${modeLabel(comparison.target)} and ${modeLabel(comparison.baseline)} data.`;
    el.className = 'mt-4 rounded-xs border border-gray-800 bg-gray-900/40 px-4 py-3 text-sm text-gray-400';
    return;
  }
  const meanGain = gains.reduce((a, b) => a + b, 0) / gains.length;
  const geomeanRatio = Math.exp(ratios.reduce((a, b) => a + Math.log(b), 0) / ratios.length);
  const geoGain = geomeanRatio - 1;
  const medianGain = aggregate(gains, 'median');
  const wins = gains.filter((g) => g > 0.02).length;
  const losses = gains.filter((g) => g < -0.02).length;

  const label = `${modeLabel(comparison.target)} vs ${modeLabel(comparison.baseline)}`;
  const mainColor =
    meanGain > 0.02 ? 'text-emerald-300' : meanGain < -0.02 ? 'text-rose-300' : 'text-gray-200';

  el.className = 'mt-4 rounded-xs border border-gray-800 bg-gray-900/40 px-4 py-3 text-sm';
  el.replaceChildren();
  const line1 = document.createElement('div');
  line1.className = 'text-gray-400';
  line1.textContent = `Avg gain (${label}) across ${gains.length} tests:`;
  const line2 = document.createElement('div');
  line2.className = `mt-1 text-lg font-semibold tabular-nums ${mainColor}`;
  line2.textContent = fmtPct(meanGain);
  const line3 = document.createElement('div');
  line3.className = 'mt-1 text-xs text-gray-500 tabular-nums';
  line3.textContent = `median ${fmtPct(medianGain)} · geomean ${fmtPct(geoGain)} · ${wins} wins, ${losses} regressions (|Δ| > 2%)`;
  el.append(line1, line2, line3);
}

function isEntryEnabled(entry) {
  const gas = extractGasLimit(entry.test);
  return !state.disabledGasLimits.has(gasLimitKey(gas));
}

function compareBySortKey(a, b) {
  const ak = extractSortKey(a.test);
  const bk = extractSortKey(b.test);
  if (ak !== bk) return ak.localeCompare(bk);

  const ag = extractGbSize(a.test);
  const bg = extractGbSize(b.test);
  if (ag != null && bg != null && ag !== bg) return ag - bg;
  if (ag != null && bg == null) return -1;
  if (ag == null && bg != null) return 1;

  return a.test.localeCompare(b.test);
}

function render() {
  const method = document.getElementById('method').value;
  const comparisonKey = document.getElementById('comparison').value;
  const comparison = COMPARISONS[comparisonKey];
  const info = tabInfo();

  renderHead(info);
  if (info.showGain) {
    document.getElementById('gain-header').textContent = `Gain (${modeLabel(comparison.target)} vs ${modeLabel(comparison.baseline)})`;
  }

  let entries;
  if (info.kind === 'clients') {
    entries = buildClientFullEntries(method).filter(isEntryEnabled);
  } else {
    entries = buildEntries(method).filter(isEntryEnabled);
  }

  let displayed;
  if (info.kind === 'modes' && info.slowestN != null) {
    displayed = entries
      .filter((e) => e.aggs[comparison.baseline] != null)
      .sort((a, b) => a.aggs[comparison.baseline] - b.aggs[comparison.baseline])
      .slice(0, info.slowestN);
  } else if (info.kind === 'clients' && state.clientSort.column) {
    const col = state.clientSort.column;
    const dir = state.clientSort.direction === 'desc' ? -1 : 1;
    displayed = [...entries].sort((a, b) => {
      const av = a.aggs[col];
      const bv = b.aggs[col];
      if (av == null && bv == null) return compareBySortKey(a, b);
      if (av == null) return 1;
      if (bv == null) return -1;
      if (av === bv) return compareBySortKey(a, b);
      return (av - bv) * dir;
    });
  } else {
    displayed = [...entries].sort(compareBySortKey);
  }

  const slowestEntry = findSlowestEntry(displayed, info.slowCols);
  const tbody = document.getElementById('results-body');
  tbody.replaceChildren();
  for (const entry of displayed) {
    tbody.appendChild(
      makeRow(entry, {
        info,
        comparison: info.showGain ? comparison : null,
        isSlowest: entry === slowestEntry,
      }),
    );
  }

  if (info.kind === 'clients') {
    renderSpreadSummary(displayed, info);
  } else {
    renderGainSummary(displayed, comparison);
  }

  const totalRows = Object.values(state.rowsByClient).reduce((a, r) => a + (r?.length || 0), 0);
  document.getElementById('summary').textContent =
    info.kind === 'clients'
      ? `${displayed.length} tests · ${method} · ${totalRows} total rows (all clients)`
      : `${displayed.length} rows · ${method} · ${state.rows.length} rows for ${state.client}`;
}

function setTab(tab) {
  state.tab = tab;
  for (const btn of document.querySelectorAll('.tab')) {
    const active = btn.dataset.tab === tab;
    btn.classList.toggle('border-emerald-500', active);
    btn.classList.toggle('text-gray-100', active);
    btn.classList.toggle('border-transparent', !active);
    btn.classList.toggle('text-gray-400', !active);
  }
  render();
}

let chartInstances = [];

function buildFamilyBuckets(rows, familyKey) {
  const buckets = new Map();
  const uniqTests = new Set();
  let samples = 0;
  for (const row of rows) {
    if (extractFamilyKey(row.test_name) !== familyKey) continue;
    const mode = modeFromRunId(row.run_id);
    if (!mode) continue;
    if (row.test_mgas_s == null || row.test_mgas_s <= 0) continue;
    const gas = extractGasLimit(row.test_name);
    if (gas == null) continue;
    if (!buckets.has(gas)) buckets.set(gas, { sequential: [], nobatchio: [], full: [] });
    buckets.get(gas)[mode].push(row.test_mgas_s);
    uniqTests.add(row.test_name);
    samples += 1;
  }
  return { buckets, uniqTests, samples };
}

function lineChartOptions() {
  const grid = '#1f2937';
  const text = '#9ca3af';
  return {
    responsive: true,
    maintainAspectRatio: false,
    interaction: { mode: 'index', intersect: false },
    scales: {
      x: {
        title: { display: true, text: 'Gas limit', color: text },
        ticks: { color: text },
        grid: { color: grid },
      },
      y: {
        title: { display: true, text: 'MGas/s', color: text },
        beginAtZero: true,
        ticks: { color: text },
        grid: { color: grid },
      },
    },
    plugins: {
      legend: { labels: { color: '#e5e7eb' } },
      tooltip: {
        backgroundColor: '#111827',
        borderColor: '#374151',
        borderWidth: 1,
        titleColor: '#f3f4f6',
        bodyColor: '#d1d5db',
        callbacks: {
          label: (ctx) =>
            `${ctx.dataset.label}: ${ctx.parsed.y == null ? '—' : ctx.parsed.y.toFixed(2)}`,
        },
      },
    },
  };
}

function renderClientChart(client, buckets, method) {
  const gasLimits = [...buckets.keys()].sort((a, b) => a - b);
  const datasets = MODES.map((mode) => ({
    label: modeLabel(mode),
    data: gasLimits.map((g) => aggregate(buckets.get(g)[mode], method)),
    borderColor: MODE_COLORS[mode],
    backgroundColor: MODE_COLORS[mode],
    spanGaps: true,
    tension: 0.2,
    pointRadius: 4,
    pointHoverRadius: 6,
  }));

  const canvas = document.createElement('canvas');
  const chart = new Chart(canvas, {
    type: 'line',
    data: { labels: gasLimits.map((g) => `${g}M`), datasets },
    options: lineChartOptions(),
  });
  chartInstances.push(chart);
  return canvas;
}

function renderFullAcrossClientsChart(familyKey, method) {
  const perClient = {};
  const allGas = new Set();
  for (const client of CLIENTS) {
    const rows = state.rowsByClient[client] || [];
    const bucket = new Map();
    for (const row of rows) {
      if (extractFamilyKey(row.test_name) !== familyKey) continue;
      if (modeFromRunId(row.run_id) !== 'full') continue;
      if (row.test_mgas_s == null || row.test_mgas_s <= 0) continue;
      const gas = extractGasLimit(row.test_name);
      if (gas == null) continue;
      if (!bucket.has(gas)) bucket.set(gas, []);
      bucket.get(gas).push(row.test_mgas_s);
      allGas.add(gas);
    }
    perClient[client] = bucket;
  }
  const gasLimits = [...allGas].sort((a, b) => a - b);
  const datasets = [];
  for (const client of CLIENTS) {
    const bucket = perClient[client];
    if (!bucket || !bucket.size) continue;
    datasets.push({
      label: client,
      data: gasLimits.map((g) => aggregate(bucket.get(g) || [], method)),
      borderColor: CLIENT_COLORS[client],
      backgroundColor: CLIENT_COLORS[client],
      spanGaps: true,
      tension: 0.2,
      pointRadius: 4,
      pointHoverRadius: 6,
    });
  }
  if (!datasets.length) return null;
  const canvas = document.createElement('canvas');
  const chart = new Chart(canvas, {
    type: 'line',
    data: { labels: gasLimits.map((g) => `${g}M`), datasets },
    options: lineChartOptions(),
  });
  chartInstances.push(chart);
  return canvas;
}

function showChart(testName) {
  const method = document.getElementById('method').value;
  const familyKey = extractFamilyKey(testName);

  for (const c of chartInstances) c.destroy();
  chartInstances = [];

  const container = document.getElementById('chart-container');
  container.replaceChildren();

  const fullCanvas = renderFullAcrossClientsChart(familyKey, method);
  if (fullCanvas) {
    const block = document.createElement('div');
    const header = document.createElement('div');
    header.className = 'mb-1 text-xs text-gray-400';
    header.innerHTML = 'All clients — <span class="font-semibold text-gray-200">full</span> mode';
    const canvasWrap = document.createElement('div');
    canvasWrap.className = 'relative h-64';
    canvasWrap.appendChild(fullCanvas);
    block.append(header, canvasWrap);
    const divider = document.createElement('div');
    divider.className = 'border-t border-gray-800';
    container.append(block, divider);
  }

  const presentClients = [];
  let totalVariants = 0;
  let totalSamples = 0;

  for (const client of CLIENTS) {
    const rows = state.rowsByClient[client] || [];
    const { buckets, uniqTests, samples } = buildFamilyBuckets(rows, familyKey);
    if (!buckets.size) continue;

    const block = document.createElement('div');
    const header = document.createElement('div');
    header.className = 'mb-1 flex items-baseline justify-between gap-2 text-xs text-gray-400';
    const name = document.createElement('span');
    name.innerHTML = `Client: <span class="text-gray-200 font-semibold">${client}</span>`;
    const meta = document.createElement('span');
    meta.textContent = `${uniqTests.size} variants · ${samples} samples`;
    header.append(name, meta);
    const canvasWrap = document.createElement('div');
    canvasWrap.className = 'relative h-64';
    canvasWrap.appendChild(renderClientChart(client, buckets, method));
    block.append(header, canvasWrap);
    container.appendChild(block);

    presentClients.push(client);
    totalVariants += uniqTests.size;
    totalSamples += samples;
  }

  if (!presentClients.length) {
    const p = document.createElement('p');
    p.className = 'text-xs text-gray-500';
    p.textContent = 'No gas-limit variants found for this test across any client.';
    container.appendChild(p);
  }

  document.getElementById('chart-title').textContent = displayTestName(familyKey);
  document.getElementById('chart-note').textContent = presentClients.length
    ? `${presentClients.join(', ')} · ${totalVariants} variants · ${totalSamples} samples · aggregation: ${method}`
    : '';
  document.getElementById('chart-modal').showModal();
}

function renderGasLimitFilters() {
  const container = document.getElementById('gas-limit-filters');
  const found = new Set();
  for (const client of CLIENTS) {
    for (const row of state.rowsByClient[client] || []) {
      found.add(extractGasLimit(row.test_name));
    }
  }
  const limits = [...found].sort((a, b) => {
    if (a == null) return 1;
    if (b == null) return -1;
    return a - b;
  });
  container.replaceChildren();
  if (!limits.length) return;
  for (const gas of limits) {
    const key = gasLimitKey(gas);
    const label = document.createElement('label');
    label.className = 'flex items-center gap-1.5';
    const cb = document.createElement('input');
    cb.type = 'checkbox';
    cb.className = 'size-3.5 accent-emerald-500';
    cb.checked = !state.disabledGasLimits.has(key);
    cb.addEventListener('change', () => {
      if (cb.checked) state.disabledGasLimits.delete(key);
      else state.disabledGasLimits.add(key);
      render();
    });
    const span = document.createElement('span');
    span.textContent = gas == null ? 'other' : `${gas}M`;
    label.append(cb, span);
    container.appendChild(label);
  }
}

function selectClient(client) {
  state.client = client;
  state.rows = state.rowsByClient[client] || [];
  render();
}

async function reloadAll() {
  const status = document.getElementById('status');
  if (!getApiKey()) {
    status.textContent = 'API key required';
    status.className = 'text-amber-400';
    openApiKeyModal();
    return;
  }
  status.textContent = 'loading…';
  status.className = 'text-amber-400';
  const [results, runsResult] = await Promise.all([
    Promise.all(
      CLIENTS.map((c) =>
        fetchAll(c).then(
          (rows) => ({ client: c, rows, error: null }),
          (e) => ({ client: c, rows: [], error: e }),
        ),
      ),
    ),
    fetchRuns().then(
      (rows) => ({ rows, error: null }),
      (e) => ({ rows: [], error: e }),
    ),
  ]);
  state.runs = runsResult.rows || [];
  state.rowsByClient = {};
  const errors = [];
  for (const r of results) {
    state.rowsByClient[r.client] = r.rows;
    if (r.error) errors.push(`${r.client}: ${r.error.message}`);
  }
  if (errors.length === CLIENTS.length) {
    const first = results.find((r) => r.error)?.error;
    status.textContent = `error: ${first?.message || 'load failed'}`;
    status.className = 'text-rose-400';
    if (first?.code === 'UNAUTHORIZED') openApiKeyModal(true);
    return;
  }
  status.textContent = errors.length
    ? `ok · ${CLIENTS.length - errors.length}/${CLIENTS.length} clients (errors: ${errors.join('; ')})`
    : `ok · ${CLIENTS.length} clients loaded`;
  status.className = errors.length ? 'text-amber-400' : 'text-emerald-400';
  renderGasLimitFilters();
  selectClient(document.getElementById('client').value);
}

function openApiKeyModal(showError = false) {
  const dlg = document.getElementById('api-key-modal');
  const input = document.getElementById('api-key-input');
  const errEl = document.getElementById('api-key-error');
  const currentEl = document.getElementById('api-key-current');
  const current = getApiKey();
  currentEl.textContent = current ? `Current: ${maskKey(current)}` : 'No key set.';
  errEl.textContent = showError ? 'The stored key was rejected — please enter a new one.' : '';
  errEl.classList.toggle('hidden', !showError);
  input.value = '';
  dlg.showModal();
  input.focus();
}

function closeApiKeyModal() {
  document.getElementById('api-key-modal').close();
}

function saveApiKeyFromForm(e) {
  e.preventDefault();
  const input = document.getElementById('api-key-input');
  const value = input.value.trim();
  if (!value) return;
  setApiKey(value);
  closeApiKeyModal();
  reloadAll();
}

function init() {
  const clientSel = document.getElementById('client');
  for (const c of CLIENTS) {
    const o = document.createElement('option');
    o.value = c;
    o.textContent = c;
    clientSel.appendChild(o);
  }
  clientSel.addEventListener('change', () => selectClient(clientSel.value));
  document.getElementById('method').addEventListener('change', render);
  document.getElementById('comparison').addEventListener('change', render);
  document.getElementById('raw-names-toggle').addEventListener('change', render);
  for (const btn of document.querySelectorAll('.tab')) {
    btn.addEventListener('click', () => setTab(btn.dataset.tab));
  }
  const dlg = document.getElementById('chart-modal');
  document.getElementById('chart-close').addEventListener('click', () => dlg.close());
  dlg.addEventListener('click', (e) => {
    if (e.target === dlg) dlg.close();
  });

  document.getElementById('api-key-btn').addEventListener('click', () => openApiKeyModal());
  document.getElementById('api-key-form').addEventListener('submit', saveApiKeyFromForm);
  document.getElementById('api-key-cancel').addEventListener('click', closeApiKeyModal);

  const versionsDlg = document.getElementById('versions-modal');
  document.getElementById('versions-btn').addEventListener('click', openVersionsModal);
  document.getElementById('versions-close').addEventListener('click', () => versionsDlg.close());
  versionsDlg.addEventListener('click', (e) => {
    if (e.target === versionsDlg) versionsDlg.close();
  });

  setTab('all');
  reloadAll();
}

init();
