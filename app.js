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
  columnSort: { column: null, direction: null },
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
  const min = minTimestampFilter();
  for (const run of state.runs || []) {
    const client = run.client;
    const image = run.image;
    if (!client || !image) continue;
    if (min && run.timestamp && run.timestamp < min) continue;
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

const EXECUTION_SPECS_REPO = 'https://github.com/ethereum/execution-specs';
const EXECUTION_SPECS_BRANCH = 'forks/amsterdam';

const TEST_FILE_PATHS = {
  'test_block_access_lists_compute_then_sload.py':
    'tests/benchmark/stateful/eip7928_block_level_access_lists/test_block_access_lists_compute_then_sload.py',
  'test_block_access_lists_max_accounts.py':
    'tests/benchmark/stateful/eip7928_block_level_access_lists/test_block_access_lists_max_accounts.py',
  'test_block_access_lists_max_sloads.py':
    'tests/benchmark/stateful/eip7928_block_level_access_lists/test_block_access_lists_max_sloads.py',
  'test_block_access_lists_pointer_chase.py':
    'tests/benchmark/stateful/eip7928_block_level_access_lists/test_block_access_lists_pointer_chase.py',
  'test_single_opcode.py': 'tests/benchmark/stateful/bloatnet/test_single_opcode.py',
  'test_multi_opcode.py': 'tests/benchmark/stateful/bloatnet/test_multi_opcode.py',
  'test_transient_storage.py': 'tests/benchmark/stateful/bloatnet/test_transient_storage.py',
  'test_create2_access.py': 'tests/benchmark/stateful/bloatnet/test_create2_access.py',
  'test_extcodesize_bytecode_sizes.py':
    'tests/benchmark/stateful/bloatnet/test_extcodesize_bytecode_sizes.py',
  'test_account_query.py': 'tests/benchmark/stateful/bloatnet/test_account_query.py',
};

const TEST_FUNC_LINES = {
  test_bal_compute_then_sload: 70,
  test_bal_max_sloads: 64,
  test_bal_max_account_access: 85,
  test_bal_max_pointer_chase: 62,
  test_sload_bloated: 198,
  test_sload_bloated_prefetch_miss: 256,
  test_sload_bloated_multi_contract: 416,
  test_sstore_bloated: 579,
  test_sload_erc20_generic: 665,
  test_sstore_erc20_generic: 796,
  test_sstore_variants: 1238,
  test_tstore_unique_keys: 55,
  test_tstore_same_key: 118,
  test_bloatnet_balance_opcode: 69,
  test_bloatnet_call_value_existing: 223,
  test_bloatnet_call_value_new_account: 320,
  test_mixed_sload_sstore: 415,
  test_create2_immediate_access: 66,
  test_extcodesize_bytecode_sizes: 173,
  test_ext_account_query_warm: 65,
};

function testSourceUrl(testName) {
  const parsed = parseTestName(testName);
  if (!parsed.file || !parsed.func) return null;
  const path = TEST_FILE_PATHS[parsed.file];
  if (!path) return null;
  const line = TEST_FUNC_LINES[parsed.func];
  const frag = line ? `#L${line}` : '';
  return `${EXECUTION_SPECS_REPO}/blob/${EXECUTION_SPECS_BRANCH}/${path}${frag}`;
}

function makeExternalLinkIcon(url, { title = 'Open test source on GitHub', size = 'size-3.5' } = {}) {
  const a = document.createElement('a');
  a.href = url;
  a.target = '_blank';
  a.rel = 'noopener noreferrer';
  a.title = title;
  a.className = `inline-flex shrink-0 items-center text-gray-500 hover:text-emerald-400 ${size}`;
  a.addEventListener('click', (e) => e.stopPropagation());
  const svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
  svg.setAttribute('viewBox', '0 0 20 20');
  svg.setAttribute('fill', 'currentColor');
  svg.setAttribute('aria-hidden', 'true');
  svg.classList.add('size-3.5');
  const p1 = document.createElementNS('http://www.w3.org/2000/svg', 'path');
  p1.setAttribute('d', 'M11 3a1 1 0 1 0 0 2h3.586L8.293 11.293a1 1 0 1 0 1.414 1.414L16 6.414V10a1 1 0 1 0 2 0V4a1 1 0 0 0-1-1h-6z');
  const p2 = document.createElementNS('http://www.w3.org/2000/svg', 'path');
  p2.setAttribute('d', 'M5 5a2 2 0 0 0-2 2v8a2 2 0 0 0 2 2h8a2 2 0 0 0 2-2v-3a1 1 0 0 0-2 0v3H5V7h3a1 1 0 0 0 0-2H5z');
  svg.append(p1, p2);
  a.appendChild(svg);
  return a;
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
  const sourceUrl = testSourceUrl(testName);
  if (showRaw) {
    const inner = document.createElement('div');
    inner.className = 'flex items-start gap-1.5';
    const nameSpan = document.createElement('span');
    nameSpan.className = 'break-all font-mono';
    nameSpan.textContent = displayTestName(testName);
    inner.append(icon, nameSpan);
    if (sourceUrl) inner.appendChild(makeExternalLinkIcon(sourceUrl));
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
  if (sourceUrl) line1.appendChild(makeExternalLinkIcon(sourceUrl));
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

async function fetchCache() {
  try {
    const res = await fetch('./data/cache.json', { cache: 'no-cache' });
    if (!res.ok) return null;
    return await res.json();
  } catch {
    return null;
  }
}

function expandCachedRow(r, runIds, testNames) {
  return {
    id: r[0],
    run_id: runIds[r[1]],
    test_name: testNames[r[2]],
    test_mgas_s: r[3],
  };
}

async function fetchDeltaSince(minId) {
  const all = [];
  let offset = 0;
  while (true) {
    const url = `${CONFIG.BASE_URL}?suite_hash=eq.${CONFIG.SUITE_HASH}&id=gt.${minId}&select=id,run_id,test_name,test_mgas_s,client&limit=${CONFIG.PAGE_SIZE}&offset=${offset}`;
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

function formatCacheAge(ts) {
  if (!ts) return '';
  const age = Math.max(0, Math.floor(Date.now() / 1000 - ts));
  if (age < 60) return `${age}s ago`;
  if (age < 3600) return `${Math.floor(age / 60)}m ago`;
  if (age < 86400) return `${Math.floor(age / 3600)}h ago`;
  return `${Math.floor(age / 86400)}d ago`;
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

function timestampFromRunId(runId) {
  const m = runId.match(/^(\d+)_/);
  return m ? parseInt(m[1], 10) : null;
}

function minTimestampFilter() {
  const sel = document.getElementById('runs-after');
  if (!sel) return 0;
  const v = parseInt(sel.value, 10);
  return Number.isFinite(v) ? v : 0;
}

function isRowInTimeWindow(row) {
  const min = minTimestampFilter();
  if (!min) return true;
  const ts = timestampFromRunId(row.run_id);
  return ts != null && ts >= min;
}

function successfulRunsForClientMode(client, mode) {
  const set = new Set();
  for (const row of state.rowsByClient[client] || []) {
    if (!isRowInTimeWindow(row)) continue;
    if (row.test_mgas_s == null || row.test_mgas_s <= 0) continue;
    if (mode && modeFromRunId(row.run_id) !== mode) continue;
    set.add(row.run_id);
  }
  return set.size;
}

function successfulRunsByClient(modeFilter = null) {
  const map = {};
  for (const client of CLIENTS) {
    map[client] = successfulRunsForClientMode(client, modeFilter);
  }
  return map;
}

const HIGH_COV_THRESHOLD = 0.2;

function makeHighVarianceIcon(cov) {
  const svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
  svg.setAttribute('viewBox', '0 0 20 20');
  svg.setAttribute('fill', 'currentColor');
  svg.setAttribute('aria-hidden', 'true');
  svg.classList.add('ml-1', 'inline-block', 'size-3.5', 'text-amber-400', 'align-text-bottom');
  const p = document.createElementNS('http://www.w3.org/2000/svg', 'path');
  p.setAttribute('fill-rule', 'evenodd');
  p.setAttribute('clip-rule', 'evenodd');
  p.setAttribute(
    'd',
    'M8.485 2.495c.673-1.167 2.357-1.167 3.03 0l6.28 10.875c.673 1.167-.17 2.625-1.516 2.625H3.72c-1.347 0-2.189-1.458-1.515-2.625L8.485 2.495zM10 6a.75.75 0 0 1 .75.75v3.5a.75.75 0 0 1-1.5 0v-3.5A.75.75 0 0 1 10 6zm0 9a1 1 0 1 0 0-2 1 1 0 0 0 0 2z',
  );
  svg.appendChild(p);
  const title = document.createElementNS('http://www.w3.org/2000/svg', 'title');
  title.textContent = `High run-to-run variance · CoV ${(cov * 100).toFixed(1)}% (≥ ${(HIGH_COV_THRESHOLD * 100).toFixed(0)}%)`;
  svg.appendChild(title);
  return svg;
}

function computeStats(values, method) {
  const n = values.length;
  if (n === 0) return { value: null, n: 0, std: null, cov: null };
  const value = aggregate(values, method);
  if (n < 2) return { value, n, std: null, cov: null };
  const mean = values.reduce((a, b) => a + b, 0) / n;
  const sumSq = values.reduce((s, v) => s + (v - mean) ** 2, 0);
  const std = Math.sqrt(sumSq / n);
  const cov = mean > 0 ? std / mean : null;
  return { value, n, std, cov };
}

function hourBucket(ts) {
  return Math.floor(ts / 3600) * 3600;
}

function formatHourOption(hourTs) {
  const d = new Date(hourTs * 1000);
  return d.toLocaleString(undefined, {
    month: 'short',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
  });
}

function renderRunsFilter() {
  const sel = document.getElementById('runs-after');
  if (!sel) return;
  const prev = sel.value;
  const hours = new Set();
  for (const run of state.runs || []) {
    if (run.timestamp) hours.add(hourBucket(run.timestamp));
  }
  for (const client of CLIENTS) {
    for (const row of state.rowsByClient[client] || []) {
      const ts = timestampFromRunId(row.run_id);
      if (ts != null) hours.add(hourBucket(ts));
    }
  }
  const sorted = [...hours].sort((a, b) => b - a);
  sel.replaceChildren();
  const allOpt = document.createElement('option');
  allOpt.value = '0';
  allOpt.textContent = 'All runs';
  sel.appendChild(allOpt);
  for (const h of sorted) {
    const opt = document.createElement('option');
    opt.value = String(h);
    opt.textContent = formatHourOption(h);
    sel.appendChild(opt);
  }
  if (prev && [...sel.options].some((o) => o.value === prev)) {
    sel.value = prev;
  }
}

function groupByTest(rows) {
  const groups = new Map();
  for (const row of rows) {
    const mode = modeFromRunId(row.run_id);
    if (!mode) continue;
    if (row.test_mgas_s == null || row.test_mgas_s <= 0) continue;
    if (!isRowInTimeWindow(row)) continue;
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
    const stds = {};
    const covs = {};
    for (const mode of MODES) {
      const s = computeStats(g[mode], method);
      aggs[mode] = s.value;
      counts[mode] = s.n;
      stds[mode] = s.std;
      covs[mode] = s.cov;
    }
    entries.push({ test, aggs, counts, stds, covs });
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
      if (!isRowInTimeWindow(row)) continue;
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
    e.stds = {};
    e.covs = {};
    for (const c of CLIENTS) {
      const s = computeStats(e._raw[c], method);
      e.aggs[c] = s.value;
      e.counts[c] = s.n;
      e.stds[c] = s.std;
      e.covs[c] = s.cov;
    }
    delete e._raw;
    entries.push(e);
  }
  return entries;
}

function tabInfo() {
  if (state.tab === 'leaderboard') {
    return { kind: 'leaderboard' };
  }
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
      const n = entry.counts[col];
      const std = entry.stds?.[col];
      const cov = entry.covs?.[col];
      const valueSpan = document.createElement('span');
      valueSpan.textContent = v.toFixed(2);
      td.appendChild(valueSpan);
      if (cov != null && cov >= HIGH_COV_THRESHOLD) {
        td.appendChild(makeHighVarianceIcon(cov));
      }
      let title = `n=${n} run${n === 1 ? '' : 's'}`;
      if (std != null) {
        title += ` · σ=${std.toFixed(2)} MGas/s`;
        if (cov != null) title += ` · CoV=${(cov * 100).toFixed(1)}%`;
      }
      td.title = title;
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

function cycleColumnSort(col) {
  const s = state.columnSort;
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
  const clientRunCounts = info.kind === 'clients' ? successfulRunsByClient('full') : null;
  for (const col of info.columns) {
    const th = document.createElement('th');
    th.className = `${stickyBase} text-right`;
    let label;
    let titleSuffix = '';
    if (info.kind === 'modes') {
      const n = successfulRunsForClientMode(state.client, col);
      label = `${modeLabel(col)} (${n})`;
      titleSuffix = ` — ${n} successful ${modeLabel(col)} runs for ${state.client}`;
    } else {
      const n = clientRunCounts[col] ?? 0;
      label = `${col} (${n})`;
      titleSuffix = ` — ${n} successful Full runs`;
    }
    if (state.columnSort.column === col) {
      label += state.columnSort.direction === 'asc' ? ' ▲' : ' ▼';
    }
    th.textContent = label;
    th.classList.add('cursor-pointer', 'select-none', 'hover:text-gray-200');
    th.title = `${col}${titleSuffix}. Click to sort.`;
    th.addEventListener('click', () => cycleColumnSort(col));
    tr.appendChild(th);
  }
  if (info.showGain) {
    const gainTh = document.createElement('th');
    gainTh.id = 'gain-header';
    gainTh.className = `${stickyBase} text-right`;
    const gainKey = '__gain__';
    if (state.columnSort.column === gainKey) {
      gainTh.dataset.sort = state.columnSort.direction;
    }
    gainTh.classList.add('cursor-pointer', 'select-none', 'hover:text-gray-200');
    gainTh.title = 'Click to sort ascending · click again for descending · once more to clear';
    gainTh.addEventListener('click', () => cycleColumnSort(gainKey));
    tr.appendChild(gainTh);
  }
}

function renderSpreadSummary(entries, info) {
  const el = document.getElementById('gain-summary');
  el.className = 'mt-4 rounded-xs border border-gray-800 bg-gray-900/40 px-4 py-3 text-sm';
  el.replaceChildren();

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

  const spreadBlock = document.createElement('div');
  if (!ratios.length) {
    spreadBlock.className = 'text-gray-400';
    spreadBlock.textContent = 'No tests have full-mode data from 2+ clients.';
  } else {
    const meanRatio = ratios.reduce((a, b) => a + b, 0) / ratios.length;
    const medianRatio = aggregate(ratios, 'median');
    const maxRatio = Math.max(...ratios);
    const meanGap = gaps.reduce((a, b) => a + b, 0) / gaps.length;
    const l1 = document.createElement('div');
    l1.className = 'text-gray-400';
    l1.textContent = `Cross-client spread across ${ratios.length} tests (fastest / slowest):`;
    const l2 = document.createElement('div');
    l2.className = 'mt-1 text-lg font-semibold tabular-nums text-gray-100';
    l2.textContent = `${meanRatio.toFixed(2)}× (mean)`;
    const l3 = document.createElement('div');
    l3.className = 'mt-1 text-xs text-gray-500 tabular-nums';
    l3.textContent = `median ${medianRatio.toFixed(2)}× · max ${maxRatio.toFixed(2)}× · avg gap ${fmtPct(meanGap)}`;
    spreadBlock.append(l1, l2, l3);
  }
  el.appendChild(spreadBlock);

  const perClientCovs = {};
  for (const c of CLIENTS) perClientCovs[c] = [];
  for (const entry of entries) {
    if (!entry.covs) continue;
    for (const c of CLIENTS) {
      if (entry.covs[c] != null) perClientCovs[c].push(entry.covs[c]);
    }
  }
  const varianceStats = [];
  for (const c of CLIENTS) {
    const covs = perClientCovs[c];
    if (!covs.length) continue;
    varianceStats.push({
      client: c,
      median: aggregate(covs, 'median'),
      max: Math.max(...covs),
      n: covs.length,
    });
  }
  if (!varianceStats.length) return;

  const divider = document.createElement('div');
  divider.className = 'my-3 border-t border-gray-800';
  el.appendChild(divider);

  const title = document.createElement('div');
  title.className = 'text-gray-400';
  title.textContent = 'Run-to-run variance per client (median CoV across tests — higher = flakier):';
  el.appendChild(title);

  varianceStats.sort((a, b) => b.median - a.median);
  const worstMedian = varianceStats[0].median;

  const pills = document.createElement('div');
  pills.className = 'mt-2 flex flex-wrap gap-2';
  for (const s of varianceStats) {
    const pill = document.createElement('span');
    const isWorst = s.median === worstMedian && varianceStats.length > 1 && s.median > 0.01;
    pill.className =
      'rounded-xs border px-2 py-1 text-xs tabular-nums ' +
      (isWorst
        ? 'border-rose-800 bg-rose-950/50 text-rose-300'
        : s.median < 0.01
          ? 'border-emerald-800 bg-emerald-950/40 text-emerald-300'
          : s.median < 0.03
            ? 'border-gray-700 bg-gray-800/60 text-gray-300'
            : 'border-amber-800 bg-amber-950/40 text-amber-300');
    const clientSpan = document.createElement('span');
    clientSpan.className = 'font-semibold';
    clientSpan.style.color = CLIENT_COLORS[s.client] || '';
    clientSpan.textContent = s.client;
    const sep = document.createElement('span');
    sep.className = 'mx-1 text-gray-500';
    sep.textContent = '·';
    const medSpan = document.createElement('span');
    medSpan.textContent = `CoV ${(s.median * 100).toFixed(2)}%`;
    pill.append(clientSpan, sep, medSpan);
    pill.title = `${s.client} — median CoV ${(s.median * 100).toFixed(2)}% · worst test CoV ${(s.max * 100).toFixed(2)}% · across ${s.n} tests`;
    pills.appendChild(pill);
  }
  el.appendChild(pills);

  const hint = document.createElement('div');
  hint.className = 'mt-2 text-xs text-gray-500';
  hint.textContent =
    'CoV = σ / mean of a client\'s repeated runs of a single test. Hover any cell in the table to see the per-test σ and CoV.';
  el.appendChild(hint);
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

function refreshClientDropdownLabels() {
  const sel = document.getElementById('client');
  if (!sel) return;
  const counts = successfulRunsByClient(null);
  for (const opt of sel.options) {
    const c = opt.value;
    opt.textContent = counts[c] != null ? `${c} (${counts[c]} runs)` : c;
  }
}

function render() {
  refreshClientDropdownLabels();
  const method = document.getElementById('method').value;
  const comparisonKey = document.getElementById('comparison').value;
  const comparison = COMPARISONS[comparisonKey];
  const info = tabInfo();

  if (info.kind === 'leaderboard') {
    setLeaderboardVisibility(true);
    renderLeaderboard();
    document.getElementById('summary').textContent = '🏆 Leaderboard · Full mode only';
    return;
  }
  setLeaderboardVisibility(false);

  renderHead(info);
  if (info.showGain) {
    const base = `Gain (${modeLabel(comparison.target)} vs ${modeLabel(comparison.baseline)})`;
    const arrow = state.columnSort.column === '__gain__'
      ? (state.columnSort.direction === 'asc' ? ' ▲' : ' ▼')
      : '';
    document.getElementById('gain-header').textContent = base + arrow;
  }

  let entries;
  if (info.kind === 'clients') {
    entries = buildClientFullEntries(method).filter(isEntryEnabled);
  } else {
    entries = buildEntries(method).filter(isEntryEnabled);
  }

  const activeSort = state.columnSort.column;
  const sortableKeys = new Set(info.columns);
  if (info.showGain) sortableKeys.add('__gain__');
  const sortActive = activeSort && sortableKeys.has(activeSort);

  const gainValue = (entry) => {
    const b = entry.aggs[comparison.baseline];
    const t = entry.aggs[comparison.target];
    if (b == null || t == null || b <= 0) return null;
    return (t - b) / b;
  };

  const sortByActive = (arr) => {
    const dir = state.columnSort.direction === 'desc' ? -1 : 1;
    const getVal = activeSort === '__gain__' ? gainValue : (e) => e.aggs[activeSort];
    return [...arr].sort((a, b) => {
      const av = getVal(a);
      const bv = getVal(b);
      if (av == null && bv == null) return compareBySortKey(a, b);
      if (av == null) return 1;
      if (bv == null) return -1;
      if (av === bv) return compareBySortKey(a, b);
      return (av - bv) * dir;
    });
  };

  let displayed;
  if (info.kind === 'modes' && info.slowestN != null) {
    displayed = entries
      .filter((e) => e.aggs[comparison.baseline] != null)
      .sort((a, b) => a.aggs[comparison.baseline] - b.aggs[comparison.baseline])
      .slice(0, info.slowestN);
    if (sortActive) displayed = sortByActive(displayed);
  } else if (sortActive) {
    displayed = sortByActive(entries);
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
let leaderboardChart = null;

const LEADERBOARD_BADGES = [
  { emoji: '🥇', glow: 'shadow-lg shadow-emerald-500/20' },
  { emoji: '🥈', glow: 'shadow-md shadow-gray-500/10' },
  { emoji: '🥉', glow: 'shadow-md shadow-amber-500/10' },
  { emoji: '🎖️', glow: '' },
  { emoji: '🏅', glow: '' },
  { emoji: '🔹', glow: '' },
];

function leaderboardBadge(rank) {
  return LEADERBOARD_BADGES[rank - 1] || { emoji: `#${rank}`, glow: '' };
}

function isTestNameEnabledByGas(testName) {
  const gas = extractGasLimit(testName);
  return !state.disabledGasLimits.has(gasLimitKey(gas));
}

function computeLeaderboard(method) {
  const results = [];
  for (const client of CLIENTS) {
    const rows = state.rowsByClient[client] || [];
    const perTest = new Map();
    for (const row of rows) {
      if (modeFromRunId(row.run_id) !== 'full') continue;
      if (row.test_mgas_s == null || row.test_mgas_s <= 0) continue;
      if (!isRowInTimeWindow(row)) continue;
      if (!isTestNameEnabledByGas(row.test_name)) continue;
      if (!perTest.has(row.test_name)) perTest.set(row.test_name, []);
      perTest.get(row.test_name).push(row.test_mgas_s);
    }
    const tests = [];
    for (const [test, vals] of perTest) {
      const agg = aggregate(vals, method);
      if (agg != null) tests.push({ test, mgas: agg, n: vals.length });
    }
    tests.sort((a, b) => a.mgas - b.mgas);
    const worst10 = tests.slice(0, 10);
    if (!worst10.length) continue;
    const meanScore = worst10.reduce((s, t) => s + t.mgas, 0) / worst10.length;
    const medianScore = aggregate(worst10.map((t) => t.mgas), 'median');
    const worstTest = worst10[0];
    results.push({
      client,
      worst10,
      meanScore,
      medianScore,
      minScore: worstTest.mgas,
      worstTest,
      totalTests: tests.length,
    });
  }
  results.sort((a, b) => b.meanScore - a.meanScore);
  return results;
}

function makeLeaderboardHeader() {
  const header = document.createElement('div');
  header.className = 'mt-4';
  const banner = document.createElement('div');
  banner.className = 'flex flex-wrap items-baseline gap-3';
  const h2 = document.createElement('h2');
  h2.className = 'text-2xl font-bold text-gray-100';
  h2.textContent = '🏆 Leaderboard';
  banner.appendChild(h2);
  const rule = document.createElement('span');
  rule.className = 'rounded-xs border border-emerald-800 bg-emerald-950/60 px-2 py-0.5 text-xs font-semibold text-emerald-300';
  rule.textContent = 'Full mode · 10 slowest tests per client';
  banner.appendChild(rule);
  header.appendChild(banner);
  const sub = document.createElement('p');
  sub.className = 'mt-2 text-sm/6 text-gray-400';
  sub.innerHTML =
    'Each client is scored on the <span class="font-semibold text-gray-200">mean MGas/s across its 10 slowest Full-mode tests</span>. ' +
    'The gas-limit and runs-after filters apply.';
  header.appendChild(sub);
  return header;
}

function makeLeaderboardPodium(top) {
  const grid = document.createElement('div');
  grid.className = 'mt-6 grid gap-4 md:grid-cols-3';
  const heights = ['md:order-2 md:-mt-4', 'md:order-1', 'md:order-3 md:mt-4'];
  top.forEach((entry, idx) => {
    const rank = idx + 1;
    const badge = leaderboardBadge(rank);
    const color = CLIENT_COLORS[entry.client] || '#e5e7eb';
    const card = document.createElement('div');
    card.className = `relative flex flex-col items-center gap-2 rounded-sm border border-gray-800 bg-gray-900/60 p-5 text-center ${badge.glow} ${heights[idx] || ''}`;
    card.style.borderTop = `3px solid ${color}`;
    const ribbon = document.createElement('div');
    ribbon.className = 'text-5xl';
    ribbon.textContent = badge.emoji;
    card.appendChild(ribbon);
    const client = document.createElement('div');
    client.className = 'text-xl font-bold';
    client.style.color = color;
    client.textContent = entry.client;
    card.appendChild(client);
    const score = document.createElement('div');
    score.className = 'flex items-baseline gap-1';
    const scoreNum = document.createElement('span');
    scoreNum.className = 'text-3xl font-bold tabular-nums text-gray-100';
    scoreNum.textContent = entry.meanScore.toFixed(1);
    const scoreUnit = document.createElement('span');
    scoreUnit.className = 'text-xs text-gray-400';
    scoreUnit.textContent = 'MGas/s';
    score.append(scoreNum, scoreUnit);
    card.appendChild(score);
    const worst = document.createElement('div');
    worst.className = 'mt-2 flex flex-wrap items-center justify-center gap-1 text-xs text-gray-500';
    const wParsed = parseTestName(entry.worstTest.test);
    const worstLabel = wParsed.funcLabel || displayTestName(entry.worstTest.test);
    const labelSpan = document.createElement('span');
    labelSpan.textContent = 'Slowest test: ';
    const nameSpan = document.createElement('span');
    nameSpan.className = 'font-mono text-rose-300';
    nameSpan.textContent = worstLabel;
    const scoreSpan = document.createElement('span');
    scoreSpan.textContent = ` @ ${entry.worstTest.mgas.toFixed(1)} MGas/s`;
    worst.append(labelSpan, nameSpan);
    const worstUrl = testSourceUrl(entry.worstTest.test);
    if (worstUrl) worst.appendChild(makeExternalLinkIcon(worstUrl));
    worst.appendChild(scoreSpan);
    card.appendChild(worst);
    grid.appendChild(card);
  });
  return grid;
}

function makeLeaderboardChart(entries) {
  const block = document.createElement('div');
  block.className = 'mt-6 rounded-sm border border-gray-800 bg-gray-900/40 p-4';
  const h3 = document.createElement('h3');
  h3.className = 'text-sm font-semibold text-gray-200';
  h3.textContent = 'Scoreboard — mean MGas/s of each client\'s 10 slowest tests';
  block.appendChild(h3);
  const wrap = document.createElement('div');
  wrap.className = 'relative mt-3';
  wrap.style.height = `${Math.max(180, entries.length * 44)}px`;
  const canvas = document.createElement('canvas');
  wrap.appendChild(canvas);
  block.appendChild(wrap);
  const labels = entries.map((e, i) => `${leaderboardBadge(i + 1).emoji}  ${e.client}`);
  const values = entries.map((e) => e.meanScore);
  const colors = entries.map((e) => CLIENT_COLORS[e.client] || '#94a3b8');
  if (leaderboardChart) leaderboardChart.destroy();
  leaderboardChart = new Chart(canvas, {
    type: 'bar',
    data: {
      labels,
      datasets: [
        {
          data: values,
          backgroundColor: colors,
          borderWidth: 0,
          borderRadius: 4,
        },
      ],
    },
    options: {
      indexAxis: 'y',
      responsive: true,
      maintainAspectRatio: false,
      scales: {
        x: {
          beginAtZero: true,
          ticks: { color: '#9ca3af' },
          grid: { color: '#1f2937' },
          title: { display: true, text: 'MGas/s', color: '#9ca3af' },
        },
        y: {
          ticks: { color: '#e5e7eb', font: { weight: 'bold', size: 14 } },
          grid: { display: false },
        },
      },
      plugins: {
        legend: { display: false },
        tooltip: {
          backgroundColor: '#111827',
          borderColor: '#374151',
          borderWidth: 1,
          titleColor: '#f3f4f6',
          bodyColor: '#d1d5db',
          callbacks: {
            label: (ctx) => `${ctx.parsed.x.toFixed(2)} MGas/s`,
          },
        },
      },
    },
  });
  return block;
}

function makeRemainingRanks(rest, offset) {
  const block = document.createElement('div');
  block.className = 'mt-6 flex flex-col gap-2';
  rest.forEach((entry, idx) => {
    const rank = offset + idx + 1;
    const badge = leaderboardBadge(rank);
    const color = CLIENT_COLORS[entry.client] || '#e5e7eb';
    const row = document.createElement('div');
    row.className = 'flex items-center gap-4 rounded-sm border border-gray-800 bg-gray-900/40 px-4 py-3';
    row.style.borderLeft = `3px solid ${color}`;
    const rankEl = document.createElement('div');
    rankEl.className = 'text-2xl';
    rankEl.textContent = badge.emoji;
    row.appendChild(rankEl);
    const info = document.createElement('div');
    info.className = 'flex-1';
    const client = document.createElement('div');
    client.className = 'font-semibold';
    client.style.color = color;
    client.textContent = entry.client;
    info.append(client);
    row.appendChild(info);
    const score = document.createElement('div');
    score.className = 'text-right';
    score.innerHTML = `<div class="text-lg font-bold tabular-nums text-gray-100">${entry.meanScore.toFixed(1)}</div><div class="text-xs text-gray-500">MGas/s</div>`;
    row.appendChild(score);
    block.appendChild(row);
  });
  return block;
}

function makeLeaderboardDrillDown(entries) {
  const wrap = document.createElement('div');
  wrap.className = 'mt-6 flex flex-col gap-3';
  const title = document.createElement('h3');
  title.className = 'text-sm font-semibold text-gray-200';
  title.textContent = 'Each client\'s 10 slowest Full-mode tests';
  wrap.appendChild(title);
  entries.forEach((entry, idx) => {
    const rank = idx + 1;
    const badge = leaderboardBadge(rank);
    const color = CLIENT_COLORS[entry.client] || '#e5e7eb';
    const details = document.createElement('details');
    details.className = 'overflow-hidden rounded-sm border border-gray-800 bg-gray-900/40';
    if (rank === 1) details.open = true;
    const summary = document.createElement('summary');
    summary.className = 'flex cursor-pointer items-center gap-3 px-4 py-3 text-sm hover:bg-gray-900/80';
    const badgeEl = document.createElement('span');
    badgeEl.className = 'text-xl';
    badgeEl.textContent = badge.emoji;
    summary.appendChild(badgeEl);
    const clientEl = document.createElement('span');
    clientEl.className = 'font-semibold';
    clientEl.style.color = color;
    clientEl.textContent = entry.client;
    summary.appendChild(clientEl);
    const score = document.createElement('span');
    score.className = 'ml-auto text-xs text-gray-400';
    score.innerHTML = `mean <span class="font-semibold tabular-nums text-gray-100">${entry.meanScore.toFixed(1)}</span> · median <span class="tabular-nums text-gray-200">${entry.medianScore.toFixed(1)}</span> · min <span class="tabular-nums text-rose-300">${entry.minScore.toFixed(1)}</span> MGas/s`;
    summary.appendChild(score);
    details.appendChild(summary);
    const listWrap = document.createElement('div');
    listWrap.className = 'border-t border-gray-800 px-4 py-3';
    const list = document.createElement('ol');
    list.className = 'flex flex-col gap-1 text-xs';
    entry.worst10.forEach((t, i) => {
      const li = document.createElement('li');
      li.className = 'flex items-center gap-3';
      const num = document.createElement('span');
      num.className = 'w-6 shrink-0 text-right tabular-nums text-gray-500';
      num.textContent = `${i + 1}.`;
      li.appendChild(num);
      const parsed = parseTestName(t.test);
      const name = document.createElement('span');
      name.className = 'min-w-0 flex-1 truncate font-mono text-gray-300';
      name.textContent = parsed.funcLabel ? `${parsed.funcLabel} [${parsed.fileLabel || ''}]` : displayTestName(t.test);
      name.title = displayTestName(t.test);
      li.appendChild(name);
      const url = testSourceUrl(t.test);
      if (url) li.appendChild(makeExternalLinkIcon(url));
      const mgas = document.createElement('span');
      mgas.className = 'tabular-nums text-rose-300';
      mgas.textContent = `${t.mgas.toFixed(2)} MGas/s`;
      li.appendChild(mgas);
      const n = document.createElement('span');
      n.className = 'w-10 shrink-0 text-right tabular-nums text-gray-500';
      n.textContent = `n=${t.n}`;
      li.appendChild(n);
      list.appendChild(li);
    });
    listWrap.appendChild(list);
    details.appendChild(listWrap);
    wrap.appendChild(details);
  });
  return wrap;
}

function renderLeaderboard() {
  const method = document.getElementById('method').value;
  const container = document.getElementById('leaderboard-content');
  container.replaceChildren();
  if (leaderboardChart) {
    leaderboardChart.destroy();
    leaderboardChart = null;
  }
  const entries = computeLeaderboard(method);
  container.appendChild(makeLeaderboardHeader());
  if (!entries.length) {
    const empty = document.createElement('p');
    empty.className = 'mt-6 rounded-xs border border-gray-800 bg-gray-900/40 px-4 py-3 text-sm text-gray-400';
    empty.textContent = 'No Full-mode data yet, or filters exclude everything. Adjust the gas-limit checkboxes or the Runs-from selector.';
    container.appendChild(empty);
    return;
  }
  container.appendChild(makeLeaderboardPodium(entries.slice(0, 3)));
  if (entries.length > 3) {
    container.appendChild(makeRemainingRanks(entries.slice(3), 3));
  }
  container.appendChild(makeLeaderboardChart(entries));
  container.appendChild(makeLeaderboardDrillDown(entries));
}

function setLeaderboardVisibility(show) {
  const leaderboardEl = document.getElementById('leaderboard-content');
  const tableSection = document.getElementById('table-section');
  const gainSummary = document.getElementById('gain-summary');
  const legend = document.getElementById('table-legend');
  leaderboardEl.classList.toggle('hidden', !show);
  tableSection.classList.toggle('hidden', show);
  gainSummary.classList.toggle('hidden', show);
  legend.classList.toggle('hidden', show);
}

function buildFamilyBuckets(rows, familyKey) {
  const buckets = new Map();
  const uniqTests = new Set();
  let samples = 0;
  for (const row of rows) {
    if (extractFamilyKey(row.test_name) !== familyKey) continue;
    const mode = modeFromRunId(row.run_id);
    if (!mode) continue;
    if (row.test_mgas_s == null || row.test_mgas_s <= 0) continue;
    if (!isRowInTimeWindow(row)) continue;
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
      if (!isRowInTimeWindow(row)) continue;
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
  const allKeys = limits.map(gasLimitKey);
  for (const gas of limits) {
    const key = gasLimitKey(gas);
    const label = document.createElement('label');
    label.className = 'flex cursor-pointer items-center gap-1.5 select-none';
    label.title = 'Click to toggle · double-click to isolate';
    const cb = document.createElement('input');
    cb.type = 'checkbox';
    cb.className = 'size-3.5 accent-emerald-500';
    cb.checked = !state.disabledGasLimits.has(key);
    cb.addEventListener('change', () => {
      if (cb.checked) state.disabledGasLimits.delete(key);
      else state.disabledGasLimits.add(key);
      render();
    });
    label.addEventListener('dblclick', (e) => {
      e.preventDefault();
      const onlyThis = new Set(allKeys.filter((k) => k !== key));
      state.disabledGasLimits = onlyThis;
      renderGasLimitFilters();
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

function applyCacheToState(cache) {
  state.rowsByClient = {};
  const runIds = cache.run_ids || [];
  const testNames = cache.test_names || [];
  for (const c of CLIENTS) {
    const cached = cache.test_stats?.[c] || [];
    state.rowsByClient[c] = cached.map((r) => expandCachedRow(r, runIds, testNames));
  }
  state.runs = cache.runs || [];
}

function mergeDeltaIntoState(deltaRows) {
  let added = 0;
  for (const row of deltaRows) {
    const c = row.client;
    if (!c) continue;
    if (!state.rowsByClient[c]) state.rowsByClient[c] = [];
    state.rowsByClient[c].push({
      id: row.id,
      run_id: row.run_id,
      test_name: row.test_name,
      test_mgas_s: row.test_mgas_s,
    });
    added += 1;
  }
  return added;
}

async function fullFetchFallback(status) {
  status.textContent = 'no cache · loading…';
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
    return false;
  }
  state.runs = runsResult.rows || [];
  status.textContent = errors.length
    ? `ok · ${CLIENTS.length - errors.length}/${CLIENTS.length} clients loaded (no cache)`
    : `ok · ${CLIENTS.length} clients loaded (no cache)`;
  status.className = errors.length ? 'text-amber-400' : 'text-emerald-400';
  return true;
}

async function reloadAll() {
  const status = document.getElementById('status');
  status.textContent = 'loading cache…';
  status.className = 'text-amber-400';

  const cache = await fetchCache();
  let cacheMaxId = 0;
  let cacheAge = '';
  if (cache) {
    applyCacheToState(cache);
    cacheMaxId = cache.max_test_stats_id || 0;
    cacheAge = formatCacheAge(cache.generated_at);
  } else {
    state.rowsByClient = Object.fromEntries(CLIENTS.map((c) => [c, []]));
    state.runs = [];
  }

  renderGasLimitFilters();
  renderRunsFilter();
  selectClient(document.getElementById('client').value);

  const haveKey = !!getApiKey();

  if (!haveKey && !cache) {
    status.textContent = 'API key required (no cache available)';
    status.className = 'text-amber-400';
    openApiKeyModal();
    return;
  }
  if (!haveKey && cache) {
    status.textContent = `cache only · ${cacheAge} (add API key for latest)`;
    status.className = 'text-amber-400';
    openApiKeyModal();
    return;
  }

  if (!cache) {
    const ok = await fullFetchFallback(status);
    if (!ok) return;
    renderGasLimitFilters();
    renderRunsFilter();
    selectClient(document.getElementById('client').value);
    return;
  }

  status.textContent = `cache ${cacheAge} · loading delta…`;
  try {
    const [deltaRows, runsRows] = await Promise.all([
      fetchDeltaSince(cacheMaxId),
      fetchRuns().catch(() => state.runs),
    ]);
    const added = mergeDeltaIntoState(deltaRows);
    if (runsRows && runsRows.length) state.runs = runsRows;
    renderGasLimitFilters();
    renderRunsFilter();
    selectClient(document.getElementById('client').value);
    status.textContent = `ok · cache ${cacheAge} + ${added} new row${added === 1 ? '' : 's'}`;
    status.className = 'text-emerald-400';
  } catch (e) {
    status.textContent = `cache only (${cacheAge}) · delta failed: ${e.message}`;
    status.className = e.code === 'UNAUTHORIZED' ? 'text-rose-400' : 'text-amber-400';
    if (e.code === 'UNAUTHORIZED') openApiKeyModal(true);
  }
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
  document.getElementById('runs-after').addEventListener('change', render);
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
