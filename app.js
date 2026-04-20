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
const CLIENTS = ['besu', 'geth', 'nethermind'];

const MODE_COLORS = {
  sequential: '#f87171',
  nobatchio: '#60a5fa',
  full: '#34d399',
};

const CLIENT_COLORS = {
  besu: '#a78bfa',
  geth: '#fbbf24',
  nethermind: '#22d3ee',
};

const COMPARISONS = {
  'full-vs-sequential': { baseline: 'sequential', target: 'full' },
  'full-vs-nobatchio': { baseline: 'nobatchio', target: 'full' },
};

const state = {
  rowsByClient: {},
  rows: [],
  client: null,
  tab: 'all',
  disabledGasLimits: new Set(),
};

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

function modeFromRunId(runId) {
  const m = runId.match(/-bal-(sequential|nobatchio|full)$/);
  return m ? m[1] : null;
}

function groupByTest(rows) {
  const groups = new Map();
  for (const row of rows) {
    const mode = modeFromRunId(row.run_id);
    if (!mode) continue;
    if (row.total_mgas_s == null) continue;
    if (!groups.has(row.test_name)) {
      groups.set(row.test_name, { sequential: [], nobatchio: [], full: [] });
    }
    groups.get(row.test_name)[mode].push(row.total_mgas_s);
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
      if (row.total_mgas_s == null) continue;
      let e = byTest.get(row.test_name);
      if (!e) {
        e = { test: row.test_name, aggs: {}, counts: {}, _raw: {} };
        for (const c of CLIENTS) {
          e._raw[c] = [];
          e.counts[c] = 0;
        }
        byTest.set(row.test_name, e);
      }
      e._raw[client].push(row.total_mgas_s);
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
  const slowLabel = info.slowCols.join(' + ');
  tr.title = isSlowest
    ? `Slowest row across ${slowLabel} — click for chart`
    : 'Click to chart MGas/s vs gas limit';
  if (isSlowest) {
    tr.classList.add('bg-rose-950/40', 'border-l-2', 'border-l-rose-500');
  }

  const tdTest = document.createElement('td');
  tdTest.className = 'px-3 py-2 font-mono text-xs text-gray-300';
  const inner = document.createElement('div');
  inner.className = 'flex items-start gap-1.5';
  const nameSpan = document.createElement('span');
  nameSpan.className = 'break-all';
  nameSpan.textContent = displayTestName(entry.test);
  inner.append(makeChartIcon(), nameSpan);
  tdTest.appendChild(inner);
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

function renderHead(info) {
  const tr = document.getElementById('head-row');
  tr.replaceChildren();
  const testTh = document.createElement('th');
  testTh.className = 'px-3 py-2 text-left font-medium';
  testTh.textContent = 'Test';
  tr.appendChild(testTh);
  for (const col of info.columns) {
    const th = document.createElement('th');
    th.className = 'px-3 py-2 text-right font-medium';
    th.textContent = col;
    tr.appendChild(th);
  }
  if (info.showGain) {
    const gainTh = document.createElement('th');
    gainTh.id = 'gain-header';
    gainTh.className = 'px-3 py-2 text-right font-medium';
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
    el.textContent = `No tests have both ${comparison.target} and ${comparison.baseline} data.`;
    el.className = 'mt-4 rounded-xs border border-gray-800 bg-gray-900/40 px-4 py-3 text-sm text-gray-400';
    return;
  }
  const meanGain = gains.reduce((a, b) => a + b, 0) / gains.length;
  const geomeanRatio = Math.exp(ratios.reduce((a, b) => a + Math.log(b), 0) / ratios.length);
  const geoGain = geomeanRatio - 1;
  const medianGain = aggregate(gains, 'median');
  const wins = gains.filter((g) => g > 0.02).length;
  const losses = gains.filter((g) => g < -0.02).length;

  const label = `${comparison.target} vs ${comparison.baseline}`;
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
    document.getElementById('gain-header').textContent = `Gain (${comparison.target} vs ${comparison.baseline})`;
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
    if (row.total_mgas_s == null) continue;
    const gas = extractGasLimit(row.test_name);
    if (gas == null) continue;
    if (!buckets.has(gas)) buckets.set(gas, { sequential: [], nobatchio: [], full: [] });
    buckets.get(gas)[mode].push(row.total_mgas_s);
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
    label: mode,
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
      if (row.total_mgas_s == null) continue;
      const gas = extractGasLimit(row.test_name);
      if (gas == null) continue;
      if (!bucket.has(gas)) bucket.set(gas, []);
      bucket.get(gas).push(row.total_mgas_s);
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
  const results = await Promise.all(
    CLIENTS.map((c) =>
      fetchAll(c).then(
        (rows) => ({ client: c, rows, error: null }),
        (e) => ({ client: c, rows: [], error: e }),
      ),
    ),
  );
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

  setTab('all');
  reloadAll();
}

init();
