/* =========================================================
   app.js — Anniversary Watch Calendar
   ========================================================= */

const LEVELS = ['NONE', 'BASELINE', 'MODERATE', 'ELEVATED', 'CRITICAL'];
const LEVEL_RANK = Object.fromEntries(LEVELS.map((l, i) => [l, i]));

const LEAD_DAYS = { NONE: 0, BASELINE: 3, MODERATE: 7, ELEVATED: 14, CRITICAL: 30 };

const STRAND_LABELS = {
  SJ: 'Sunni-jihadist',
  SI: 'Shia-Iranian',
  PAL: 'Palestinian',
  MB: 'Muslim Brotherhood',
  PI: 'Pan-Islamist',
};

const STANDING_ORDERS = {
  NONE: { heading: 'No posture', bullets: ['No active anniversaries in horizon.'] },
  BASELINE: {
    heading: 'Routine awareness',
    bullets: [
      'Maintain routine OSINT collection.',
      'Log baseline indicators.',
      'Review daily digest.',
    ],
  },
  MODERATE: {
    heading: 'Increased situational awareness',
    bullets: [
      'Pre-position alerting on relevant accounts.',
      'Brief watch officer on active events.',
      'Review T-7 horizon for additional convergence.',
      'Increase collection cadence on tagged accounts.',
    ],
  },
  ELEVATED: {
    heading: 'Active monitoring posture',
    bullets: [
      'Daily updates to watch officer.',
      'Spin up dedicated monitoring channel.',
      'Coordinate with partner teams.',
      'Document indicator deviations in real time.',
      'Pre-draft situational report.',
    ],
  },
  CRITICAL: {
    heading: 'Heightened operational alert',
    bullets: [
      '24-hour watch posture active.',
      'Hourly indicator review.',
      'Immediate escalation paths engaged.',
      'All compound-window strands monitored concurrently.',
      'Situational report frequency: per shift.',
      'Pre-position rapid-response communications.',
    ],
  },
};

/* =========================================================
   Date utilities
   ========================================================= */

function parseDate(str) {
  // "~YYYY-MM-DD" or "YYYY-MM-DD"
  const clean = str.replace(/^~/, '');
  const [y, m, d] = clean.split('-').map(Number);
  return new Date(y, m - 1, d);
}

function parseDateRange(str) {
  // May be "~YYYY-MM-DD/~YYYY-MM-DD" or "YYYY-MM-DD/YYYY-MM-DD" or just "YYYY-MM-DD"
  if (str.includes('/')) {
    const parts = str.split('/');
    return { start: parseDate(parts[0]), end: parseDate(parts[1]) };
  }
  const d = parseDate(str);
  return { start: d, end: d };
}

function toKey(d) {
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`;
}

function addDays(d, n) {
  const r = new Date(d);
  r.setDate(r.getDate() + n);
  return r;
}

function daysBetween(a, b) {
  return Math.round((b - a) / 86400000);
}

function formatDate(d) {
  return d.toLocaleDateString('en-US', { weekday: 'long', year: 'numeric', month: 'long', day: 'numeric' });
}

function monthName(d) {
  return d.toLocaleDateString('en-US', { month: 'long', year: 'numeric' });
}

/* =========================================================
   Build day-index from events + compound windows
   Each key "YYYY-MM-DD" maps to:
     { level, strandLevels: {SJ,SI,...}, anchors: [...eventNames], postures: [...], windows: [...] }
   ========================================================= */

function buildDayIndex(data) {
  const index = {}; // key → { level, strandLevels, anchors, postures, windows }

  function ensure(key) {
    if (!index[key]) {
      index[key] = {
        level: 'NONE',
        strandLevels: {},
        anchors: [],   // events whose T-0 is this day
        postures: [],  // {event, tMinus, level} — posture phase days
        windows: [],   // compound window IDs
      };
    }
    return index[key];
  }

  function raiseLevel(key, lvl) {
    const entry = ensure(key);
    if (LEVEL_RANK[lvl] > LEVEL_RANK[entry.level]) entry.level = lvl;
  }

  function raiseStrand(key, strand, lvl) {
    const entry = ensure(key);
    const cur = entry.strandLevels[strand] || 'NONE';
    if (LEVEL_RANK[lvl] > LEVEL_RANK[cur]) entry.strandLevels[strand] = lvl;
  }

  // Process events
  for (const ev of data.events) {
    const { start, end } = parseDateRange(ev.date);
    const evLevel = ev.vigilance_overall;
    const lead = ev.lead_time_days || LEAD_DAYS[evLevel] || 0;

    // Mark every day in the event's own span as T-0 anchors
    let cur = new Date(start);
    while (cur <= end) {
      const k = toKey(cur);
      const entry = ensure(k);
      raiseLevel(k, evLevel);
      entry.anchors.push(ev);
      for (const [s, sl] of Object.entries(ev.vigilance_by_strand || {})) {
        raiseStrand(k, s, sl);
      }
      cur = addDays(cur, 1);
    }

    // Mark posture window days (lead up to start of event range)
    // Phases: CRITICAL → T-30,T-14,T-7,T-3; ELEVATED → T-14,T-7,T-3; etc.
    // We just mark the entire window from (start - lead) to (start - 1) with a posture entry
    const windowStart = addDays(start, -lead);
    let d = new Date(windowStart);
    while (d < start) {
      const k = toKey(d);
      const tMinus = daysBetween(d, start);
      const entry = ensure(k);
      raiseLevel(k, evLevel);
      entry.postures.push({ event: ev, tMinus });
      for (const [s, sl] of Object.entries(ev.vigilance_by_strand || {})) {
        raiseStrand(k, s, sl);
      }
      d = addDays(d, 1);
    }
  }

  // Process compound windows
  for (const cw of data.compound_windows || []) {
    const wStart = parseDate(cw.start);
    const wEnd = parseDate(cw.end);
    let d = new Date(wStart);
    while (d <= wEnd) {
      const k = toKey(d);
      raiseLevel(k, cw.level);
      ensure(k).windows.push(cw);
      d = addDays(d, 1);
    }
  }

  return index;
}

/* =========================================================
   App state
   ========================================================= */

const today = new Date();
today.setHours(0, 0, 0, 0);

let state = {
  data: null,
  dayIndex: null,
  strandIndex: null, // per-strand day maps (built lazily below)
  reuters: null,
  reutersByDate: {},
  year: today.getFullYear(),
  month: today.getMonth(),
  selectedKey: toKey(today),
  activeStrand: 'overall', // 'overall' | 'SJ' | 'SI' | 'PAL' | 'MB' | 'PI'
  showPostures: true,
  showWindows: true,
  soundEnabled: false,
};

/* =========================================================
   Level helpers for CSS custom properties
   ========================================================= */

const LEVEL_COLORS = {
  NONE:     '#1d2530',
  BASELINE: '#6b7280',
  MODERATE: '#d6b341',
  ELEVATED: '#e07b2b',
  CRITICAL: '#d83a3a',
};
const LEVEL_COLORS_SOFT = {
  NONE:     '#1d2530',
  BASELINE: '#2a313c',
  MODERATE: '#3a3322',
  ELEVATED: '#3a2a1d',
  CRITICAL: '#3a1d1d',
};

function applyLevelCSS(lvl) {
  document.documentElement.setAttribute('data-vigilance', lvl);
  document.documentElement.style.setProperty('--lvl-current', LEVEL_COLORS[lvl] || LEVEL_COLORS.NONE);
  document.documentElement.style.setProperty('--lvl-current-soft', LEVEL_COLORS_SOFT[lvl] || LEVEL_COLORS_SOFT.NONE);
}

/* =========================================================
   Strand index — same as dayIndex but for strand-level view
   ========================================================= */

function buildStrandIndex(dayIndex) {
  const result = {};
  for (const strand of Object.keys(STRAND_LABELS)) {
    const idx = {};
    for (const [k, v] of Object.entries(dayIndex)) {
      idx[k] = { level: v.strandLevels[strand] || 'NONE' };
    }
    result[strand] = idx;
  }
  return result;
}

const todayKey = toKey(today);

function isPast(key) {
  return key < todayKey;
}

function getDayLevel(key) {
  if (isPast(key)) return 'NONE';
  if (state.activeStrand === 'overall') {
    return state.dayIndex[key]?.level || 'NONE';
  }
  return state.strandIndex[state.activeStrand]?.[key]?.level || 'NONE';
}

function getHistoricalDayLevel(key) {
  if (state.activeStrand === 'overall') {
    return state.dayIndex[key]?.level || 'NONE';
  }
  return state.strandIndex[state.activeStrand]?.[key]?.level || 'NONE';
}

/* =========================================================
   Reuters correlation helpers
   ========================================================= */

function escapeHtml(value) {
  return String(value ?? '').replace(/[&<>"']/g, ch => ({
    '&': '&amp;',
    '<': '&lt;',
    '>': '&gt;',
    '"': '&quot;',
    "'": '&#39;',
  }[ch]));
}

function safeReutersUrl(value) {
  try {
    const url = new URL(value, window.location.href);
    const isReuters = url.hostname === 'reuters.com' || url.hostname.endsWith('.reuters.com');
    if (url.protocol === 'https:' && isReuters) return url.href;
  } catch (_) {}
  return '';
}

function formatReutersTime(value) {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value || 'Unknown time';
  return date.toLocaleString('en-US', {
    year: 'numeric',
    month: 'short',
    day: 'numeric',
    hour: 'numeric',
    minute: '2-digit',
  });
}

function buildReutersIndex(reutersData) {
  const byDate = {};
  const items = Array.isArray(reutersData) ? reutersData : (reutersData?.items || []);
  for (const item of items) {
    if (!item?.date) continue;
    if (!byDate[item.date]) byDate[item.date] = [];
    byDate[item.date].push(item);
  }
  for (const dayItems of Object.values(byDate)) {
    dayItems.sort((a, b) => String(b.publication_date || '').localeCompare(String(a.publication_date || '')));
  }
  return byDate;
}

function itemMatchesActiveStrand(item) {
  if (state.activeStrand === 'overall') return true;
  return (item.matched_strands || []).includes(state.activeStrand);
}

function getReutersItems(key) {
  return (state.reutersByDate[key] || []).filter(itemMatchesActiveStrand);
}

function renderReutersFeed(key) {
  const sec = document.getElementById('detail-reuters-sec');
  const list = document.getElementById('detail-reuters');
  const items = getReutersItems(key);
  if (!items.length) {
    sec.style.display = 'none';
    list.innerHTML = '';
    return;
  }

  sec.style.display = '';
  list.innerHTML = items.slice(0, 12).map(item => {
    const url = safeReutersUrl(item.url);
    const imageUrl = safeReutersUrl(item.image_url);
    const title = escapeHtml(item.title || 'Reuters story');
    const section = escapeHtml(item.section || 'Reuters');
    const confidence = escapeHtml(item.confidence || 'MEDIUM');
    const strands = (item.matched_strands || [])
      .map(strand => `<span class="tag reuters-strand">${escapeHtml(strand)}</span>`)
      .join('');
    const reasons = (item.match_reasons || []).map(escapeHtml).join(' · ');
    const watch = (item.matched_watch_events || []).slice(0, 4).map(escapeHtml).join(' · ');
    const titleNode = url
      ? `<a href="${escapeHtml(url)}" target="_blank" rel="noopener noreferrer">${title}</a>`
      : `<span>${title}</span>`;

    return `
      <li class="item reuters-item ${imageUrl ? '' : 'no-thumb'}">
        ${imageUrl ? `<img class="reuters-thumb" src="${escapeHtml(imageUrl)}" alt="" loading="lazy" />` : ''}
        <div class="reuters-body">
          <div class="item-title reuters-title">
            ${titleNode}
            <span class="tag confidence-${confidence}">${confidence}</span>
          </div>
          <div class="item-meta">Reuters · ${section} · ${escapeHtml(formatReutersTime(item.publication_date))}</div>
          ${strands ? `<div class="item-meta reuters-tags">${strands}</div>` : ''}
          ${watch ? `<div class="item-notes">Watch context: ${watch}</div>` : ''}
          ${reasons ? `<div class="item-notes reuters-reasons">Match basis: ${reasons}</div>` : ''}
        </div>
      </li>
    `;
  }).join('');
}

/* =========================================================
   Next upcoming CRITICAL / ELEVATED finder
   ========================================================= */

function findNextSignificant(afterKey) {
  const afterDate = parseDate(afterKey);
  const candidates = [];
  for (const ev of state.data.events) {
    const { start } = parseDateRange(ev.date);
    if (start > afterDate &&
        (ev.vigilance_overall === 'CRITICAL' || ev.vigilance_overall === 'ELEVATED')) {
      candidates.push({ ev, start });
    }
  }
  if (!candidates.length) return null;
  candidates.sort((a, b) => a.start - b.start);
  return candidates[0];
}

/* =========================================================
   Standing orders renderer
   ========================================================= */

function renderStandingOrders(lvl) {
  const orders = STANDING_ORDERS[lvl] || STANDING_ORDERS.NONE;
  const el = document.getElementById('standing-orders');
  el.innerHTML = `
    <span class="lvl-tag">${lvl}</span>
    <strong style="margin-left:8px;font-size:13px;">${orders.heading}</strong>
    <ul>${orders.bullets.map(b => `<li>${b}</li>`).join('')}</ul>
  `;
}

/* =========================================================
   Alert bar / marquee
   ========================================================= */

function updateAlertBar(dayEntry, lvl) {
  const bar = document.getElementById('alert-bar');
  const marquee = document.getElementById('alert-marquee');
  if (lvl !== 'ELEVATED' && lvl !== 'CRITICAL') {
    marquee.textContent = '';
    return;
  }
  const parts = [];
  if (dayEntry) {
    for (const ev of (dayEntry.anchors || [])) {
      parts.push(`● T-0 · ${ev.name} [${ev.vigilance_overall}]`);
    }
    for (const p of (dayEntry.postures || [])) {
      parts.push(`⬛ T-${p.tMinus} posture · ${p.event.name} [${p.event.vigilance_overall}]`);
    }
    for (const w of (dayEntry.windows || [])) {
      parts.push(`◈ ${w.id} · ${w.label} [${w.level}]`);
    }
  }
  if (!parts.length) parts.push(`${lvl} posture active`);
  marquee.textContent = parts.join('     ');
}

/* =========================================================
   Detail panel renderer
   ========================================================= */

function renderDetail(key) {
  const date = parseDate(key);
  const entry = state.dayIndex[key] || { level: 'NONE', anchors: [], postures: [], windows: [], strandLevels: {} };
  const lvl = getDayLevel(key);

  document.getElementById('detail-date').textContent = formatDate(date);

  const pill = document.getElementById('detail-pill');
  pill.textContent = lvl;
  pill.className = `detail-pill`;
  applyLevelCSS(lvl);

  // Anchors
  const evList = document.getElementById('detail-events');
  const evSec = document.getElementById('detail-events-sec');
  if (entry.anchors.length) {
    evSec.style.display = '';
    evList.innerHTML = entry.anchors.map(ev => `
      <li class="item">
        <div class="item-title">
          <span>${ev.name}</span>
          <span class="tag lvl-${ev.vigilance_overall}">${ev.vigilance_overall}</span>
        </div>
        <div class="item-meta">Origin year: ${ev.year_origin || '—'} · Lead: T-${ev.lead_time_days}</div>
        ${ev.notes ? `<div class="item-notes">${ev.notes}</div>` : ''}
        ${Object.keys(ev.vigilance_by_strand || {}).length
          ? `<div class="item-meta">Strands: ${Object.entries(ev.vigilance_by_strand).map(([s,l]) => `<span class="tag lvl-${l}" style="margin:0 3px;">${s}·${l}</span>`).join('')}</div>`
          : ''}
      </li>
    `).join('');
  } else {
    evSec.style.display = 'none';
  }

  // Postures
  const poList = document.getElementById('detail-postures');
  const poSec = document.getElementById('detail-postures-sec');
  const postures = state.showPostures ? (entry.postures || []) : [];
  if (postures.length) {
    poSec.style.display = '';
    poList.innerHTML = postures.map(p => `
      <li class="item">
        <div class="item-title">
          <span>T-${p.tMinus} · ${p.event.name}</span>
          <span class="tag lvl-${p.event.vigilance_overall}">${p.event.vigilance_overall}</span>
        </div>
        <div class="item-meta">Anniversary: ${p.event.date.replace(/^~/, '')} · Lead: T-${p.event.lead_time_days}</div>
      </li>
    `).join('');
  } else {
    poSec.style.display = (state.showPostures && entry.anchors.length === 0) ? '' : 'none';
    if (poSec.style.display !== 'none') poList.innerHTML = '<li class="empty-note">No posture phases on this day.</li>';
  }

  // Windows
  const winList = document.getElementById('detail-windows');
  const winSec = document.getElementById('detail-windows-sec');
  const wins = state.showWindows ? (entry.windows || []) : [];
  if (wins.length) {
    winSec.style.display = '';
    winList.innerHTML = wins.map(w => `
      <li class="item">
        <div class="item-title">
          <span>${w.id} · ${w.label}</span>
          <span class="tag lvl-${w.level}">${w.level}</span>
        </div>
        <div class="item-meta">${w.start} → ${w.end}</div>
        ${w.rationale ? `<div class="item-notes">${w.rationale}</div>` : ''}
      </li>
    `).join('');
  } else {
    winSec.style.display = 'none';
  }

  // Strand bars
  const strands = document.getElementById('detail-strands');
  const strandsSec = document.getElementById('detail-strands-sec');
  const strandEntries = Object.entries(entry.strandLevels || {});
  if (strandEntries.length) {
    strandsSec.style.display = '';
    strands.innerHTML = Object.keys(STRAND_LABELS).map(s => {
      const sl = entry.strandLevels[s] || 'NONE';
      const rank = LEVEL_RANK[sl];
      const maxRank = LEVELS.length - 1;
      const pct = Math.round((rank / maxRank) * 100);
      const color = LEVEL_COLORS[sl];
      return `
        <div class="strand-bar">
          <span class="key">${s}</span>
          <div class="meter"><span style="width:${pct}%;background:${color};"></span></div>
          <span class="lvl">${sl}</span>
        </div>
      `;
    }).join('');
  } else {
    strandsSec.style.display = 'none';
  }

  // Reuters retrospective context
  renderReutersFeed(key);

  // Next significant
  const next = findNextSignificant(key);
  const nc = document.getElementById('next-critical');
  if (next) {
    const daysAway = daysBetween(date, next.start);
    nc.innerHTML = `
      <div class="nc-name">${next.ev.name}</div>
      <div class="nc-when">${formatDate(next.start)} — ${daysAway} day${daysAway !== 1 ? 's' : ''} away</div>
      <div class="nc-when" style="margin-top:3px"><span class="tag lvl-${next.ev.vigilance_overall}">${next.ev.vigilance_overall}</span></div>
    `;
  } else {
    nc.innerHTML = '<span class="empty-note">No upcoming critical/elevated events found.</span>';
  }

  // Update topbar status
  const context = entry.anchors.length
    ? entry.anchors.map(e => e.name).join(' · ')
    : entry.postures.length
      ? entry.postures.map(p => `T-${p.tMinus} · ${p.event.name}`).join(' · ')
      : entry.windows.length
        ? entry.windows.map(w => w.label).join(' · ')
        : 'No active anniversaries';
  document.getElementById('current-level').textContent = lvl;
  document.getElementById('current-date').textContent = formatDate(date);
  document.getElementById('current-context').textContent = context;

  // Update alert bar
  updateAlertBar(entry, lvl);

  // Sound cue
  if (state.soundEnabled && (lvl === 'ELEVATED' || lvl === 'CRITICAL')) {
    playAlert(lvl);
  }
}

/* =========================================================
   Sound
   ========================================================= */

let audioCtx = null;
function getAudioCtx() {
  if (!audioCtx) audioCtx = new (window.AudioContext || window.webkitAudioContext)();
  return audioCtx;
}

function playAlert(lvl) {
  try {
    const ctx = getAudioCtx();
    const freqs = lvl === 'CRITICAL' ? [880, 1100] : [660];
    freqs.forEach((freq, i) => {
      const osc = ctx.createOscillator();
      const gain = ctx.createGain();
      osc.connect(gain);
      gain.connect(ctx.destination);
      osc.type = 'sine';
      osc.frequency.value = freq;
      gain.gain.setValueAtTime(0, ctx.currentTime + i * 0.12);
      gain.gain.linearRampToValueAtTime(0.18, ctx.currentTime + i * 0.12 + 0.05);
      gain.gain.exponentialRampToValueAtTime(0.001, ctx.currentTime + i * 0.12 + 0.5);
      osc.start(ctx.currentTime + i * 0.12);
      osc.stop(ctx.currentTime + i * 0.12 + 0.55);
    });
  } catch (_) {}
}

/* =========================================================
   Calendar grid renderer
   ========================================================= */

const DOW = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];

function renderMonthGrid() {
  const grid = document.getElementById('month-grid');
  const year = state.year;
  const month = state.month;
  const todayKey = toKey(today);

  document.getElementById('month-title').textContent =
    new Date(year, month, 1).toLocaleDateString('en-US', { month: 'long', year: 'numeric' });

  const firstDay = new Date(year, month, 1).getDay(); // 0=Sun
  const daysInMonth = new Date(year, month + 1, 0).getDate();
  const prevMonthDays = new Date(year, month, 0).getDate();

  let html = DOW.map(d => `<div class="dow">${d}</div>`).join('');

  // leading blanks
  for (let i = 0; i < firstDay; i++) {
    const pDay = prevMonthDays - firstDay + 1 + i;
    html += `<div class="day outside" data-level="NONE"><span class="day-num">${pDay}</span></div>`;
  }

  for (let d = 1; d <= daysInMonth; d++) {
    const cellDate = new Date(year, month, d);
    const key = toKey(cellDate);
    const lvl = getDayLevel(key);
    const entry = state.dayIndex[key] || { anchors: [], postures: [], windows: [] };
    const isToday = key === todayKey;
    const isSelected = key === state.selectedKey;
    const isAnchor = entry.anchors.length > 0;
    const hasWindow = state.showWindows && entry.windows.length > 0;
    const hasPosture = state.showPostures && entry.postures.length > 0;
    const reutersCount = getReutersItems(key).length;

    const classes = [
      'day',
      isToday ? 'today' : '',
      isSelected ? 'selected' : '',
      isAnchor ? 'day-anchor' : '',
      reutersCount ? 'has-reuters' : '',
    ].filter(Boolean).join(' ');

    const flags = [];
    if (isAnchor) flags.push(...entry.anchors.map(e => `<span class="day-flag">${e.vigilance_overall[0]}</span>`));
    else if (hasPosture) flags.push(...[...new Set(entry.postures.map(p => `T-${p.tMinus}`))].slice(0, 2).map(t => `<span class="day-flag">${t}</span>`));

    html += `
      <div class="${classes}" data-level="${lvl}" data-key="${key}" title="${key}${reutersCount ? ` · ${reutersCount} Reuters item${reutersCount !== 1 ? 's' : ''}` : ''}">
        <span class="day-num">${d}</span>
        <div class="day-flags">${flags.join('')}</div>
        ${reutersCount ? `<span class="day-reuters-count">${reutersCount}</span>` : ''}
        ${hasWindow ? '<div class="day-window-band"></div>' : ''}
      </div>
    `;
  }

  // trailing blanks
  const totalCells = firstDay + daysInMonth;
  const trailing = totalCells % 7 === 0 ? 0 : 7 - (totalCells % 7);
  for (let i = 1; i <= trailing; i++) {
    html += `<div class="day outside" data-level="NONE"><span class="day-num">${i}</span></div>`;
  }

  grid.innerHTML = html;

  // Attach click listeners
  grid.querySelectorAll('.day:not(.outside)').forEach(el => {
    el.addEventListener('click', () => {
      state.selectedKey = el.dataset.key;
      renderMonthGrid();
      renderDetail(state.selectedKey);
    });
  });
}

/* =========================================================
   Year strip renderer
   ========================================================= */

const MONTHS_SHORT = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];

function renderYearStrip() {
  const strip = document.getElementById('year-strip');
  const todayKey = toKey(today);
  let html = '';
  for (let m = 0; m < 12; m++) {
    const daysInM = new Date(state.year, m + 1, 0).getDate();
    html += `<div class="ys-month">${MONTHS_SHORT[m]}</div>`;
    html += `<div class="ys-row">`;
    for (let d = 1; d <= 31; d++) {
      if (d > daysInM) {
        html += `<div class="ys-cell empty"></div>`;
      } else {
        const key = `${state.year}-${String(m + 1).padStart(2, '0')}-${String(d).padStart(2, '0')}`;
        const lvl = getDayLevel(key);
        const isToday = key === todayKey;
        const reutersCount = getReutersItems(key).length;
        html += `<div class="ys-cell${isToday ? ' today' : ''}${reutersCount ? ' has-reuters' : ''}" data-level="${lvl}" data-key="${key}" data-reuters-count="${reutersCount || ''}" title="${key}${reutersCount ? ` · ${reutersCount} Reuters item${reutersCount !== 1 ? 's' : ''}` : ''}"></div>`;
      }
    }
    html += `</div>`;
  }
  strip.innerHTML = html;

  strip.querySelectorAll('.ys-cell:not(.empty)').forEach(el => {
    el.addEventListener('click', () => {
      const [y, m, d] = el.dataset.key.split('-').map(Number);
      state.year = y;
      state.month = m - 1;
      state.selectedKey = el.dataset.key;
      renderMonthGrid();
      renderYearStrip();
      renderDetail(state.selectedKey);
    });
  });
}

/* =========================================================
   Strand filter panel
   ========================================================= */

function renderStrandPanel() {
  const list = document.getElementById('strand-list');
  const opts = [['overall', 'Overall', 'All strands combined'], ...Object.entries(STRAND_LABELS).map(([k, v]) => [k, k, v])];
  list.innerHTML = opts.map(([val, key, label]) => `
    <div class="strand-row ${state.activeStrand === val ? 'active' : ''}" data-strand="${val}">
      <span class="strand-key">${key}</span>
      <span class="strand-label">${label}</span>
    </div>
  `).join('');

  list.querySelectorAll('.strand-row').forEach(el => {
    el.addEventListener('click', () => {
      state.activeStrand = el.dataset.strand;
      renderStrandPanel();
      renderMonthGrid();
      renderYearStrip();
      renderDetail(state.selectedKey);
    });
  });
}

/* =========================================================
   Year tab panel
   ========================================================= */

function renderYearTabs() {
  const tabs = document.getElementById('year-tabs');
  tabs.innerHTML = [2026, 2027].map(y => `
    <button class="year-tab ${state.year === y ? 'active' : ''}" data-year="${y}">${y}</button>
  `).join('');

  tabs.querySelectorAll('.year-tab').forEach(el => {
    el.addEventListener('click', () => {
      state.year = Number(el.dataset.year);
      state.month = 0;
      state.selectedKey = `${state.year}-01-01`;
      renderYearTabs();
      renderMonthGrid();
      renderYearStrip();
      renderDetail(state.selectedKey);
    });
  });
}

/* =========================================================
   Full render
   ========================================================= */

function renderAll() {
  renderYearTabs();
  renderStrandPanel();
  renderMonthGrid();
  renderYearStrip();
  renderDetail(state.selectedKey);
  renderStandingOrders(getDayLevel(state.selectedKey));
}

/* =========================================================
   Topbar level for today (always shows today's level)
   ========================================================= */

function syncTodayStatus() {
  const todayLvl = getDayLevel(toKey(today));
  applyLevelCSS(todayLvl);
  document.getElementById('current-level').textContent = todayLvl;
  document.getElementById('current-date').textContent = formatDate(today);
  const entry = state.dayIndex[toKey(today)];
  const ctx = entry?.anchors?.length
    ? entry.anchors.map(e => e.name).join(' · ')
    : entry?.postures?.length
      ? entry.postures.map(p => `T-${p.tMinus} · ${p.event.name}`).join(' · ')
      : 'No active anniversaries';
  document.getElementById('current-context').textContent = ctx;
  renderStandingOrders(todayLvl);
  updateAlertBar(entry, todayLvl);
}

/* =========================================================
   Wire up controls
   ========================================================= */

function wireControls() {
  document.getElementById('btn-today').addEventListener('click', () => {
    state.year = today.getFullYear();
    state.month = today.getMonth();
    state.selectedKey = toKey(today);
    renderYearTabs();
    renderMonthGrid();
    renderYearStrip();
    renderDetail(state.selectedKey);
  });

  document.getElementById('prev-month').addEventListener('click', () => {
    state.month--;
    if (state.month < 0) { state.month = 11; state.year--; }
    renderYearTabs();
    renderMonthGrid();
    renderYearStrip();
  });

  document.getElementById('next-month').addEventListener('click', () => {
    state.month++;
    if (state.month > 11) { state.month = 0; state.year++; }
    renderYearTabs();
    renderMonthGrid();
    renderYearStrip();
  });

  document.getElementById('show-postures').addEventListener('change', e => {
    state.showPostures = e.target.checked;
    renderMonthGrid();
    renderDetail(state.selectedKey);
  });

  document.getElementById('show-windows').addEventListener('change', e => {
    state.showWindows = e.target.checked;
    renderMonthGrid();
    renderYearStrip();
    renderDetail(state.selectedKey);
  });

  const soundBtn = document.getElementById('btn-sound');
  soundBtn.addEventListener('click', () => {
    state.soundEnabled = !state.soundEnabled;
    soundBtn.textContent = `Sound: ${state.soundEnabled ? 'on' : 'off'}`;
    soundBtn.setAttribute('aria-pressed', String(state.soundEnabled));
    if (state.soundEnabled) playAlert('ELEVATED');
  });
}

/* =========================================================
   Schema metadata
   ========================================================= */

function renderMeta(data) {
  document.getElementById('schema-version').textContent = data.schema_version || '?';
  document.getElementById('generated').textContent = data.generated || '?';
}

/* =========================================================
   Init
   ========================================================= */

(function init() {
  const data = window.CALENDAR_DATA;
  if (!data) {
    document.body.innerHTML = '<p style="color:#d83a3a;padding:40px">Error: data.js not loaded.</p>';
    return;
  }

  state.data = data;
  state.dayIndex = buildDayIndex(data);
  state.strandIndex = buildStrandIndex(state.dayIndex);
  state.reuters = window.REUTERS_CORRELATIONS || { items: [] };
  state.reutersByDate = buildReutersIndex(state.reuters);

  renderMeta(data);
  wireControls();
  renderAll();
  syncTodayStatus();
})();
