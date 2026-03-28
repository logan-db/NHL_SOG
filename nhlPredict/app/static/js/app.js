/**
 * SOG Predict — NHL Shots on Goal App
 * Filterable games, predictions, and player drill-down
 * Favorites & picks saved to Lakebase
 */

const API_BASE = '';
const USER_ID_KEY = 'sog_predict_user_id';

// ----- Pipeline status (data freshness) -----
async function loadPipelineStatus() {
  const el = document.getElementById('pipeline-status');
  if (!el) return;
  try {
    const data = await fetchAPI('/pipeline-status');
    const textEl = el.querySelector('.status-text');
    if (!textEl) return;
    if (data?.last_completed_at) {
      const ts = data.last_completed_at;
      const ms = typeof ts === 'number' ? ts : parseInt(ts, 10);
      const d = new Date(isNaN(ms) ? ts : ms);
      textEl.textContent = `Data: ${d.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' })} ${d.toLocaleTimeString('en-US', { hour: 'numeric', minute: '2-digit' })}`;
      el.classList.remove('status-error');
      el.classList.add('status-ok');
      if (data.run_url) {
        el.title = 'Click to view last run';
        el.style.cursor = 'pointer';
        el.onclick = () => window.open(data.run_url, '_blank');
      }
    } else {
      textEl.textContent = data?.error ? `Data: ${data.error}` : 'Data: unknown';
      el.classList.add('status-error');
    }
  } catch (e) {
    const t = el.querySelector('.status-text');
    if (t) t.textContent = 'Data: unavailable';
    el.classList.add('status-error');
  }
}

function getUserId() {
  let id = localStorage.getItem(USER_ID_KEY);
  if (!id) {
    id = 'u_' + Math.random().toString(36).slice(2) + Date.now().toString(36);
    localStorage.setItem(USER_ID_KEY, id);
  }
  return id;
}

const FETCH_TIMEOUT_MS = 20000;

function getAPIBase() {
  // Use server-injected base when app is mounted (e.g. Databricks Apps at /apps/xxx)
  if (typeof window !== 'undefined' && window.APP_API_BASE !== undefined) {
    return window.APP_API_BASE;
  }
  return API_BASE;
}

async function fetchAPI(path, params = {}, options = {}) {
  const { method = 'GET', body } = options;
  const headers = { 'X-User-Id': getUserId(), ...(options.headers || {}) };
  const qs = new URLSearchParams({ user_id: getUserId(), ...params }).toString();
  const base = getAPIBase();
  const url = `${base}/api${path}${qs ? '?' + qs : ''}`;
  const fetchOpts = {
    method,
    headers: { ...headers },
    body: body ? JSON.stringify(body) : undefined,
  };
  if (body) fetchOpts.headers['Content-Type'] = 'application/json';
  try {
    const ctrl = new AbortController();
    const timeoutId = setTimeout(() => ctrl.abort(), FETCH_TIMEOUT_MS);
    const res = await fetch(url, { ...fetchOpts, signal: ctrl.signal });
    clearTimeout(timeoutId);
    if (!res.ok) throw new Error(res.statusText);
    return await res.json();
  } catch (e) {
    console.warn(`API ${path} failed:`, e);
    return null;
  }
}

/** User's local date as YYYY-MM-DD (for yesterday-results; avoids server timezone mismatch with Lakebase). */
function getLocalDateString() {
  const d = new Date();
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`;
}

/** Parse date-only string (YYYY-MM-DD) as local calendar date to avoid UTC timezone shift. */
function parseDateOnly(s) {
  if (!s) return null;
  const str = String(s).slice(0, 10);
  if (!/^\d{4}-\d{2}-\d{2}$/.test(str)) return new Date(s);
  const [y, m, d] = str.split('-').map(Number);
  const d2 = new Date(y, m - 1, d);
  return isNaN(d2.getTime()) ? new Date(s) : d2;
}

function formatDate(s) {
  if (!s) return '—';
  const d = parseDateOnly(s);
  return !d || isNaN(d.getTime()) ? s : d.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' });
}

/** Game card: date without year, optionally with time (e.g. "Feb 25, 7:00 PM") */
function formatGameDate(s, timeStr) {
  if (!s) return '—';
  const d = parseDateOnly(s);
  if (!d || isNaN(d.getTime())) return s;
  const datePart = d.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
  return timeStr ? `${datePart}, ${timeStr}` : datePart;
}

/** My Picks: date with full year + game time when available (e.g. "Feb 26, 2026, 7:00 PM") */
function formatPickDateAndTime(p) {
  const date = p.display_game_date || p.game_date;
  const time = p.display_game_time;
  if (!date) return '—';
  const d = parseDateOnly(date);
  if (!d || isNaN(d.getTime())) return String(date);
  const datePart = d.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' });
  return time ? `${datePart}, ${time}` : datePart;
}

function pct(v) {
  if (v == null) return '—';
  const n = parseFloat(v);
  return isNaN(n) ? '—' : (n * 100).toFixed(1) + '%';
}

/** Hit rate 0–1: show "Insufficient data" when null (common for rookies/low-usage players). */
function pctHitRate(v) {
  if (v == null) return '<span class="stat-placeholder">Insufficient data</span>';
  const n = parseFloat(v);
  return isNaN(n) ? '<span class="stat-placeholder">Insufficient data</span>' : (n * 100).toFixed(1) + '%';
}

function formatHistoricalResult(isWin) {
  if (isWin === 'Yes' || isWin === true) return 'W';
  if (isWin === 'No' || isWin === false) return 'L';
  return '—';
}

function num(v, decimals = 2) {
  if (v == null) return '—';
  const n = parseFloat(v);
  return isNaN(n) ? '—' : decimals === 0 ? String(Math.round(n)) : n.toFixed(decimals);
}

/** Percentile rank 0–1: higher = better. Returns { text, className }. */
function rankClass(v) {
  if (v == null) return { text: '—', className: '' };
  const n = parseFloat(v);
  if (isNaN(n)) return { text: '—', className: '' };
  const pct = (n * 100).toFixed(1) + '%';
  if (n >= 0.9) return { text: pct, className: 'rank-best' };
  if (n >= 0.7) return { text: pct, className: 'rank-good' };
  if (n >= 0.5) return { text: pct, className: 'rank-ok' };
  return { text: pct, className: 'rank-low' };
}

/**
 * Percentile rank 0–1 displayed as ordinal "X of Y".
 *
 * Default (inverted=false): rank 1 = best (highest perc_rank).
 *   perc_rank formula: perc = (total - rank) / (total - 1)
 *   → rank = total - perc * (total - 1)
 *   Color: high perc_rank = green.
 *
 * Inverted (inverted=true): rank 1 = best (lowest perc_rank), e.g. SOG Against
 *   where fewest shots allowed = strongest defense = rank 1.
 *   → rank = 1 + (total - 1) * perc_rank
 *   Color: low perc_rank = green (rank 1 = strong defense).
 */
function rankClassOrdinal(v, total, inverted = false) {
  if (v == null) return { text: '—', className: '' };
  const n = parseFloat(v);
  const t = parseInt(total, 10);
  if (isNaN(n) || isNaN(t) || t <= 0) return { text: '—', className: '' };
  const rank = inverted
    ? Math.round(1 + (t - 1) * n)
    : Math.round(t - (t - 1) * n);
  const colorN = inverted ? 1 - n : n;
  const text = `${rank} of ${t}`;
  if (colorN >= 0.9) return { text, className: 'rank-best' };
  if (colorN >= 0.7) return { text, className: 'rank-good' };
  if (colorN >= 0.5) return { text, className: 'rank-ok' };
  return { text, className: 'rank-low' };
}

/** Ice time rank (int): lower = better, 1 = most ice time. Returns { text, className }.
 *  Assumes ~18-20 skaters per team: top 3 = elite, top 6 = above average, top 12 = average. */
function iceTimeRankClass(v) {
  if (v == null) return { text: '—', className: '' };
  const n = parseInt(v, 10);
  if (isNaN(n)) return { text: '—', className: '' };
  if (n <= 3) return { text: String(n), className: 'rank-best' };
  if (n <= 6) return { text: String(n), className: 'rank-good' };
  if (n <= 12) return { text: String(n), className: 'rank-ok' };
  return { text: String(n), className: 'rank-low' };
}

/** PP SOG % (raw decimal, e.g. 0.33 = 33%): higher = better. Returns { text, className }. */
function ppSOGPctClass(v) {
  if (v == null) return { text: '—', className: '' };
  const n = parseFloat(v);
  if (isNaN(n)) return { text: '—', className: '' };
  const pct = (n * 100).toFixed(1) + '%';
  if (n >= 0.25) return { text: pct, className: 'rank-best' };
  if (n >= 0.12) return { text: pct, className: 'rank-good' };
  if (n >= 0.05) return { text: pct, className: 'rank-ok' };
  return { text: pct, className: 'rank-low' };
}

function escapeHtml(s) {
  if (s == null) return '';
  const div = document.createElement('div');
  div.textContent = s;
  return div.innerHTML;
}

/** Convert markdown-style AI analysis to HTML (##, ###, **, -, paragraphs). */
function formatAIAnalysisMarkdown(text) {
  if (!text || typeof text !== 'string') return '';
  const esc = (s) => {
    const d = document.createElement('div');
    d.textContent = s;
    return d.innerHTML;
  };
  let html = esc(text);
  // **bold** -> <strong>bold</strong>
  html = html.replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>');
  const lines = html.split('\n');
  const out = [];
  let inList = false;
  for (let i = 0; i < lines.length; i++) {
    let line = lines[i];
    const trimmed = line.trim();
    if (!trimmed) {
      if (inList) { out.push('</ul>'); inList = false; }
      continue;
    }
    const bulletMatch = line.match(/^\s*-\s+(.+)$/);
    if (bulletMatch) {
      if (!inList) { out.push('<ul>'); inList = true; }
      out.push('<li>' + bulletMatch[1] + '</li>');
    } else if (/^#+ /.test(trimmed)) {
      if (inList) { out.push('</ul>'); inList = false; }
      if (/^### /.test(trimmed)) out.push('<h4>' + trimmed.slice(4) + '</h4>');
      else if (/^## /.test(trimmed)) out.push('<h3>' + trimmed.slice(3) + '</h3>');
      else if (/^# /.test(trimmed)) out.push('<h2>' + trimmed.slice(2) + '</h2>');
    } else {
      if (inList) { out.push('</ul>'); inList = false; }
      out.push('<p>' + trimmed + '</p>');
    }
  }
  if (inList) out.push('</ul>');
  return '<div class="ai-analysis-markdown">' + out.join('') + '</div>';
}

function dateStr(d) {
  if (!d) return '';
  const s = String(d);
  if (/^\d{4}-\d{2}-\d{2}/.test(s)) return s.slice(0, 10);
  const x = new Date(d);
  return isNaN(x.getTime()) ? '' : x.toISOString().slice(0, 10);
}

/** Match prediction to schedule game: (playerTeam, opposingTeam) = (AWAY, HOME) or (HOME, AWAY).
 * Allows ±1 day date tolerance to handle UTC vs Eastern pipeline date offset
 * (pipeline stores gameDate as UTC midnight; 7 PM ET game on Mar 19 = Mar 20 UTC). */
function predictionMatchesGame(p, home, away, gameDate) {
  const d = dateStr(p.game_date);
  const gd = dateStr(gameDate);
  if (!d || !gd) return false;
  if (d !== gd) {
    const diffMs = Math.abs(new Date(d + 'T12:00:00').getTime() - new Date(gd + 'T12:00:00').getTime());
    if (diffMs > 36 * 60 * 60 * 1000) return false; // more than 1.5 days apart → definitely wrong game
  }
  return (
    (p.player_team === away && p.opposing_team === home) ||
    (p.player_team === home && p.opposing_team === away)
  );
}

/** Build game key for grouping */
function gameKey(home, away, date) {
  return `${dateStr(date)}|${home}|${away}`;
}

// ----- Filter options -----
let filterOptions = { teams: [], players: [] };

async function loadFilterOptions() {
  const data = await fetchAPI('/filter-options');
  if (!data) return;
  filterOptions = { teams: data.teams || [], players: data.players || [] };

  const teamSelect = document.getElementById('filter-team');
  if (teamSelect) {
    teamSelect.innerHTML = '<option value="">All teams</option>' +
      filterOptions.teams.map(t => `<option value="${escapeHtml(t)}">${escapeHtml(t)}</option>`).join('');
  }
  // Also populate historical tab team filter
  const histTeamSelect = document.getElementById('historical-filter-team');
  if (histTeamSelect) {
    histTeamSelect.innerHTML = '<option value="">All teams</option>' +
      filterOptions.teams.map(t => `<option value="${escapeHtml(t)}">${escapeHtml(t)}</option>`).join('');
  }
  // Populate upcoming games section team filter
  const gamesTeamSelect = document.getElementById('games-filter-team');
  if (gamesTeamSelect) {
    gamesTeamSelect.innerHTML = '<option value="">All teams</option>' +
      filterOptions.teams.map(t => `<option value="${escapeHtml(t)}">${escapeHtml(t)}</option>`).join('');
  }
  // Re-render favorite teams chips now that we have the full team list for the dropdown
  renderFavoriteTeamsChips();
}

// ----- Favorites & Picks -----
let favoriteSet = new Set(); // "playerName|playerTeam"
let favoriteTeamsSet = new Set(); // "TOR", "NYR", ...

const PICK_TYPE_LABELS = {
  goal: 'Goal',
  point: 'Point',
  assist: 'Assist',
  sog_2: '2+ SOG',
  sog_3: '3+ SOG',
  sog_4: '4+ SOG',
  sog: 'SOG'
};

async function loadFavorites() {
  const [favRes, teamsRes] = await Promise.all([
    fetchAPI('/favorites'),
    fetchAPI('/favorite-teams').catch(() => ({ teams: [] })),
  ]);
  favoriteSet.clear();
  (favRes?.favorites || []).forEach(f => favoriteSet.add(`${f.player_name}|${f.player_team}`));
  favoriteTeamsSet.clear();
  (teamsRes?.teams || []).forEach(t => favoriteTeamsSet.add((t.team || '').toUpperCase()));
}

async function loadFavoriteTeams() {
  const data = await fetchAPI('/favorite-teams');
  favoriteTeamsSet.clear();
  (data?.teams || []).forEach(t => favoriteTeamsSet.add((t.team || '').toUpperCase()));
  renderFavoriteTeamsChips();
}

function isFavorite(playerName, playerTeam) {
  return favoriteSet.has(`${playerName}|${playerTeam}`);
}

async function toggleFavorite(playerName, playerTeam) {
  const key = `${playerName}|${playerTeam}`;
  if (favoriteSet.has(key)) {
    const res = await fetchAPI('/favorites', {}, { method: 'DELETE', body: { player_name: playerName, player_team: playerTeam } });
    if (res && !res.error) favoriteSet.delete(key);
  } else {
    const res = await fetchAPI('/favorites', {}, { method: 'POST', body: { player_name: playerName, player_team: playerTeam } });
    if (res && !res.error) favoriteSet.add(key);
  }
  renderGames();
  renderPredictionsTable();
}

function showToast(message, type = 'success') {
  const el = document.getElementById('toast');
  if (!el) return;
  el.textContent = message;
  el.className = `toast ${type} visible`;
  clearTimeout(el._toastTimer);
  el._toastTimer = setTimeout(() => { el.classList.remove('visible'); }, 2500);
}

let pickModalContext = null;

function openPickModal(gameDate, home, away, playerName, playerTeam, opposingTeam, predictedSog, playerId) {
  pickModalContext = { gameDate, home, away, playerName, playerTeam, opposingTeam, predictedSog, playerId };
  document.getElementById('pick-modal-player').textContent = `${playerName} (${playerTeam})`;
  document.getElementById('pick-modal-overlay').classList.add('visible');
}

function closePickModal() {
  pickModalContext = null;
  document.getElementById('pick-modal-overlay').classList.remove('visible');
}

async function addPick(gameDate, home, away, playerName, playerTeam, opposingTeam, predictedSog, playerId, pickType = 'sog') {
  const body = { game_date: dateStr(gameDate), home_team: home, away_team: away, player_name: playerName, player_team: playerTeam, opposing_team: opposingTeam, predicted_sog: predictedSog ?? 0, pick_type: pickType };
  if (playerId) body.player_id = playerId;
  const res = await fetchAPI('/picks', {}, { method: 'POST', body });
  if (res && !res.error) {
    closePickModal();
    await loadPicks();
    const label = PICK_TYPE_LABELS[pickType] || pickType;
    showToast(`Picked ${playerName} - ${label} for ${away} @ ${home}`);
    showTab('my-picks');
  } else {
    showToast(res?.error || 'Failed to save pick', 'error');
  }
}

// ----- Games + predictions combined -----
let allGames = [];
let allPredictions = [];
let gamesView = 'today'; // 'today' | 'tomorrow'
let predictionsSortBy = 'pred_sog';
let predictionsSortDir = 'desc';

function renderFavoriteTeamsChips() {
  const chipsEl = document.getElementById('favorite-teams-chips');
  const addSelect = document.getElementById('add-favorite-team');
  if (!chipsEl || !addSelect) return;

  const teams = Array.from(favoriteTeamsSet).filter(Boolean).sort();
  chipsEl.innerHTML = teams.map(t => `
    <span class="favorite-chip">${escapeHtml(t)} <button type="button" class="chip-remove" data-team="${escapeHtml(t)}" aria-label="Remove">×</button></span>
  `).join('') || '<span class="favorite-chip empty">None</span>';

  chipsEl.querySelectorAll('.chip-remove').forEach(btn => {
    btn.addEventListener('click', () => toggleFavoriteTeam(btn.dataset.team));
  });

  const allTeams = [...new Set([...(filterOptions.teams || []), ...teams])].sort();
  addSelect.innerHTML = '<option value="">+ Add team</option>' +
    allTeams.filter(t => !favoriteTeamsSet.has(t)).map(t => `<option value="${escapeHtml(t)}">${escapeHtml(t)}</option>`).join('');
  addSelect.onchange = () => {
    const v = addSelect.value;
    if (v) { toggleFavoriteTeam(v); addSelect.value = ''; }
  };
}

async function toggleFavoriteTeam(team) {
  const t = (team || '').toUpperCase();
  if (!t) return;
  if (favoriteTeamsSet.has(t)) {
    const res = await fetchAPI('/favorite-teams', {}, { method: 'DELETE', body: { team: t } });
    if (res && !res.error) favoriteTeamsSet.delete(t);
  } else {
    const res = await fetchAPI('/favorite-teams', {}, { method: 'POST', body: { team: t } });
    if (res && !res.error) favoriteTeamsSet.add(t);
  }
  renderFavoriteTeamsChips();
  renderGames();
}

// ----- Hit Rate Leaderboard -----
let hitRateChart = null;
let hitRateData = [];
let hitRateSortBy = '2plus';

async function loadHitRateLeaderboard() {
  const data = await fetchAPI('/hit-rate-leaderboard', { client_date: getLocalDateString() }).catch(() => null);
  hitRateData = data?.players || [];
  renderHitRateLeaderboard();
}

function renderHitRateLeaderboard() {
  const canvas = document.getElementById('hit-rate-chart');
  const emptyEl = document.getElementById('hit-rate-empty');
  if (!canvas) return;

  if (!hitRateData.length) {
    if (emptyEl) emptyEl.style.display = '';
    canvas.style.display = 'none';
    return;
  }
  if (emptyEl) emptyEl.style.display = 'none';
  canvas.style.display = '';

  const sorted = [...hitRateData].sort((a, b) => {
    const aVal = parseFloat(hitRateSortBy === '2plus' ? a.hit_rate_2plus : a.hit_rate_3plus) || 0;
    const bVal = parseFloat(hitRateSortBy === '2plus' ? b.hit_rate_2plus : b.hit_rate_3plus) || 0;
    return bVal - aVal;
  }).slice(0, 20);

  const labels = sorted.map(p => {
    const name = p.shooter_name || '—';
    const parts = name.trim().split(' ');
    const shortName = parts.length > 1 ? `${parts[parts.length - 1]}, ${parts[0][0]}.` : name;
    return `${shortName} (${p.player_team || ''})`;
  });

  const data2plus = sorted.map(p => parseFloat(p.hit_rate_2plus) || 0);
  const data3plus = sorted.map(p => parseFloat(p.hit_rate_3plus) || 0);

  const accent = getComputedStyle(document.documentElement).getPropertyValue('--accent').trim() || '#6366f1';
  const color3plus = '#10b981'; // emerald green — visually distinct from indigo 2+
  const chartFont = { family: "'Inter', -apple-system, sans-serif", size: 11 };

  if (hitRateChart) { hitRateChart.destroy(); hitRateChart = null; }

  // Resize canvas container to fit all players
  const rowHeight = 30;
  canvas.parentElement.style.height = `${sorted.length * rowHeight + 70}px`;

  const ctx = canvas.getContext('2d');
  hitRateChart = new Chart(ctx, {
    type: 'bar',
    data: {
      labels,
      datasets: [
        {
          label: '2+ SOG %',
          data: data2plus,
          backgroundColor: `${accent}bb`,
          borderColor: accent,
          borderWidth: 1,
          borderRadius: 3,
        },
        {
          label: '3+ SOG %',
          data: data3plus,
          backgroundColor: `${color3plus}88`,
          borderColor: color3plus,
          borderWidth: 1,
          borderRadius: 3,
        },
      ],
    },
    options: {
      indexAxis: 'y',
      responsive: true,
      maintainAspectRatio: false,
      onClick: (event, elements) => {
        if (!elements.length) return;
        const p = sorted[elements[0].index];
        if (p) navigateToPlayerPage(p.shooter_name, p.player_team, p.opposing_team, p.game_date);
      },
      onHover: (event, elements) => {
        event.native.target.style.cursor = elements.length ? 'pointer' : 'default';
      },
      plugins: {
        legend: {
          position: 'top',
          labels: { font: chartFont, boxWidth: 12, padding: 10 },
        },
        tooltip: {
          callbacks: {
            label: ctx => `${ctx.dataset.label}: ${ctx.parsed.x.toFixed(1)}%`,
            afterLabel: ctx => {
              const p = sorted[ctx.dataIndex];
              const lines = [`vs ${p.opposing_team || '—'}`, `Pred SOG: ${p.predicted_sog || '—'}`];
              if (p.avg_sog_last7) lines.push(`Avg Last 7g: ${p.avg_sog_last7}`);
              if (hitRateSortBy === '2plus' && p.hit_rate_2plus_30d != null)
                lines.push(`2+ Last 30d: ${p.hit_rate_2plus_30d}%`);
              if (hitRateSortBy === '3plus' && p.hit_rate_3plus_30d != null)
                lines.push(`3+ Last 30d: ${p.hit_rate_3plus_30d}%`);
              lines.push('Click to view player analysis →');
              return lines;
            },
          },
        },
      },
      scales: {
        x: {
          min: 0,
          max: 100,
          ticks: { callback: v => `${v}%`, font: { size: 10 } },
          grid: { color: 'rgba(0,0,0,0.06)' },
          title: { display: true, text: 'Season Hit Rate %', font: { size: 11 } },
        },
        y: {
          ticks: { font: { ...chartFont, size: 10 } },
          grid: { display: false },
        },
      },
    },
  });
}

async function loadData() {
  // Fetch Latest Results (independent from team season SOG)
  fetchAPI('/yesterday-results', { client_date: getLocalDateString() })
    .then(res => renderYesterdayResults(res))
    .catch(e => { console.warn('yesterday-results failed:', e); renderYesterdayResults(null); });

  // Season team SOG rankings load separately into their own section
  loadTeamSeasonSog();

  // Load hit rate leaderboard independently (non-blocking)
  loadHitRateLeaderboard();

  try {
    const [gamesRes, predRes, favRes] = await Promise.all([
      fetchAPI('/upcoming-games', { view: gamesView, client_date: getLocalDateString() }),
      gamesView === 'today'
        ? fetchAPI('/upcoming-predictions', { ...getFilterParams(), by_date: 'true', game_date: getLocalDateString() })
        : fetchAPI('/upcoming-predictions', getFilterParams()),
      fetchAPI('/favorites').then(d => { if (d?.favorites) favoriteSet = new Set(d.favorites.map(f => `${f.player_name}|${f.player_team}`)); return d; }),
      fetchAPI('/favorite-teams').then(d => { if (d?.teams) favoriteTeamsSet = new Set((d.teams || []).map(t => (t.team || '').toUpperCase())); return d; }).catch(() => null),
    ]);
    allGames = gamesRes?.games || [];
    allPredictions = predRes?.predictions || [];
    renderGames();
    renderPredictionsTable();
    renderTopPicks();
    renderFavoriteTeamsChips();
    loadPicks();
  } catch (e) {
    console.error('loadData failed:', e);
    const grid = document.getElementById('games-grid');
    if (grid) grid.innerHTML = '<div class="empty-state">Failed to load data. <button type="button" class="btn-clear" onclick="loadData()">Retry</button></div>';
    const tbody = document.getElementById('predictions-body');
    if (tbody) tbody.innerHTML = '<tr><td colspan="11" class="empty-state">Failed to load predictions.</td></tr>';
  }
}

function getFilterParams() {
  const date = document.getElementById('filter-date')?.value?.trim();
  const team = document.getElementById('filter-team')?.value?.trim();
  const player = document.getElementById('filter-player')?.value?.trim();
  const params = {};
  if (date) params.game_date = date;
  if (team) params.player_team = team;
  if (player) params.player = player;
  return params;
}

function renderTopPicks() {
  const grid = document.getElementById('top-picks-grid');
  if (!grid) return;
  const topN = allPredictions
    .filter(p => p.predicted_sog != null)
    .sort((a, b) => (parseFloat(b.predicted_sog) || 0) - (parseFloat(a.predicted_sog) || 0))
    .slice(0, 8);
  if (!topN.length) {
    grid.innerHTML = '<p class="empty-state" style="padding:0.5rem;font-size:0.85rem;">No predictions yet.</p>';
    return;
  }
  const maxPred = Math.max(...topN.map(p => parseFloat(p.predicted_sog) || 0), 1);
  const SOG_TOOLTIP = 'Predicted shots on goal for this game';
  const OPP_TOOLTIP = "Opponent's SOG-against rank (1 of 32 = fewest shots allowed = strongest defense; 32 of 32 = softest defense).";
  grid.innerHTML = topN.map(p => {
    const oppRank = rankClassOrdinal(p.opp_sog_against_rank, 32, true);
    const predVal = parseFloat(p.predicted_sog) || 0;
    const barPct = maxPred > 0 ? (predVal / maxPred) * 100 : 0;
    return `
      <div class="top-pick-card" data-player="${escapeHtml(p.shooter_name)}" data-team="${escapeHtml(p.player_team)}" data-opp="${escapeHtml(p.opposing_team)}" data-date="${dateStr(p.game_date)}">
        <div class="top-pick-name">${escapeHtml(p.shooter_name)}</div>
        <div class="top-pick-matchup">${escapeHtml(p.player_team)} vs ${escapeHtml(p.opposing_team)}</div>
        <div class="top-pick-bar-wrap">
          <div class="top-pick-bar" style="width: ${barPct}%;" title="${num(p.predicted_sog)} predicted SOG"></div>
        </div>
        <div class="top-pick-stats">
          <span class="stat-tooltip-wrap"><span class="pred">${num(p.predicted_sog)} SOG</span><span class="stat-tooltip">${escapeHtml(SOG_TOOLTIP)}</span></span>
          <span class="stat-tooltip-wrap"><span class="opp ${oppRank.className}">${oppRank.text} opp</span><span class="stat-tooltip">${escapeHtml(OPP_TOOLTIP)}</span></span>
        </div>
      </div>
    `;
  }).join('');
  grid.querySelectorAll('.top-pick-card').forEach(card => {
    card.addEventListener('click', () => {
      navigateToPlayerPage(card.dataset.player, card.dataset.team, card.dataset.opp, card.dataset.date);
    });
  });
}

function renderYesterdayResults(data) {
  const headingEl = document.getElementById('yesterday-heading');
  const subtitleEl = document.getElementById('yesterday-subtitle');
  const scoresEl = document.getElementById('yesterday-scores');
  const shootersEl = document.getElementById('yesterday-shooters');
  const scoresEmpty = document.getElementById('yesterday-scores-empty');
  const shootersEmpty = document.getElementById('yesterday-shooters-empty');
  if (!scoresEl || !shootersEl) return;

  const scores = data?.scores || [];
  const shooters = data?.top_shooters || [];
  const daysAgo = data?.days_ago;
  const isYesterday = data?.is_yesterday;
  const dataStale = data?.data_stale;
  const latestDate = data?.latest_date;

  if (headingEl) {
    headingEl.textContent = isYesterday ? "Yesterday's Results" : 'Latest Results';
  }
  if (subtitleEl) {
    if (dataStale && latestDate) {
      subtitleEl.textContent = scores.length || shooters.length
        ? `Showing most recent available: ${formatDate(latestDate)}. Pipeline or Lakebase sync may need to run for today.`
        : `No data in the last 14 days. Latest game in database: ${formatDate(latestDate)}. Pipeline or Lakebase sync may need to run.`;
      subtitleEl.style.color = 'var(--warn)';
    } else if (daysAgo != null && daysAgo > 1) {
      const d = new Date();
      d.setDate(d.getDate() - daysAgo);
      subtitleEl.textContent = `Showing ${formatDate(d.toISOString().slice(0, 10))} (data may be delayed)`;
      subtitleEl.style.color = 'var(--text-muted)';
    } else {
      subtitleEl.textContent = '';
    }
  }

  if (!scores.length) {
    scoresEl.innerHTML = '';
    if (scoresEmpty) {
      scoresEmpty.style.display = '';
      const apiUrl = `${getAPIBase()}/api/yesterday-results?client_date=${encodeURIComponent(getLocalDateString())}`;
      scoresEmpty.innerHTML = 'No games played yesterday. <a href="' + apiUrl + '" target="_blank" rel="noopener" style="font-size:0.85em;">Open API</a> · <a href="' + getAPIBase() + '/api/debug-yesterday?client_date=' + encodeURIComponent(getLocalDateString()) + '" target="_blank" rel="noopener" style="font-size:0.85em;">Troubleshoot</a>';
    }
  } else {
    if (scoresEmpty) scoresEmpty.style.display = 'none';
    scoresEl.innerHTML = scores.map(g => {
      const gf = g.goals_for ?? '—';
      const ga = g.goals_against ?? '—';
      const away = (g.away || '').trim();
      const home = (g.home || '').trim();
      return `<div class="score-row"><span class="score-teams">${escapeHtml(away)} @ ${escapeHtml(home)}</span><span class="score-result">${gf}–${ga}</span></div>`;
    }).join('');
  }

  if (!shooters.length) {
    shootersEl.innerHTML = '';
    if (shootersEmpty) shootersEmpty.style.display = '';
  } else {
    if (shootersEmpty) shootersEmpty.style.display = 'none';
    const maxSog = Math.max(...shooters.map(s => parseInt(s.sog, 10) || 0), 1);
    shootersEl.innerHTML = shooters.map((s, i) => {
      const sog = parseInt(s.sog, 10) || 0;
      const pct = maxSog > 0 ? (sog / maxSog) * 100 : 0;
      const goals = s.goals != null ? s.goals : '—';
      const assists = s.assists != null ? s.assists : '—';
      return `
        <div class="shooter-row">
          <span class="shooter-rank">${i + 1}</span>
          <div class="shooter-info">
            <span class="shooter-name">${escapeHtml(s.shooter_name)}</span>
            <span class="shooter-matchup">${escapeHtml(s.player_team || '')} vs ${escapeHtml(s.opposing_team || '')}</span>
          </div>
          <div class="shooter-bar-wrap">
            <div class="shooter-bar" style="width: ${pct}%;" title="${sog} SOG"></div>
          </div>
          <span class="shooter-sog">${sog}</span>
          <span class="shooter-points">${goals}G / ${assists}A</span>
        </div>`;
    }).join('');
  }

}

// ----- Season Team SOG Rankings (standalone section) -----
let teamSeasonSogData = null;
let teamSeasonShowAll = false;

async function loadTeamSeasonSog() {
  const data = await fetchAPI('/team-season-sog').catch(() => null);
  teamSeasonSogData = data;
  renderTeamSeasonSog(data, teamSeasonShowAll);
}

function renderTeamSeasonSog(data, showAll) {
  const shootingEl = document.getElementById('team-sog-shooting');
  const allowedEl = document.getElementById('team-sog-allowed');
  const shootingEmpty = document.getElementById('team-sog-shooting-empty');
  const allowedEmpty = document.getElementById('team-sog-allowed-empty');
  if (!shootingEl || !allowedEl) return;

  const topShooting = data?.top_shooting || [];
  const mostAllowed = data?.most_allowed || [];
  const topErr = data?._top_error;
  const allowedErr = data?._allowed_error;
  const limit = showAll ? 32 : 10;

  if (shootingEl) {
    if (topErr) {
      shootingEl.innerHTML = `<p class="empty-hint" style="color:var(--warn);font-size:0.8rem;">Query error: ${escapeHtml(topErr)}</p>`;
      if (shootingEmpty) shootingEmpty.style.display = 'none';
    } else if (!topShooting.length) {
      shootingEl.innerHTML = '';
      if (shootingEmpty) shootingEmpty.style.display = '';
    } else {
      if (shootingEmpty) shootingEmpty.style.display = 'none';
      const slice = topShooting.slice(0, limit);
      const vals = slice.map(t => parseFloat(t.avg_sog ?? t.sog) || 0);
      const maxVal = Math.max(...vals, 1);
      shootingEl.innerHTML = slice.map((t, i) => {
        const val = parseFloat(t.avg_sog ?? t.sog) || 0;
        const barPct = maxVal > 0 ? (val / maxVal) * 100 : 0;
        const display = t.avg_sog != null ? val.toFixed(1) : String(val | 0);
        const titleText = `${display} avg SOG/game (${t.games ?? '?'} games)`;
        return `<div class="team-sog-row">
          <span class="team-sog-rank">${i + 1}</span>
          <div class="team-sog-team-cell">
            <img src="${nhlLogoUrl(t.team)}" alt="" class="team-sog-logo" onerror="this.style.display='none';">
            <span class="team-sog-abbrev">${escapeHtml(t.team || '—')}</span>
          </div>
          <div class="team-sog-bar-wrap"><div class="team-sog-bar-shooting" style="width: ${barPct}%;" title="${titleText}"></div></div>
          <span class="team-sog-value">${display}</span>
        </div>`;
      }).join('');
    }
  }

  if (allowedEl) {
    if (allowedErr) {
      allowedEl.innerHTML = `<p class="empty-hint" style="color:var(--warn);font-size:0.8rem;">Query error: ${escapeHtml(allowedErr)}</p>`;
      if (allowedEmpty) allowedEmpty.style.display = 'none';
    } else if (!mostAllowed.length) {
      allowedEl.innerHTML = '';
      if (allowedEmpty) allowedEmpty.style.display = '';
    } else {
      if (allowedEmpty) allowedEmpty.style.display = 'none';
      const slice = mostAllowed.slice(0, limit);
      const totalTeams = mostAllowed.length || 32;
      const vals = slice.map(t => parseFloat(t.avg_sog_allowed ?? t.sog_allowed) || 0);
      const maxVal = Math.max(...vals, 1);
      allowedEl.innerHTML = slice.map((t, i) => {
        const val = parseFloat(t.avg_sog_allowed ?? t.sog_allowed) || 0;
        const barPct = maxVal > 0 ? (val / maxVal) * 100 : 0;
        const display = t.avg_sog_allowed != null ? val.toFixed(1) : String(val | 0);
        const defRank = totalTeams - i;
        const titleText = `${display} avg SOGA/game (${t.games ?? '?'} games) — defense rank ${defRank} of ${totalTeams}`;
        return `<div class="team-sog-row">
          <span class="team-sog-rank">${defRank}</span>
          <div class="team-sog-team-cell">
            <img src="${nhlLogoUrl(t.team)}" alt="" class="team-sog-logo" onerror="this.style.display='none';">
            <span class="team-sog-abbrev">${escapeHtml(t.team || '—')}</span>
          </div>
          <div class="team-sog-bar-wrap"><div class="team-sog-bar-allowed" style="width: ${barPct}%;" title="${titleText}"></div></div>
          <span class="team-sog-value">${display}</span>
        </div>`;
      }).join('');
    }
  }
}

function renderGames() {
  const grid = document.getElementById('games-grid');
  if (!grid) return;

  if (!allGames.length) {
    grid.innerHTML = '<div class="empty-state">No upcoming games found.</div>';
    const badge = document.getElementById('games-date-badge');
    if (badge) badge.style.display = 'none';
    return;
  }

  // Show game date badge (today vs tomorrow etc.)
  const badgeEl = document.getElementById('games-date-badge');
  if (badgeEl && allGames.length) {
    const today = getLocalDateString();
    const gameDates = [...new Set(allGames.map(g => g.game_date).filter(Boolean))].sort();
    const firstDate = gameDates[0];
    if (firstDate) {
      const isToday = firstDate === today;
      const isTomorrow = firstDate === (() => { const d = new Date(); d.setDate(d.getDate() + 1); return `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}-${String(d.getDate()).padStart(2,'0')}`; })();
      const dateLabel = isToday ? 'Today' : isTomorrow ? 'Tomorrow' : formatDate(firstDate);
      const multiDay = gameDates.length > 1 ? ` + ${gameDates.length - 1} more day${gameDates.length > 2 ? 's' : ''}` : '';
      badgeEl.textContent = `${dateLabel}${multiDay}`;
      badgeEl.className = `games-date-badge ${isToday ? 'badge-today' : 'badge-future'}`;
      badgeEl.style.display = '';
    }
  }

  const expandedKey = grid.dataset.expandedKey || '';
  const hasFilters = !!(
    document.getElementById('filter-date')?.value ||
    document.getElementById('filter-team')?.value ||
    document.getElementById('filter-player')?.value
  );

  // When filters applied, only show games that have matching predictions
  let gamesToShow = hasFilters
    ? allGames.filter(g => {
        const preds = allPredictions.filter(p =>
          predictionMatchesGame(p, g.home, g.away, g.game_date)
        );
        return preds.length > 0;
      })
    : allGames;

  // Section-specific upcoming games filters
  const gamesFilterTeam = (document.getElementById('games-filter-team')?.value || '').toUpperCase();
  const gamesFilterPlayer = (document.getElementById('games-filter-player')?.value || '').toLowerCase().trim();
  const gamesSortDate = document.getElementById('games-sort-date')?.value || 'asc';

  if (gamesFilterTeam) {
    gamesToShow = gamesToShow.filter(g =>
      (g.home || '').toUpperCase() === gamesFilterTeam ||
      (g.away || '').toUpperCase() === gamesFilterTeam
    );
  }
  if (gamesFilterPlayer) {
    gamesToShow = gamesToShow.filter(g => {
      const preds = allPredictions.filter(p => predictionMatchesGame(p, g.home, g.away, g.game_date));
      return preds.some(p => (p.shooter_name || '').toLowerCase().includes(gamesFilterPlayer));
    });
  }

  // Sort by date with favorites as tiebreaker
  gamesToShow = [...gamesToShow].sort((a, b) => {
    const aDate = a.game_date || '';
    const bDate = b.game_date || '';
    const dateCompare = gamesSortDate === 'desc'
      ? (aDate > bDate ? -1 : aDate < bDate ? 1 : 0)
      : (aDate < bDate ? -1 : aDate > bDate ? 1 : 0);
    if (dateCompare !== 0) return dateCompare;
    if (favoriteTeamsSet.size > 0) {
      const aHasFav = favoriteTeamsSet.has((a.home || '').toUpperCase()) || favoriteTeamsSet.has((a.away || '').toUpperCase());
      const bHasFav = favoriteTeamsSet.has((b.home || '').toUpperCase()) || favoriteTeamsSet.has((b.away || '').toUpperCase());
      if (aHasFav && !bHasFav) return -1;
      if (!aHasFav && bHasFav) return 1;
    }
    return 0;
  });

  if (!gamesToShow.length) {
    grid.innerHTML = '<div class="empty-state">No games match the current filters.</div>';
    return;
  }

  grid.innerHTML = gamesToShow.map(g => {
    const home = g.home || '';
    const away = g.away || '';
    const gd = g.game_date;
    const key = gameKey(home, away, gd);

    const predsForGame = allPredictions.filter(p =>
      predictionMatchesGame(p, home, away, gd)
    );

    const topSog = predsForGame.length
      ? predsForGame.reduce((a, b) => (a.predicted_sog > b.predicted_sog ? a : b))
      : null;
    const topVar = predsForGame.length
      ? predsForGame.reduce((a, b) =>
          (parseFloat(a.abs_variance_avg_last7_sog) || 0) > (parseFloat(b.abs_variance_avg_last7_sog) || 0) ? a : b
        )
      : null;

    const isExpanded = key === expandedKey;
    const gameSortKey = grid.dataset.gameSortKey || 'predicted_sog';
    const gameSortDir = grid.dataset.gameSortDir || 'desc';
    const sortKeys = {
      player: 'shooter_name',
      team: 'player_team',
      predicted_sog: 'predicted_sog',
      variance: 'abs_variance_avg_last7_sog',
      avg7: 'player_avg_sog_last7',
      hit2: 'player_2plus_season_hit_rate',
      hit3: 'player_3plus_season_hit_rate',
      opp: 'opp_sog_against_rank',
    };
    const getSortVal = (p, k) => {
      if (k === 'hit2') return p.player_2plus_season_hit_rate ?? p.player_2plus_last30_hit_rate ?? 0;
      if (k === 'hit3') return p.player_3plus_season_hit_rate ?? p.player_3plus_last30_hit_rate ?? 0;
      const v = p[sortKeys[k] || k];
      return k === 'player' || k === 'team' ? (v || '').toString().toLowerCase() : (parseFloat(v) || 0);
    };
    const sortedPreds = [...predsForGame].sort((a, b) => {
      const va = getSortVal(a, gameSortKey);
      const vb = getSortVal(b, gameSortKey);
      if (gameSortDir === 'asc') return va > vb ? 1 : va < vb ? -1 : 0;
      return va < vb ? 1 : va > vb ? -1 : 0;
    });

    return `
      <div class="game-card ${isExpanded ? 'expanded' : ''}" data-key="${escapeHtml(key)}" data-home="${escapeHtml(home)}" data-away="${escapeHtml(away)}" data-date="${escapeHtml(dateStr(gd))}">
        <div class="matchup">
          <div class="matchup-team away-team-block">
            <img src="${nhlLogoUrl(away)}" alt="${escapeHtml(away)}" class="matchup-logo" onerror="this.style.display='none';">
            <span class="away-team">${escapeHtml(away)}</span>
            <button type="button" class="btn-last-game" data-team="${escapeHtml(away)}" title="View ${escapeHtml(away)} last game" aria-label="Last game for ${escapeHtml(away)}">Last game ↗</button>
          </div>
          <span class="vs">@</span>
          <div class="matchup-team home-team-block">
            <img src="${nhlLogoUrl(home)}" alt="${escapeHtml(home)}" class="matchup-logo" onerror="this.style.display='none';">
            <span class="home-team">${escapeHtml(home)}</span>
            <button type="button" class="btn-last-game" data-team="${escapeHtml(home)}" title="View ${escapeHtml(home)} last game" aria-label="Last game for ${escapeHtml(home)}">Last game ↗</button>
          </div>
        </div>
        <div id="last-game-panel-${escapeHtml(key)}" class="last-game-panel" style="display:none;"></div>
        <div class="game-date">${formatGameDate(gd, g.game_time)}</div>
        <div class="top-stats">
          ${topSog ? `<div class="top-stat">🔥 Top SOG: <strong>${num(topSog.predicted_sog)}</strong> (${escapeHtml(topSog.shooter_name)})</div>` : ''}
          ${topVar ? `<div class="top-stat">Variance: <strong>${num(topVar.abs_variance_avg_last7_sog)}</strong> (${escapeHtml(topVar.shooter_name)})</div>` : ''}
        </div>
        ${isExpanded ? `
        <div class="game-analysis-section" data-game-key="${escapeHtml(key)}">
          <h4 class="game-analysis-title">Game Analysis</h4>
          <div class="game-analysis-content"><span class="loading-inline">Loading…</span></div>
        </div>
        ` : ''}
        <div class="game-predictions-table">
          ${predsForGame.length ? `
            <table class="game-players-table">
              <thead><tr>
                <th>Player</th><th>Team</th>
                <th class="numeric sortable" data-sort="predicted_sog" title="Click to sort">Pred SOG ${gameSortKey === 'predicted_sog' ? (gameSortDir === 'desc' ? '▼' : '▲') : ''}</th>
                <th class="numeric sortable" data-sort="variance" title="Click to sort">Variance ${gameSortKey === 'variance' ? (gameSortDir === 'desc' ? '▼' : '▲') : ''}</th>
                <th class="numeric sortable" data-sort="avg7" title="Click to sort">Avg 7 ${gameSortKey === 'avg7' ? (gameSortDir === 'desc' ? '▼' : '▲') : ''}</th>
                <th class="numeric sortable" data-sort="hit2" title="Click to sort">2+ % ${gameSortKey === 'hit2' ? (gameSortDir === 'desc' ? '▼' : '▲') : ''}</th>
                <th class="numeric sortable" data-sort="hit3" title="Click to sort">3+ % ${gameSortKey === 'hit3' ? (gameSortDir === 'desc' ? '▼' : '▲') : ''}</th>
                <th class="numeric sortable" data-sort="opp" title="Click to sort">Opp Rank ${gameSortKey === 'opp' ? (gameSortDir === 'desc' ? '▼' : '▲') : ''}</th>
                <th></th>
              </tr></thead>
              <tbody>
                ${sortedPreds
                  .map(p => {
                    const oppRank = rankClassOrdinal(p.opp_sog_against_rank, 32, true);
                    const fav = isFavorite(p.shooter_name, p.player_team);
                    return `<tr class="player-row" data-player="${escapeHtml(p.shooter_name)}" data-team="${escapeHtml(p.player_team)}" data-opp="${escapeHtml(p.opposing_team)}" data-date="${dateStr(p.game_date)}" data-pred="${p.predicted_sog}" data-home="${escapeHtml(home)}" data-away="${escapeHtml(away)}" data-gd="${dateStr(gd)}" data-player-id="${escapeHtml((p.player_id || '').toString())}">
                        <td><button type="button" class="btn-star ${fav ? 'active' : ''}" aria-label="Favorite" title="Favorite" data-player="${escapeHtml(p.shooter_name)}" data-team="${escapeHtml(p.player_team)}">${fav ? '★' : '☆'}</button> ${escapeHtml(p.shooter_name)}</td>
                        <td>${escapeHtml(p.player_team)}</td>
                        <td class="numeric">${num(p.predicted_sog)}</td>
                        <td class="numeric">${num(p.abs_variance_avg_last7_sog)}</td>
                        <td class="numeric">${num(p.player_avg_sog_last7)}</td>
                        <td class="numeric">${pctHitRate(p.player_2plus_season_hit_rate ?? p.player_2plus_last30_hit_rate)}</td>
                        <td class="numeric">${pctHitRate(p.player_3plus_season_hit_rate ?? p.player_3plus_last30_hit_rate)}</td>
                        <td class="numeric ${oppRank.className}">${oppRank.text}</td>
                        <td><button type="button" class="btn-pick" title="Pick for this game">Pick</button></td>
                      </tr>`;
                  })
                  .join('')}
              </tbody>
            </table>
          ` : '<p class="empty-state" style="padding:1rem;">No predictions for this game.</p>'}
        </div>
      </div>
    `;
  }).join('');

  grid.dataset.expandedKey = expandedKey;

  // Event delegation
  grid.querySelectorAll('.game-card').forEach(card => {
    card.addEventListener('click', e => {
      if (e.target.closest('.player-row')) return;
      if (e.target.closest('.close-btn')) return;
      if (e.target.closest('th.sortable')) return;
      if (e.target.closest('.btn-last-game')) return;
      if (e.target.closest('.last-game-panel')) return;
      const key = card.dataset.key;
      grid.dataset.expandedKey = grid.dataset.expandedKey === key ? '' : key;
      renderGames();
    });
  });

  // Last-game button handlers
  grid.querySelectorAll('.btn-last-game').forEach(btn => {
    btn.addEventListener('click', async e => {
      e.stopPropagation();
      const team = btn.dataset.team;
      const card = btn.closest('.game-card');
      const key = card?.dataset.key;
      const panel = document.getElementById(`last-game-panel-${key}`);
      if (!panel) return;
      // Toggle: if already showing this team's data, close
      if (panel.style.display !== 'none' && panel.dataset.team === team) {
        panel.style.display = 'none';
        panel.dataset.team = '';
        btn.classList.remove('active');
        return;
      }
      // Mark all last-game buttons in this card inactive first
      card.querySelectorAll('.btn-last-game').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      panel.dataset.team = team;
      panel.style.display = '';
      panel.innerHTML = '<div class="last-game-loading">Loading last game…</div>';
      const data = await fetchAPI('/team-last-game', { team });
      const game = data?.game;
      if (!game) {
        panel.innerHTML = '<p class="empty-hint">No recent game found for this team.</p>';
        return;
      }
      const gameDate = game.game_date || '';
      const home = (game.home || '').toUpperCase();
      const away = (game.away || '').toUpperCase();
      const homeGoals = parseInt(game.goals_for) || 0;
      const awayGoals = parseInt(game.goals_against) || 0;
      const isWin = (game.is_win || '').toLowerCase() === 'yes';
      const gameId = game.game_id || '';
      // Now fetch player stats for this last game
      const params2 = gameId ? { game_id: gameId } : { game_date: gameDate, home, away };
      const statsData = await fetchAPI('/historical-game-stats', params2);
      panel.innerHTML = `<div class="last-game-panel-inner">
        <div class="last-game-panel-header">
          <span class="last-game-panel-title">Last Game — ${escapeHtml(team)}</span>
          <button type="button" class="last-game-panel-close" aria-label="Close">×</button>
        </div>
        ${renderGameBreakdownHtml(statsData || {}, home, away, homeGoals, awayGoals, isWin, gameDate)}
      </div>`;
      panel.querySelector('.last-game-panel-close')?.addEventListener('click', e2 => {
        e2.stopPropagation();
        panel.style.display = 'none';
        panel.dataset.team = '';
        card.querySelectorAll('.btn-last-game').forEach(b => b.classList.remove('active'));
      });
    });
  });

  grid.querySelectorAll('.player-row').forEach(row => {
    row.addEventListener('click', e => {
      if (e.target.closest('.btn-star') || e.target.closest('.btn-pick')) return;
      e.stopPropagation();
      navigateToPlayerPage(
        row.dataset.player,
        row.dataset.team,
        row.dataset.opp,
        row.dataset.date
      );
    });
  });
  grid.querySelectorAll('.btn-star').forEach(btn => {
    btn.addEventListener('click', e => { e.stopPropagation(); toggleFavorite(btn.dataset.player, btn.dataset.team); });
  });

  // Sortable headers
  grid.addEventListener('click', (e) => {
    const th = e.target.closest('th.sortable');
    if (!th || !th.dataset.sort) return;
    e.stopPropagation();
    const key = th.dataset.sort;
    const prevKey = grid.dataset.gameSortKey || 'predicted_sog';
    const prevDir = grid.dataset.gameSortDir || 'desc';
    grid.dataset.gameSortKey = key;
    grid.dataset.gameSortDir = prevKey === key && prevDir === 'desc' ? 'asc' : 'desc';
    renderGames();
  });

  // Load game analysis for expanded card
  if (expandedKey) loadGameAnalysis(expandedKey);
}

const gameAnalysisCache = {};
const gameAnalysisCharts = {};

function nhlLogoUrl(abbrev) {
  if (!abbrev) return '';
  const a = String(abbrev).trim().toUpperCase();
  const map = { 'NJ': 'NJD', 'SJ': 'SJS', 'TB': 'TBL', 'LA': 'LAK', 'VGK': 'VGK', 'WPG': 'WPG', 'ARI': 'ARI', 'UTA': 'UTA' };
  const code = map[a] || a;
  return `https://assets.nhle.com/logos/nhl/svg/${code}_light.svg`;
}

async function loadGameAnalysis(gameKey) {
  const section = document.querySelector(`.game-analysis-section[data-game-key="${gameKey}"]`);
  if (!section) return;
  const content = section.querySelector('.game-analysis-content');
  if (!content) return;
  const parts = gameKey.split('|');
  const [gameDate, home, away] = parts.length >= 3 ? parts : ['', '', ''];
  if (!gameDate || !home || !away) {
    content.innerHTML = '<span class="empty-state">Unable to load game analysis.</span>';
    return;
  }
  let res;
  if (gameAnalysisCache[gameKey]) {
    res = gameAnalysisCache[gameKey];
  } else {
    res = await fetchAPI('/game-analysis', { home, away, game_date: gameDate });
    if (res && res.error) {
      content.innerHTML = `<span class="empty-state">${escapeHtml(res.error)}</span>`;
      return;
    }
    gameAnalysisCache[gameKey] = res;
  }
  const h = res?.home || {};
  const a = res?.away || {};
  const fmt = (v, type) => {
    if (v == null) return { text: '—', cls: '' };
    if (type === 'rank' || type === 'rank-inv') {
      const n = parseFloat(v);
      if (isNaN(n)) return { text: '—', cls: '' };
      const pctVal = n > 1 && n <= 100 ? n / 100 : n;
      const r = rankClassOrdinal(pctVal, 32, type === 'rank-inv');
      return { text: r.text, cls: r.className };
    }
    if (type === 'decimal') {
      const n = parseFloat(v);
      return { text: isNaN(n) ? '—' : n.toFixed(1), cls: '' };
    }
    if (type === 'streak') {
      const s = String(v).trim();
      if (!s) return { text: '—', cls: '' };
      const isWin = /^W\d*$/i.test(s);
      return { text: s, cls: isWin ? 'streak-win' : 'streak-loss' };
    }
    return { text: String(v), cls: '' };
  };
  const row = (label, v1, v2, type = 'decimal') => {
    const a2 = fmt(v1, type);
    const b2 = fmt(v2, type);
    return `<div class="game-analysis-row"><span class="label">${escapeHtml(label)}</span><span class="val away ${a2.cls}">${escapeHtml(a2.text)}</span><span class="val home ${b2.cls}">${escapeHtml(b2.text)}</span></div>`;
  };

  // Destroy previous charts for this game
  if (gameAnalysisCharts[gameKey]) {
    gameAnalysisCharts[gameKey].sog?.destroy();
    gameAnalysisCharts[gameKey].goals?.destroy();
    gameAnalysisCharts[gameKey] = {};
  }

  // Use distinct away (orange) vs home (blue) colors — more visually separable than both-purple
  const accent = '#f97316';   // away team: orange
  const accent2 = '#3b82f6';  // home team: blue
  const safeId = gameKey.replace(/[^a-zA-Z0-9-]/g, '_');

  content.innerHTML = `
    <div class="game-analysis-visual">
      <div class="game-analysis-teams">
        <div class="game-analysis-team away">
          <img src="${nhlLogoUrl(away)}" alt="${escapeHtml(away)}" class="team-logo" onerror="this.style.display='none';this.nextElementSibling.style.display='block';">
          <span class="team-abbrev-fallback" style="display:none;">${escapeHtml(away)}</span>
          <span class="team-name">${escapeHtml(away)}</span>
          ${a.streak ? `<span class="team-streak ${(a.streak || '').startsWith('W') ? 'win' : 'loss'}">${escapeHtml(a.streak)}</span>` : ''}
        </div>
        <span class="game-analysis-vs">vs</span>
        <div class="game-analysis-team home">
          <img src="${nhlLogoUrl(home)}" alt="${escapeHtml(home)}" class="team-logo" onerror="this.style.display='none';this.nextElementSibling.style.display='block';">
          <span class="team-abbrev-fallback" style="display:none;">${escapeHtml(home)}</span>
          <span class="team-name">${escapeHtml(home)}</span>
          ${h.streak ? `<span class="team-streak ${(h.streak || '').startsWith('W') ? 'win' : 'loss'}">${escapeHtml(h.streak)}</span>` : ''}
        </div>
      </div>
      <div class="game-analysis-stats-table">
        <div class="game-analysis-header"><span class="label">Metric</span><span class="val away">${escapeHtml(away)}</span><span class="val home">${escapeHtml(home)}</span></div>
        ${row('SOG Rank', a.sog_rank, h.sog_rank, 'rank')}
        ${row('SOG Against Rank', a.sog_against_rank, h.sog_against_rank, 'rank-inv')}
        ${row('Avg SOG (7g)', a.avg_sog, h.avg_sog)}
        ${row('Avg SOGA (7g)', a.avg_sog_against, h.avg_sog_against)}
        ${row('Streak', a.streak, h.streak, 'streak')}
        ${row('Avg Goals For', a.avg_goals_for, h.avg_goals_for)}
        ${row('Avg Goals Against', a.avg_goals_against, h.avg_goals_against)}
      </div>
      <div class="game-analysis-legend">
        <span class="ga-legend-item" title="SOG Rank: rank 1 of 32 = best shooting team (generates the most shots per game).">SOG Rank: 1 of 32 = best offense</span>
        <span class="ga-legend-item" title="SOG Against Rank: rank 1 of 32 = strongest defense (fewest shots allowed); rank 32 of 32 = softest defense (most shots allowed).">SOG Against Rank: 1 of 32 = strongest defense</span>
      </div>
      <div class="game-analysis-charts">
        <div class="game-analysis-chart-wrap"><canvas id="ga-sog-${safeId}" height="140"></canvas></div>
        <div class="game-analysis-chart-wrap"><canvas id="ga-goals-${safeId}" height="120"></canvas></div>
      </div>
    </div>
  `;

  gameAnalysisCharts[gameKey] = {};
  const canvasSog = document.getElementById(`ga-sog-${safeId}`);
  const canvasGoals = document.getElementById(`ga-goals-${safeId}`);

  if (canvasSog && window.Chart) {
    const awaySog = parseFloat(a.avg_sog) || 0;
    const homeSog = parseFloat(h.avg_sog) || 0;
    const awaySoga = parseFloat(a.avg_sog_against) || 0;
    const homeSoga = parseFloat(h.avg_sog_against) || 0;
    const maxVal = Math.max(awaySog, homeSog, awaySoga, homeSoga, 1);
    gameAnalysisCharts[gameKey].sog = new Chart(canvasSog, {
      type: 'bar',
      data: {
        labels: ['Avg SOG', 'Avg SOGA'],
        datasets: [
          { label: away, data: [awaySog, awaySoga], backgroundColor: accent, borderRadius: 4 },
          { label: home, data: [homeSog, homeSoga], backgroundColor: accent2, borderRadius: 4 },
        ],
      },
      options: {
        indexAxis: 'y',
        responsive: true,
        maintainAspectRatio: false,
        plugins: { legend: { position: 'top' }, title: { display: true, text: 'Shots on Goal (7g avg)' } },
        scales: { x: { beginAtZero: true, max: Math.ceil(maxVal * 1.1) } },
      },
    });
  }
  if (canvasGoals && window.Chart) {
    const awayGf = parseFloat(a.avg_goals_for) || 0;
    const homeGf = parseFloat(h.avg_goals_for) || 0;
    const total = awayGf + homeGf;
    const awayPct = total > 0 ? Math.round((awayGf / total) * 100) : 50;
    gameAnalysisCharts[gameKey].goals = new Chart(canvasGoals, {
      type: 'doughnut',
      data: {
        labels: [away, home],
        datasets: [{ data: [awayGf, homeGf], backgroundColor: [accent, accent2], borderWidth: 2 }],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: { position: 'bottom' },
          title: { display: true, text: 'Avg goals for (7g)' },
          tooltip: { callbacks: { label: (ctx) => `${ctx.label}: ${ctx.raw} avg · ${total > 0 ? Math.round((ctx.raw / total) * 100) : 0}%` } },
        },
      },
    });
  }
}

function getPredictionsSortValue(p, key) {
  const fieldMap = {
    date: 'game_date',
    player: 'shooter_name',
    team: 'player_team',
    opp: 'opposing_team',
    pred_sog: 'predicted_sog',
    variance: 'abs_variance_avg_last7_sog',
    avg7: 'player_avg_sog_last7',
    hit2: 'player_2plus_season_hit_rate',
    hit3: 'player_3plus_season_hit_rate',
    opp_rank: 'opp_sog_against_rank',
  };
  const field = fieldMap[key] || key;
  let val = p[field];
  if (field === 'player_2plus_season_hit_rate') val = val ?? p.player_2plus_last30_hit_rate;
  if (field === 'player_3plus_season_hit_rate') val = val ?? p.player_3plus_last30_hit_rate;
  if (val == null) return -Infinity;
  if (typeof val === 'string' && !isNaN(parseFloat(val))) return parseFloat(val);
  return val;
}

function sortPredictions(data) {
  const sorted = [...data];
  const numericKeys = new Set(['pred_sog', 'variance', 'avg7', 'hit2', 'hit3', 'opp_rank']);
  const isNumeric = numericKeys.has(predictionsSortBy);
  sorted.sort((a, b) => {
    let va = getPredictionsSortValue(a, predictionsSortBy);
    let vb = getPredictionsSortValue(b, predictionsSortBy);
    let cmp;
    if (isNumeric) {
      cmp = (parseFloat(va) || 0) - (parseFloat(vb) || 0);
    } else {
      cmp = String(va).localeCompare(String(vb));
    }
    return predictionsSortDir === 'desc' ? -cmp : cmp;
  });
  return sorted;
}

function updatePredictionsSortIndicators() {
  document.querySelectorAll('#predictions-table th.sortable').forEach(th => {
    const ind = th.querySelector('.sort-indicator');
    if (!ind) return;
    if (predictionsSortBy !== th.dataset.sort) {
      ind.textContent = '';
      return;
    }
    ind.textContent = predictionsSortDir === 'asc' ? ' ▲' : ' ▼';
  });
}

function ensurePredictionsSortHandlers() {
  const table = document.getElementById('predictions-table');
  if (!table || table._sortHandlersAttached) return;
  table._sortHandlersAttached = true;
  table.addEventListener('click', (e) => {
    const th = e.target.closest('th.sortable');
    if (!th || !th.dataset.sort) return;
    const sort = th.dataset.sort;
    if (predictionsSortBy === sort) {
      predictionsSortDir = predictionsSortDir === 'asc' ? 'desc' : 'asc';
    } else {
      predictionsSortBy = sort;
      const numericKeys = new Set(['pred_sog', 'variance', 'avg7', 'hit2', 'hit3', 'opp_rank']);
      predictionsSortDir = numericKeys.has(sort) ? 'desc' : 'asc';
    }
    renderPredictionsTable();
  });
}

function renderPredictionsTable() {
  const tbody = document.getElementById('predictions-body');
  if (!tbody) return;

  if (!allPredictions.length) {
    tbody.innerHTML = '<tr><td colspan="11" class="empty-state">No predictions. Adjust filters or wait for data.</td></tr>';
    return;
  }

  const sorted = sortPredictions(allPredictions);

  tbody.innerHTML = sorted.map(p => {
    const date = dateStr(p.game_date);
    const oppRank = rankClassOrdinal(p.opp_sog_against_rank, 32, true);
    const fav = isFavorite(p.shooter_name, p.player_team);
    return `
    <tr class="player-row" data-player="${escapeHtml(p.shooter_name)}" data-team="${escapeHtml(p.player_team)}" data-opp="${escapeHtml(p.opposing_team)}" data-date="${date}">
      <td>${formatDate(p.game_date)}</td>
      <td><button type="button" class="btn-star ${fav ? 'active' : ''}" aria-label="Favorite" title="Favorite" data-player="${escapeHtml(p.shooter_name)}" data-team="${escapeHtml(p.player_team)}">${fav ? '★' : '☆'}</button> ${escapeHtml(p.shooter_name)}</td>
      <td>${escapeHtml(p.player_team)}</td>
      <td>${escapeHtml(p.opposing_team)}</td>
      <td class="numeric">${num(p.predicted_sog)}</td>
      <td class="numeric">${num(p.abs_variance_avg_last7_sog)}</td>
      <td class="numeric">${num(p.player_avg_sog_last7)}</td>
      <td class="numeric">${pctHitRate(p.player_2plus_season_hit_rate ?? p.player_2plus_last30_hit_rate)}</td>
      <td class="numeric">${pctHitRate(p.player_3plus_season_hit_rate ?? p.player_3plus_last30_hit_rate)}</td>
      <td class="numeric ${oppRank.className}">${oppRank.text}</td>
      <td>${escapeHtml((p.explanation || '').slice(0, 60))}${(p.explanation && p.explanation.length > 60) ? '…' : ''}</td>
    </tr>
  `;
  }).join('');

  updatePredictionsSortIndicators();
  ensurePredictionsSortHandlers();

  tbody.querySelectorAll('.player-row').forEach(row => {
    row.addEventListener('click', e => {
      if (e.target.closest('.btn-star')) return;
      navigateToPlayerPage(row.dataset.player, row.dataset.team, row.dataset.opp, row.dataset.date);
    });
  });
  tbody.querySelectorAll('.btn-star').forEach(btn => {
    btn.addEventListener('click', e => { e.stopPropagation(); toggleFavorite(btn.dataset.player, btn.dataset.team); });
  });
}

// ----- Player page (full view) & routing -----
let playerPageCharts = { sogTrend: null, hitRates: null };

function getPlayerPageParams() {
  const hash = (window.location.hash || '').replace(/^#/, '');
  if (!hash.startsWith('player')) return null;
  const q = hash.indexOf('?');
  if (q < 0) return null;
  const params = new URLSearchParams(hash.slice(q));
  const player = params.get('player');
  if (!player) return null;
  return {
    player,
    player_team: params.get('player_team') || params.get('team') || '',
    opposing_team: params.get('opposing_team') || params.get('opp') || '',
    game_date: params.get('game_date') || params.get('date') || '',
  };
}

function navigateToPlayerPage(player, team, opp, date) {
  const p = new URLSearchParams({ player });
  if (team) p.set('player_team', team);
  if (opp) p.set('opposing_team', opp);
  if (date) p.set('game_date', date);
  window.location.hash = 'player?' + p.toString();
}

function hidePlayerPage() {
  const playerPage = document.getElementById('player-page');
  const mainApp = document.getElementById('main-app');
  if (playerPage) playerPage.style.display = 'none';
  if (mainApp) mainApp.style.display = '';
  if (playerPageCharts.sogTrend) {
    playerPageCharts.sogTrend.destroy();
    playerPageCharts.sogTrend = null;
  }
  if (playerPageCharts.hitRates) {
    playerPageCharts.hitRates.destroy();
    playerPageCharts.hitRates = null;
  }
}

function showPlayerPage() {
  const playerPage = document.getElementById('player-page');
  const mainApp = document.getElementById('main-app');
  if (playerPage) playerPage.style.display = 'block';
  if (mainApp) mainApp.style.display = 'none';
}

function renderHitRatesChart(canvasId, points) {
  const wrapper = document.getElementById(canvasId)?.closest('.player-chart-wrapper');
  const canvas = document.getElementById(canvasId);
  if (!canvas || !window.Chart) return;
  if (playerPageCharts.hitRates) {
    playerPageCharts.hitRates.destroy();
    playerPageCharts.hitRates = null;
  }
  const pts = points || [];
  if (pts.length === 0) {
    if (wrapper) {
      canvas.style.display = 'none';
      let msg = wrapper.querySelector('.chart-no-data');
      if (!msg) {
        msg = document.createElement('div');
        msg.className = 'chart-no-data';
        msg.textContent = 'No game history yet';
        wrapper.appendChild(msg);
      }
      msg.style.display = '';
    }
    return;
  }
  if (wrapper) {
    const msg = wrapper.querySelector('.chart-no-data');
    if (msg) msg.style.display = 'none';
    canvas.style.display = '';
  }
  const labels = pts.map((x) => {
    const d = x.game_date;
    if (!d) return '';
    const m = d.slice(5, 7);
    const day = d.slice(8, 10);
    return `${parseInt(m, 10)}/${parseInt(day, 10)}`;
  });
  const data2plus = pts.map((x) => (x.hit_rate_2plus != null ? (parseFloat(x.hit_rate_2plus) * 100).toFixed(1) : null));
  const data3plus = pts.map((x) => (x.hit_rate_3plus != null ? (parseFloat(x.hit_rate_3plus) * 100).toFixed(1) : null));
  const chartFont = { family: "'Inter', -apple-system, sans-serif", size: 10 };
  const accent = getComputedStyle(document.documentElement).getPropertyValue('--accent').trim() || '#6366f1';
  const accent2 = getComputedStyle(document.documentElement).getPropertyValue('--accent-secondary').trim() || '#8b5cf6';
  playerPageCharts.hitRates = new Chart(canvas, {
    type: 'line',
    data: {
      labels,
      datasets: [
        {
          label: '2+ SOG %',
          data: data2plus,
          borderColor: accent,
          backgroundColor: accent + '20',
          fill: true,
          tension: 0.3,
          pointRadius: pts.length > 20 ? 0 : 2,
          pointHoverRadius: 4,
        },
        {
          label: '3+ SOG %',
          data: data3plus,
          borderColor: accent2,
          backgroundColor: accent2 + '20',
          fill: true,
          tension: 0.3,
          pointRadius: pts.length > 20 ? 0 : 2,
          pointHoverRadius: 4,
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: { intersect: false, mode: 'index' },
      plugins: {
        legend: { display: true, position: 'top', labels: { font: chartFont, usePointStyle: true } },
      },
      scales: {
        x: {
          ticks: { font: chartFont, maxTicksLimit: 10, maxRotation: 45 },
          grid: { display: false },
        },
        y: {
          beginAtZero: true,
          max: 100,
          ticks: { font: chartFont, callback: (v) => v + '%' },
          border: { display: false },
          grid: { color: 'rgba(0,0,0,0.06)' },
        },
      },
    },
  });
}

function renderSogTrendChart(canvasId, chartPoints) {
  const canvas = document.getElementById(canvasId);
  if (!canvas || !window.Chart) return;
  if (playerPageCharts.sogTrend) {
    playerPageCharts.sogTrend.destroy();
    playerPageCharts.sogTrend = null;
  }
  const labels = (chartPoints || []).map((x) => x.label);
  const values = (chartPoints || []).map((x) => parseFloat(x.value) || 0);
  const isPredicted = (chartPoints || []).map((x) => x.label === 'Predicted');
  const chartFont = { family: "'Inter', -apple-system, sans-serif", size: 11 };
  playerPageCharts.sogTrend = new Chart(canvas, {
    type: 'bar',
    data: {
      labels,
      datasets: [{
        label: 'SOG',
        data: values,
        backgroundColor: values.map((_, i) => (isPredicted[i] ? 'rgba(99, 102, 241, 0.85)' : 'rgba(99, 102, 241, 0.5)')),
        borderRadius: 4,
        borderSkipped: false,
      }],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: { legend: { display: false } },
      scales: {
        x: {
          ticks: { font: chartFont },
          grid: { display: false },
        },
        y: {
          beginAtZero: true,
          ticks: { font: chartFont },
          border: { display: false },
          grid: { color: 'rgba(0,0,0,0.06)' },
        },
      },
    },
  });
}

async function loadAndRenderPlayerPage(params) {
  const playerPage = document.getElementById('player-page');
  if (!playerPage) return;
  showPlayerPage();
  playerPage.querySelector('#player-page-name').textContent = 'Loading…';
  playerPage.querySelector('#player-page-matchup').textContent = '';

  const apiParams = { player: params.player };
  if (params.player_team) apiParams.player_team = params.player_team;
  if (params.opposing_team) apiParams.opposing_team = params.opposing_team;
  if (params.game_date) apiParams.game_date = params.game_date;

  const [detailRes, chartRes, hitRatesRes] = await Promise.all([
    fetchAPI('/player-detail', apiParams),
    fetchAPI('/player-sog-chart', { player: params.player, ...(params.game_date ? { game_date: params.game_date } : {}) }),
    fetchAPI('/player-hit-rates-history', { player: params.player, player_team: params.player_team }),
  ]);
  const data = detailRes;
  if (!data?.player) {
    playerPage.querySelector('#player-page-name').textContent = 'Player not found';
    return;
  }
  const p = data.player;
  const chartPoints = chartRes?.points || [];
  const hitRatesPoints = hitRatesRes?.points || [];

  document.getElementById('player-page-name').textContent = p.shooter_name;
  document.getElementById('player-page-matchup').textContent = `${p.player_team} vs ${p.opposing_team}`;
  document.getElementById('player-page-team-label').textContent = `Team Rankings (${p.player_team})`;

  const fav = isFavorite(p.shooter_name, p.player_team);
  const favBtn = document.getElementById('player-page-fav-btn');
  if (favBtn) {
    favBtn.textContent = fav ? '★ Favorited' : '☆ Favorite';
    favBtn.classList.toggle('active', fav);
    favBtn.onclick = async () => {
      await toggleFavorite(p.shooter_name, p.player_team);
      favBtn.textContent = isFavorite(p.shooter_name, p.player_team) ? '★ Favorited' : '☆ Favorite';
      favBtn.classList.toggle('active', isFavorite(p.shooter_name, p.player_team));
    };
  }
  const pickBtn = document.getElementById('player-page-pick-btn');
  const dateVal = params.game_date || dateStr(p.game_date);
  if (pickBtn && dateVal) {
    const game = allGames.find((g) =>
      (g.home === p.player_team && g.away === p.opposing_team) || (g.home === p.opposing_team && g.away === p.player_team)
    );
    const gd = game ? dateStr(game.game_date) : dateStr(dateVal);
    const [home, away] = game ? [game.home, game.away] : [p.opposing_team, p.player_team];
    pickBtn.style.display = '';
    pickBtn.onclick = () => openPickModal(gd, home, away, p.shooter_name, p.player_team, p.opposing_team, p.predicted_sog, p.player_id);
  }

  document.getElementById('player-page-metrics').innerHTML = `
    <div class="key-metric"><div class="value">${num(p.predicted_sog)}</div><div class="label">Predicted SOG</div></div>
    <div class="key-metric"><div class="value">${num(p.player_avg_sog_last7)}</div><div class="label">Avg SOG (7g)</div></div>
    <div class="key-metric"><div class="value">${num(p.previous_player_total_shotsongoal, 0)}</div><div class="label">Last Game</div></div>
    <div class="key-metric"><div class="value">${num(p.abs_variance_avg_last7_sog)}</div><div class="label">Variance</div></div>
  `;

  renderSogTrendChart('player-sog-trend-chart', chartPoints);
  renderHitRatesChart('player-hit-rates-chart', hitRatesPoints);

  const hitRateTip = 'Not enough games played yet to calculate.';
  document.getElementById('player-page-hit-rates').innerHTML = `
    <div class="stat-row" title="${escapeHtml(hitRateTip)}"><span>2+ SOG % (Season)</span><span class="key-metric-value">${pctHitRate(p.player_2plus_season_hit_rate)}</span></div>
    <div class="stat-row" title="${escapeHtml(hitRateTip)}"><span>3+ SOG % (Season)</span><span class="key-metric-value">${pctHitRate(p.player_3plus_season_hit_rate)}</span></div>
    <div class="stat-row" title="${escapeHtml(hitRateTip)}"><span>2+ SOG % (vs Opp)</span><span class="key-metric-value">${pctHitRate(p.player_2plus_season_matchup_hit_rate)}</span></div>
    <div class="stat-row" title="${escapeHtml(hitRateTip)}"><span>3+ SOG % (vs Opp)</span><span class="key-metric-value">${pctHitRate(p.player_3plus_season_matchup_hit_rate)}</span></div>
    <div class="stat-row" title="${escapeHtml(hitRateTip)}"><span>2+ SOG % (Last 30d)</span><span class="key-metric-value">${pctHitRate(p.player_2plus_last30_hit_rate)}</span></div>
    <div class="stat-row" title="${escapeHtml(hitRateTip)}"><span>3+ SOG % (Last 30d)</span><span class="key-metric-value">${pctHitRate(p.player_3plus_last30_hit_rate)}</span></div>
  `;

  document.getElementById('player-page-stats').innerHTML = `
    ${statRowWithRank('Player SOG rank', p.player_sog_rank, true, 'Rank among teammates by SOG (1 = top shooter on team).', p.team_skater_count)}
    ${(() => { const r = ppSOGPctClass(p.player_last7_pp_sog_pct); return `<div class="stat-row" title="% of shots taken on the powerplay over last 7 games"><span>PP SOG % (7g)</span><span class="${r.className}">${r.text}</span></div>`; })()}
    ${statRowIceTimeRank('Ice time rank', p.player_ice_time_rank, 'Ice time rank among teammates in last game (1 = most ice time)')}
    ${statRowIceTimeRank('PP ice time rank', p.player_pp_ice_time_rank, 'Powerplay ice time rank among teammates in last game (1 = most PP ice time)')}
    ${statRowMatchup('Matchup last SOG', p.matchup_last_sog)}
    ${statRowMatchup('Matchup avg SOG (7g)', p.matchup_avg_sog_last7)}
  `;

  document.getElementById('player-page-team').innerHTML = `
    ${statRowWithRank('SOG rank', p.team_sog_for_rank, true, '', 32)}
    ${statRowWithRank('Goals rank', p.team_goals_for_rank, true, '', 32)}
    ${statRowWithRank('PP SOG rank', p.team_pp_sog_rank, true, '', 32)}
  `;

  document.getElementById('player-page-opponent').innerHTML = `
    ${statRowWithRank('Goals against rank', p.opp_goals_against_rank, true, '', 32, true)}
    ${statRowWithRank('SOG against rank', p.opp_sog_against_rank, true, '', 32, true)}
    ${statRowWithRank('PK SOG rank', p.opp_pk_sog_rank, true, '', 32, true)}
    ${statRowWithRank('Penalties rank', p.opp_penalties_rank, true, '', 32, false)}
  `;

  const explEl = document.getElementById('player-page-explanation');
  const genBtn = document.getElementById('player-page-btn-ai-generate');
  const loadEl = document.getElementById('player-page-ai-loading');
  if (explEl) {
    explEl.innerHTML = p.explanation
      ? formatAIAnalysisMarkdown(p.explanation)
      : '<p class="empty-state" style="font-size:0.9rem;">No pre-computed analysis. Click "Generate AI analysis" below.</p>';
  }
  if (genBtn) {
    genBtn.style.display = dateVal ? '' : 'none';
    genBtn.textContent = 'Generate AI analysis';
    genBtn.onclick = async () => {
      genBtn.style.display = 'none';
      if (loadEl) loadEl.style.display = '';
      const apiParams2 = { player: p.shooter_name };
      if (p.player_team) apiParams2.player_team = p.player_team;
      if (p.opposing_team) apiParams2.opposing_team = p.opposing_team;
      if (dateVal) apiParams2.game_date = dateVal;
      const res = await fetchAPI('/player-ai-analysis', apiParams2);
      if (loadEl) loadEl.style.display = 'none';
      genBtn.style.display = '';
      if (res?.analysis) {
        explEl.innerHTML = formatAIAnalysisMarkdown(res.analysis);
        genBtn.textContent = 'Regenerate analysis';
      } else {
        showToast(res?.error || 'AI analysis failed', 'error');
      }
    };
  }
}

function handleRoute() {
  const params = getPlayerPageParams();
  if (params) {
    loadAndRenderPlayerPage(params);
  } else {
    hidePlayerPage();
  }
}

// ----- Player detail (legacy overlay - now redirects to full page) -----
function statRowWithRank(label, value, isRank = true, title = '', total = null, inverted = false) {
  let r;
  if (isRank) {
    r = total != null ? rankClassOrdinal(value, total, inverted) : rankClass(value);
  } else {
    r = { text: num(value), className: '' };
  }
  const titleAttr = title ? ` title="${escapeHtml(title)}"` : '';
  return `<div class="stat-row"${titleAttr}><span>${label}</span><span class="${r.className}">${r.text}</span></div>`;
}

/** Stat row for ice time rank (int, lower=better) or raw number. */
function statRowIceTimeRank(label, value, title = '') {
  const r = iceTimeRankClass(value);
  const titleAttr = title ? ` title="${escapeHtml(title)}"` : '';
  return `<div class="stat-row"${titleAttr}><span>${label}</span><span class="${r.className}">${r.text}</span></div>`;
}

/** Stat row for matchup stats: "No prior games" when null (first matchup or no games vs opponent). */
function statRowMatchup(label, value, title = "Player hasn't faced this opponent this season.") {
  const text = (value == null || (typeof value === 'number' && isNaN(value)))
    ? '<span class="stat-placeholder">No prior games</span>' : num(value, 2);
  return `<div class="stat-row" title="${escapeHtml(title)}"><span>${label}</span><span>${text}</span></div>`;
}

async function openPlayerDetail(player, team, opp, date) {
  const params = { player };
  if (team) params.player_team = team;
  if (opp) params.opposing_team = opp;
  if (date) params.game_date = date;

  const [detailRes, chartRes] = await Promise.all([
    fetchAPI('/player-detail', params),
    fetchAPI('/player-sog-chart', { player }),
  ]);
  const data = detailRes;
  if (!data?.player) return;
  const p = data.player;

  document.getElementById('player-detail-name').textContent = `${p.shooter_name} — ${p.player_team} vs ${p.opposing_team}`;
  document.getElementById('player-detail-team-label').textContent = `Team Rankings (${p.player_team})`;

  const fav = isFavorite(p.shooter_name, p.player_team);
  const favBtn = document.getElementById('player-detail-fav-btn');
  if (favBtn) {
    favBtn.textContent = fav ? '★ Favorited' : '☆ Favorite';
    favBtn.classList.toggle('active', fav);
    favBtn.onclick = async () => {
      await toggleFavorite(p.shooter_name, p.player_team);
      favBtn.textContent = isFavorite(p.shooter_name, p.player_team) ? '★ Favorited' : '☆ Favorite';
      favBtn.classList.toggle('active', isFavorite(p.shooter_name, p.player_team));
    };
  }
  const pickBtn = document.getElementById('player-detail-pick-btn');
  if (pickBtn && date) {
    // Prefer schedule game date over prediction date (pipeline dates can differ from NHL schedule)
    const game = allGames.find(g =>
      (g.home === p.player_team && g.away === p.opposing_team) || (g.home === p.opposing_team && g.away === p.player_team)
    );
    const gd = game ? dateStr(game.game_date) : (dateStr(date) || dateStr(p.game_date));
    const [home, away] = game ? [game.home, game.away] : [p.opposing_team, p.player_team]; // fallback: opp @ player
    pickBtn.style.display = '';
    pickBtn.onclick = () => openPickModal(gd, home, away, p.shooter_name, p.player_team, p.opposing_team, p.predicted_sog, p.player_id);
  }

  // Key metrics: Predicted SOG, Avg SOG, Last Game, Games Played (or variance if unavailable)
  document.getElementById('player-key-metrics').innerHTML = `
    <div class="key-metric">
      <div class="value">${num(p.predicted_sog)}</div>
      <div class="label">Predicted SOG Next</div>
    </div>
    <div class="key-metric">
      <div class="value">${num(p.player_avg_sog_last7)}</div>
      <div class="label">Avg SOG per Game</div>
    </div>
    <div class="key-metric">
      <div class="value">${num(p.previous_player_total_shotsongoal, 0)}</div>
      <div class="label">SOG Last Game</div>
    </div>
    <div class="key-metric">
      <div class="value">${num(p.abs_variance_avg_last7_sog)}</div>
      <div class="label">Pred Variance</div>
    </div>
  `;

  // SOG chart: Avg 7, Avg 3, Last Game, Predicted
  const chartPoints = chartRes?.points || [];
  const maxVal = Math.max(...chartPoints.map(x => parseFloat(x.value) || 0), 1);
  document.getElementById('player-sog-chart').innerHTML = chartPoints.length
    ? chartPoints.map((pt) => {
        const val = parseFloat(pt.value) || 0;
        const hPct = maxVal > 0 ? (val / maxVal) * 100 : 0;
        const isPred = pt.label === 'Predicted';
        return `
        <div class="sog-chart-bar">
          <div class="bar-wrap">
            <div class="bar ${isPred ? 'predicted' : ''}" style="height: ${hPct}%"></div>
          </div>
          <div class="value">${num(pt.value)}</div>
          <div class="label">${escapeHtml(pt.label)}</div>
        </div>`;
      }).join('')
    : '<p class="empty-state" style="padding:0.5rem;font-size:0.8rem;">No chart data</p>';

  // 2+ / 3+ SOG % (Season, Season vs Matchup, Last 30 days) - from clean_prediction_summary
  const hitRateTip = 'Not enough games played yet to calculate. Common for rookies or players with limited ice time.';
  const sogRatesEl = document.getElementById('player-detail-sog-rates');
  if (sogRatesEl) {
    sogRatesEl.innerHTML = `
      <div class="stat-row" title="${escapeHtml(hitRateTip)}"><span>2+ SOG % (Season)</span><span class="key-metric-value">${pctHitRate(p.player_2plus_season_hit_rate)}</span></div>
      <div class="stat-row" title="${escapeHtml(hitRateTip)}"><span>3+ SOG % (Season)</span><span class="key-metric-value">${pctHitRate(p.player_3plus_season_hit_rate)}</span></div>
      <div class="stat-row" title="${escapeHtml(hitRateTip)}"><span>2+ SOG % (vs Opp)</span><span class="key-metric-value">${pctHitRate(p.player_2plus_season_matchup_hit_rate)}</span></div>
      <div class="stat-row" title="${escapeHtml(hitRateTip)}"><span>3+ SOG % (vs Opp)</span><span class="key-metric-value">${pctHitRate(p.player_3plus_season_matchup_hit_rate)}</span></div>
      <div class="stat-row" title="${escapeHtml(hitRateTip)}"><span>2+ SOG % (Last 30d)</span><span class="key-metric-value">${pctHitRate(p.player_2plus_last30_hit_rate)}</span></div>
      <div class="stat-row" title="${escapeHtml(hitRateTip)}"><span>3+ SOG % (Last 30d)</span><span class="key-metric-value">${pctHitRate(p.player_3plus_last30_hit_rate)}</span></div>
    `;
  }

  // Player stats (formatted like team/opponent rankings: stat-row with rank-style highlighting)
  document.getElementById('player-detail-stats').innerHTML = `
    ${statRowWithRank('Player SOG rank', p.player_sog_rank, true, 'Rank among teammates by SOG (1 = top shooter on team).', p.team_skater_count)}
    ${(() => { const r = ppSOGPctClass(p.player_last7_pp_sog_pct); return `<div class="stat-row" title="% of shots taken on the powerplay over last 7 games"><span>PP SOG % (7g)</span><span class="${r.className}">${r.text}</span></div>`; })()}
    ${statRowIceTimeRank('Ice time rank', p.player_ice_time_rank, 'Ice time rank among teammates in last game (1 = most ice time on team)')}
    ${statRowIceTimeRank('PP ice time rank', p.player_pp_ice_time_rank, 'Powerplay ice time rank among teammates in last game (1 = most PP ice time on team)')}
    ${statRowWithRank('Avg SOG (3g)', p.player_avg_sog_last3, false)}
    ${statRowMatchup('Matchup last SOG', p.matchup_last_sog)}
    ${statRowMatchup('Matchup avg SOG (7g)', p.matchup_avg_sog_last7)}
  `;

  // Team rankings with highlight
  document.getElementById('player-detail-team').innerHTML = `
    ${statRowWithRank('SOG rank', p.team_sog_for_rank, true, '', 32)}
    ${statRowWithRank('Goals rank', p.team_goals_for_rank, true, '', 32)}
    ${statRowWithRank('PP SOG rank', p.team_pp_sog_rank, true, 'Powerplay Shots on Goal rank (1 = best PP shooting team)', 32)}
  `;

  // Opponent rankings: SOG/Goals/PK against = lower rank = opponent allows more = better for player; Penalties = higher = more PP opportunities
  document.getElementById('player-detail-opponent').innerHTML = `
    ${statRowWithRank('Goals against rank', p.opp_goals_against_rank, true, '', 32, true)}
    ${statRowWithRank('SOG against rank', p.opp_sog_against_rank, true, '', 32, true)}
    ${statRowWithRank('PK SOG rank', p.opp_pk_sog_rank, true, '', 32, true)}
    ${statRowWithRank('Penalties rank', p.opp_penalties_rank, true, '', 32, false)}
  `;

  // AI Analysis: pre-computed explanation + on-demand generate
  const explEl = document.getElementById('player-detail-explanation');
  const genBtn = document.getElementById('btn-ai-generate');
  const loadEl = document.getElementById('player-ai-loading');
  const dateVal = date || dateStr(p.game_date);
  if (explEl) {
    explEl.innerHTML = p.explanation
      ? formatAIAnalysisMarkdown(p.explanation)
      : '<p class="empty-state" style="font-size:0.85rem;">No pre-computed analysis. Click "Generate AI analysis" below.</p>';
  }
  if (genBtn) {
    genBtn.style.display = dateVal ? '' : 'none';
    genBtn.textContent = 'Generate AI analysis';
    genBtn.onclick = async () => {
      genBtn.style.display = 'none';
      if (loadEl) loadEl.style.display = '';
      const params = { player: p.shooter_name };
      if (p.player_team) params.player_team = p.player_team;
      if (p.opposing_team) params.opposing_team = p.opposing_team;
      if (dateVal) params.game_date = dateVal;
      const res = await fetchAPI('/player-ai-analysis', params);
      if (loadEl) loadEl.style.display = 'none';
      genBtn.style.display = '';
      if (res?.analysis) {
        explEl.innerHTML = formatAIAnalysisMarkdown(res.analysis);
        genBtn.textContent = 'Regenerate analysis';
      } else {
        showToast(res?.error || 'AI analysis failed', 'error');
      }
    };
  }

  document.getElementById('player-detail-overlay').classList.add('visible');
}

function closePlayerDetail() {
  document.getElementById('player-detail-overlay').classList.remove('visible');
}

// ----- My Picks -----
let allPicks = [];

async function loadPicks() {
  const data = await fetchAPI('/picks');
  allPicks = data?.picks || [];
  renderPicks();
}

function renderPicksAccuracyChart() {
  const chartEl = document.getElementById('picks-accuracy-chart');
  const summaryEl = document.getElementById('picks-accuracy-summary');
  if (!chartEl || !summaryEl) return;

  const withResult = allPicks.filter(p => p.hit !== null && p.hit !== undefined);
  const sogPicks = allPicks.filter(p => ['sog', 'sog_2', 'sog_3', 'sog_4'].includes(p.pick_type || 'sog') && p.actual_sog != null);
  const hits = withResult.filter(p => p.hit === true);

  if (!allPicks.length) {
    chartEl.innerHTML = '';
    summaryEl.innerHTML = '<p class="picks-accuracy-empty">No picks yet. Pick players from upcoming games.</p>';
    return;
  }

  summaryEl.innerHTML = withResult.length
    ? `<div class="picks-accuracy-stats">
         <span class="stat-highlight">${hits.length}/${withResult.length}</span> picks hit
         ${sogPicks.length ? ` · <span class="stat-highlight">${sogPicks.filter(p => Math.abs((parseFloat(p.predicted_sog) || 0) - (parseFloat(p.actual_sog) || 0)) <= 1).length}/${sogPicks.length}</span> SOG picks within 1 of prediction` : ''}
       </div>`
    : '<p class="picks-accuracy-empty">No completed picks yet. Results will appear after games are played.</p>';

  if (!sogPicks.length) {
    chartEl.innerHTML = '<div class="picks-chart-title">Predicted vs Actual SOG</div><p class="picks-accuracy-empty" style="padding:0.5rem;font-size:0.85rem;">SOG-type pick results will show here.</p>';
    return;
  }

  const maxVal = Math.ceil(Math.max(...sogPicks.map(p => Math.max(parseFloat(p.predicted_sog) || 0, parseFloat(p.actual_sog) || 0)), 1));
  const scaleMax = Math.max(maxVal, 6);
  const sortDate = (p) => dateStr(p.display_game_date || p.game_date) || '';
  const sorted = [...sogPicks].sort((a, b) => sortDate(b).localeCompare(sortDate(a)));

  const scaleTicks = [0, Math.ceil(scaleMax / 2), scaleMax];

  chartEl.innerHTML = `
    <div class="picks-chart-title">Predicted vs Actual SOG</div>
    <div class="picks-chart-bars">
      <div class="picks-chart-scale-row">
        <div></div>
        <div class="picks-chart-scale" style="--scale-max: ${scaleMax};">
          ${scaleTicks.map(t => `<span class="scale-tick">${t}</span>`).join('')}
        </div>
        <div></div>
      </div>
      ${sorted.map(p => {
        const pred = parseFloat(p.predicted_sog) || 0;
        const act = parseFloat(p.actual_sog) || 0;
        const predPct = scaleMax > 0 ? (pred / scaleMax) * 100 : 0;
        const actPct = scaleMax > 0 ? (act / scaleMax) * 100 : 0;
        const diff = act - pred;
        const diffClass = Math.abs(diff) <= 1 ? 'diff-close' : (diff > 0 ? 'diff-over' : 'diff-under');
        const diffLabel = diff >= 0 ? `+${num(diff)}` : num(diff);
        const label = `${p.player_name} (${formatPickDateAndTime(p)})`;
        return `
        <div class="picks-chart-row">
          <div class="picks-chart-label" title="${escapeHtml(label)}">
            <span class="label-name">${escapeHtml(p.player_name)}</span>
            <span class="label-date">${formatPickDateAndTime(p)}</span>
          </div>
          <div class="picks-chart-track" style="--scale-max: ${scaleMax};">
            <div class="picks-bar-row" title="Predicted: ${num(pred)}">
              <span class="bar-label">Pred</span>
              <div class="bar-track">
                <div class="picks-bar predicted" style="width: ${predPct}%;"></div>
              </div>
              <span class="bar-val pred-val">${num(pred)}</span>
            </div>
            <div class="picks-bar-row" title="Actual: ${num(act, 0)}">
              <span class="bar-label">Act</span>
              <div class="bar-track">
                <div class="picks-bar actual" style="width: ${actPct}%;"></div>
              </div>
              <span class="bar-val act-val">${num(act, 0)}</span>
            </div>
          </div>
          <div class="picks-chart-diff ${diffClass}" title="Difference: actual − predicted">
            ${diffLabel}
          </div>
        </div>`;
      }).join('')}
    </div>
    <div class="picks-chart-legend">
      <span><i class="legend-dot predicted"></i> Predicted</span>
      <span><i class="legend-dot actual"></i> Actual</span>
      <span class="legend-diff"><i class="legend-dot diff-close"></i> ±1</span>
      <span class="legend-diff"><i class="legend-dot diff-over"></i> Over</span>
      <span class="legend-diff"><i class="legend-dot diff-under"></i> Under</span>
    </div>
  `;
}

function formatPickActual(p) {
  const pt = (p.pick_type || 'sog');
  if (['sog', 'sog_2', 'sog_3', 'sog_4'].includes(pt) && p.actual_sog != null) return num(p.actual_sog, 0);
  if (pt === 'goal' && p.actual_goal != null) return num(p.actual_goal, 0);
  if (pt === 'assist' && p.actual_assist != null) return num(p.actual_assist, 0);
  if (pt === 'point') {
    const g = (p.actual_goal ?? 0) + (p.actual_assist ?? 0);
    if (p.actual_goal != null || p.actual_assist != null) return String(Math.round(g));
  }
  return null;
}

function formatPickResult(p) {
  if (p.hit === true) return '<span class="pick-hit">✓</span>';
  if (p.hit === false) return '<span class="pick-miss">✗</span>';
  const pickDate = p.display_game_date || p.game_date;
  const isPast = dateStr(pickDate) < new Date().toISOString().slice(0, 10);
  return isPast ? '—' : 'TBD';
}

function renderPicks() {
  renderPicksAccuracyChart();

  const tbody = document.getElementById('picks-body');
  if (!tbody) return;

  if (!allPicks.length) {
    tbody.innerHTML = '<tr><td colspan="8" class="empty-state">No picks yet. Click "Pick" next to a player in a game.</td></tr>';
    return;
  }

  tbody.innerHTML = allPicks.map(p => {
    const gameStr = `${p.away_team} @ ${p.home_team}`;
    const pt = (p.pick_type || 'sog');
    const actualVal = formatPickActual(p);
    const predVal = ['sog', 'sog_2', 'sog_3', 'sog_4'].includes(pt) ? num(p.predicted_sog) : '—';
    const dateDisplay = formatPickDateAndTime(p);
    return `
    <tr data-pick-id="${p.id}">
      <td>${dateDisplay}</td>
      <td>${escapeHtml(gameStr)}</td>
      <td>${escapeHtml(p.player_name)} (${escapeHtml(p.player_team)})</td>
      <td>${escapeHtml(PICK_TYPE_LABELS[pt] || pt)}</td>
      <td class="numeric">${predVal}</td>
      <td class="numeric">${actualVal ?? (dateStr(p.display_game_date || p.game_date) < new Date().toISOString().slice(0, 10) ? '—' : 'TBD')}</td>
      <td>${formatPickResult(p)}</td>
      <td><button type="button" class="btn-remove-pick" data-id="${p.id}" title="Remove pick">✕</button></td>
    </tr>
  `;
  }).join('');

  tbody.querySelectorAll('.btn-remove-pick').forEach(btn => {
    btn.addEventListener('click', async () => {
      const id = btn.dataset.id;
      await fetchAPI(`/picks/${id}`, {}, { method: 'DELETE' });
      loadPicks();
    });
  });
}

// ----- Historical -----
let historicalGames = [];  // cache for client-side sort state
let historicalSortBy = 'date';
let historicalSortDir = 'desc';

async function loadHistorical() {
  const dateFrom = document.getElementById('historical-date-from')?.value?.trim() || '';
  const dateTo = document.getElementById('historical-date-to')?.value?.trim() || '';
  const team = document.getElementById('historical-filter-team')?.value || '';

  // No date filter by default - backend returns most recent 100 games (gameId IS NOT NULL)
  const params = { limit: 100 };
  if (dateFrom) params.game_date_from = dateFrom;
  if (dateTo) params.game_date_to = dateTo;
  if (team) params.team = team;
  params.sort_by = historicalSortBy;
  params.sort_dir = historicalSortDir;

  const data = await fetchAPI('/historical-games', params);
  const tbody = document.getElementById('historical-body');
  const hintEl = document.getElementById('historical-data-hint');
  if (!tbody) return;

  if (!data?.games?.length) {
    historicalGames = [];
    tbody.innerHTML = '<tr><td colspan="7" class="empty-state">No historical games found.</td></tr>';
    if (hintEl) { hintEl.style.display = 'none'; hintEl.textContent = ''; }
    return;
  }

  historicalGames = data.games;
  if (hintEl && data.data_through) {
    const throughDate = parseDateOnly(data.data_through);
    const today = new Date();
    today.setHours(0, 0, 0, 0);
    const daysOld = throughDate ? Math.floor((today - throughDate) / (24 * 60 * 60 * 1000)) : 0;
    if (daysOld > 3) {
      hintEl.textContent = `Data through ${formatDate(data.data_through)}. Pipeline or Lakebase sync may need to run for recent games.`;
      hintEl.style.display = 'block';
    } else {
      hintEl.style.display = 'none';
      hintEl.textContent = '';
    }
  }
  renderHistoricalRows(tbody);
  attachHistoricalExpandHandlers(tbody);
  ensureHistoricalSortHandlers();
}

function ensureHistoricalSortHandlers() {
  const table = document.getElementById('historical-table');
  if (!table || table._sortHandlersAttached) return;
  table._sortHandlersAttached = true;
  table.addEventListener('click', (e) => {
    const th = e.target.closest('th.sortable');
    if (!th) return;
    const sort = th.dataset.sort;
    if (!sort) return;
    if (historicalSortBy === sort) {
      historicalSortDir = historicalSortDir === 'asc' ? 'desc' : 'asc';
    } else {
      historicalSortBy = sort;
      historicalSortDir = sort === 'date' ? 'desc' : 'asc';
    }
    loadHistorical();
  });
}

function renderHistoricalRows(tbody) {
  tbody.innerHTML = historicalGames.map(g => `
    <tr class="historical-game-row" data-game-date="${dateStr(g.game_date)}" data-home="${escapeHtml(g.home || '')}" data-away="${escapeHtml(g.away || '')}" data-game-id="${escapeHtml((g.game_id || '').toString())}">
      <td class="expand-cell"><button type="button" class="btn-expand" aria-label="Expand">▶</button></td>
      <td>${formatDate(g.game_date)}</td>
      <td>${escapeHtml(g.home)}</td>
      <td>${escapeHtml(g.away)}</td>
      <td class="numeric">${num(g.goals_for, 0)}</td>
      <td class="numeric">${num(g.goals_against, 0)}</td>
      <td><span class="${(g.is_win === 'Yes' || g.is_win === true) ? 'win' : 'loss'}" title="Home team result">${formatHistoricalResult(g.is_win)}</span></td>
    </tr>
  `).join('');
  updateHistoricalSortIndicators();
}

function attachHistoricalExpandHandlers(tbody) {
  tbody.querySelectorAll('.historical-game-row').forEach(row => {
    const btn = row.querySelector('.btn-expand');
    if (btn) {
      btn.addEventListener('click', (e) => {
        e.stopPropagation();
        toggleHistoricalExpand(row);
      });
    }
    row.addEventListener('click', (e) => {
      if (!e.target.closest('.btn-expand')) toggleHistoricalExpand(row);
    });
  });
}

function updateHistoricalSortIndicators() {
  document.querySelectorAll('#historical-table th.sortable').forEach(th => {
    const ind = th.querySelector('.sort-indicator');
    if (!ind) return;
    const sort = th.dataset.sort;
    if (historicalSortBy !== sort) {
      ind.textContent = '';
      ind.title = 'Click to sort';
      return;
    }
    ind.textContent = historicalSortDir === 'asc' ? ' ▲' : ' ▼';
    ind.title = `Sorted ${historicalSortDir === 'asc' ? 'ascending' : 'descending'}; click to reverse`;
  });
}

async function toggleHistoricalExpand(row) {
  const next = row.nextElementSibling;
  const isExpanded = row.classList.contains('expanded');

  if (isExpanded) {
    if (next?.classList.contains('historical-player-stats-row')) next.remove();
    row.classList.remove('expanded');
    row.querySelector('.btn-expand')?.classList.remove('expanded');
    return;
  }

  const gameId = row.dataset.gameId;
  const gameDate = row.dataset.gameDate;
  const home = row.dataset.home;
  const away = row.dataset.away;

  // Remove any existing expanded row for another game
  const existing = row.parentElement.querySelector('.historical-player-stats-row');
  if (existing) existing.remove();
  row.parentElement.querySelectorAll('.historical-game-row.expanded').forEach(r => {
    r.classList.remove('expanded');
    r.querySelector('.btn-expand')?.classList.remove('expanded');
  });

  row.classList.add('expanded');
  row.querySelector('.btn-expand')?.classList.add('expanded');

  // Insert loading row immediately
  const loadingRow = document.createElement('tr');
  loadingRow.className = 'historical-player-stats-row historical-stats-loading';
  loadingRow.innerHTML = '<td colspan="7" class="player-stats-cell"><div class="player-stats-inner">Loading player stats…</div></td>';
  row.after(loadingRow);

  const useGameId = gameId && gameId !== 'undefined' && gameId !== 'null' && gameId !== 'None';
  const params = useGameId ? { game_id: gameId } : { game_date: gameDate, home, away };
  let data = await fetchAPI('/historical-game-stats', params);
  loadingRow.remove();

  if (!data) {
    data = { players: [], team_totals: [], error: 'Failed to load (network or server error)' };
  }

  // Derive score from the parent row data attributes
  const goalsFor = parseInt(row.cells[4]?.textContent) || 0;
  const goalsAgainst = parseInt(row.cells[5]?.textContent) || 0;
  const isWin = row.cells[6]?.querySelector('.win') != null;

  const expandRow = document.createElement('tr');
  expandRow.className = 'historical-player-stats-row';
  const td = document.createElement('td');
  td.colSpan = 7;
  td.className = 'player-stats-cell';
  td.innerHTML = renderGameBreakdownHtml(data, home, away, goalsFor, goalsAgainst, isWin, gameDate);
  expandRow.appendChild(td);
  row.after(expandRow);
}

/** Render rich game breakdown HTML (scoreboard + SOG bars + player stats table). */
function renderGameBreakdownHtml(data, home, away, homeGoals, awayGoals, homeWon, gameDate) {
  const players = data?.players || [];
  const teamTotals = data?.team_totals || [];
  const err = data?.error;

  // Build team totals lookup: {TEAM -> {sog, goals}}
  const totalsMap = {};
  for (const t of teamTotals) {
    totalsMap[(t.team || '').toUpperCase()] = t;
  }
  // Fallback: aggregate from players when team_totals is missing or all-zero
  const totalSogFromMap = Object.values(totalsMap).reduce((s, t) => s + (t.sog || 0), 0);
  if ((!totalsMap[home] || totalSogFromMap === 0) && players.length) {
    const homePlayers = players.filter(p => (p.team || '').toUpperCase() === home);
    const awayPlayers = players.filter(p => (p.team || '').toUpperCase() === away);
    if (homePlayers.length) totalsMap[home] = { team: home, sog: homePlayers.reduce((s, p) => s + (parseInt(p.sog) || 0), 0), goals: homeGoals };
    if (awayPlayers.length) totalsMap[away] = { team: away, sog: awayPlayers.reduce((s, p) => s + (parseInt(p.sog) || 0), 0), goals: awayGoals };
  }

  const homeSog = totalsMap[home]?.sog ?? null;
  const awaySog = totalsMap[away]?.sog ?? null;
  const maxSog = Math.max(homeSog || 0, awaySog || 0, 1);

  // Players split by team, sorted by SOG desc
  const homePlayers = players.filter(p => (p.team || '').toUpperCase() === home);
  const awayPlayers = players.filter(p => (p.team || '').toUpperCase() === away);
  const allSorted = [...players].sort((a, b) => (parseInt(b.sog) || 0) - (parseInt(a.sog) || 0));
  const useTeamSplit = homePlayers.length > 0 && awayPlayers.length > 0;

  function playerRow(p) {
    const sogVal = parseInt(p.sog) || 0;
    const sogBarPct = homeSog || awaySog ? (sogVal / Math.max(homeSog || 0, awaySog || 0, 1)) * 100 : (sogVal / 8) * 100;
    return `<tr class="hist-player-row">
      <td>${escapeHtml(p.player_name || '—')}</td>
      <td class="hist-sog-bar-cell">
        <div class="hist-sog-bar-track">
          <div class="hist-sog-bar-fill" style="width:${Math.min(sogBarPct, 100)}%" title="${sogVal} SOG"></div>
        </div>
      </td>
      <td class="numeric sog-val">${sogVal}</td>
      <td class="numeric">${parseInt(p.goals) || 0}G</td>
      <td class="numeric">${parseInt(p.assists) || 0}A</td>
    </tr>`;
  }

  const teamSogSection = (homeSog !== null || awaySog !== null) ? `
    <div class="hist-team-sog">
      <div class="hist-sog-label">Shots on Goal</div>
      ${[{ team: away, sog: awaySog, cls: 'away' }, { team: home, sog: homeSog, cls: 'home' }].map(({ team, sog, cls }) => {
        if (sog === null) return '';
        const pct = maxSog > 0 ? (sog / maxSog) * 100 : 0;
        return `<div class="hist-sog-row">
          <div class="hist-sog-team">
            <img src="${nhlLogoUrl(team)}" alt="${escapeHtml(team)}" class="team-sog-logo" onerror="this.style.display='none';">
            <span class="hist-sog-abbrev">${escapeHtml(team)}</span>
          </div>
          <div class="hist-sog-bar-outer">
            <div class="hist-sog-bar-main ${cls}" style="width:${pct}%;"></div>
          </div>
          <span class="hist-sog-val">${sog}</span>
        </div>`;
      }).join('')}
    </div>` : '';

  const playerSection = players.length ? `
    <div class="hist-players">
      ${useTeamSplit ? `
        <div class="hist-team-col">
          <div class="hist-team-header">
            <img src="${nhlLogoUrl(away)}" alt="" class="team-sog-logo" onerror="this.style.display='none';">
            <span>${escapeHtml(away)}</span>
          </div>
          <table class="hist-player-table"><tbody>${awayPlayers.map(playerRow).join('')}</tbody></table>
        </div>
        <div class="hist-team-col">
          <div class="hist-team-header">
            <img src="${nhlLogoUrl(home)}" alt="" class="team-sog-logo" onerror="this.style.display='none';">
            <span>${escapeHtml(home)}</span>
          </div>
          <table class="hist-player-table"><tbody>${homePlayers.map(playerRow).join('')}</tbody></table>
        </div>
      ` : `<div class="hist-team-col-full">
        <table class="hist-player-table">
          <thead><tr><th>Player</th><th class="hist-sog-bar-cell">SOG</th><th class="numeric"></th><th class="numeric">G</th><th class="numeric">A</th></tr></thead>
          <tbody>${allSorted.map(playerRow).join('')}</tbody>
        </table>
      </div>`}
    </div>` : (err ? `<p class="empty-hint" style="padding:0.5rem;">${escapeHtml(err)}</p>` : '<p class="empty-hint" style="padding:0.5rem;">Player stats not yet available.</p>');

  return `<div class="hist-game-breakdown">
    <div class="hist-scoreboard">
      <div class="hist-score-team away">
        <img src="${nhlLogoUrl(away)}" alt="${escapeHtml(away)}" class="hist-score-logo" onerror="this.style.display='none';">
        <span class="hist-score-abbrev">${escapeHtml(away)}</span>
        <span class="hist-score-val ${!homeWon ? 'winner' : ''}">${awayGoals}</span>
      </div>
      <div class="hist-score-divider">
        <span class="hist-score-dash">–</span>
        <span class="hist-score-date">${gameDate ? formatDate(gameDate) : ''}</span>
        <span class="hist-score-final">Final</span>
      </div>
      <div class="hist-score-team home">
        <span class="hist-score-val ${homeWon ? 'winner' : ''}">${homeGoals}</span>
        <span class="hist-score-abbrev">${escapeHtml(home)}</span>
        <img src="${nhlLogoUrl(home)}" alt="${escapeHtml(home)}" class="hist-score-logo" onerror="this.style.display='none';">
      </div>
    </div>
    ${teamSogSection}
    ${playerSection}
  </div>`;
}

// ----- Filters -----
function applyFilters() {
  loadData();
}

// ----- Tabs -----
function showTab(tabId) {
  document.querySelectorAll('.tab-panel').forEach(p => p.classList.remove('active'));
  document.querySelectorAll('.nav-link').forEach(l => l.classList.remove('active'));
  const panel = document.getElementById(`tab-${tabId}`);
  const link = document.querySelector(`.nav-link[data-tab="${tabId}"]`);
  if (panel) panel.classList.add('active');
  if (link) link.classList.add('active');
  // Show filters only on dashboard and predictions
  const filters = document.getElementById('filters');
  if (filters) filters.style.display = (tabId === 'dashboard' || tabId === 'predictions') ? '' : 'none';
  // Reload historical when switching to that tab
  if (tabId === 'historical') loadHistorical();
}

// ----- Ask Genie -----
let genieConversationId = null;

function appendGenieMessage(role, content, sql, columns, data) {
  const el = document.getElementById('genie-messages');
  if (!el) return;
  const msg = document.createElement('div');
  msg.className = `genie-msg genie-msg-${role}`;
  let html = `<div class="genie-msg-content">${escapeHtml(content).replace(/\n/g, '<br>')}</div>`;
  if (role === 'assistant' && sql) {
    html += `<details class="genie-sql-details"><summary>SQL</summary><pre class="genie-sql">${escapeHtml(sql)}</pre></details>`;
  }
  if (role === 'assistant' && columns?.length && data?.length) {
    const rows = data.slice(0, 20).map(r => '<tr>' + r.map(c => `<td>${escapeHtml(String(c ?? ''))}</td>`).join('') + '</tr>').join('');
    html += `<div class="genie-table-wrap"><table class="genie-result-table"><thead><tr>${columns.map(c => `<th>${escapeHtml(c)}</th>`).join('')}</tr></thead><tbody>${rows}</tbody></table></div>`;
  }
  msg.innerHTML = html;
  el.appendChild(msg);
  el.scrollTop = el.scrollHeight;
}

async function sendGenieMessage() {
  const input = document.getElementById('genie-input');
  const sendBtn = document.getElementById('genie-send');
  const errEl = document.getElementById('genie-error');
  if (!input || !sendBtn) return;
  const question = input.value.trim();
  if (!question) return;

  sendBtn.disabled = true;
  if (errEl) { errEl.style.display = 'none'; errEl.textContent = ''; }
  appendGenieMessage('user', question);
  input.value = '';

  const body = { question };
  if (genieConversationId) body.conversation_id = genieConversationId;

  const res = await fetchAPI('/genie-chat', {}, { method: 'POST', body });
  sendBtn.disabled = false;

  if (!res) {
    appendGenieMessage('assistant', 'Sorry, the request failed. Please try again.');
    if (errEl) { errEl.textContent = 'Connection error'; errEl.style.display = 'block'; }
    return;
  }
  if (res.error) {
    appendGenieMessage('assistant', `Error: ${res.error}`);
    if (errEl) { errEl.textContent = res.error; errEl.style.display = 'block'; }
    return;
  }
  genieConversationId = res.conversation_id;
  appendGenieMessage('assistant', res.text || 'No response.', res.sql, res.columns, res.data);
}

// ----- Init -----
document.addEventListener('DOMContentLoaded', async () => {
  // Load filter options in background — don't block games/predictions if it hangs
  loadFilterOptions().catch(() => {});

  document.getElementById('filter-date')?.addEventListener('change', applyFilters);
  document.getElementById('filter-team')?.addEventListener('change', applyFilters);
  document.getElementById('filter-player')?.addEventListener('input', debounce(applyFilters, 300));

  document.getElementById('clear-filters')?.addEventListener('click', () => {
    document.getElementById('filter-date').value = '';
    document.getElementById('filter-team').value = '';
    document.getElementById('filter-player').value = '';
    applyFilters();
  });

  // Today / Tomorrow toggle for upcoming games
  document.getElementById('games-view-toggle')?.addEventListener('click', async e => {
    const btn = e.target.closest('.btn-games-view');
    if (!btn) return;
    const view = btn.dataset.view;
    if (view === gamesView) return;
    gamesView = view;
    document.querySelectorAll('#games-view-toggle .btn-games-view').forEach(b => b.classList.toggle('active', b.dataset.view === view));
    const grid = document.getElementById('games-grid');
    if (grid) grid.innerHTML = '<div class="empty-state">Loading…</div>';
    try {
      const predParams = gamesView === 'today'
        ? { ...getFilterParams(), by_date: 'true', game_date: getLocalDateString() }
        : getFilterParams();
      const [gRes, pRes] = await Promise.all([
        fetchAPI('/upcoming-games', { view: gamesView, client_date: getLocalDateString() }),
        fetchAPI('/upcoming-predictions', predParams),
      ]);
      allGames = gRes?.games || [];
      allPredictions = pRes?.predictions || [];
      // Clear analysis cache so switching views always fetches fresh streak/SOG data
      Object.keys(gameAnalysisCache).forEach(k => delete gameAnalysisCache[k]);
    } catch (e) {
      console.warn('games view reload failed:', e);
      allGames = [];
    }
    renderGames();
  });

  // Upcoming Games section-specific filter/sort controls
  document.getElementById('games-sort-date')?.addEventListener('change', () => renderGames());
  document.getElementById('games-filter-team')?.addEventListener('change', () => renderGames());
  document.getElementById('games-filter-player')?.addEventListener('input', debounce(() => renderGames(), 300));
  document.getElementById('games-clear-filters')?.addEventListener('click', () => {
    const sortEl = document.getElementById('games-sort-date');
    const teamEl = document.getElementById('games-filter-team');
    const playerEl = document.getElementById('games-filter-player');
    if (sortEl) sortEl.value = 'asc';
    if (teamEl) teamEl.value = '';
    if (playerEl) playerEl.value = '';
    renderGames();
  });

  document.getElementById('close-player-detail')?.addEventListener('click', closePlayerDetail);
  document.getElementById('player-detail-overlay')?.addEventListener('click', e => {
    if (e.target === e.currentTarget) closePlayerDetail();
  });

  // Tab navigation — clear player page hash first so Dashboard/nav reliably shows main app
  document.querySelectorAll('.nav-link[data-tab]').forEach(link => {
    link.addEventListener('click', e => {
      e.preventDefault();
      if (window.location.hash && window.location.hash.startsWith('#player')) {
        window.location.hash = '';
      }
      showTab(link.dataset.tab);
    });
  });

  // Logo click → go to dashboard
  document.querySelector('.logo')?.addEventListener('click', () => {
    if (window.location.hash && window.location.hash.startsWith('#player')) {
      window.location.hash = '';
    }
    showTab('dashboard');
  });

  // Event delegation for Pick button - open modal
  document.getElementById('games-grid')?.addEventListener('click', e => {
    const btn = e.target.closest('.btn-pick');
    if (!btn) return;
    e.stopPropagation();
    e.preventDefault();
    const row = btn.closest('.player-row');
    if (!row) return;
    openPickModal(
      row.dataset.gd,
      row.dataset.home,
      row.dataset.away,
      row.dataset.player,
      row.dataset.team,
      row.dataset.opp || '',
      row.dataset.pred,
      row.dataset.playerId || undefined
    );
  });

  document.getElementById('pick-modal-cancel')?.addEventListener('click', closePickModal);
  document.getElementById('pick-modal-overlay')?.addEventListener('click', e => {
    if (e.target === e.currentTarget) closePickModal();
  });
  document.querySelectorAll('.pick-type-btn').forEach(btn => {
    btn.addEventListener('click', () => {
      if (!pickModalContext) return;
      const c = pickModalContext;
      addPick(c.gameDate, c.home, c.away, c.playerName, c.playerTeam, c.opposingTeam, c.predictedSog, c.playerId, btn.dataset.type);
    });
  });

  document.getElementById('historical-apply')?.addEventListener('click', () => loadHistorical());

  // Hit rate leaderboard sort toggle
  document.querySelectorAll('.btn-hit-toggle').forEach(btn => {
    btn.addEventListener('click', () => {
      document.querySelectorAll('.btn-hit-toggle').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      hitRateSortBy = btn.dataset.sort;
      renderHitRateLeaderboard();
    });
  });

  // Season team SOG show top10 / all toggle
  document.querySelectorAll('.btn-team-show-toggle').forEach(btn => {
    btn.addEventListener('click', () => {
      document.querySelectorAll('.btn-team-show-toggle').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      teamSeasonShowAll = btn.dataset.show === 'all';
      renderTeamSeasonSog(teamSeasonSogData, teamSeasonShowAll);
    });
  });

  // Ask Genie
  document.getElementById('genie-send')?.addEventListener('click', sendGenieMessage);
  document.getElementById('genie-input')?.addEventListener('keydown', e => {
    if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); sendGenieMessage(); }
  });

  document.getElementById('player-page-back')?.addEventListener('click', (e) => {
    e.preventDefault();
    window.location.hash = '';
  });
  window.addEventListener('hashchange', handleRoute);
  handleRoute();

  loadPipelineStatus();
  showTab('dashboard');
  loadData();
  loadHistorical();
});

function debounce(fn, ms) {
  let t;
  return (...a) => {
    clearTimeout(t);
    t = setTimeout(() => fn(...a), ms);
  };
}
