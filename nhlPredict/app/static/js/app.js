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

/** Ice time rank (int): lower = better, 1 = most ice time. Returns { text, className }. */
function iceTimeRankClass(v) {
  if (v == null) return { text: '—', className: '' };
  const n = parseInt(v, 10);
  if (isNaN(n)) return { text: '—', className: '' };
  if (n <= 1) return { text: String(n), className: 'rank-best' };
  if (n <= 2) return { text: String(n), className: 'rank-good' };
  if (n <= 4) return { text: String(n), className: 'rank-ok' };
  return { text: String(n), className: 'rank-low' };
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

/** Match prediction to schedule game: (playerTeam, opposingTeam) = (AWAY, HOME) or (HOME, AWAY) */
function predictionMatchesGame(p, home, away, gameDate) {
  const d = dateStr(p.game_date);
  const gd = dateStr(gameDate);
  if (d !== gd) return false;
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

async function loadData() {
  // Fetch Latest Results separately so it's not affected by other fetches
  fetchAPI('/yesterday-results', { client_date: getLocalDateString() })
    .then(res => renderYesterdayResults(res))
    .catch(e => { console.warn('yesterday-results failed:', e); renderYesterdayResults(null); });

  try {
    const [gamesRes, predRes, favRes] = await Promise.all([
      fetchAPI('/upcoming-games'),
      fetchAPI('/upcoming-predictions', getFilterParams()),
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
  const OPP_TOOLTIP = "Opponent's SOG-against rank (percentile). Lower = opponent allows more shots = better for this player.";
  grid.innerHTML = topN.map(p => {
    const oppRank = rankClass(p.opp_sog_against_rank);
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
      openPlayerDetail(card.dataset.player, card.dataset.team, card.dataset.opp, card.dataset.date);
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

function renderGames() {
  const grid = document.getElementById('games-grid');
  if (!grid) return;

  if (!allGames.length) {
    grid.innerHTML = '<div class="empty-state">No upcoming games found.</div>';
    return;
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

  if (favoriteTeamsSet.size > 0) {
    gamesToShow = [...gamesToShow].sort((a, b) => {
      const aHasFav = favoriteTeamsSet.has((a.home || '').toUpperCase()) || favoriteTeamsSet.has((a.away || '').toUpperCase());
      const bHasFav = favoriteTeamsSet.has((b.home || '').toUpperCase()) || favoriteTeamsSet.has((b.away || '').toUpperCase());
      if (aHasFav && !bHasFav) return -1;
      if (!aHasFav && bHasFav) return 1;
      return 0;
    });
  }

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

    return `
      <div class="game-card ${isExpanded ? 'expanded' : ''}" data-key="${escapeHtml(key)}">
        <div class="matchup">
          <span class="away-team">${escapeHtml(away)}</span>
          <span class="vs">@</span>
          <span class="home-team">${escapeHtml(home)}</span>
        </div>
        <div class="game-date">${formatGameDate(gd, g.game_time)}</div>
        <div class="top-stats">
          ${topSog ? `<div class="top-stat">🔥 Top SOG: <strong>${num(topSog.predicted_sog)}</strong> (${escapeHtml(topSog.shooter_name)})</div>` : ''}
          ${topVar ? `<div class="top-stat">Variance: <strong>${num(topVar.abs_variance_avg_last7_sog)}</strong> (${escapeHtml(topVar.shooter_name)})</div>` : ''}
        </div>
        <div class="game-predictions-table">
          ${predsForGame.length ? `
            <table>
              <thead><tr>
                <th>Player</th><th>Team</th><th class="numeric">Pred SOG</th><th class="numeric">Variance</th>
                <th class="numeric">Avg 7</th><th class="numeric">2+ %</th><th class="numeric">3+ %</th><th class="numeric">Opp %</th><th></th>
              </tr></thead>
              <tbody>
                ${predsForGame
                  .sort((a, b) => (parseFloat(b.predicted_sog) || 0) - (parseFloat(a.predicted_sog) || 0))
                  .map(p => {
                    const oppRank = rankClass(p.opp_sog_against_rank);
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
      const key = card.dataset.key;
      grid.dataset.expandedKey = grid.dataset.expandedKey === key ? '' : key;
      renderGames();
    });
  });

  grid.querySelectorAll('.player-row').forEach(row => {
    row.addEventListener('click', e => {
      if (e.target.closest('.btn-star') || e.target.closest('.btn-pick')) return;
      e.stopPropagation();
      openPlayerDetail(
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
  // Pick buttons use event delegation (handled in games-grid click)
}

function renderPredictionsTable() {
  const tbody = document.getElementById('predictions-body');
  if (!tbody) return;

  if (!allPredictions.length) {
    tbody.innerHTML = '<tr><td colspan="11" class="empty-state">No predictions. Adjust filters or wait for data.</td></tr>';
    return;
  }

  tbody.innerHTML = allPredictions.map(p => {
    const date = dateStr(p.game_date);
    const oppRank = rankClass(p.opp_sog_against_rank);
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

  tbody.querySelectorAll('.player-row').forEach(row => {
    row.addEventListener('click', e => {
      if (e.target.closest('.btn-star')) return;
      openPlayerDetail(row.dataset.player, row.dataset.team, row.dataset.opp, row.dataset.date);
    });
  });
  tbody.querySelectorAll('.btn-star').forEach(btn => {
    btn.addEventListener('click', e => { e.stopPropagation(); toggleFavorite(btn.dataset.player, btn.dataset.team); });
  });
}

// ----- Player detail -----
function statRowWithRank(label, value, isRank = true, title = '') {
  const r = isRank ? rankClass(value) : { text: num(value), className: '' };
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

  // 2+ / 3+ SOG % (Season, Season vs Matchup, Last 30 days)
  // Use pctHitRate for null-friendly display: "Insufficient data" when not enough games played
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
    ${statRowWithRank('PP SOG % (7g)', p.player_last7_pp_sog_pct, true, 'PP SOG %: % of their total shots that are Powerplay Shots on goal')}
    ${statRowIceTimeRank('Ice time rank', p.player_ice_time_rank, 'Player ice time rank among teammates in last game (1 = most ice time on team)')}
    ${statRowIceTimeRank('PP ice time rank', p.player_pp_ice_time_rank, 'Player powerplay ice time rank among teammates in last game (1 = most PP ice time on team)')}
    ${statRowWithRank('Avg SOG (3g)', p.player_avg_sog_last3, false)}
    ${statRowMatchup('Matchup last SOG', p.matchup_last_sog)}
    ${statRowMatchup('Matchup avg SOG (7g)', p.matchup_avg_sog_last7)}
  `;

  // Team rankings with highlight
  document.getElementById('player-detail-team').innerHTML = `
    ${statRowWithRank('SOG % rank', p.team_sog_for_rank)}
    ${statRowWithRank('Goals % rank', p.team_goals_for_rank)}
    ${statRowWithRank('PP SOG rank', p.team_pp_sog_rank, true, 'Powerplay Shots on Goal Percentile Rank on their team')}
  `;

  // Opponent rankings: SOG/Goals/PK against = lower rank = opponent allows more = better for player; Penalties = higher = more PP opportunities
  document.getElementById('player-detail-opponent').innerHTML = `
    ${statRowWithRank('Goals against rank', p.opp_goals_against_rank)}
    ${statRowWithRank('SOG against rank', p.opp_sog_against_rank)}
    ${statRowWithRank('PK SOG rank', p.opp_pk_sog_rank)}
    ${statRowWithRank('Penalties rank', p.opp_penalties_rank)}
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
    data = { players: [], error: 'Failed to load (network or server error)' };
  }
  const players = data?.players || [];

  const statsHtml = players.length
    ? `<td colspan="7" class="player-stats-cell">
        <div class="player-stats-inner">
          <table class="player-stats-table">
            <thead><tr><th>Player</th><th>Team</th><th class="numeric">SOG</th><th class="numeric">G</th><th class="numeric">A</th><th class="numeric">Pts</th></tr></thead>
            <tbody>
              ${players.map(p => `
                <tr>
                  <td>${escapeHtml(p.player_name || '—')}</td>
                  <td>${escapeHtml(p.team || '—')}</td>
                  <td class="numeric">${num(p.sog, 0)}</td>
                  <td class="numeric">${num(p.goals, 0)}</td>
                  <td class="numeric">${num(p.assists, 0)}</td>
                  <td class="numeric">${num(p.points, 0)}</td>
                </tr>
              `).join('')}
            </tbody>
          </table>
        </div>
      </td>`
    : `<td colspan="7" class="empty-state">${data?.error || 'Player stats for this game are not yet available. gold_player_stats_clean may still be syncing.'}</td>`;

  const expandRow = document.createElement('tr');
  expandRow.className = 'historical-player-stats-row';
  expandRow.innerHTML = statsHtml;
  row.after(expandRow);
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

  document.getElementById('close-player-detail')?.addEventListener('click', closePlayerDetail);
  document.getElementById('player-detail-overlay')?.addEventListener('click', e => {
    if (e.target === e.currentTarget) closePlayerDetail();
  });

  // Tab navigation
  document.querySelectorAll('.nav-link[data-tab]').forEach(link => {
    link.addEventListener('click', e => {
      e.preventDefault();
      showTab(link.dataset.tab);
    });
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
