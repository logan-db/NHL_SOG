/**
 * NHL Game & Player Analysis - Frontend
 * Fetches data from API and populates dashboard sections
 */

const API_BASE = '';

async function fetchAPI(path) {
  try {
    const res = await fetch(`${API_BASE}/api${path}`);
    if (!res.ok) throw new Error(res.statusText);
    return await res.json();
  } catch (e) {
    console.warn(`API ${path} failed:`, e);
    return null;
  }
}

function formatDate(s) {
  if (!s) return '—';
  const d = new Date(s);
  return isNaN(d.getTime()) ? s : d.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: '2-digit' });
}

function pct(v) {
  if (v == null) return '—';
  const n = parseFloat(v);
  return isNaN(n) ? '—' : (n * 100).toFixed(1) + '%';
}

function num(v, decimals = 2) {
  if (v == null) return '—';
  const n = parseFloat(v);
  return isNaN(n) ? '—' : decimals === 0 ? String(Math.round(n)) : n.toFixed(decimals);
}

// Upcoming games
async function loadUpcomingGames() {
  const container = document.getElementById('upcoming-games');
  const data = await fetchAPI('/upcoming-games');
  if (!data || !data.games?.length) {
    container.innerHTML = '<div class="empty-state"><span class="empty-state-icon">🏒</span><p>No upcoming games yet. Data will appear once the database is populated.</p></div>';
    return;
  }
  container.innerHTML = data.games.map(g => `
    <div class="game-card">
      <div class="matchup">
        <div class="away-team">${escapeHtml(g.away || '—')}</div>
        <div class="vs">at</div>
        <div class="home-team">${escapeHtml(g.home || '—')}</div>
      </div>
      <div class="game-date">${formatDate(g.game_date)}</div>
    </div>
  `).join('');
}

// Upcoming predictions table
async function loadUpcomingPredictions() {
  const tbody = document.getElementById('upcoming-predictions-body');
  const data = await fetchAPI('/upcoming-predictions');
  if (!data || !data.predictions?.length) {
    tbody.innerHTML = '<tr><td colspan="9" class="empty-state">No predictions yet. Data will appear once populated.</td></tr>';
    return;
  }
  tbody.innerHTML = data.predictions.map(p => `
    <tr>
      <td>${formatDate(p.game_date)}</td>
      <td>${escapeHtml(p.shooter_name || '—')}</td>
      <td>${escapeHtml(p.player_team || '—')}</td>
      <td>${escapeHtml(p.opposing_team || '—')}</td>
      <td class="numeric">${num(p.predicted_sog)}</td>
      <td class="numeric">${num(p.abs_variance_avg_last7_sog)}</td>
      <td class="numeric">${num(p.player_avg_sog_last7)}</td>
      <td class="numeric">${pct(p.opp_sog_against_rank)}</td>
      <td>${escapeHtml((p.explanation || '').slice(0, 80))}${(p.explanation && p.explanation.length > 80) ? '…' : ''}</td>
    </tr>
  `).join('');
}

// Player stats summary cards (from first prediction if available)
async function loadPlayerStatsCards() {
  const data = await fetchAPI('/upcoming-predictions');
  if (!data?.predictions?.length) return;
  const p = data.predictions[0];
  const v = (id, val) => { const el = document.getElementById(id); if (el) el.textContent = val; };
  v('stat-avg-sog', num(p.average_player_total_shotsongoal_last_7_games));
  v('stat-last-sog', num(p.previous_player_total_shotsongoal, 0));
  v('stat-pred-sog', num(p.predicted_sog));
  v('stat-games-played', num(p.player_games_played_rolling, 0));
}

// Player stats table
async function loadPlayerStats() {
  const tbody = document.getElementById('player-stats-body');
  const data = await fetchAPI('/player-stats');
  if (!data || !data.players?.length) {
    tbody.innerHTML = '<tr><td colspan="6" class="empty-state">No player stats yet.</td></tr>';
    return;
  }
  tbody.innerHTML = data.players.map(p => `
    <tr>
      <td>${escapeHtml(p.shooter_name || '—')}</td>
      <td class="numeric">${pct(p.player_sog_rank)}</td>
      <td class="numeric">${pct(p.player_goals_for_rank)}</td>
      <td class="numeric">${pct(p.player_pp_sog_rank)}</td>
      <td class="numeric">${pct(p.player_last7_pp_sog_pct)}</td>
      <td class="numeric">${num(p.player_ice_time_rank, 0)}</td>
    </tr>
  `).join('');
}

// Team stats table
async function loadTeamStats() {
  const tbody = document.getElementById('team-stats-body');
  const data = await fetchAPI('/team-stats');
  if (!data || !data.teams?.length) {
    tbody.innerHTML = '<tr><td colspan="4" class="empty-state">No team stats yet.</td></tr>';
    return;
  }
  tbody.innerHTML = data.teams.map(t => `
    <tr>
      <td>${escapeHtml(t.player_team || '—')}</td>
      <td class="numeric">${pct(t.team_sog_for_rank)}</td>
      <td class="numeric">${pct(t.team_goals_for_rank)}</td>
      <td class="numeric">${pct(t.team_pp_sog_rank)}</td>
    </tr>
  `).join('');
}

// Opponent stats table
async function loadOpponentStats() {
  const tbody = document.getElementById('opponent-stats-body');
  const data = await fetchAPI('/opponent-stats');
  if (!data || !data.opponents?.length) {
    tbody.innerHTML = '<tr><td colspan="5" class="empty-state">No opponent stats yet.</td></tr>';
    return;
  }
  tbody.innerHTML = data.opponents.map(o => `
    <tr>
      <td>${escapeHtml(o.opposing_team || '—')}</td>
      <td class="numeric">${num(o.opp_sog_against)}</td>
      <td class="numeric">${pct(o.opp_goals_against_rank)}</td>
      <td class="numeric">${pct(o.opp_pk_sog_rank)}</td>
      <td class="numeric">${pct(o.opp_penalties_rank)}</td>
    </tr>
  `).join('');
}

// Historical games table
async function loadHistoricalGames() {
  const tbody = document.getElementById('historical-games-body');
  const data = await fetchAPI('/historical-games');
  if (!data || !data.games?.length) {
    tbody.innerHTML = '<tr><td colspan="6" class="empty-state">No historical games yet.</td></tr>';
    return;
  }
  tbody.innerHTML = data.games.map(g => `
    <tr>
      <td>${formatDate(g.game_date)}</td>
      <td>${escapeHtml(g.home || '—')}</td>
      <td>${escapeHtml(g.away || '—')}</td>
      <td class="numeric">${num(g.goals_for, 0)}</td>
      <td class="numeric">${num(g.goals_against, 0)}</td>
      <td><span class="${(g.is_win === 'Yes' || g.is_win === true) ? 'win' : 'loss'}">${escapeHtml(g.is_win || '—')}</span></td>
    </tr>
  `).join('');
}

function escapeHtml(s) {
  if (s == null) return '';
  const div = document.createElement('div');
  div.textContent = s;
  return div.innerHTML;
}

// Smooth scroll for nav
document.querySelectorAll('.nav-link').forEach(link => {
  link.addEventListener('click', e => {
    e.preventDefault();
    const id = link.getAttribute('href').slice(1);
    document.getElementById(id)?.scrollIntoView({ behavior: 'smooth' });
  });
});

// Load all on DOM ready
document.addEventListener('DOMContentLoaded', () => {
  loadUpcomingGames();
  loadUpcomingPredictions();
  loadPlayerStatsCards();
  loadPlayerStats();
  loadTeamStats();
  loadOpponentStats();
  loadHistoricalGames();
});
