#!/usr/bin/env python3
"""
generate_snapshot.py — Create a self-contained HTML snapshot of the SOG Predict dashboard.

The output file has no backend dependency: all dashboard data is embedded as JSON,
CSS and JS are inlined, and images are base64-encoded. It can be opened locally
in any browser or sent via email as an attachment.

Usage:

  # Against the deployed Databricks App (auto-detects token from CLI):
  python generate_snapshot.py \\
      --url https://nhl-predict-app-1444828305810485.aws.databricksapps.com \\
      --output "snapshot_$(date +%Y-%m-%d).html"

  # Or pass a token explicitly:
  TOKEN=$(databricks auth token --profile DEFAULT 2>/dev/null | python3 -c "import sys,json; print(json.load(sys.stdin)['access_token'])")
  python generate_snapshot.py \\
      --url https://nhl-predict-app-1444828305810485.aws.databricksapps.com \\
      --token "$TOKEN" \\
      --output "snapshot_$(date +%Y-%m-%d).html"

  # Against the app running locally:
  python generate_snapshot.py --url http://localhost:5000

Requirements: Python 3.8+ (stdlib only — no extra packages needed)
"""

import argparse
import base64
import json
import os
import re
import subprocess
import sys
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from pathlib import Path


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_token_from_cli(profile: str = "DEFAULT", host: str = "") -> str | None:
    """Try to get a Databricks auth token from the CLI (databricks auth token)."""
    try:
        cmd = ["databricks", "auth", "token", "--profile", profile]
        if host:
            cmd += ["--host", host]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
        if result.returncode != 0:
            return None
        out = result.stdout.strip()
        # JSON output: {"access_token": "...", ...}
        try:
            parsed = json.loads(out)
            return parsed.get("access_token") or parsed.get("token")
        except json.JSONDecodeError:
            pass
        # Legacy plain-text output: "Token: dapi..."
        for line in out.splitlines():
            if "token" in line.lower():
                parts = line.strip().split()
                if parts:
                    return parts[-1]
    except Exception:
        pass
    return None


def _make_headers(token: str | None) -> dict:
    headers: dict = {"Accept": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


def fetch_json(base_url: str, path: str, params: dict | None = None, token: str | None = None) -> dict | None:
    """GET a JSON endpoint; return parsed dict or None on error."""
    url = base_url.rstrip("/") + path
    if params:
        url += "?" + urllib.parse.urlencode(params)
    try:
        req = urllib.request.Request(url, headers=_make_headers(token))
        with urllib.request.urlopen(req, timeout=20) as r:
            return json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        if e.code == 401:
            print(f"    AUTH  {path} → 401 Unauthorized (pass --token)", file=sys.stderr)
        else:
            print(f"    WARN  {path} → HTTP {e.code}", file=sys.stderr)
        return None
    except Exception as e:
        print(f"    WARN  {path} → {e}", file=sys.stderr)
        return None


def fetch_text(base_url: str, path: str, token: str | None = None) -> str | None:
    """GET a text/html endpoint; return body as str or None on error."""
    url = base_url.rstrip("/") + path
    try:
        req = urllib.request.Request(url, headers={**_make_headers(token), "Accept": "text/html"})
        with urllib.request.urlopen(req, timeout=20) as r:
            return r.read().decode(errors="replace")
    except urllib.error.HTTPError as e:
        if e.code == 401:
            print(f"    AUTH  {path} → 401 Unauthorized (pass --token)", file=sys.stderr)
        else:
            print(f"    WARN  {path} → HTTP {e.code}", file=sys.stderr)
        return None
    except Exception as e:
        print(f"    WARN  {path} → {e}", file=sys.stderr)
        return None


def b64_image(path: Path) -> str:
    """Return a data URI for a binary image file."""
    if not path.exists():
        return ""
    ext = path.suffix.lower().lstrip(".")
    mime = {"png": "image/png", "svg": "image/svg+xml", "ico": "image/x-icon",
            "jpg": "image/jpeg", "jpeg": "image/jpeg"}.get(ext, "image/png")
    data = base64.b64encode(path.read_bytes()).decode()
    return f"data:{mime};base64,{data}"


def json_for_html(data: object) -> str:
    """JSON-encode data safe for embedding inside an HTML <script> block.

    Python's json.dumps does not escape forward-slashes, so a value like
    '</script>' would terminate the enclosing <script> tag and corrupt all
    subsequent data. We escape every '</' sequence to prevent this.
    JSON allows \\/ as an equivalent of /, so browsers parse it correctly.
    """
    return json.dumps(data, separators=(",", ":")).replace("</", "<\\/")


def normalize_date(val: object) -> str:
    """Return YYYY-MM-DD for any date-like value (string, timestamp, None)."""
    if not val:
        return ""
    s = str(val).strip()
    # Already YYYY-MM-DD (possibly with time suffix)
    if len(s) >= 10 and s[4] == "-" and s[7] == "-":
        return s[:10]
    return s


def extract_main_content(html: str) -> str:
    """Pull everything inside <main ...>…</main> from the rendered page."""
    m = re.search(r'<main[^>]*>(.*?)</main>', html, re.DOTALL | re.IGNORECASE)
    if m:
        return m.group(1).strip()
    # Fallback: return full body
    m2 = re.search(r'<body[^>]*>(.*?)</body>', html, re.DOTALL | re.IGNORECASE)
    return m2.group(1).strip() if m2 else html


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate a self-contained HTML snapshot of SOG Predict",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--url", default="http://localhost:5000",
                        help="Base URL of the running app (default: http://localhost:5000)")
    parser.add_argument("--token",
                        help="Databricks PAT or OAuth token (needed for deployed apps). "
                             "Omit to auto-detect from 'databricks auth token'.")
    parser.add_argument("--profile", default="DEFAULT",
                        help="Databricks CLI profile for auto-token detection (default: DEFAULT)")
    parser.add_argument("--output", default="sog_predict_snapshot.html",
                        help="Output filename (default: sog_predict_snapshot.html)")
    args = parser.parse_args()

    base_url = args.url.rstrip("/")

    # Resolve auth token
    token: str | None = args.token
    if not token and "localhost" not in base_url and "127.0.0.1" not in base_url:
        print("No --token supplied; attempting auto-detection via Databricks CLI …")
        token = _get_token_from_cli(profile=args.profile)
        if token:
            print(f"  ✓ Token obtained from CLI profile '{args.profile}'")
        else:
            print("  ✗ Could not auto-detect token. "
                  "Pass --token <PAT> if the app requires authentication.", file=sys.stderr)
    today = date.today().isoformat()
    snapshot_label = date.today().strftime("%B %d, %Y")

    app_dir = Path(__file__).parent

    # -----------------------------------------------------------------------
    # 1. Fetch rendered page (for <main> content)
    # -----------------------------------------------------------------------
    print(f"\nConnecting to {base_url} …")
    rendered_html = fetch_text(base_url, "/", token=token)
    if not rendered_html:
        print("ERROR: Could not fetch the app root page. Is the app running?", file=sys.stderr)
        sys.exit(1)
    main_content = extract_main_content(rendered_html)
    print("  ✓ Fetched rendered page")

    # -----------------------------------------------------------------------
    # 2. Pre-fetch all dashboard API endpoints
    # -----------------------------------------------------------------------
    print("\nFetching dashboard data …")

    # Each tuple: (api_path, query_params, snapshot_key)
    # snapshot_key is what fetchAPI() in app.js calls (without /api prefix)
    endpoints = [
        ("/api/pipeline-status",     {},                                       "/pipeline-status"),
        ("/api/filter-options",      {},                                       "/filter-options"),
        ("/api/yesterday-results",   {"client_date": today},                   "/yesterday-results"),
        ("/api/upcoming-games",      {"view": "today",     "client_date": today}, "/upcoming-games?view=today"),
        ("/api/upcoming-games",      {"view": "tomorrow",  "client_date": today}, "/upcoming-games?view=tomorrow"),
        # Predictions: today + ungated (Predictions tab)
        ("/api/upcoming-predictions",
         {"by_date": "true", "game_date": today, "client_date": today},
         "/upcoming-predictions?by_date=true"),
        ("/api/upcoming-predictions", {},                                      "/upcoming-predictions"),
        ("/api/team-season-sog",     {},                                       "/team-season-sog"),
        ("/api/hit-rate-leaderboard",{},                                       "/hit-rate-leaderboard"),
        ("/api/team-sog-rankings",   {},                                       "/team-sog-rankings"),
    ]

    snapshot_data: dict = {}
    for api_path, params, snap_key in endpoints:
        label = api_path + ("?" + urllib.parse.urlencode(params) if params else "")
        print(f"  GET {label}", end="  ", flush=True)
        data = fetch_json(base_url, api_path, params, token=token)
        if data is not None:
            snapshot_data[snap_key] = data
            print("✓")
        else:
            print("✗ (skipped)")

    # -----------------------------------------------------------------------
    # Pre-fetch /api/game-analysis for every game in today's + tomorrow's schedule.
    # gameKey format in JS: "YYYY-MM-DD|HOME|AWAY" → params: { home, away, game_date }
    # Storage key: /game-analysis?home=HOME&away=AWAY&game_date=DATE
    # -----------------------------------------------------------------------
    all_games_for_analysis: list[dict] = []
    seen_games: set[str] = set()
    for view_key in ("/upcoming-games?view=today", "/upcoming-games?view=tomorrow"):
        for g in (snapshot_data.get(view_key) or {}).get("games", []):
            home = (g.get("home") or "").strip()
            away = (g.get("away") or "").strip()
            gd = normalize_date(g.get("game_date"))
            uid = f"{gd}|{home}|{away}"
            if uid and uid not in seen_games:
                seen_games.add(uid)
                all_games_for_analysis.append({"home": home, "away": away, "date": gd})

    if all_games_for_analysis:
        print(f"\nFetching game analysis data for {len(all_games_for_analysis)} games …")
        for g in all_games_for_analysis:
            label = f"/api/game-analysis?home={g['home']}&away={g['away']}&game_date={g['date']}"
            print(f"  GET {label}", end="  ", flush=True)
            data = fetch_json(base_url, "/api/game-analysis",
                              {"home": g["home"], "away": g["away"], "game_date": g["date"]},
                              token=token)
            if data is not None:
                # Key must match JS: new URLSearchParams({home, away, game_date}).toString()
                snap_key = (
                    f"/game-analysis?"
                    f"home={urllib.parse.quote_plus(g['home'])}"
                    f"&away={urllib.parse.quote_plus(g['away'])}"
                    f"&game_date={urllib.parse.quote_plus(g['date'])}"
                )
                snapshot_data[snap_key] = data
                print("✓")
            else:
                print("✗ (skipped)")

    # -----------------------------------------------------------------------
    # Pre-fetch /api/team-last-game for every team in today's + tomorrow's games
    # Stored as /team-last-game?team=XXX so the fetchAPI override can find them.
    # -----------------------------------------------------------------------
    teams: set[str] = set()
    for view_key in ("/upcoming-games?view=today", "/upcoming-games?view=tomorrow"):
        games_data = snapshot_data.get(view_key) or {}
        for game in games_data.get("games", []):
            if game.get("home"):
                teams.add(game["home"].strip().upper())
            if game.get("away"):
                teams.add(game["away"].strip().upper())

    if teams:
        print(f"\nFetching last-game data for {len(teams)} teams …")
        for team in sorted(teams):
            print(f"  GET /api/team-last-game?team={team}", end="  ", flush=True)
            data = fetch_json(base_url, "/api/team-last-game", {"team": team}, token=token)
            if data is not None:
                snapshot_data[f"/team-last-game?team={team}"] = data
                print("✓")
            else:
                print("✗ (skipped)")

    # -----------------------------------------------------------------------
    # Pre-fetch player-detail, player-sog-chart, player-hit-rates-history
    # for every player in today's predictions so the player analysis page works.
    # Keys are stored exactly as JS's URLSearchParams would build them so the
    # generic qs-lookup in the override finds them. A __players__ index is also
    # stored as a fast name-based fallback.
    # -----------------------------------------------------------------------
    players: list[dict] = []
    seen: set[str] = set()
    for pred_key in ("/upcoming-predictions?by_date=true", "/upcoming-predictions"):
        preds = snapshot_data.get(pred_key) or {}
        for p in preds.get("predictions", []):
            name = (p.get("shooter_name") or "").strip()
            team = (p.get("player_team") or "").strip()
            if not name or name in seen:
                continue
            seen.add(name)
            # Normalize to YYYY-MM-DD — must match JS's dateStr() which does the same
            gd = normalize_date(p.get("game_date"))
            opp = (p.get("opposing_team") or "").strip()
            players.append({"name": name, "team": team, "opp": opp, "date": gd})

    if players:
        print(f"\nFetching player analysis data for {len(players)} players (parallel) …")

        # Build the exact param-ordered keys the JS override will look up
        def _player_keys(p: dict) -> tuple[str, str, str]:
            """Return (detail_key, chart_key, hit_rates_key) matching JS URLSearchParams order."""
            enc = urllib.parse.urlencode  # shorthand
            detail_qs = enc([
                ("player",        p["name"]),
                ("player_team",   p["team"]),
                ("opposing_team", p["opp"]),
                ("game_date",     p["date"]),
            ])
            chart_qs = enc([("player", p["name"]), ("game_date", p["date"])])
            hr_qs    = enc([("player", p["name"]), ("player_team", p["team"])])
            return (
                f"/player-detail?{detail_qs}",
                f"/player-sog-chart?{chart_qs}",
                f"/player-hit-rates-history?{hr_qs}",
            )

        def _fetch_player(p: dict) -> dict:
            """Fetch all 3 endpoints for one player; return merged results."""
            dk, ck, hrk = _player_keys(p)
            detail   = fetch_json(base_url, "/api/player-detail",
                                  {"player": p["name"], "player_team": p["team"],
                                   "opposing_team": p["opp"], "game_date": p["date"]},
                                  token=token)
            chart    = fetch_json(base_url, "/api/player-sog-chart",
                                  {"player": p["name"], "game_date": p["date"]},
                                  token=token)
            hit_rates = fetch_json(base_url, "/api/player-hit-rates-history",
                                   {"player": p["name"], "player_team": p["team"]},
                                   token=token)
            return {
                "player": p,
                "detail_key":    dk,    "detail":    detail,
                "chart_key":     ck,    "chart":     chart,
                "hr_key":        hrk,   "hit_rates": hit_rates,
            }

        players_index: dict = {}
        done = 0
        with ThreadPoolExecutor(max_workers=10) as pool:
            futures = {pool.submit(_fetch_player, p): p for p in players}
            for fut in as_completed(futures):
                res = fut.result()
                p = res["player"]
                done += 1
                ok = "✓" if res["detail"] else "✗"
                print(f"  [{done:>3}/{len(players)}] {p['name']:<28} {ok}", flush=True)
                if res["detail"]:
                    snapshot_data[res["detail_key"]]    = res["detail"]
                if res["chart"]:
                    snapshot_data[res["chart_key"]]     = res["chart"]
                if res["hit_rates"]:
                    snapshot_data[res["hr_key"]]        = res["hit_rates"]
                # Name-based index for fallback lookup
                players_index[p["name"]] = {
                    "detail":    res["detail"],
                    "chart":     res["chart"],
                    "hit_rates": res["hit_rates"],
                }

        snapshot_data["__players__"] = players_index
        print(f"  ✓ {sum(1 for v in players_index.values() if v['detail'])} / {len(players)} players fetched")

    # Stub out user-specific write endpoints so the JS doesn't error
    snapshot_data.setdefault("/favorites",      {"favorites": []})
    snapshot_data.setdefault("/favorite-teams", {"teams": []})
    snapshot_data.setdefault("/picks",          {"picks": []})

    # -----------------------------------------------------------------------
    # 3. Read & inline static assets
    # -----------------------------------------------------------------------
    print("\nReading static assets …")

    css_path     = app_dir / "static" / "css" / "style.css"
    js_path      = app_dir / "static" / "js" / "app.js"
    favicon_path = app_dir / "static" / "favicon.png"
    logo_path    = app_dir / "static" / "logo.png"

    css_content  = css_path.read_text(encoding="utf-8") if css_path.exists() else ""
    js_content   = js_path.read_text(encoding="utf-8")  if js_path.exists()  else ""
    favicon_b64  = b64_image(favicon_path)
    logo_b64     = b64_image(logo_path)

    print("  ✓ style.css, app.js, images")

    # -----------------------------------------------------------------------
    # 4. Replace dynamic img src references in main_content
    # -----------------------------------------------------------------------
    # Jinja2 url_for() has already been rendered to /static/… paths; replace them
    main_content = main_content.replace(
        'src="/static/favicon.png"', f'src="{favicon_b64}"'
    ).replace(
        'src="/static/logo.png"', f'src="{logo_b64}"'
    ).replace(
        "src='/static/favicon.png'", f"src='{favicon_b64}'"
    ).replace(
        "src='/static/logo.png'", f"src='{logo_b64}'"
    )

    # -----------------------------------------------------------------------
    # 5. Build the snapshot JS override (replaces fetchAPI with static lookup)
    # -----------------------------------------------------------------------
    snapshot_js_data = json_for_html(snapshot_data)

    override_script = f"""
// ── SOG Predict Static Snapshot ──────────────────────────────────────────
// Generated: {snapshot_label}
// All API calls are intercepted and served from pre-fetched static data.
// Write operations (favorites, picks, AI analysis) are silently stubbed out.
// ──────────────────────────────────────────────────────────────────────────
(function () {{
  window.__SNAPSHOT_MODE = true;
  window.__SNAPSHOT_DATE = {json.dumps(today)};
  const _D = {snapshot_js_data};

  // URL-encode a value the same way Python's urlencode / JS URLSearchParams does
  // (spaces → +, not %20, to match stored keys)
  function _enc(v) {{
    return encodeURIComponent(String(v)).replace(/%20/g, '+');
  }}

  window.__snapFetchAPI = async function snapFetchAPI(path, params, options) {{
    params  = params  || {{}};
    options = options || {{}};
    var method = ((options.method || 'GET') + '').toUpperCase();

    // Stub all writes silently
    if (method !== 'GET') {{
      console.debug('[SNAPSHOT] stub write:', method, path);
      return null;
    }}

    // Exact key match
    if (_D[path] !== undefined) return _D[path];

    // Key + serialised params
    var qs = new URLSearchParams(params).toString();
    if (qs) {{
      var full = path + '?' + qs;
      if (_D[full] !== undefined) return _D[full];
    }}

    // /upcoming-games: match by view param
    if (path === '/upcoming-games') {{
      var vk = '/upcoming-games?view=' + (params.view || 'today');
      if (_D[vk] !== undefined) return _D[vk];
    }}

    // /upcoming-predictions: fall back to ungated copy
    if (path === '/upcoming-predictions') {{
      if (_D['/upcoming-predictions'] !== undefined) return _D['/upcoming-predictions'];
    }}

    // /team-last-game: match by team param (case-insensitive)
    if (path === '/team-last-game' && params.team) {{
      var tk = '/team-last-game?team=' + params.team.toString().toUpperCase();
      if (_D[tk] !== undefined) return _D[tk];
    }}

    // /player-detail, /player-sog-chart, /player-hit-rates-history
    // Primary: try building the exact key that Python stored (same param order as JS buildss them).
    // Fallback: look up in __players__ index by name.
    var _playerPaths = ['/player-detail', '/player-sog-chart', '/player-hit-rates-history'];
    if (_playerPaths.indexOf(path) !== -1 && params.player) {{
      // Build canonical key matching URLSearchParams insertion order
      var _pqs = '';
      if (path === '/player-detail') {{
        _pqs = 'player=' + _enc(params.player);
        if (params.player_team)   _pqs += '&player_team='   + _enc(params.player_team);
        if (params.opposing_team) _pqs += '&opposing_team=' + _enc(params.opposing_team);
        if (params.game_date)     _pqs += '&game_date='     + _enc(params.game_date);
      }} else if (path === '/player-sog-chart') {{
        _pqs = 'player=' + _enc(params.player);
        if (params.game_date) _pqs += '&game_date=' + _enc(params.game_date);
      }} else {{
        _pqs = 'player=' + _enc(params.player);
        if (params.player_team) _pqs += '&player_team=' + _enc(params.player_team);
      }}
      var _pk = path + '?' + _pqs;
      if (_D[_pk] !== undefined) return _D[_pk];

      // Fallback: __players__ name-based index
      var _pi = _D['__players__'];
      if (_pi && _pi[params.player]) {{
        var _pd = _pi[params.player];
        if (path === '/player-detail')            return _pd.detail    || null;
        if (path === '/player-sog-chart')         return _pd.chart     || null;
        if (path === '/player-hit-rates-history') return _pd.hit_rates || null;
      }}
    }}

    console.debug('[SNAPSHOT] no data for:', path, params);
    return null;
  }};
}})();
"""

    # Post-load script to wire up the override and clean up snapshot-irrelevant UI
    post_script = """
// Wire fetchAPI override (runs after app.js has defined the original function)
if (typeof window.__snapFetchAPI === 'function') {
  // Re-assign in global scope so all existing closures pick it up
  /* jshint ignore:start */
  fetchAPI = window.__snapFetchAPI;          // eslint-disable-line no-global-assign
  /* jshint ignore:end */
}

// Disable interactive features that require a live backend
document.addEventListener('DOMContentLoaded', function () {
  // AI generate buttons
  document.querySelectorAll(
    '.btn-ai-generate, #player-page-btn-ai-generate, #btn-ai-generate'
  ).forEach(function (btn) {
    btn.disabled = true;
    btn.textContent = 'AI analysis – not available in snapshot';
    btn.style.opacity = '0.5';
    btn.style.cursor = 'not-allowed';
  });

  // Genie chat input
  var gInput = document.getElementById('genie-input');
  var gSend  = document.getElementById('genie-send');
  if (gInput) { gInput.disabled = true; gInput.placeholder = 'Genie chat not available in snapshot'; }
  if (gSend)  { gSend.disabled  = true; }

  // Keep only Dashboard tab active; all others already hidden in nav HTML
  document.querySelectorAll('.nav-link:not([data-tab="dashboard"])').forEach(function (el) {
    el.style.opacity = '0.35';
    el.style.pointerEvents = 'none';
    el.title = 'Not available in snapshot';
  });

  // Pipeline status: show snapshot date
  var statusEl = document.querySelector('#pipeline-status .status-text');
  if (statusEl) statusEl.textContent = 'Snapshot: """ + snapshot_label + """';
  var pipeEl = document.getElementById('pipeline-status');
  if (pipeEl) { pipeEl.classList.remove('status-error'); pipeEl.classList.add('status-ok'); }
});
"""

    # -----------------------------------------------------------------------
    # 6. Assemble final HTML
    # -----------------------------------------------------------------------
    snapshot_banner = f"""
<div class="snapshot-banner">
  📸 <strong>STATIC SNAPSHOT</strong> &nbsp;—&nbsp; Dashboard data as of <strong>{snapshot_label}</strong>.
  &nbsp; Click any game or player for full analysis. Live app required for Picks, Genie &amp; AI analysis.
</div>"""

    snapshot_banner_css = """
.snapshot-banner {
  background: linear-gradient(90deg, #0f172a 0%, #1e293b 100%);
  border-bottom: 2px solid #f59e0b;
  color: #fbbf24;
  text-align: center;
  padding: 0.45rem 1rem;
  font-size: 0.78rem;
  font-family: 'Inter', sans-serif;
  letter-spacing: 0.04em;
  position: sticky;
  top: 0;
  z-index: 9999;
}
.snapshot-banner strong { color: #fde68a; }
"""

    html_out = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>SOG Predict — Snapshot {snapshot_label}</title>
  <link rel="icon" type="image/png" href="{favicon_b64}">
  <link rel="apple-touch-icon" href="{logo_b64}">
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
  <style>
{css_content}
{snapshot_banner_css}
  </style>
</head>
<body>
{snapshot_banner}

  <header class="app-header">
    <div class="header-content">
      <h1 class="logo">
        <img src="{favicon_b64}" alt="SOG Predict logo" class="logo-img">
        <span class="logo-text">SOG Predict</span>
        <span class="logo-sub">NHL Shots on Goal</span>
      </h1>
      <div class="header-meta">
        <div id="pipeline-status" class="pipeline-status status-ok">
          <span class="status-icon">📸</span>
          <span class="status-text">Snapshot: {snapshot_label}</span>
        </div>
      </div>
      <nav class="main-nav">
        <a href="#" class="nav-link active" data-tab="dashboard">Dashboard</a>
        <a href="#" class="nav-link" data-tab="my-picks" style="opacity:0.35;pointer-events:none;" title="Not available in snapshot">My Picks</a>
        <a href="#" class="nav-link" data-tab="historical" style="opacity:0.35;pointer-events:none;" title="Not available in snapshot">Historical</a>
        <a href="#" class="nav-link" data-tab="ask" style="opacity:0.35;pointer-events:none;" title="Not available in snapshot">Ask</a>
      </nav>
    </div>
  </header>

  <main class="app-main">
{main_content}
  </main>

  <footer class="app-footer">
    <p>SOG Predict Snapshot — {snapshot_label} &nbsp;•&nbsp; Powered by Lakebase &nbsp;•&nbsp; Databricks ML</p>
  </footer>

  <div id="toast" class="toast" role="status" aria-live="polite"></div>

  <!-- Pre-fetched snapshot data; fetchAPI override (must come before app.js) -->
  <script>
window.APP_API_BASE = '';
{override_script}
  </script>

  <!-- Inlined app.js -->
  <script>
{js_content}
  </script>

  <!-- Wire override + disable snapshot-incompatible UI -->
  <script>
{post_script}
  </script>
</body>
</html>
"""

    # -----------------------------------------------------------------------
    # 7. Write output file
    # -----------------------------------------------------------------------
    out_path = Path(args.output)
    out_path.write_text(html_out, encoding="utf-8")
    size_kb = out_path.stat().st_size // 1024
    print(f"\n✓  Snapshot written → {out_path}  ({size_kb} KB)")
    print(f"   Open locally:  open \"{out_path}\"")
    print(f"   Send via email as an attachment (.html file).")


if __name__ == "__main__":
    main()
