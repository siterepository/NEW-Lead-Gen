"""
Lead Gen Dashboard - Clean, fast leads table.

Run with:  python3 -m leadgen.dashboard.app
"""

import csv
import io
import json
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from flask import Flask, Response, jsonify, render_template_string, request

# ---------------------------------------------------------------------------
# App setup
# ---------------------------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent
DB_PATH = BASE_DIR / "data" / "leadgen.db"

app = Flask(__name__)

# Load NWM employees for connection matching
NWM_EMPLOYEES = []
NWM_NAMES_LOWER = set()
_nwm_cache = Path.home() / ".leadgen" / "nwm_employees.json"
if _nwm_cache.exists():
    try:
        _nwm_data = json.loads(_nwm_cache.read_text())
        NWM_EMPLOYEES = _nwm_data.get("employees", [])
        NWM_NAMES_LOWER = {e.get("name", "").lower().strip() for e in NWM_EMPLOYEES if e.get("name")}
    except Exception:
        pass


def _find_nwm_connections(lead_name, lead_city=""):
    """Find NWM employees who could be mutual connections.

    Returns a list of NWM reps in the same city or region.
    """
    if not NWM_EMPLOYEES:
        return []

    connections = []
    lead_city_lower = (lead_city or "").lower().strip()

    for emp in NWM_EMPLOYEES:
        emp_name = emp.get("name", "")
        emp_city = emp.get("city", "").lower().strip()
        emp_linkedin = emp.get("linkedin_url", "")
        emp_title = emp.get("title", "")[:80]

        # Same city match or general Utah match
        is_match = (
            (lead_city_lower and emp_city and lead_city_lower in emp_city)
            or emp_city in ("utah", "ut", "")
            or lead_city_lower in ("utah", "ut", "salt lake city", "slc", "")
        )

        if is_match:
            connections.append({
                "name": emp_name,
                "title": emp_title,
                "linkedin_url": emp_linkedin,
            })

    return connections[:5]  # Top 5 most relevant


def get_db():
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def safe_json(payload_str):
    try:
        return json.loads(payload_str) if payload_str else {}
    except (json.JSONDecodeError, TypeError):
        return {}


def _esc(text):
    if not text:
        return ""
    return (
        str(text)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def _parse_lead(row):
    """Parse a jobs row into a flat lead dict."""
    p = safe_json(row["payload"])
    data = p.get("data", {})

    name = data.get("name", "")
    first_name = data.get("first_name", "")
    last_name = data.get("last_name", "")
    if first_name or last_name:
        name = f"{first_name} {last_name}".strip()
    title = data.get("title", "")
    display_name = name or title or "Unknown"

    url = data.get("source_url", data.get("url", ""))
    linkedin_url = data.get("linkedin_url", "")
    has_linkedin = "linkedin.com" in url or bool(linkedin_url)
    best_linkedin = linkedin_url if linkedin_url else (url if "linkedin.com" in url else "")

    score = data.get("_relevance_score", data.get("total_score", 0))
    reason = data.get("_relevance_reason", "")
    platform = p.get("platform", data.get("platform", "unknown"))
    scraped_at = p.get("scraped_at", row["created_at"])
    enhanced = data.get("enhanced", False)

    if score >= 75:
        tier = "A"
    elif score >= 50:
        tier = "B"
    elif score >= 25:
        tier = "C"
    else:
        tier = "D"

    # Determine WHY they were scraped: post vs account status
    snippet = data.get("snippet", data.get("description", data.get("source_post_text", "")))
    search_query = data.get("search_query", "")
    detected_platform = data.get("detected_platform", "")

    # Classify source type
    if "reddit.com" in url:
        source_type = "post"
        source_label = "Reddit post"
    elif "facebook.com" in url and "/posts/" in url:
        source_type = "post"
        source_label = "Facebook post"
    elif "linkedin.com/in/" in url:
        source_type = "profile"
        source_label = "LinkedIn profile status"
    elif "linkedin.com/jobs/" in url or "linkedin.com/pulse/" in url:
        source_type = "post"
        source_label = "LinkedIn post"
    elif "indeed.com" in url:
        source_type = "listing"
        source_label = "Indeed listing"
    elif "craigslist.org" in url:
        source_type = "post"
        source_label = "Craigslist post"
    elif "ksl.com" in url:
        source_type = "listing"
        source_label = "KSL listing"
    else:
        source_type = "web"
        source_label = "Web page"

    # Find NWM connections for this lead
    city = data.get("location_city", "")
    nwm_connections = _find_nwm_connections(display_name, city)

    return {
        "id": row["id"],
        "name": display_name,
        "title": title,
        "url": url,
        "linkedin_url": best_linkedin,
        "has_linkedin": has_linkedin,
        "reason": reason,
        "snippet": (snippet or "")[:300],
        "search_query": search_query,
        "source_type": source_type,
        "source_label": source_label,
        "platform": platform,
        "detected_platform": detected_platform or platform,
        "score": score,
        "tier": tier,
        "scraped_at": scraped_at,
        "enhanced": enhanced,
        "city": city,
        "nwm_connections": nwm_connections,
        "has_nwm": len(nwm_connections) > 0,
    }


def _get_all_leads():
    """Fetch and parse all leads from the database."""
    db = get_db()
    try:
        rows = db.execute(
            "SELECT id, payload, created_at FROM jobs "
            "WHERE job_type='raw_scrape' ORDER BY created_at DESC"
        ).fetchall()
        return [_parse_lead(r) for r in rows]
    finally:
        db.close()


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    return render_template_string(PAGE_HTML)


@app.route("/api/leads")
def api_leads():
    leads = _get_all_leads()
    return jsonify(leads)


@app.route("/export.csv")
def export_csv():
    leads = _get_all_leads()
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["Name", "LinkedIn", "Reason", "Source", "Platform", "Score", "Tier", "Date", "Enhanced"])
    for ld in leads:
        writer.writerow([
            ld["name"],
            ld["linkedin_url"] or "",
            ld["reason"],
            ld["url"],
            ld["platform"],
            ld["score"],
            ld["tier"],
            ld["scraped_at"],
            "Yes" if ld["enhanced"] else "No",
        ])
    output.seek(0)
    return Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": "attachment; filename=leads.csv"},
    )


@app.route("/api/enhance/<lead_id>", methods=["POST"])
def api_enhance(lead_id):
    """Search Serper.dev for more info on a lead."""
    db = get_db()
    try:
        row = db.execute("SELECT * FROM jobs WHERE id=?", (lead_id,)).fetchone()
        if not row:
            return jsonify({"error": "Lead not found"}), 404

        lead = _parse_lead(row)
        name = lead["name"]
        city = lead["city"] or "Utah"

        if not name or name == "Unknown":
            return jsonify({"error": "Lead has no name to search for"}), 400

        serper_key = os.environ.get("SERPER_API_KEY", "")
        if not serper_key:
            return jsonify({"error": "SERPER_API_KEY not set. Export it to enable enhance."}), 400

        import httpx

        searches = []
        found_data = {}

        queries = [
            f'"{name}" LinkedIn profile',
            f'"{name}" "{city}" professional',
            f'"{name}" email OR phone OR contact',
        ]

        for query in queries:
            try:
                resp = httpx.post(
                    "https://google.serper.dev/search",
                    json={"q": query, "num": 5, "gl": "us", "hl": "en"},
                    headers={"X-API-KEY": serper_key, "Content-Type": "application/json"},
                    timeout=15.0,
                )
                resp.raise_for_status()
                data = resp.json()
                results = []
                for item in data.get("organic", []):
                    url = item.get("link", "")
                    results.append({
                        "title": item.get("title", ""),
                        "url": url,
                        "snippet": item.get("snippet", ""),
                    })
                    if "linkedin.com/in/" in url and not found_data.get("linkedin_url"):
                        found_data["linkedin_url"] = url
                searches.append({"query": query, "results": results})
            except Exception as e:
                searches.append({"query": query, "results": [], "error": str(e)})

        # Save enhanced flag + any found linkedin
        payload = safe_json(row["payload"])
        payload_data = payload.get("data", {})
        payload_data["enhanced"] = True
        if found_data.get("linkedin_url"):
            payload_data["linkedin_url"] = found_data["linkedin_url"]
        payload["data"] = payload_data
        db.execute("UPDATE jobs SET payload=? WHERE id=?", (json.dumps(payload), lead_id))
        db.commit()

        return jsonify({"success": True, "searches": searches, "found_data": found_data})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        db.close()


@app.route("/api/enhance/<lead_id>/save", methods=["POST"])
def api_enhance_save(lead_id):
    """Save found enhancement data back to the lead."""
    db = get_db()
    try:
        row = db.execute("SELECT * FROM jobs WHERE id=?", (lead_id,)).fetchone()
        if not row:
            return jsonify({"error": "Lead not found"}), 404

        save_data = request.get_json() or {}
        payload = safe_json(row["payload"])
        payload_data = payload.get("data", {})

        if save_data.get("linkedin_url"):
            payload_data["linkedin_url"] = save_data["linkedin_url"]
        if save_data.get("email"):
            payload_data["contact_email"] = save_data["email"]
        if save_data.get("phone"):
            payload_data["contact_phone"] = save_data["phone"]

        payload_data["enhanced"] = True
        payload["data"] = payload_data
        db.execute("UPDATE jobs SET payload=? WHERE id=?", (json.dumps(payload), lead_id))
        db.commit()
        return jsonify({"success": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        db.close()


# ---------------------------------------------------------------------------
# Overview + Agents API endpoints
# ---------------------------------------------------------------------------

@app.route("/api/overview")
def api_overview():
    """Return aggregated data for the Power BI-style overview page."""
    leads = _get_all_leads()

    # Platform counts
    platform_counts = {}
    tier_counts = {"A": 0, "B": 0, "C": 0, "D": 0}
    daily_counts = {}
    nwm_count = 0
    linkedin_count = 0

    for ld in leads:
        # Platform
        p = (ld["platform"] or "unknown").lower()
        platform_counts[p] = platform_counts.get(p, 0) + 1

        # Tier
        tier_counts[ld["tier"]] = tier_counts.get(ld["tier"], 0) + 1

        # NWM
        if ld.get("has_nwm"):
            nwm_count += 1

        # LinkedIn
        if ld.get("has_linkedin"):
            linkedin_count += 1

        # Daily activity
        date_str = (ld.get("scraped_at") or "")[:10]
        if date_str:
            daily_counts[date_str] = daily_counts.get(date_str, 0) + 1

    # Sort daily counts and take last 30 days
    sorted_days = sorted(daily_counts.items())[-30:]

    # Top 5 leads by score
    top_leads = sorted(leads, key=lambda x: x.get("score", 0), reverse=True)[:5]

    # Recent activity - agent runs
    db = get_db()
    try:
        runs = db.execute(
            "SELECT id, payload, created_at FROM jobs "
            "WHERE job_type='agent_run_log' ORDER BY created_at DESC LIMIT 10"
        ).fetchall()
        recent_runs = []
        for r in runs:
            p = safe_json(r["payload"])
            recent_runs.append({
                "id": r["id"],
                "agent": p.get("agent_name", p.get("platform", "unknown")),
                "status": p.get("status", "completed"),
                "items_found": p.get("items_found", 0),
                "items_relevant": p.get("items_relevant", 0),
                "created_at": r["created_at"],
            })
    except Exception:
        recent_runs = []
    finally:
        db.close()

    # Platform-specific counts for KPI cards
    reddit_count = platform_counts.get("reddit", 0)
    craigslist_count = platform_counts.get("craigslist", 0)
    web_search_count = platform_counts.get("search", 0) + platform_counts.get("web_search", 0) + platform_counts.get("google", 0)

    return jsonify({
        "total": len(leads),
        "linkedin_count": linkedin_count,
        "reddit_count": reddit_count,
        "craigslist_count": craigslist_count,
        "web_search_count": web_search_count,
        "nwm_count": nwm_count,
        "platform_counts": platform_counts,
        "tier_counts": tier_counts,
        "daily_counts": sorted_days,
        "top_leads": [{
            "name": tl["name"],
            "reason": tl["reason"],
            "score": tl["score"],
            "tier": tl["tier"],
            "platform": tl["platform"],
            "url": tl["url"],
        } for tl in top_leads],
        "recent_runs": recent_runs,
        "leads": leads,
    })


@app.route("/api/agents")
def api_agents():
    """Return agent run history."""
    db = get_db()
    try:
        runs = db.execute(
            "SELECT id, payload, created_at FROM jobs "
            "WHERE job_type='agent_run_log' ORDER BY created_at DESC"
        ).fetchall()
        agents = {}
        all_runs = []
        for r in runs:
            p = safe_json(r["payload"])
            name = p.get("agent_name", p.get("platform", "unknown"))
            run = {
                "id": r["id"],
                "agent": name,
                "platform": p.get("platform", "unknown"),
                "status": p.get("status", "completed"),
                "items_found": p.get("items_found", 0),
                "items_relevant": p.get("items_relevant", 0),
                "duration_s": p.get("duration_s", 0),
                "error": p.get("error", ""),
                "created_at": r["created_at"],
            }
            all_runs.append(run)
            if name not in agents:
                agents[name] = {
                    "name": name,
                    "platform": p.get("platform", "unknown"),
                    "last_run": r["created_at"],
                    "total_runs": 0,
                    "total_found": 0,
                    "total_relevant": 0,
                    "last_status": p.get("status", "completed"),
                }
            agents[name]["total_runs"] += 1
            agents[name]["total_found"] += p.get("items_found", 0)
            agents[name]["total_relevant"] += p.get("items_relevant", 0)

        return jsonify({
            "agents": list(agents.values()),
            "runs": all_runs,
        })
    except Exception:
        return jsonify({"agents": [], "runs": []})
    finally:
        db.close()


@app.route("/overview")
def overview_page():
    return render_template_string(OVERVIEW_HTML)


@app.route("/agents")
def agents_page():
    return render_template_string(AGENTS_HTML)


# ---------------------------------------------------------------------------
# Overview HTML - Power BI Style Dashboard
# ---------------------------------------------------------------------------

OVERVIEW_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Lead Gen - Overview</title>
<script src="https://cdn.tailwindcss.com"></script>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
<script>tailwind.config = { darkMode: 'class' }</script>
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap');
body { font-family: 'Inter', sans-serif; }
::-webkit-scrollbar { width: 6px; }
::-webkit-scrollbar-track { background: #0f172a; }
::-webkit-scrollbar-thumb { background: #334155; border-radius: 3px; }
.kpi-card { transition: all 0.2s ease; cursor: pointer; }
.kpi-card:hover { transform: translateY(-2px); box-shadow: 0 8px 25px rgba(0,0,0,0.3); }
.kpi-card.active { ring: 2px; }
.fade-in { animation: fadeIn .3s ease-out; }
@keyframes fadeIn { from { opacity:0; transform:translateY(8px); } to { opacity:1; transform:translateY(0); } }
.chart-container { position: relative; }
</style>
</head>
<body class="bg-slate-950 text-slate-200 min-h-screen">

<!-- Nav Bar -->
<nav class="sticky top-0 z-50 bg-slate-900 border-b border-slate-800">
  <div class="max-w-screen-2xl mx-auto px-6 flex items-center h-12 gap-6">
    <div class="flex items-center gap-2">
      <div class="w-7 h-7 rounded-lg bg-gradient-to-br from-blue-500 to-purple-600 flex items-center justify-center">
        <svg class="w-4 h-4 text-white" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 10V3L4 14h7v7l9-11h-7z"/></svg>
      </div>
      <span class="text-white font-semibold text-sm">Lead Gen</span>
    </div>
    <a href="/overview" class="text-sm text-white bg-slate-800 px-3 py-1.5 rounded-lg font-medium">Overview</a>
    <a href="/" class="text-sm text-slate-400 hover:text-white transition-colors px-3 py-1.5 rounded-lg hover:bg-slate-800">Leads</a>
    <a href="/agents" class="text-sm text-slate-400 hover:text-white transition-colors px-3 py-1.5 rounded-lg hover:bg-slate-800">Agents</a>
  </div>
</nav>

<div class="max-w-screen-2xl mx-auto px-6 py-6">

  <!-- Active Filter Banner -->
  <div id="filterBanner" class="hidden mb-4 bg-blue-500/10 border border-blue-500/30 rounded-xl px-5 py-3 flex items-center justify-between fade-in">
    <div class="flex items-center gap-3">
      <svg class="w-5 h-5 text-blue-400" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M3 4a1 1 0 011-1h16a1 1 0 011 1v2.586a1 1 0 01-.293.707l-6.414 6.414a1 1 0 00-.293.707V17l-4 4v-6.586a1 1 0 00-.293-.707L3.293 7.293A1 1 0 013 6.586V4z"/></svg>
      <span class="text-blue-300 text-sm font-medium">Filtered: <span id="filterLabel" class="text-white"></span></span>
      <span class="text-blue-400/60 text-sm">(<span id="filterCount">0</span> leads)</span>
    </div>
    <button onclick="clearFilter()" class="text-xs bg-blue-500/20 hover:bg-blue-500/30 text-blue-300 px-3 py-1.5 rounded-lg transition-colors">Clear Filter</button>
  </div>

  <!-- Loading -->
  <div id="loading" class="text-center py-20 text-slate-500">
    <svg class="w-8 h-8 text-blue-400 mx-auto animate-spin mb-4" fill="none" viewBox="0 0 24 24">
      <circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle>
      <path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8v4a4 4 0 00-4 4H4z"></path>
    </svg>
    Loading dashboard data...
  </div>

  <div id="dashboard" class="hidden space-y-6">

    <!-- Row 1: KPI Cards -->
    <div class="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-4">
      <div class="kpi-card bg-slate-900 border border-slate-800 rounded-xl p-5 hover:border-slate-600" onclick="setFilter('all')" id="kpi-all">
        <p class="text-xs text-slate-500 uppercase tracking-wider font-semibold mb-2">Total Leads</p>
        <p class="text-3xl font-extrabold text-white" id="kpiTotal">0</p>
        <p class="text-xs text-slate-500 mt-1">All sources</p>
      </div>
      <div class="kpi-card bg-slate-900 border border-slate-800 rounded-xl p-5 hover:border-blue-500/50" onclick="setFilter('linkedin')" id="kpi-linkedin">
        <p class="text-xs text-blue-400 uppercase tracking-wider font-semibold mb-2">LinkedIn</p>
        <p class="text-3xl font-extrabold text-white" id="kpiLinkedin">0</p>
        <p class="text-xs text-slate-500 mt-1"><span id="kpiLinkedinPct">0</span>% of total</p>
      </div>
      <div class="kpi-card bg-slate-900 border border-slate-800 rounded-xl p-5 hover:border-orange-500/50" onclick="setFilter('reddit')" id="kpi-reddit">
        <p class="text-xs text-orange-400 uppercase tracking-wider font-semibold mb-2">Reddit</p>
        <p class="text-3xl font-extrabold text-white" id="kpiReddit">0</p>
        <p class="text-xs text-slate-500 mt-1">Posts found</p>
      </div>
      <div class="kpi-card bg-slate-900 border border-slate-800 rounded-xl p-5 hover:border-purple-500/50" onclick="setFilter('craigslist')" id="kpi-craigslist">
        <p class="text-xs text-purple-400 uppercase tracking-wider font-semibold mb-2">Craigslist</p>
        <p class="text-3xl font-extrabold text-white" id="kpiCraigslist">0</p>
        <p class="text-xs text-slate-500 mt-1">Posts found</p>
      </div>
      <div class="kpi-card bg-slate-900 border border-slate-800 rounded-xl p-5 hover:border-green-500/50" onclick="setFilter('web_search')" id="kpi-web_search">
        <p class="text-xs text-green-400 uppercase tracking-wider font-semibold mb-2">Web Search</p>
        <p class="text-3xl font-extrabold text-white" id="kpiWebSearch">0</p>
        <p class="text-xs text-slate-500 mt-1">Results found</p>
      </div>
      <div class="kpi-card bg-slate-900 border border-slate-800 rounded-xl p-5 hover:border-amber-500/50" onclick="setFilter('nwm')" id="kpi-nwm">
        <p class="text-xs text-amber-400 uppercase tracking-wider font-semibold mb-2">NWM Connected</p>
        <p class="text-3xl font-extrabold text-white" id="kpiNwm">0</p>
        <p class="text-xs text-slate-500 mt-1">Have NWM reps</p>
      </div>
    </div>

    <!-- Row 2: Charts -->
    <div class="grid grid-cols-1 lg:grid-cols-3 gap-4">
      <div class="bg-slate-900 border border-slate-800 rounded-xl p-5">
        <h3 class="text-sm font-semibold text-slate-400 mb-4">Leads by Platform</h3>
        <div class="chart-container" style="height: 260px;">
          <canvas id="platformChart"></canvas>
        </div>
      </div>
      <div class="bg-slate-900 border border-slate-800 rounded-xl p-5">
        <h3 class="text-sm font-semibold text-slate-400 mb-4">Lead Quality Distribution</h3>
        <div class="chart-container" style="height: 260px;">
          <canvas id="tierChart"></canvas>
        </div>
      </div>
      <div class="bg-slate-900 border border-slate-800 rounded-xl p-5">
        <h3 class="text-sm font-semibold text-slate-400 mb-4">Scraping Activity (Last 30 Days)</h3>
        <div class="chart-container" style="height: 260px;">
          <canvas id="activityChart"></canvas>
        </div>
      </div>
    </div>

    <!-- Row 3: Drill-Down Panel -->
    <div id="drillDown" class="bg-slate-900 border border-slate-800 rounded-xl p-5">
      <div class="flex items-center justify-between mb-4">
        <h3 class="text-sm font-semibold text-slate-400">
          <span id="drillTitle">All Leads</span>
          <span class="text-slate-600 ml-2" id="drillCount"></span>
        </h3>
      </div>
      <div class="overflow-x-auto">
        <table class="w-full text-sm">
          <thead>
            <tr class="border-b border-slate-800 text-left text-xs text-slate-500 uppercase tracking-wider">
              <th class="py-2 px-3">Name</th>
              <th class="py-2 px-3">Why Scraped</th>
              <th class="py-2 px-3">Platform</th>
              <th class="py-2 px-3">Snippet</th>
              <th class="py-2 px-3 text-center">Score</th>
              <th class="py-2 px-3">Source</th>
            </tr>
          </thead>
          <tbody id="drillBody"></tbody>
        </table>
      </div>
      <div id="drillEmpty" class="hidden text-center py-8 text-slate-600 text-sm">No leads match this filter.</div>
    </div>

    <!-- Row 4: Recent Activity + Top Leads -->
    <div class="grid grid-cols-1 lg:grid-cols-2 gap-4">
      <div class="bg-slate-900 border border-slate-800 rounded-xl p-5">
        <h3 class="text-sm font-semibold text-slate-400 mb-4">Recent Agent Activity</h3>
        <div id="recentRuns" class="space-y-2"></div>
        <div id="noRuns" class="hidden text-center py-6 text-slate-600 text-sm">No agent runs recorded yet.</div>
      </div>
      <div class="bg-slate-900 border border-slate-800 rounded-xl p-5">
        <h3 class="text-sm font-semibold text-slate-400 mb-4">Top Scoring Leads</h3>
        <div id="topLeads" class="space-y-2"></div>
      </div>
    </div>

  </div>
</div>

<script>
// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------
let overviewData = null;
let currentFilter = { type: 'all', value: null, label: 'All Leads' };
let platformChartInstance = null;
let tierChartInstance = null;
let activityChartInstance = null;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
function esc(s) {
  if (!s) return '';
  const d = document.createElement('div'); d.textContent = s; return d.innerHTML;
}

function scoreBadge(score, tier) {
  const colors = {
    'A': 'bg-green-500/20 text-green-400 border-green-500/40',
    'B': 'bg-blue-500/20 text-blue-400 border-blue-500/40',
    'C': 'bg-yellow-500/20 text-yellow-400 border-yellow-500/40',
    'D': 'bg-red-500/20 text-red-400 border-red-500/40',
  };
  const cls = colors[tier] || colors['D'];
  return '<span class="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs font-semibold border ' + cls + '">' + score + ' <span class="opacity-60">' + tier + '</span></span>';
}

function platformColor(p) {
  const map = {
    'linkedin': '#0A66C2',
    'reddit': '#FF4500',
    'craigslist': '#8b5cf6',
    'ksl': '#f59e0b',
    'facebook': '#1877F2',
    'search': '#22c55e',
    'web_search': '#22c55e',
    'google': '#22c55e',
    'indeed': '#2557a7',
  };
  return map[(p || '').toLowerCase()] || '#64748b';
}

// ---------------------------------------------------------------------------
// Filter logic
// ---------------------------------------------------------------------------
function setFilter(type, value) {
  if (type === 'all') {
    currentFilter = { type: 'all', value: null, label: 'All Leads' };
  } else if (type === 'linkedin') {
    currentFilter = { type: 'linkedin', value: true, label: 'LinkedIn Profiles' };
  } else if (type === 'reddit') {
    currentFilter = { type: 'platform', value: 'reddit', label: 'Reddit Posts' };
  } else if (type === 'craigslist') {
    currentFilter = { type: 'platform', value: 'craigslist', label: 'Craigslist Posts' };
  } else if (type === 'web_search') {
    currentFilter = { type: 'web_search', value: true, label: 'Web Search Results' };
  } else if (type === 'nwm') {
    currentFilter = { type: 'nwm', value: true, label: 'NWM Connected' };
  } else if (type === 'platform') {
    currentFilter = { type: 'platform', value: value, label: (value || 'Unknown') + ' Leads' };
  } else if (type === 'tier') {
    currentFilter = { type: 'tier', value: value, label: 'Tier ' + value + ' Leads' };
  } else if (type === 'day') {
    currentFilter = { type: 'day', value: value, label: 'Leads from ' + value };
  }

  updateFilterBanner();
  updateKPIHighlights();
  updateDrillDown();
  updateCharts();
}

function clearFilter() {
  setFilter('all');
}

function getFilteredLeads() {
  if (!overviewData || !overviewData.leads) return [];
  const leads = overviewData.leads;
  const f = currentFilter;

  if (f.type === 'all') return leads;
  if (f.type === 'linkedin') return leads.filter(l => l.has_linkedin);
  if (f.type === 'platform') return leads.filter(l => (l.platform || '').toLowerCase() === f.value);
  if (f.type === 'web_search') return leads.filter(l => ['search', 'web_search', 'google'].includes((l.platform || '').toLowerCase()));
  if (f.type === 'nwm') return leads.filter(l => l.has_nwm);
  if (f.type === 'tier') return leads.filter(l => l.tier === f.value);
  if (f.type === 'day') return leads.filter(l => (l.scraped_at || '').startsWith(f.value));
  return leads;
}

function updateFilterBanner() {
  const banner = document.getElementById('filterBanner');
  if (currentFilter.type === 'all') {
    banner.classList.add('hidden');
  } else {
    banner.classList.remove('hidden');
    document.getElementById('filterLabel').textContent = currentFilter.label;
    document.getElementById('filterCount').textContent = getFilteredLeads().length;
  }
}

function updateKPIHighlights() {
  const cards = ['all', 'linkedin', 'reddit', 'craigslist', 'web_search', 'nwm'];
  cards.forEach(c => {
    const el = document.getElementById('kpi-' + c);
    if (!el) return;
    const isActive = (currentFilter.type === 'all' && c === 'all') ||
                     (currentFilter.type === 'linkedin' && c === 'linkedin') ||
                     (currentFilter.type === 'platform' && currentFilter.value === c) ||
                     (c === 'reddit' && currentFilter.type === 'platform' && currentFilter.value === 'reddit') ||
                     (c === 'craigslist' && currentFilter.type === 'platform' && currentFilter.value === 'craigslist') ||
                     (c === 'web_search' && currentFilter.type === 'web_search') ||
                     (c === 'nwm' && currentFilter.type === 'nwm');
    if (isActive) {
      el.classList.add('ring-2', 'ring-blue-500', 'border-blue-500/50');
    } else {
      el.classList.remove('ring-2', 'ring-blue-500', 'border-blue-500/50');
    }
  });
}

// ---------------------------------------------------------------------------
// Drill-Down Table
// ---------------------------------------------------------------------------
function updateDrillDown() {
  const leads = getFilteredLeads();
  const body = document.getElementById('drillBody');
  const empty = document.getElementById('drillEmpty');
  const title = document.getElementById('drillTitle');
  const count = document.getElementById('drillCount');

  title.textContent = currentFilter.label;
  count.textContent = '(' + leads.length + ' leads)';

  if (leads.length === 0) {
    body.innerHTML = '';
    empty.classList.remove('hidden');
    return;
  }
  empty.classList.add('hidden');

  // Show top 50 in drill-down
  const shown = leads.slice(0, 50);
  body.innerHTML = shown.map((l, i) => {
    const rowBg = i % 2 === 0 ? 'bg-slate-950' : 'bg-slate-900/30';
    const snippet = l.snippet ? esc(l.snippet).substring(0, 120) + (l.snippet.length > 120 ? '...' : '') : '<span class="text-slate-600">--</span>';
    const reason = l.reason ? esc(l.reason) : '<span class="text-slate-600">--</span>';
    return '<tr class="' + rowBg + ' hover:bg-slate-800/50 transition-colors border-b border-slate-800/50">' +
      '<td class="py-2.5 px-3"><span class="text-white font-medium text-sm">' + esc(l.name) + '</span></td>' +
      '<td class="py-2.5 px-3 text-xs text-slate-400 max-w-xs truncate">' + reason + '</td>' +
      '<td class="py-2.5 px-3"><span class="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs border border-slate-700 bg-slate-800/50 text-slate-300"><span class="w-2 h-2 rounded-full inline-block" style="background:' + platformColor(l.platform) + '"></span>' + esc(l.platform) + '</span></td>' +
      '<td class="py-2.5 px-3 text-xs text-slate-500 max-w-sm">' + snippet + '</td>' +
      '<td class="py-2.5 px-3 text-center">' + scoreBadge(l.score, l.tier) + '</td>' +
      '<td class="py-2.5 px-3">' + (l.url ? '<a href="' + esc(l.url) + '" target="_blank" class="text-blue-400 hover:text-blue-300 text-xs hover:underline">View</a>' : '--') + '</td>' +
      '</tr>';
  }).join('');
}

// ---------------------------------------------------------------------------
// Charts
// ---------------------------------------------------------------------------
function buildCharts() {
  buildPlatformChart();
  buildTierChart();
  buildActivityChart();
}

function updateCharts() {
  // When filtered, update chart visuals to reflect the selection
  if (platformChartInstance) updatePlatformChart();
  if (tierChartInstance) updateTierChart();
  if (activityChartInstance) updateActivityChart();
}

function buildPlatformChart() {
  const ctx = document.getElementById('platformChart').getContext('2d');
  const pc = overviewData.platform_counts;
  const labels = Object.keys(pc);
  const data = Object.values(pc);
  const colors = labels.map(l => platformColor(l));

  platformChartInstance = new Chart(ctx, {
    type: 'doughnut',
    data: {
      labels: labels.map(l => l.charAt(0).toUpperCase() + l.slice(1)),
      datasets: [{
        data: data,
        backgroundColor: colors,
        borderColor: '#0f172a',
        borderWidth: 3,
        hoverOffset: 8,
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      onClick: (evt, elements) => {
        if (elements.length > 0) {
          const idx = elements[0].index;
          const platform = labels[idx];
          setFilter('platform', platform);
        }
      },
      plugins: {
        legend: {
          position: 'right',
          labels: { color: '#94a3b8', font: { size: 11 }, padding: 12, usePointStyle: true, pointStyleWidth: 10 }
        }
      },
      cutout: '60%',
    }
  });
}

function updatePlatformChart() {
  if (!platformChartInstance) return;
  const leads = getFilteredLeads();
  const pc = {};
  leads.forEach(l => {
    const p = (l.platform || 'unknown').toLowerCase();
    pc[p] = (pc[p] || 0) + 1;
  });
  const labels = Object.keys(pc);
  const data = Object.values(pc);
  const colors = labels.map(l => platformColor(l));

  platformChartInstance.data.labels = labels.map(l => l.charAt(0).toUpperCase() + l.slice(1));
  platformChartInstance.data.datasets[0].data = data;
  platformChartInstance.data.datasets[0].backgroundColor = colors;
  platformChartInstance.update();
}

function buildTierChart() {
  const ctx = document.getElementById('tierChart').getContext('2d');
  const tc = overviewData.tier_counts;

  tierChartInstance = new Chart(ctx, {
    type: 'bar',
    data: {
      labels: ['A (75+)', 'B (50-74)', 'C (25-49)', 'D (<25)'],
      datasets: [{
        label: 'Leads',
        data: [tc['A'] || 0, tc['B'] || 0, tc['C'] || 0, tc['D'] || 0],
        backgroundColor: ['#22c55e40', '#3b82f640', '#eab30840', '#ef444440'],
        borderColor: ['#22c55e', '#3b82f6', '#eab308', '#ef4444'],
        borderWidth: 1.5,
        borderRadius: 6,
        barPercentage: 0.7,
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      onClick: (evt, elements) => {
        if (elements.length > 0) {
          const idx = elements[0].index;
          const tiers = ['A', 'B', 'C', 'D'];
          setFilter('tier', tiers[idx]);
        }
      },
      scales: {
        y: {
          beginAtZero: true,
          grid: { color: '#1e293b' },
          ticks: { color: '#64748b', font: { size: 11 } }
        },
        x: {
          grid: { display: false },
          ticks: { color: '#94a3b8', font: { size: 11, weight: 600 } }
        }
      },
      plugins: { legend: { display: false } }
    }
  });
}

function updateTierChart() {
  if (!tierChartInstance) return;
  const leads = getFilteredLeads();
  const tc = { A: 0, B: 0, C: 0, D: 0 };
  leads.forEach(l => { tc[l.tier] = (tc[l.tier] || 0) + 1; });
  tierChartInstance.data.datasets[0].data = [tc['A'], tc['B'], tc['C'], tc['D']];
  tierChartInstance.update();
}

function buildActivityChart() {
  const ctx = document.getElementById('activityChart').getContext('2d');
  const dc = overviewData.daily_counts;
  const labels = dc.map(d => {
    const parts = d[0].split('-');
    return parts[1] + '/' + parts[2];
  });
  const data = dc.map(d => d[1]);

  activityChartInstance = new Chart(ctx, {
    type: 'line',
    data: {
      labels: labels,
      datasets: [{
        label: 'Leads scraped',
        data: data,
        borderColor: '#3b82f6',
        backgroundColor: 'rgba(59, 130, 246, 0.1)',
        borderWidth: 2,
        fill: true,
        tension: 0.3,
        pointRadius: 4,
        pointHoverRadius: 7,
        pointBackgroundColor: '#3b82f6',
        pointBorderColor: '#0f172a',
        pointBorderWidth: 2,
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      onClick: (evt, elements) => {
        if (elements.length > 0) {
          const idx = elements[0].index;
          const day = overviewData.daily_counts[idx][0];
          setFilter('day', day);
        }
      },
      scales: {
        y: {
          beginAtZero: true,
          grid: { color: '#1e293b' },
          ticks: { color: '#64748b', font: { size: 11 } }
        },
        x: {
          grid: { display: false },
          ticks: { color: '#64748b', font: { size: 10 }, maxRotation: 45 }
        }
      },
      plugins: { legend: { display: false } }
    }
  });
}

function updateActivityChart() {
  if (!activityChartInstance) return;
  const leads = getFilteredLeads();
  const dc = {};
  leads.forEach(l => {
    const d = (l.scraped_at || '').substring(0, 10);
    if (d) dc[d] = (dc[d] || 0) + 1;
  });
  const sorted = Object.entries(dc).sort().slice(-30);
  const labels = sorted.map(d => { const p = d[0].split('-'); return p[1] + '/' + p[2]; });
  const data = sorted.map(d => d[1]);

  activityChartInstance.data.labels = labels;
  activityChartInstance.data.datasets[0].data = data;
  activityChartInstance.update();
}

// ---------------------------------------------------------------------------
// Bottom sections
// ---------------------------------------------------------------------------
function renderRecentRuns() {
  const runs = overviewData.recent_runs || [];
  const container = document.getElementById('recentRuns');
  const noRuns = document.getElementById('noRuns');

  if (runs.length === 0) {
    container.innerHTML = '';
    noRuns.classList.remove('hidden');
    return;
  }
  noRuns.classList.add('hidden');

  container.innerHTML = runs.map(r => {
    const statusColor = r.status === 'completed' ? 'text-green-400' : r.status === 'running' ? 'text-blue-400' : 'text-red-400';
    const statusIcon = r.status === 'completed'
      ? '<svg class="w-4 h-4 text-green-400" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 13l4 4L19 7"/></svg>'
      : r.status === 'running'
      ? '<svg class="w-4 h-4 text-blue-400 animate-spin" fill="none" viewBox="0 0 24 24"><circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle><path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8v4a4 4 0 00-4 4H4z"></path></svg>'
      : '<svg class="w-4 h-4 text-red-400" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"/></svg>';
    const dateStr = r.created_at ? new Date(r.created_at).toLocaleString('en-US', { month:'short', day:'numeric', hour:'numeric', minute:'2-digit' }) : '--';
    return '<div class="flex items-center gap-3 bg-slate-800/40 rounded-lg px-4 py-3">' +
      statusIcon +
      '<div class="flex-1 min-w-0">' +
        '<p class="text-sm text-white font-medium truncate">' + esc(r.agent) + '</p>' +
        '<p class="text-xs text-slate-500">' + dateStr + '</p>' +
      '</div>' +
      '<div class="text-right">' +
        '<p class="text-xs text-slate-400">' + (r.items_found || 0) + ' found</p>' +
        '<p class="text-xs text-slate-500">' + (r.items_relevant || 0) + ' relevant</p>' +
      '</div>' +
    '</div>';
  }).join('');
}

function renderTopLeads() {
  const leads = overviewData.top_leads || [];
  const container = document.getElementById('topLeads');

  if (leads.length === 0) {
    container.innerHTML = '<div class="text-center py-6 text-slate-600 text-sm">No leads yet.</div>';
    return;
  }

  container.innerHTML = leads.map((l, i) => {
    return '<div class="flex items-center gap-3 bg-slate-800/40 rounded-lg px-4 py-3">' +
      '<div class="w-7 h-7 rounded-full bg-gradient-to-br from-blue-500/30 to-purple-500/30 flex items-center justify-center text-xs font-bold text-white">' + (i+1) + '</div>' +
      '<div class="flex-1 min-w-0">' +
        '<p class="text-sm text-white font-medium truncate">' + esc(l.name) + '</p>' +
        '<p class="text-xs text-slate-500 truncate">' + esc(l.reason || l.platform) + '</p>' +
      '</div>' +
      '<div class="text-right">' +
        scoreBadge(l.score, l.tier) +
      '</div>' +
      (l.url ? '<a href="' + esc(l.url) + '" target="_blank" class="text-blue-400 hover:text-blue-300 ml-2"><svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M10 6H6a2 2 0 00-2 2v10a2 2 0 002 2h10a2 2 0 002-2v-4M14 4h6m0 0v6m0-6L10 14"/></svg></a>' : '') +
    '</div>';
  }).join('');
}

// ---------------------------------------------------------------------------
// Init
// ---------------------------------------------------------------------------
async function init() {
  try {
    const resp = await fetch('/api/overview');
    overviewData = await resp.json();

    // Populate KPIs
    document.getElementById('kpiTotal').textContent = overviewData.total;
    document.getElementById('kpiLinkedin').textContent = overviewData.linkedin_count;
    document.getElementById('kpiLinkedinPct').textContent = overviewData.total > 0 ? Math.round(overviewData.linkedin_count / overviewData.total * 100) : 0;
    document.getElementById('kpiReddit').textContent = overviewData.reddit_count;
    document.getElementById('kpiCraigslist').textContent = overviewData.craigslist_count;
    document.getElementById('kpiWebSearch').textContent = overviewData.web_search_count;
    document.getElementById('kpiNwm').textContent = overviewData.nwm_count;

    // Build charts
    buildCharts();

    // Bottom sections
    renderRecentRuns();
    renderTopLeads();

    // Initial drill-down
    updateDrillDown();
    updateKPIHighlights();

    // Show dashboard
    document.getElementById('loading').classList.add('hidden');
    document.getElementById('dashboard').classList.remove('hidden');
  } catch (e) {
    document.getElementById('loading').textContent = 'Failed to load: ' + e.message;
  }
}

init();
</script>
</body>
</html>"""


# ---------------------------------------------------------------------------
# Agents HTML - Agent Status & Run History
# ---------------------------------------------------------------------------

AGENTS_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Lead Gen - Agents</title>
<script src="https://cdn.tailwindcss.com"></script>
<script>tailwind.config = { darkMode: 'class' }</script>
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
body { font-family: 'Inter', sans-serif; }
::-webkit-scrollbar { width: 6px; }
::-webkit-scrollbar-track { background: #0f172a; }
::-webkit-scrollbar-thumb { background: #334155; border-radius: 3px; }
.fade-in { animation: fadeIn .25s ease-out; }
@keyframes fadeIn { from { opacity:0; transform:translateY(8px); } to { opacity:1; transform:translateY(0); } }
</style>
</head>
<body class="bg-slate-950 text-slate-200 min-h-screen">

<!-- Nav Bar -->
<nav class="sticky top-0 z-50 bg-slate-900 border-b border-slate-800">
  <div class="max-w-screen-2xl mx-auto px-6 flex items-center h-12 gap-6">
    <div class="flex items-center gap-2">
      <div class="w-7 h-7 rounded-lg bg-gradient-to-br from-blue-500 to-purple-600 flex items-center justify-center">
        <svg class="w-4 h-4 text-white" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 10V3L4 14h7v7l9-11h-7z"/></svg>
      </div>
      <span class="text-white font-semibold text-sm">Lead Gen</span>
    </div>
    <a href="/overview" class="text-sm text-slate-400 hover:text-white transition-colors px-3 py-1.5 rounded-lg hover:bg-slate-800">Overview</a>
    <a href="/" class="text-sm text-slate-400 hover:text-white transition-colors px-3 py-1.5 rounded-lg hover:bg-slate-800">Leads</a>
    <a href="/agents" class="text-sm text-white bg-slate-800 px-3 py-1.5 rounded-lg font-medium">Agents</a>
  </div>
</nav>

<div class="max-w-screen-2xl mx-auto px-6 py-6">

  <div class="flex items-center justify-between mb-6">
    <h1 class="text-xl font-bold text-white">Agent Status &amp; Run History</h1>
    <span id="agentCount" class="text-sm text-slate-500"></span>
  </div>

  <!-- Loading -->
  <div id="loading" class="text-center py-20 text-slate-500">Loading agent data...</div>

  <div id="content" class="hidden space-y-6">

    <!-- Agents Summary Table -->
    <div class="bg-slate-900 border border-slate-800 rounded-xl overflow-hidden">
      <table class="w-full text-sm">
        <thead>
          <tr class="border-b border-slate-800 text-left text-xs text-slate-500 uppercase tracking-wider bg-slate-900/80">
            <th class="py-3 px-4">Agent</th>
            <th class="py-3 px-4">Platform</th>
            <th class="py-3 px-4">Last Run</th>
            <th class="py-3 px-4 text-center">Total Runs</th>
            <th class="py-3 px-4 text-center">Items Found</th>
            <th class="py-3 px-4 text-center">Items Relevant</th>
            <th class="py-3 px-4 text-center">Status</th>
          </tr>
        </thead>
        <tbody id="agentsBody"></tbody>
      </table>
      <div id="noAgents" class="hidden text-center py-12 text-slate-600">
        <svg class="w-12 h-12 mx-auto mb-3 text-slate-700" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="1.5" d="M9.75 17L9 20l-1 1h8l-1-1-.75-3M3 13h18M5 17h14a2 2 0 002-2V5a2 2 0 00-2-2H5a2 2 0 00-2 2v10a2 2 0 002 2z"/></svg>
        <p class="text-sm">No agent runs recorded yet.</p>
        <p class="text-xs text-slate-700 mt-1">Run your scraping agents to see data here.</p>
      </div>
    </div>

    <!-- Run History (shown when agent is clicked) -->
    <div id="runHistory" class="hidden bg-slate-900 border border-slate-800 rounded-xl p-5 fade-in">
      <div class="flex items-center justify-between mb-4">
        <h3 class="text-sm font-semibold text-white">Run History: <span id="historyAgentName" class="text-blue-400"></span></h3>
        <button onclick="hideHistory()" class="text-xs bg-slate-800 hover:bg-slate-700 text-slate-400 px-3 py-1.5 rounded-lg transition-colors">Close</button>
      </div>
      <table class="w-full text-sm">
        <thead>
          <tr class="border-b border-slate-800 text-left text-xs text-slate-500 uppercase tracking-wider">
            <th class="py-2 px-3">Date</th>
            <th class="py-2 px-3 text-center">Found</th>
            <th class="py-2 px-3 text-center">Relevant</th>
            <th class="py-2 px-3 text-center">Duration</th>
            <th class="py-2 px-3 text-center">Status</th>
            <th class="py-2 px-3">Error</th>
          </tr>
        </thead>
        <tbody id="historyBody"></tbody>
      </table>
    </div>

    <!-- All Runs Timeline -->
    <div class="bg-slate-900 border border-slate-800 rounded-xl p-5">
      <h3 class="text-sm font-semibold text-slate-400 mb-4">Full Run Timeline</h3>
      <div id="timeline" class="space-y-2 max-h-96 overflow-y-auto"></div>
      <div id="noTimeline" class="hidden text-center py-6 text-slate-600 text-sm">No runs to display.</div>
    </div>

  </div>
</div>

<script>
let agentData = null;

function esc(s) {
  if (!s) return '';
  const d = document.createElement('div'); d.textContent = s; return d.innerHTML;
}

function platformColor(p) {
  const map = { 'linkedin':'#0A66C2', 'reddit':'#FF4500', 'craigslist':'#8b5cf6', 'ksl':'#f59e0b', 'facebook':'#1877F2', 'search':'#22c55e' };
  return map[(p||'').toLowerCase()] || '#64748b';
}

function formatDate(d) {
  if (!d) return '--';
  try { return new Date(d).toLocaleString('en-US', { month:'short', day:'numeric', hour:'numeric', minute:'2-digit' }); }
  catch(_) { return d.substring(0,16); }
}

function statusBadge(status) {
  if (status === 'completed') return '<span class="px-2 py-0.5 rounded-full text-xs font-medium bg-green-500/15 text-green-400 border border-green-500/30">Completed</span>';
  if (status === 'running') return '<span class="px-2 py-0.5 rounded-full text-xs font-medium bg-blue-500/15 text-blue-400 border border-blue-500/30">Running</span>';
  return '<span class="px-2 py-0.5 rounded-full text-xs font-medium bg-red-500/15 text-red-400 border border-red-500/30">Failed</span>';
}

function showHistory(agentName) {
  const panel = document.getElementById('runHistory');
  document.getElementById('historyAgentName').textContent = agentName;
  const runs = (agentData.runs || []).filter(r => r.agent === agentName);
  const body = document.getElementById('historyBody');

  body.innerHTML = runs.map((r, i) => {
    const bg = i % 2 === 0 ? 'bg-slate-950' : 'bg-slate-900/30';
    const dur = r.duration_s ? r.duration_s + 's' : '--';
    return '<tr class="' + bg + ' border-b border-slate-800/50">' +
      '<td class="py-2 px-3 text-xs text-slate-300">' + formatDate(r.created_at) + '</td>' +
      '<td class="py-2 px-3 text-center text-xs text-white font-medium">' + (r.items_found || 0) + '</td>' +
      '<td class="py-2 px-3 text-center text-xs text-white font-medium">' + (r.items_relevant || 0) + '</td>' +
      '<td class="py-2 px-3 text-center text-xs text-slate-400">' + dur + '</td>' +
      '<td class="py-2 px-3 text-center">' + statusBadge(r.status) + '</td>' +
      '<td class="py-2 px-3 text-xs text-red-400 max-w-xs truncate">' + esc(r.error || '') + '</td>' +
    '</tr>';
  }).join('');

  panel.classList.remove('hidden');
  panel.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
}

function hideHistory() {
  document.getElementById('runHistory').classList.add('hidden');
}

function renderAgents() {
  const agents = agentData.agents || [];
  const body = document.getElementById('agentsBody');
  const noAgents = document.getElementById('noAgents');

  document.getElementById('agentCount').textContent = agents.length + ' agent' + (agents.length !== 1 ? 's' : '');

  if (agents.length === 0) {
    noAgents.classList.remove('hidden');
    return;
  }
  noAgents.classList.add('hidden');

  body.innerHTML = agents.map((a, i) => {
    const bg = i % 2 === 0 ? 'bg-slate-950' : 'bg-slate-900/30';
    return '<tr class="' + bg + ' hover:bg-slate-800/50 transition-colors border-b border-slate-800/50 cursor-pointer" onclick="showHistory(\'' + esc(a.name).replace(/'/g, "\\'") + '\')">' +
      '<td class="py-3 px-4"><span class="text-white font-medium">' + esc(a.name) + '</span></td>' +
      '<td class="py-3 px-4"><span class="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs border border-slate-700 bg-slate-800/50 text-slate-300"><span class="w-2 h-2 rounded-full inline-block" style="background:' + platformColor(a.platform) + '"></span>' + esc(a.platform) + '</span></td>' +
      '<td class="py-3 px-4 text-xs text-slate-400">' + formatDate(a.last_run) + '</td>' +
      '<td class="py-3 px-4 text-center text-white font-medium">' + a.total_runs + '</td>' +
      '<td class="py-3 px-4 text-center text-white font-medium">' + a.total_found + '</td>' +
      '<td class="py-3 px-4 text-center text-white font-medium">' + a.total_relevant + '</td>' +
      '<td class="py-3 px-4 text-center">' + statusBadge(a.last_status) + '</td>' +
    '</tr>';
  }).join('');
}

function renderTimeline() {
  const runs = agentData.runs || [];
  const container = document.getElementById('timeline');
  const noTimeline = document.getElementById('noTimeline');

  if (runs.length === 0) {
    noTimeline.classList.remove('hidden');
    return;
  }
  noTimeline.classList.add('hidden');

  container.innerHTML = runs.slice(0, 50).map(r => {
    const statusIcon = r.status === 'completed'
      ? '<div class="w-8 h-8 rounded-full bg-green-500/15 flex items-center justify-center flex-shrink-0"><svg class="w-4 h-4 text-green-400" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 13l4 4L19 7"/></svg></div>'
      : '<div class="w-8 h-8 rounded-full bg-red-500/15 flex items-center justify-center flex-shrink-0"><svg class="w-4 h-4 text-red-400" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"/></svg></div>';
    return '<div class="flex items-center gap-3 bg-slate-800/30 rounded-lg px-4 py-3">' +
      statusIcon +
      '<div class="flex-1 min-w-0">' +
        '<p class="text-sm text-white font-medium">' + esc(r.agent) + '</p>' +
        '<p class="text-xs text-slate-500">' + formatDate(r.created_at) + '</p>' +
      '</div>' +
      '<div class="text-right text-xs">' +
        '<span class="text-slate-400">' + (r.items_found || 0) + ' found</span>' +
        '<span class="text-slate-600 mx-1">/</span>' +
        '<span class="text-slate-300">' + (r.items_relevant || 0) + ' relevant</span>' +
      '</div>' +
    '</div>';
  }).join('');
}

async function init() {
  try {
    const resp = await fetch('/api/agents');
    agentData = await resp.json();
    renderAgents();
    renderTimeline();
    document.getElementById('loading').classList.add('hidden');
    document.getElementById('content').classList.remove('hidden');
  } catch (e) {
    document.getElementById('loading').textContent = 'Failed to load: ' + e.message;
  }
}

init();
</script>
</body>
</html>"""


# ---------------------------------------------------------------------------
# Single-page HTML template (all JS is client-side)
# ---------------------------------------------------------------------------

PAGE_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Lead Gen Dashboard</title>
<script src="https://cdn.tailwindcss.com"></script>
<script>
tailwind.config = { darkMode: 'class', theme: { extend: {} } }
</script>
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
body { font-family: 'Inter', sans-serif; }
::-webkit-scrollbar { width: 6px; }
::-webkit-scrollbar-track { background: #0f172a; }
::-webkit-scrollbar-thumb { background: #334155; border-radius: 3px; }
.fade-in { animation: fadeIn .25s ease-out; }
@keyframes fadeIn { from { opacity:0; transform:translateY(8px); } to { opacity:1; transform:translateY(0); } }
.slide-in { animation: slideIn .3s ease-out; }
@keyframes slideIn { from { transform:translateX(100%); } to { transform:translateX(0); } }
</style>
</head>
<body class="bg-slate-950 text-slate-200 min-h-screen">

<!-- Nav Bar -->
<nav class="sticky top-0 z-50 bg-slate-900 border-b border-slate-800">
  <div class="max-w-screen-2xl mx-auto px-6 flex items-center h-12 gap-6">
    <div class="flex items-center gap-2">
      <div class="w-7 h-7 rounded-lg bg-gradient-to-br from-blue-500 to-purple-600 flex items-center justify-center">
        <svg class="w-4 h-4 text-white" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 10V3L4 14h7v7l9-11h-7z"/></svg>
      </div>
      <span class="text-white font-semibold text-sm">Lead Gen</span>
    </div>
    <a href="/overview" class="text-sm text-slate-400 hover:text-white transition-colors px-3 py-1.5 rounded-lg hover:bg-slate-800">Overview</a>
    <a href="/" class="text-sm text-white bg-slate-800 px-3 py-1.5 rounded-lg font-medium">Leads</a>
    <a href="/agents" class="text-sm text-slate-400 hover:text-white transition-colors px-3 py-1.5 rounded-lg hover:bg-slate-800">Agents</a>
  </div>
</nav>

<!-- Stats Bar -->
<div id="statsBar" class="bg-slate-900 border-b border-slate-800 px-6 py-3">
  <div class="max-w-screen-2xl mx-auto flex flex-wrap items-center gap-6 text-sm">
    <div class="flex items-center gap-2">
      <div class="w-8 h-8 rounded-lg bg-gradient-to-br from-blue-500 to-purple-600 flex items-center justify-center">
        <svg class="w-5 h-5 text-white" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 10V3L4 14h7v7l9-11h-7z"/></svg>
      </div>
      <span class="font-bold text-white text-base">Lead Gen</span>
    </div>
    <div class="flex flex-wrap items-center gap-4 text-slate-400">
      <span>Total: <strong class="text-white" id="statTotal">--</strong></span>
      <span class="text-slate-700">|</span>
      <span>LinkedIn: <strong class="text-blue-400" id="statLinkedin">--</strong></span>
      <span class="text-slate-700">|</span>
      <span>A-tier: <strong class="text-green-400" id="statAtier">--</strong></span>
      <span class="text-slate-700">|</span>
      <span>Enhanced: <strong class="text-purple-400" id="statEnhanced">--</strong></span>
      <span class="text-slate-700">|</span>
      <span>Last scrape: <strong class="text-slate-300" id="statLastScrape">--</strong></span>
    </div>
    <div class="ml-auto">
      <a href="/export.csv" class="text-xs bg-slate-800 hover:bg-slate-700 text-slate-300 px-3 py-1.5 rounded-lg transition-colors">Export CSV</a>
    </div>
  </div>
</div>

<!-- Filters -->
<div class="bg-slate-900/60 border-b border-slate-800 px-6 py-3 sticky top-12 z-20 backdrop-blur-sm">
  <div class="max-w-screen-2xl mx-auto flex flex-wrap items-center gap-4">
    <input id="searchBox" type="text" placeholder="Search leads..."
      class="bg-slate-800 border border-slate-700 rounded-lg px-4 py-2 text-sm text-white placeholder-slate-500 focus:outline-none focus:border-blue-500 w-64" />
    <select id="sortSelect" class="bg-slate-800 border border-slate-700 rounded-lg px-3 py-2 text-sm text-white focus:outline-none focus:border-blue-500">
      <option value="score-desc">Score (high to low)</option>
      <option value="score-asc">Score (low to high)</option>
      <option value="date-desc" selected>Date (newest)</option>
      <option value="date-asc">Date (oldest)</option>
      <option value="name-asc">Name (A-Z)</option>
      <option value="name-desc">Name (Z-A)</option>
    </select>
    <label class="flex items-center gap-2 cursor-pointer select-none">
      <div class="relative">
        <input id="linkedinToggle" type="checkbox" class="sr-only peer" />
        <div class="w-9 h-5 bg-slate-700 peer-focus:outline-none rounded-full peer peer-checked:bg-blue-600 transition-colors"></div>
        <div class="absolute left-0.5 top-0.5 w-4 h-4 bg-white rounded-full transition-transform peer-checked:translate-x-4"></div>
      </div>
      <span class="text-sm text-slate-400">Has LinkedIn</span>
    </label>
    <label class="flex items-center gap-2 cursor-pointer select-none">
      <div class="relative">
        <input id="atierToggle" type="checkbox" class="sr-only peer" />
        <div class="w-9 h-5 bg-slate-700 peer-focus:outline-none rounded-full peer peer-checked:bg-green-600 transition-colors"></div>
        <div class="absolute left-0.5 top-0.5 w-4 h-4 bg-white rounded-full transition-transform peer-checked:translate-x-4"></div>
      </div>
      <span class="text-sm text-slate-400">A-tier only</span>
    </label>
    <label class="flex items-center gap-2 cursor-pointer select-none">
      <div class="relative">
        <input id="nwmToggle" type="checkbox" class="sr-only peer" />
        <div class="w-9 h-5 bg-slate-700 peer-focus:outline-none rounded-full peer peer-checked:bg-amber-600 transition-colors"></div>
        <div class="absolute left-0.5 top-0.5 w-4 h-4 bg-white rounded-full transition-transform peer-checked:translate-x-4"></div>
      </div>
      <span class="text-sm text-slate-400">NWM connected</span>
    </label>
    <span id="resultCount" class="text-xs text-slate-500 ml-auto"></span>
  </div>
</div>

<!-- Table -->
<div class="max-w-screen-2xl mx-auto px-6 py-4">
  <div id="loading" class="text-center py-20 text-slate-500">Loading leads...</div>
  <div id="tableWrap" class="hidden">
    <table class="w-full text-sm">
      <thead>
        <tr class="border-b border-slate-800 text-left text-xs text-slate-500 uppercase tracking-wider">
          <th class="py-3 px-3 w-8">#</th>
          <th class="py-3 px-3">Name / Title</th>
          <th class="py-3 px-3">LinkedIn</th>
          <th class="py-3 px-3">Why Scraped</th>
          <th class="py-3 px-3">Source</th>
          <th class="py-3 px-3 text-center">Score</th>
          <th class="py-3 px-3">Freshness</th>
          <th class="py-3 px-3 text-center">Actions</th>
        </tr>
      </thead>
      <tbody id="leadsBody"></tbody>
    </table>
  </div>
  <div id="emptyState" class="hidden text-center py-20 text-slate-500">
    <p class="text-lg">No leads found</p>
    <p class="text-sm mt-1">Adjust your filters or run a scrape.</p>
  </div>
</div>

<!-- Enhance Slide-out Panel -->
<div id="enhancePanel" class="fixed inset-y-0 right-0 w-full max-w-lg bg-slate-900 border-l border-slate-800 shadow-2xl z-50 hidden">
  <div class="slide-in h-full flex flex-col">
    <div class="flex items-center justify-between px-6 py-4 border-b border-slate-800">
      <h3 class="text-lg font-semibold text-white" id="panelTitle">Enhance Lead</h3>
      <button onclick="closePanel()" class="text-slate-400 hover:text-white p-1">
        <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"/></svg>
      </button>
    </div>
    <div class="flex-1 overflow-y-auto p-6">
      <div id="panelInfo" class="mb-5"></div>
      <button id="panelEnhanceBtn" onclick="runEnhance()"
        class="w-full bg-gradient-to-r from-blue-600 to-purple-600 hover:from-blue-700 hover:to-purple-700 text-white font-semibold py-3 px-6 rounded-xl transition-all text-base mb-5">
        Enhance with Serper.dev
      </button>
      <div id="panelLoading" class="hidden text-center py-8">
        <svg class="w-7 h-7 text-blue-400 mx-auto animate-spin" fill="none" viewBox="0 0 24 24">
          <circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle>
          <path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8v4a4 4 0 00-4 4H4z"></path>
        </svg>
        <p class="text-sm text-slate-400 mt-3">Searching...</p>
      </div>
      <div id="panelResults" class="hidden space-y-4"></div>
      <div id="panelError" class="hidden bg-red-500/10 border border-red-500/30 rounded-lg p-4 mt-4">
        <p class="text-sm text-red-400" id="panelErrorMsg"></p>
      </div>
    </div>
  </div>
</div>
<div id="panelBackdrop" class="fixed inset-0 bg-black/40 backdrop-blur-sm z-40 hidden" onclick="closePanel()"></div>

<script>
// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------
let allLeads = [];
let currentLeadId = null;

// ---------------------------------------------------------------------------
// Init: fetch leads and render
// ---------------------------------------------------------------------------
async function init() {
  try {
    const resp = await fetch('/api/leads');
    allLeads = await resp.json();
    updateStats();
    renderTable();
    document.getElementById('loading').classList.add('hidden');
    document.getElementById('tableWrap').classList.remove('hidden');
  } catch (e) {
    document.getElementById('loading').textContent = 'Failed to load leads: ' + e.message;
  }
}

// ---------------------------------------------------------------------------
// Stats
// ---------------------------------------------------------------------------
function updateStats() {
  const total = allLeads.length;
  const linkedin = allLeads.filter(l => l.has_linkedin).length;
  const atier = allLeads.filter(l => l.tier === 'A').length;
  const enhanced = allLeads.filter(l => l.enhanced).length;
  let lastScrape = '--';
  if (allLeads.length > 0) {
    const d = allLeads[0].scraped_at;
    if (d) {
      try {
        const dt = new Date(d);
        lastScrape = dt.toLocaleDateString('en-US', { month:'short', day:'numeric', hour:'numeric', minute:'2-digit' });
      } catch(_) { lastScrape = d.substring(0,16); }
    }
  }
  document.getElementById('statTotal').textContent = total;
  document.getElementById('statLinkedin').textContent = linkedin;
  document.getElementById('statAtier').textContent = atier;
  document.getElementById('statEnhanced').textContent = enhanced;
  document.getElementById('statLastScrape').textContent = lastScrape;
}

// ---------------------------------------------------------------------------
// Freshness and detail toggle
// ---------------------------------------------------------------------------
function getFreshness(dateStr) {
  if (!dateStr) return '<span class="text-slate-600">Unknown</span>';
  try {
    const d = new Date(dateStr);
    const now = new Date();
    const hours = Math.floor((now - d) / 3600000);
    if (hours < 1) return '<span class="text-emerald-400 font-semibold">Just now</span>';
    if (hours < 24) return '<span class="text-emerald-400">' + hours + 'h ago</span>';
    const days = Math.floor(hours / 24);
    if (days <= 1) return '<span class="text-green-400">1 day</span>';
    if (days <= 7) return '<span class="text-green-400">' + days + ' days</span>';
    if (days <= 14) return '<span class="text-yellow-400">' + days + ' days</span>';
    if (days <= 30) return '<span class="text-orange-400">' + days + ' days</span>';
    return '<span class="text-red-400">' + days + ' days (stale)</span>';
  } catch(e) { return '<span class="text-slate-600">Unknown</span>'; }
}

function toggleDetail(i) {
  const row = document.getElementById('detail-' + i);
  if (row) row.classList.toggle('hidden');
}

// ---------------------------------------------------------------------------
// Filter + Sort
// ---------------------------------------------------------------------------
function getFiltered() {
  const query = document.getElementById('searchBox').value.toLowerCase().trim();
  const linkedinOnly = document.getElementById('linkedinToggle').checked;
  const atierOnly = document.getElementById('atierToggle').checked;
  const nwmOnly = document.getElementById('nwmToggle').checked;
  const sort = document.getElementById('sortSelect').value;

  let list = allLeads;

  if (linkedinOnly) list = list.filter(l => l.has_linkedin);
  if (atierOnly) list = list.filter(l => l.tier === 'A');
  if (nwmOnly) list = list.filter(l => l.has_nwm);
  if (query) {
    list = list.filter(l =>
      (l.name || '').toLowerCase().includes(query) ||
      (l.reason || '').toLowerCase().includes(query) ||
      (l.snippet || '').toLowerCase().includes(query) ||
      (l.source_label || '').toLowerCase().includes(query) ||
      (l.platform || '').toLowerCase().includes(query) ||
      (l.url || '').toLowerCase().includes(query) ||
      (l.city || '').toLowerCase().includes(query) ||
      (l.title || '').toLowerCase().includes(query)
    );
  }

  list = [...list];
  switch (sort) {
    case 'score-desc': list.sort((a,b) => b.score - a.score); break;
    case 'score-asc':  list.sort((a,b) => a.score - b.score); break;
    case 'date-desc':  list.sort((a,b) => (b.scraped_at||'').localeCompare(a.scraped_at||'')); break;
    case 'date-asc':   list.sort((a,b) => (a.scraped_at||'').localeCompare(b.scraped_at||'')); break;
    case 'name-asc':   list.sort((a,b) => (a.name||'').localeCompare(b.name||'')); break;
    case 'name-desc':  list.sort((a,b) => (b.name||'').localeCompare(a.name||'')); break;
  }

  return list;
}

// ---------------------------------------------------------------------------
// Render table
// ---------------------------------------------------------------------------
function esc(s) {
  if (!s) return '';
  const d = document.createElement('div'); d.textContent = s; return d.innerHTML;
}

function scoreBadge(score, tier) {
  const colors = {
    'A': 'bg-green-500/20 text-green-400 border-green-500/40',
    'B': 'bg-blue-500/20 text-blue-400 border-blue-500/40',
    'C': 'bg-yellow-500/20 text-yellow-400 border-yellow-500/40',
    'D': 'bg-red-500/20 text-red-400 border-red-500/40',
  };
  const cls = colors[tier] || colors['D'];
  return `<span class="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs font-semibold border ${cls}">${score} <span class="opacity-60">${tier}</span></span>`;
}

function platformIcon(platform) {
  const p = (platform || '').toLowerCase();
  const icons = {
    'linkedin':   { color: '#0A66C2', label: 'LinkedIn' },
    'craigslist': { color: '#8b5cf6', label: 'Craigslist' },
    'reddit':     { color: '#FF4500', label: 'Reddit' },
    'ksl':        { color: '#f59e0b', label: 'KSL' },
    'facebook':   { color: '#1877F2', label: 'Facebook' },
    'search':     { color: '#22c55e', label: 'Search' },
  };
  const info = icons[p] || { color: '#64748b', label: platform || 'Unknown' };
  return `<span class="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs font-medium border border-slate-700 bg-slate-800/50 text-slate-300"><span class="w-2 h-2 rounded-full inline-block" style="background:${info.color}"></span>${esc(info.label)}</span>`;
}

function formatDate(d) {
  if (!d) return '--';
  try {
    const dt = new Date(d);
    return dt.toLocaleDateString('en-US', { month:'short', day:'numeric', year:'numeric' });
  } catch(_) { return d.substring(0,10); }
}

function renderTable() {
  const list = getFiltered();
  const body = document.getElementById('leadsBody');
  const empty = document.getElementById('emptyState');
  const wrap = document.getElementById('tableWrap');

  document.getElementById('resultCount').textContent = list.length + ' lead' + (list.length !== 1 ? 's' : '');

  if (list.length === 0) {
    wrap.classList.add('hidden');
    empty.classList.remove('hidden');
    return;
  }
  wrap.classList.remove('hidden');
  empty.classList.add('hidden');

  const rows = list.map((l, i) => {
    const linkedinCell = l.has_linkedin
      ? `<a href="${esc(l.linkedin_url || l.url)}" target="_blank" rel="noopener" class="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs font-medium bg-blue-500/15 text-blue-400 border border-blue-500/30 hover:bg-blue-500/25 transition-colors"><svg class="w-3 h-3" viewBox="0 0 24 24" fill="currentColor"><path d="M20.447 20.452h-3.554v-5.569c0-1.328-.027-3.037-1.852-3.037-1.853 0-2.136 1.445-2.136 2.939v5.667H9.351V9h3.414v1.561h.046c.477-.9 1.637-1.85 3.37-1.85 3.601 0 4.267 2.37 4.267 5.455v6.286zM5.337 7.433a2.062 2.062 0 01-2.063-2.065 2.064 2.064 0 112.063 2.065zm1.782 13.019H3.555V9h3.564v11.452zM22.225 0H1.771C.792 0 0 .774 0 1.729v20.542C0 23.227.792 24 1.771 24h20.451C23.2 24 24 23.227 24 22.271V1.729C24 .774 23.2 0 22.222 0h.003z"/></svg>Profile</a>`
      : `<span class="text-slate-600">&mdash;</span>`;

    const sourceCell = l.url
      ? `<a href="${esc(l.url)}" target="_blank" rel="noopener" class="hover:text-blue-400 transition-colors">${platformIcon(l.platform)}</a>`
      : platformIcon(l.platform);

    const reason = l.reason
      ? `<span class="text-slate-400 text-xs leading-tight block max-w-xs truncate" title="${esc(l.reason)}">${esc(l.reason)}</span>`
      : `<span class="text-slate-600 text-xs">&mdash;</span>`;

    const enhanceBtn = l.has_linkedin
      ? `<button onclick="openPanel('${l.id}', ${JSON.stringify(esc(l.name))}, ${JSON.stringify(esc(l.city || 'Utah'))})" class="text-xs px-2.5 py-1 rounded-lg bg-slate-800 hover:bg-slate-700 text-slate-300 hover:text-white border border-slate-700 transition-colors">${l.enhanced ? 'View' : 'Enhance'}</button>`
      : '';

    const rowClass = i % 2 === 0 ? 'bg-slate-950' : 'bg-slate-900/30';

    // Source type badge
    const stBadge = l.source_type === 'post'
      ? '<span class="px-1.5 py-0.5 bg-purple-900/50 text-purple-300 rounded text-[10px] font-medium">POST</span>'
      : l.source_type === 'profile'
      ? '<span class="px-1.5 py-0.5 bg-blue-900/50 text-blue-300 rounded text-[10px] font-medium">PROFILE</span>'
      : '<span class="px-1.5 py-0.5 bg-slate-700 text-slate-400 rounded text-[10px] font-medium">WEB</span>';

    // Snippet (why they were scraped)
    const snippet = l.snippet ? esc(l.snippet).substring(0, 150) + (l.snippet.length > 150 ? '...' : '') : '';

    // Freshness
    const freshness = getFreshness(l.scraped_at);

    return `<tr class="${rowClass} hover:bg-slate-800/50 transition-colors border-b border-slate-800/50 cursor-pointer" onclick="toggleDetail(${i})">
      <td class="py-3 px-3 text-slate-600 text-xs">${i + 1}</td>
      <td class="py-3 px-3">
        <div class="font-medium text-white text-sm">${esc(l.name)}</div>
        ${l.title && l.title !== l.name ? `<div class="text-xs text-slate-500 truncate max-w-xs">${esc(l.title)}</div>` : ''}
        ${l.has_nwm ? `<span class="inline-flex items-center gap-1 mt-0.5 px-1.5 py-0.5 bg-amber-900/40 text-amber-400 rounded text-[10px] font-medium border border-amber-700/30">NWM Connected</span>` : ''}
      </td>
      <td class="py-3 px-3">${linkedinCell}</td>
      <td class="py-3 px-3">
        <div class="flex items-center gap-1.5 mb-1">${stBadge} ${reason}</div>
        ${snippet ? `<div class="text-[11px] text-slate-500 leading-snug max-w-sm">${snippet}</div>` : ''}
      </td>
      <td class="py-3 px-3">${sourceCell}</td>
      <td class="py-3 px-3 text-center">${scoreBadge(l.score, l.tier)}</td>
      <td class="py-3 px-3 text-xs whitespace-nowrap">${freshness}</td>
      <td class="py-3 px-3 text-center">${enhanceBtn}</td>
    </tr>
    <tr id="detail-${i}" class="hidden bg-slate-900/80 border-b border-slate-800/50">
      <td colspan="8" class="px-6 py-4">
        <div class="grid grid-cols-1 md:grid-cols-3 gap-4 text-sm">
          <div>
            <p class="text-slate-500 text-xs font-semibold uppercase mb-1">Why this lead exists</p>
            <p class="text-slate-300">${l.source_label || 'Web search'}: ${reason}</p>
            ${snippet ? `<p class="text-slate-400 text-xs mt-2 italic">"${snippet}"</p>` : ''}
          </div>
          <div>
            <p class="text-slate-500 text-xs font-semibold uppercase mb-1">Source</p>
            <p class="text-blue-400 text-xs break-all"><a href="${esc(l.url)}" target="_blank" class="hover:underline">${esc(l.url)}</a></p>
            ${l.search_query ? `<p class="text-slate-600 text-[10px] mt-1">Found via: "${esc(l.search_query)}"</p>` : ''}
          </div>
          <div>
            <p class="text-slate-500 text-xs font-semibold uppercase mb-1">NWM Connections in Area</p>
            ${l.nwm_connections && l.nwm_connections.length > 0
              ? l.nwm_connections.map(c => `<div class="flex items-center gap-2 py-1 border-b border-slate-800/50 last:border-0">
                  <div class="w-6 h-6 rounded-full bg-amber-900/50 flex items-center justify-center text-amber-400 text-[10px] font-bold">${esc(c.name.charAt(0))}</div>
                  <div class="flex-1 min-w-0">
                    <p class="text-white text-xs font-medium truncate">${esc(c.name)}</p>
                    <p class="text-slate-500 text-[10px] truncate">${esc(c.title)}</p>
                  </div>
                  ${c.linkedin_url ? `<a href="${esc(c.linkedin_url)}" target="_blank" class="text-blue-400 hover:text-blue-300 text-[10px]">LinkedIn</a>` : ''}
                </div>`).join('')
              : `<p class="text-slate-600 text-xs">No NWM reps found in this area yet</p>`
            }
          </div>
        </div>
      </td>
    </tr>`;
  });

  body.innerHTML = rows.join('');
}

// ---------------------------------------------------------------------------
// Enhance panel
// ---------------------------------------------------------------------------
function openPanel(leadId, name, city) {
  currentLeadId = leadId;
  document.getElementById('panelTitle').textContent = 'Enhance: ' + name;
  document.getElementById('panelInfo').innerHTML = `
    <div class="grid grid-cols-2 gap-3">
      <div class="bg-slate-800/50 rounded-lg p-3"><p class="text-xs text-slate-500">Name</p><p class="text-sm text-white">${name}</p></div>
      <div class="bg-slate-800/50 rounded-lg p-3"><p class="text-xs text-slate-500">Location</p><p class="text-sm text-white">${city}</p></div>
    </div>`;
  document.getElementById('panelResults').innerHTML = '';
  document.getElementById('panelResults').classList.add('hidden');
  document.getElementById('panelLoading').classList.add('hidden');
  document.getElementById('panelError').classList.add('hidden');
  document.getElementById('panelEnhanceBtn').disabled = false;
  document.getElementById('panelEnhanceBtn').textContent = 'Enhance with Serper.dev';

  document.getElementById('enhancePanel').classList.remove('hidden');
  document.getElementById('panelBackdrop').classList.remove('hidden');
}

function closePanel() {
  document.getElementById('enhancePanel').classList.add('hidden');
  document.getElementById('panelBackdrop').classList.add('hidden');
  currentLeadId = null;
}

async function runEnhance() {
  if (!currentLeadId) return;
  const btn = document.getElementById('panelEnhanceBtn');
  btn.disabled = true;
  btn.textContent = 'Searching...';
  document.getElementById('panelLoading').classList.remove('hidden');
  document.getElementById('panelResults').classList.add('hidden');
  document.getElementById('panelError').classList.add('hidden');

  try {
    const resp = await fetch('/api/enhance/' + currentLeadId, { method: 'POST' });
    const data = await resp.json();
    document.getElementById('panelLoading').classList.add('hidden');

    if (data.error) {
      document.getElementById('panelError').classList.remove('hidden');
      document.getElementById('panelErrorMsg').textContent = data.error;
      btn.disabled = false;
      btn.textContent = 'Retry';
      return;
    }

    let html = '';
    (data.searches || []).forEach(function(search) {
      html += '<div class="bg-slate-800/40 rounded-lg p-4">';
      html += '<p class="text-xs text-slate-500 mb-2 font-medium">' + esc(search.query) + '</p>';
      (search.results || []).forEach(function(r) {
        html += '<a href="' + esc(r.url) + '" target="_blank" rel="noopener" class="block hover:bg-slate-700/50 rounded-lg p-2.5 mb-1 transition-colors">';
        html += '<p class="text-sm text-blue-400 font-medium leading-tight">' + esc(r.title) + '</p>';
        html += '<p class="text-xs text-slate-600 truncate mt-0.5">' + esc(r.url) + '</p>';
        if (r.snippet) html += '<p class="text-xs text-slate-400 mt-1 line-clamp-2">' + esc(r.snippet.substring(0,200)) + '</p>';
        html += '</a>';
      });
      if (!search.results || search.results.length === 0) {
        html += '<p class="text-xs text-slate-600 italic py-2">No results</p>';
      }
      html += '</div>';
    });

    document.getElementById('panelResults').innerHTML = html;
    document.getElementById('panelResults').classList.remove('hidden');
    btn.textContent = 'Done';

    // Update local state so table reflects enhanced status
    const lead = allLeads.find(l => l.id == currentLeadId);
    if (lead) {
      lead.enhanced = true;
      if (data.found_data && data.found_data.linkedin_url) {
        lead.linkedin_url = data.found_data.linkedin_url;
        lead.has_linkedin = true;
      }
      renderTable();
    }

  } catch (e) {
    document.getElementById('panelLoading').classList.add('hidden');
    document.getElementById('panelError').classList.remove('hidden');
    document.getElementById('panelErrorMsg').textContent = 'Network error: ' + e.message;
    btn.disabled = false;
    btn.textContent = 'Retry';
  }
}

// Close panel on Escape
document.addEventListener('keydown', function(e) {
  if (e.key === 'Escape') closePanel();
});

// ---------------------------------------------------------------------------
// Wire up filter events
// ---------------------------------------------------------------------------
document.getElementById('searchBox').addEventListener('input', renderTable);
document.getElementById('sortSelect').addEventListener('change', renderTable);
document.getElementById('linkedinToggle').addEventListener('change', renderTable);
document.getElementById('atierToggle').addEventListener('change', renderTable);
document.getElementById('nwmToggle').addEventListener('change', renderTable);

// Start
init();
</script>
</body>
</html>"""

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    PORT = 8080
    print(f"\n  Lead Gen Dashboard")
    print(f"  Database: {DB_PATH}")
    print(f"  URL: http://localhost:{PORT}\n")
    app.run(host="0.0.0.0", port=PORT, debug=True)
