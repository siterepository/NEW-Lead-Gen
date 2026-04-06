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
<div class="bg-slate-900/60 border-b border-slate-800 px-6 py-3 sticky top-0 z-20 backdrop-blur-sm">
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
