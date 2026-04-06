"""
NEW Lead Gen Dashboard - Full-featured Flask web dashboard.

Visualizes scraping activity, leads, scoring, and agent status.
Reads from the SQLite database at data/leadgen.db.

Run with:  python3 -m leadgen.dashboard.app
"""

import csv
import io
import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

from flask import Flask, Response, render_template_string, request

# ---------------------------------------------------------------------------
# App setup
# ---------------------------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent  # project root
DB_PATH = BASE_DIR / "data" / "leadgen.db"
LOGS_DIR = BASE_DIR / "data" / "logs"
EXPORTS_DIR = BASE_DIR / "data" / "exports"

app = Flask(__name__)


def get_db():
    """Get a SQLite connection with row_factory."""
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def safe_json(payload_str):
    """Safely parse a JSON string, returning {} on failure."""
    try:
        return json.loads(payload_str) if payload_str else {}
    except (json.JSONDecodeError, TypeError):
        return {}


def fmt_dt(dt_str):
    """Format an ISO datetime string for display."""
    if not dt_str:
        return "N/A"
    try:
        dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
        return dt.strftime("%b %d, %Y %H:%M")
    except (ValueError, AttributeError):
        return str(dt_str)[:19]


def time_ago(dt_str):
    """Return a human-readable time-ago string."""
    if not dt_str:
        return "Never"
    try:
        dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
        now = datetime.now(timezone.utc)
        diff = now - dt
        if diff.total_seconds() < 60:
            return "Just now"
        if diff.total_seconds() < 3600:
            return f"{int(diff.total_seconds() / 60)}m ago"
        if diff.total_seconds() < 86400:
            return f"{int(diff.total_seconds() / 3600)}h ago"
        return f"{diff.days}d ago"
    except (ValueError, AttributeError):
        return "Unknown"


# ---------------------------------------------------------------------------
# Layout wrapper -- builds a complete page from content_html
# ---------------------------------------------------------------------------

def _render_page(title, subtitle, active_page, content_html):
    """Wrap page content in the shared layout shell."""
    now_str = datetime.now().strftime("%b %d, %H:%M")
    return render_template_string(
        FULL_PAGE_TEMPLATE,
        title=title,
        subtitle=subtitle,
        active_page=active_page,
        last_updated=now_str,
        content_html=content_html,
    )


FULL_PAGE_TEMPLATE = r"""<!DOCTYPE html>
<html lang="en" class="dark">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{{ title }} - Lead Gen Dashboard</title>
<script src="https://cdn.tailwindcss.com"></script>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.7/dist/chart.umd.min.js"></script>
<script>
tailwind.config = {
    darkMode: 'class',
    theme: { extend: { colors: { slate: { 850: '#172033', 950: '#0b1120' } } } }
}
</script>
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
body { font-family: 'Inter', sans-serif; }
::-webkit-scrollbar { width: 6px; height: 6px; }
::-webkit-scrollbar-track { background: #1e293b; }
::-webkit-scrollbar-thumb { background: #475569; border-radius: 3px; }
::-webkit-scrollbar-thumb:hover { background: #64748b; }
.tier-a { color: #22c55e; }
.tier-b { color: #3b82f6; }
.tier-c { color: #eab308; }
.tier-d { color: #ef4444; }
.bg-tier-a { background-color: rgba(34,197,94,0.15); border-color: #22c55e; }
.bg-tier-b { background-color: rgba(59,130,246,0.15); border-color: #3b82f6; }
.bg-tier-c { background-color: rgba(234,179,8,0.15); border-color: #eab308; }
.bg-tier-d { background-color: rgba(239,68,68,0.15); border-color: #ef4444; }
.card-glow { box-shadow: 0 0 20px rgba(59,130,246,0.08); }
.sidebar-link.active { background: linear-gradient(135deg, rgba(59,130,246,0.2), rgba(139,92,246,0.1)); border-left: 3px solid #3b82f6; }
.stat-card { transition: transform 0.2s, box-shadow 0.2s; }
.stat-card:hover { transform: translateY(-2px); box-shadow: 0 8px 25px rgba(0,0,0,0.3); }
.fade-in { animation: fadeIn 0.3s ease-in; }
@keyframes fadeIn { from { opacity: 0; transform: translateY(10px); } to { opacity: 1; transform: translateY(0); } }
.pulse-dot { animation: pulse 2s infinite; }
@keyframes pulse { 0%,100% { opacity: 1; } 50% { opacity: 0.5; } }
</style>
</head>
<body class="bg-slate-950 text-slate-200 min-h-screen">
<div class="flex min-h-screen">
<!-- Sidebar -->
<aside class="w-64 bg-slate-900 border-r border-slate-800 fixed h-full z-10 flex flex-col">
    <div class="p-5 border-b border-slate-800">
        <div class="flex items-center space-x-3">
            <div class="w-10 h-10 rounded-lg bg-gradient-to-br from-blue-500 to-purple-600 flex items-center justify-center">
                <svg class="w-6 h-6 text-white" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 10V3L4 14h7v7l9-11h-7z"/></svg>
            </div>
            <div>
                <h1 class="text-lg font-bold text-white">Lead Gen</h1>
                <p class="text-xs text-slate-400">NWM Recruiting</p>
            </div>
        </div>
    </div>
    <nav class="flex-1 p-3 space-y-1">
        <a href="/" class="sidebar-link {% if active_page == 'dashboard' %}active{% endif %} flex items-center space-x-3 px-3 py-2.5 rounded-lg text-sm text-slate-300 hover:bg-slate-800 hover:text-white transition-all">
            <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 6a2 2 0 012-2h2a2 2 0 012 2v2a2 2 0 01-2 2H6a2 2 0 01-2-2V6zm10 0a2 2 0 012-2h2a2 2 0 012 2v2a2 2 0 01-2 2h-2a2 2 0 01-2-2V6zM4 16a2 2 0 012-2h2a2 2 0 012 2v2a2 2 0 01-2 2H6a2 2 0 01-2-2v-2zm10 0a2 2 0 012-2h2a2 2 0 012 2v2a2 2 0 01-2 2h-2a2 2 0 01-2-2v-2z"/></svg>
            <span>Dashboard</span>
        </a>
        <a href="/leads" class="sidebar-link {% if active_page == 'leads' %}active{% endif %} flex items-center space-x-3 px-3 py-2.5 rounded-lg text-sm text-slate-300 hover:bg-slate-800 hover:text-white transition-all">
            <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M17 20h5v-2a3 3 0 00-5.356-1.857M17 20H7m10 0v-2c0-.656-.126-1.283-.356-1.857M7 20H2v-2a3 3 0 015.356-1.857M7 20v-2c0-.656.126-1.283.356-1.857m0 0a5.002 5.002 0 019.288 0M15 7a3 3 0 11-6 0 3 3 0 016 0z"/></svg>
            <span>Leads</span>
        </a>
        <a href="/agents" class="sidebar-link {% if active_page == 'agents' %}active{% endif %} flex items-center space-x-3 px-3 py-2.5 rounded-lg text-sm text-slate-300 hover:bg-slate-800 hover:text-white transition-all">
            <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9.75 17L9 20l-1 1h8l-1-1-.75-3M3 13h18M5 17h14a2 2 0 002-2V5a2 2 0 00-2-2H5a2 2 0 00-2 2v10a2 2 0 002 2z"/></svg>
            <span>Agents</span>
        </a>
        <a href="/analytics" class="sidebar-link {% if active_page == 'analytics' %}active{% endif %} flex items-center space-x-3 px-3 py-2.5 rounded-lg text-sm text-slate-300 hover:bg-slate-800 hover:text-white transition-all">
            <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z"/></svg>
            <span>Analytics</span>
        </a>
    </nav>
    <div class="p-4 border-t border-slate-800">
        <div class="flex items-center space-x-2 text-xs text-slate-500">
            <div class="w-2 h-2 rounded-full bg-green-500 pulse-dot"></div>
            <span>System Online</span>
        </div>
    </div>
</aside>
<!-- Main content -->
<main class="flex-1 ml-64">
    <header class="bg-slate-900/80 backdrop-blur-sm border-b border-slate-800 sticky top-0 z-10 px-8 py-4">
        <div class="flex items-center justify-between">
            <div>
                <h2 class="text-xl font-semibold text-white">{{ title }}</h2>
                <p class="text-sm text-slate-400 mt-0.5">{{ subtitle }}</p>
            </div>
            <div class="flex items-center space-x-4">
                <span class="text-xs text-slate-500">Last updated: {{ last_updated }}</span>
                <a href="/" class="text-xs bg-slate-800 hover:bg-slate-700 text-slate-300 px-3 py-1.5 rounded-lg transition-colors">Refresh</a>
            </div>
        </div>
    </header>
    <div class="p-8 fade-in">
        {{ content_html | safe }}
    </div>
</main>
</div>
</body>
</html>"""


# ---------------------------------------------------------------------------
# Helper: parse lead from a jobs row
# ---------------------------------------------------------------------------

def _parse_lead(row):
    """Parse a jobs row into a lead dict."""
    p = safe_json(row["payload"])
    data = p.get("data", {})
    name = data.get("name", "")
    title = data.get("title", "")
    first_name = data.get("first_name", "")
    last_name = data.get("last_name", "")
    if first_name or last_name:
        name = f"{first_name} {last_name}".strip()

    score = data.get("total_score", 0)
    if score >= 75:
        tier = "A"
    elif score >= 50:
        tier = "B"
    elif score >= 25:
        tier = "C"
    else:
        tier = "D"

    return {
        "id": row["id"],
        "name": name,
        "title": title,
        "score": score,
        "tier": tier,
        "platform": p.get("platform", data.get("platform", "unknown")),
        "city": data.get("location_city", ""),
        "state": data.get("location_state", ""),
        "category": data.get("category", ""),
        "scraped_at": fmt_dt(p.get("scraped_at", row["created_at"])),
        "source_url": data.get("source_url", data.get("url", "")),
        "post_id": data.get("post_id", ""),
        "description": data.get("description", ""),
        "contact_email": data.get("contact_email", ""),
        "contact_phone": data.get("contact_phone", ""),
        "contact_info": data.get("contact_info", ""),
        "price": data.get("price", ""),
        "image_url": data.get("image_url", ""),
        "agent": p.get("agent", data.get("agent", "")),
        "current_role": data.get("current_role", ""),
        "recruiting_signals": data.get("recruiting_signals", []),
        "score_career_fit": data.get("score_career_fit", 0),
        "score_motivation": data.get("score_motivation", 0),
        "score_people_skills": data.get("score_people_skills", 0),
        "score_demographics": data.get("score_demographics", 0),
        "score_data_quality": data.get("score_data_quality", 0),
        "sentiment_score": data.get("sentiment_score"),
        "enriched": data.get("enriched", False),
        "compliance_cleared": data.get("compliance_cleared", False),
        "source_post_text": data.get("source_post_text", data.get("description", "")),
    }


# ---------------------------------------------------------------------------
# Route: Main Dashboard (/)
# ---------------------------------------------------------------------------

@app.route("/")
def dashboard():
    db = get_db()
    try:
        total_leads = db.execute(
            "SELECT COUNT(*) FROM jobs WHERE job_type='raw_scrape'"
        ).fetchone()[0]

        today_start = datetime.now(timezone.utc).replace(
            hour=0, minute=0, second=0, microsecond=0
        ).isoformat()
        new_today = db.execute(
            "SELECT COUNT(*) FROM jobs WHERE job_type='raw_scrape' AND created_at >= ?",
            (today_start,),
        ).fetchone()[0]

        all_payloads = db.execute(
            "SELECT payload FROM jobs WHERE job_type='raw_scrape'"
        ).fetchall()

        tier_a = tier_b = tier_c = tier_d = 0
        platform_map = {}
        daily_map = {}

        for row in all_payloads:
            p = safe_json(row[0])
            platform = p.get("platform", "unknown")
            platform_map[platform] = platform_map.get(platform, 0) + 1

            created = p.get("scraped_at", "")
            if created:
                try:
                    day = created[:10]
                    daily_map[day] = daily_map.get(day, 0) + 1
                except (ValueError, IndexError):
                    pass

            data = p.get("data", {})
            score = data.get("total_score", 0)
            if score >= 75:
                tier_a += 1
            elif score >= 50:
                tier_b += 1
            elif score >= 25:
                tier_c += 1
            else:
                tier_d += 1

        active_agents = db.execute(
            "SELECT COUNT(DISTINCT json_extract(payload, '$.agent')) FROM jobs WHERE job_type='raw_scrape'"
        ).fetchone()[0]

        last_row = db.execute(
            "SELECT created_at FROM jobs ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
        last_scrape = time_ago(last_row[0]) if last_row else "Never"

        # Recent agent runs
        recent_run_rows = db.execute(
            "SELECT payload, created_at FROM jobs WHERE job_type='agent_run_log' ORDER BY created_at DESC LIMIT 20"
        ).fetchall()
        recent_runs = []
        for row in recent_run_rows:
            p = safe_json(row[0])
            recent_runs.append({
                "agent_name": p.get("agent_name", "Unknown"),
                "platform": p.get("platform", "?"),
                "status": p.get("status", "unknown"),
                "items_found": p.get("items_found", 0),
                "items_new": p.get("items_new", 0),
                "time_ago": time_ago(p.get("completed_at", row[1])),
            })
        ar_rows = db.execute(
            "SELECT * FROM agent_runs ORDER BY completed_at DESC LIMIT 20"
        ).fetchall()
        for row in ar_rows:
            recent_runs.append({
                "agent_name": row["agent_name"],
                "platform": row["platform"] or "?",
                "status": row["status"],
                "items_found": row["items_found"],
                "items_new": row["items_new"],
                "time_ago": time_ago(row["completed_at"]),
            })
        recent_runs = recent_runs[:20]

        now = datetime.now(timezone.utc)
        daily_labels = []
        daily_counts = []
        for i in range(29, -1, -1):
            day = (now - timedelta(days=i)).strftime("%Y-%m-%d")
            daily_labels.append((now - timedelta(days=i)).strftime("%b %d"))
            daily_counts.append(daily_map.get(day, 0))

        platform_labels = json.dumps([k.title() for k in platform_map.keys()])
        platform_counts = json.dumps(list(platform_map.values()))
        tier_counts_json = json.dumps([tier_a, tier_b, tier_c, tier_d])
        daily_labels_json = json.dumps(daily_labels)
        daily_counts_json = json.dumps(daily_counts)

        # Build activity feed HTML
        activity_html = ""
        for run in recent_runs:
            if run["status"] in ("success", "completed"):
                dot = '<div class="w-2 h-2 rounded-full bg-green-500"></div>'
            elif run["status"] in ("failed", "error"):
                dot = '<div class="w-2 h-2 rounded-full bg-red-500"></div>'
            else:
                dot = '<div class="w-2 h-2 rounded-full bg-yellow-500 pulse-dot"></div>'
            activity_html += f'''<div class="flex items-center justify-between bg-slate-800/50 rounded-lg px-4 py-2.5">
                <div class="flex items-center space-x-3">
                    {dot}
                    <div>
                        <p class="text-sm font-medium text-slate-200">{run["agent_name"]}</p>
                        <p class="text-xs text-slate-500">{run["platform"]} &middot; {run["items_found"]} found, {run["items_new"]} new</p>
                    </div>
                </div>
                <span class="text-xs text-slate-500">{run["time_ago"]}</span>
            </div>'''

        if not recent_runs:
            activity_html = '<p class="text-sm text-slate-500 text-center py-8">No agent runs recorded yet.</p>'

        content = f'''
<!-- Stat Cards -->
<div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 xl:grid-cols-6 gap-4 mb-8">
    <div class="stat-card bg-slate-900 border border-slate-800 rounded-xl p-5 card-glow">
        <div class="flex items-center justify-between mb-3">
            <span class="text-xs font-medium text-slate-400 uppercase tracking-wider">Total Leads</span>
            <div class="w-8 h-8 rounded-lg bg-blue-500/20 flex items-center justify-center">
                <svg class="w-4 h-4 text-blue-400" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M17 20h5v-2a3 3 0 00-5.356-1.857M17 20H7m10 0v-2c0-.656-.126-1.283-.356-1.857M7 20H2v-2a3 3 0 015.356-1.857M7 20v-2c0-.656.126-1.283.356-1.857m0 0a5.002 5.002 0 019.288 0M15 7a3 3 0 11-6 0 3 3 0 016 0z"/></svg>
            </div>
        </div>
        <p class="text-3xl font-bold text-white">{total_leads}</p>
        <p class="text-xs text-slate-500 mt-1">All scraped leads</p>
    </div>
    <div class="stat-card bg-slate-900 border border-slate-800 rounded-xl p-5 card-glow">
        <div class="flex items-center justify-between mb-3">
            <span class="text-xs font-medium text-slate-400 uppercase tracking-wider">New Today</span>
            <div class="w-8 h-8 rounded-lg bg-emerald-500/20 flex items-center justify-center">
                <svg class="w-4 h-4 text-emerald-400" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 6v6m0 0v6m0-6h6m-6 0H6"/></svg>
            </div>
        </div>
        <p class="text-3xl font-bold text-white">{new_today}</p>
        <p class="text-xs text-slate-500 mt-1">Scraped today</p>
    </div>
    <div class="stat-card bg-slate-900 border border-slate-800 rounded-xl p-5 card-glow">
        <div class="flex items-center justify-between mb-3">
            <span class="text-xs font-medium text-slate-400 uppercase tracking-wider">A-Tier</span>
            <div class="w-8 h-8 rounded-lg bg-green-500/20 flex items-center justify-center"><span class="text-sm font-bold text-green-400">A</span></div>
        </div>
        <p class="text-3xl font-bold tier-a">{tier_a}</p>
        <p class="text-xs text-slate-500 mt-1">Score 75+</p>
    </div>
    <div class="stat-card bg-slate-900 border border-slate-800 rounded-xl p-5 card-glow">
        <div class="flex items-center justify-between mb-3">
            <span class="text-xs font-medium text-slate-400 uppercase tracking-wider">B-Tier</span>
            <div class="w-8 h-8 rounded-lg bg-blue-500/20 flex items-center justify-center"><span class="text-sm font-bold text-blue-400">B</span></div>
        </div>
        <p class="text-3xl font-bold tier-b">{tier_b}</p>
        <p class="text-xs text-slate-500 mt-1">Score 50-74</p>
    </div>
    <div class="stat-card bg-slate-900 border border-slate-800 rounded-xl p-5 card-glow">
        <div class="flex items-center justify-between mb-3">
            <span class="text-xs font-medium text-slate-400 uppercase tracking-wider">Active Agents</span>
            <div class="w-8 h-8 rounded-lg bg-purple-500/20 flex items-center justify-center">
                <svg class="w-4 h-4 text-purple-400" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9.75 17L9 20l-1 1h8l-1-1-.75-3M3 13h18M5 17h14a2 2 0 002-2V5a2 2 0 00-2-2H5a2 2 0 00-2 2v10a2 2 0 002 2z"/></svg>
            </div>
        </div>
        <p class="text-3xl font-bold text-white">{active_agents}</p>
        <p class="text-xs text-slate-500 mt-1">Unique scrapers</p>
    </div>
    <div class="stat-card bg-slate-900 border border-slate-800 rounded-xl p-5 card-glow">
        <div class="flex items-center justify-between mb-3">
            <span class="text-xs font-medium text-slate-400 uppercase tracking-wider">Last Scrape</span>
            <div class="w-8 h-8 rounded-lg bg-amber-500/20 flex items-center justify-center">
                <svg class="w-4 h-4 text-amber-400" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 8v4l3 3m6-3a9 9 0 11-18 0 9 9 0 0118 0z"/></svg>
            </div>
        </div>
        <p class="text-xl font-bold text-white">{last_scrape}</p>
        <p class="text-xs text-slate-500 mt-1">Most recent run</p>
    </div>
</div>

<!-- Charts Row -->
<div class="grid grid-cols-1 lg:grid-cols-2 xl:grid-cols-3 gap-6 mb-8">
    <div class="bg-slate-900 border border-slate-800 rounded-xl p-6 card-glow xl:col-span-2">
        <h3 class="text-sm font-semibold text-slate-300 mb-4 uppercase tracking-wider">Leads Scraped Per Day (Last 30 Days)</h3>
        <div style="height:280px"><canvas id="leadsPerDayChart"></canvas></div>
    </div>
    <div class="bg-slate-900 border border-slate-800 rounded-xl p-6 card-glow">
        <h3 class="text-sm font-semibold text-slate-300 mb-4 uppercase tracking-wider">Leads by Platform</h3>
        <div style="height:280px"><canvas id="platformPieChart"></canvas></div>
    </div>
</div>

<div class="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-8">
    <div class="bg-slate-900 border border-slate-800 rounded-xl p-6 card-glow">
        <h3 class="text-sm font-semibold text-slate-300 mb-4 uppercase tracking-wider">Leads by Tier</h3>
        <div style="height:260px"><canvas id="tierBarChart"></canvas></div>
    </div>
    <div class="bg-slate-900 border border-slate-800 rounded-xl p-6 card-glow">
        <h3 class="text-sm font-semibold text-slate-300 mb-4 uppercase tracking-wider">Recent Agent Activity</h3>
        <div class="space-y-2 max-h-[260px] overflow-y-auto pr-2">{activity_html}</div>
    </div>
</div>

<script>
Chart.defaults.color='#94a3b8';
Chart.defaults.borderColor='rgba(51,65,85,0.5)';
new Chart(document.getElementById('leadsPerDayChart'),{{
    type:'line',
    data:{{ labels:{daily_labels_json}, datasets:[{{ label:'Leads',data:{daily_counts_json},borderColor:'#3b82f6',backgroundColor:'rgba(59,130,246,0.1)',fill:true,tension:0.4,pointBackgroundColor:'#3b82f6',pointBorderColor:'#1e293b',pointBorderWidth:2,pointRadius:3,pointHoverRadius:6 }}] }},
    options:{{ responsive:true,maintainAspectRatio:false,plugins:{{ legend:{{ display:false }} }},scales:{{ x:{{ grid:{{ display:false }},ticks:{{ font:{{ size:10 }} }} }},y:{{ beginAtZero:true,grid:{{ color:'rgba(51,65,85,0.3)' }},ticks:{{ font:{{ size:10 }} }} }} }} }}
}});
new Chart(document.getElementById('platformPieChart'),{{
    type:'doughnut',
    data:{{ labels:{platform_labels}, datasets:[{{ data:{platform_counts},backgroundColor:['#3b82f6','#8b5cf6','#06b6d4','#f59e0b','#ef4444','#22c55e','#ec4899','#14b8a6'],borderColor:'#0f172a',borderWidth:2 }}] }},
    options:{{ responsive:true,maintainAspectRatio:false,plugins:{{ legend:{{ position:'bottom',labels:{{ boxWidth:12,padding:12,font:{{ size:11 }} }} }} }} }}
}});
new Chart(document.getElementById('tierBarChart'),{{
    type:'bar',
    data:{{ labels:['A-Tier','B-Tier','C-Tier','D-Tier'], datasets:[{{ label:'Leads',data:{tier_counts_json},backgroundColor:['rgba(34,197,94,0.7)','rgba(59,130,246,0.7)','rgba(234,179,8,0.7)','rgba(239,68,68,0.7)'],borderColor:['#22c55e','#3b82f6','#eab308','#ef4444'],borderWidth:1,borderRadius:6 }}] }},
    options:{{ responsive:true,maintainAspectRatio:false,plugins:{{ legend:{{ display:false }} }},scales:{{ x:{{ grid:{{ display:false }} }},y:{{ beginAtZero:true,grid:{{ color:'rgba(51,65,85,0.3)' }} }} }} }}
}});
</script>'''

        return _render_page("Dashboard", "Overview of your lead generation pipeline", "dashboard", content)
    finally:
        db.close()


# ---------------------------------------------------------------------------
# Route: Leads Table (/leads)
# ---------------------------------------------------------------------------

@app.route("/leads")
def leads_list():
    db = get_db()
    try:
        search = request.args.get("search", "").strip()
        tier_filter = request.args.get("tier", "").strip()
        platform_filter = request.args.get("platform", "").strip()
        page = int(request.args.get("page", 1))
        per_page = 50

        platform_rows = db.execute(
            "SELECT DISTINCT json_extract(payload, '$.platform') as p FROM jobs WHERE job_type='raw_scrape' AND p IS NOT NULL"
        ).fetchall()
        platforms = sorted(set(r[0] for r in platform_rows if r[0]))

        query = "SELECT * FROM jobs WHERE job_type='raw_scrape'"
        params = []
        if platform_filter:
            query += " AND json_extract(payload, '$.platform')=?"
            params.append(platform_filter)
        if search:
            query += " AND payload LIKE ?"
            params.append(f"%{search}%")
        query += " ORDER BY created_at DESC"

        all_rows = db.execute(query, params).fetchall()
        all_leads = []
        for row in all_rows:
            lead = _parse_lead(row)
            if tier_filter and lead["tier"] != tier_filter:
                continue
            all_leads.append(lead)

        total_count = len(all_leads)
        total_pages = max(1, (total_count + per_page - 1) // per_page)
        page = min(page, total_pages)
        start = (page - 1) * per_page
        leads = all_leads[start:start + per_page]

        # Build platform options HTML
        plat_options = '<option value="">All Platforms</option>'
        for p in platforms:
            sel = "selected" if platform_filter == p else ""
            plat_options += f'<option value="{p}" {sel}>{p.title()}</option>'

        tier_options = '<option value="">All Tiers</option>'
        for t in ["A", "B", "C", "D"]:
            sel = "selected" if tier_filter == t else ""
            tier_options += f'<option value="{t}" {sel}>{t}-Tier</option>'

        # Build table rows HTML
        rows_html = ""
        for l in leads:
            display_name = l["name"] or l["title"] or "Unnamed"
            subtitle_html = ""
            if l["name"] and l["title"]:
                safe_title = l["title"].replace("<", "&lt;").replace(">", "&gt;")
                subtitle_html = f'<p class="text-xs text-slate-500 truncate max-w-xs">{safe_title}</p>'

            safe_display = display_name.replace("<", "&lt;").replace(">", "&gt;")

            tier_class = {"A": "bg-tier-a tier-a", "B": "bg-tier-b tier-b", "C": "bg-tier-c tier-c", "D": "bg-tier-d tier-d"}.get(l["tier"], "bg-tier-d tier-d")

            rows_html += f'''<tr class="hover:bg-slate-800/30 transition-colors">
                <td class="px-6 py-3"><div><p class="text-sm font-medium text-slate-200">{safe_display}</p>{subtitle_html}</div></td>
                <td class="px-4 py-3"><span class="text-sm font-mono font-medium text-slate-300">{l["score"]}</span></td>
                <td class="px-4 py-3"><span class="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-bold border {tier_class}">{l["tier"]}</span></td>
                <td class="px-4 py-3"><span class="text-sm text-slate-400">{l["platform"].title()}</span></td>
                <td class="px-4 py-3"><span class="text-sm text-slate-400">{l["city"] or "-"}</span></td>
                <td class="px-4 py-3"><span class="text-sm text-slate-400">{l["category"] or "-"}</span></td>
                <td class="px-4 py-3"><span class="text-xs text-slate-500">{l["scraped_at"]}</span></td>
                <td class="px-4 py-3"><a href="/leads/{l["id"]}" class="text-blue-400 hover:text-blue-300 text-sm font-medium transition-colors">View</a></td>
            </tr>'''

        if not leads:
            rows_html = '<tr><td colspan="8" class="px-6 py-12 text-center text-slate-500">No leads found matching your criteria.</td></tr>'

        # Pagination HTML
        pag_html = ""
        if total_pages > 1:
            pag_html = '<div class="flex items-center justify-center space-x-2 mt-6">'
            if page > 1:
                pag_html += f'<a href="/leads?page={page-1}&search={search}&tier={tier_filter}&platform={platform_filter}" class="bg-slate-800 hover:bg-slate-700 text-slate-300 text-sm px-3 py-1.5 rounded-lg transition-colors">Previous</a>'
            for p in range(1, total_pages + 1):
                if p == page:
                    pag_html += f'<span class="bg-blue-600 text-white text-sm px-3 py-1.5 rounded-lg">{p}</span>'
                elif p <= 3 or p > total_pages - 3 or (page - 1 <= p <= page + 1):
                    pag_html += f'<a href="/leads?page={p}&search={search}&tier={tier_filter}&platform={platform_filter}" class="bg-slate-800 hover:bg-slate-700 text-slate-300 text-sm px-3 py-1.5 rounded-lg transition-colors">{p}</a>'
                elif p == 4 or p == total_pages - 3:
                    pag_html += '<span class="text-slate-500 text-sm">...</span>'
            if page < total_pages:
                pag_html += f'<a href="/leads?page={page+1}&search={search}&tier={tier_filter}&platform={platform_filter}" class="bg-slate-800 hover:bg-slate-700 text-slate-300 text-sm px-3 py-1.5 rounded-lg transition-colors">Next</a>'
            pag_html += '</div>'

        content = f'''
<div class="bg-slate-900 border border-slate-800 rounded-xl p-4 mb-6 card-glow">
    <form method="GET" action="/leads" class="flex flex-wrap items-center gap-4">
        <div class="flex-1 min-w-[200px]">
            <input type="text" name="search" value="{search}" placeholder="Search leads by name, title, city..."
                   class="w-full bg-slate-800 border border-slate-700 rounded-lg px-4 py-2 text-sm text-slate-200 placeholder-slate-500 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent">
        </div>
        <select name="tier" class="bg-slate-800 border border-slate-700 rounded-lg px-3 py-2 text-sm text-slate-200 focus:outline-none focus:ring-2 focus:ring-blue-500">{tier_options}</select>
        <select name="platform" class="bg-slate-800 border border-slate-700 rounded-lg px-3 py-2 text-sm text-slate-200 focus:outline-none focus:ring-2 focus:ring-blue-500">{plat_options}</select>
        <button type="submit" class="bg-blue-600 hover:bg-blue-700 text-white text-sm font-medium px-4 py-2 rounded-lg transition-colors">Filter</button>
        <a href="/leads" class="text-sm text-slate-400 hover:text-slate-200 transition-colors">Clear</a>
        <a href="/leads/export?search={search}&tier={tier_filter}&platform={platform_filter}"
           class="ml-auto bg-slate-800 hover:bg-slate-700 text-slate-300 text-sm font-medium px-4 py-2 rounded-lg transition-colors flex items-center space-x-2">
            <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 10v6m0 0l-3-3m3 3l3-3m2 8H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z"/></svg>
            <span>Export CSV</span>
        </a>
    </form>
</div>
<div class="flex items-center justify-between mb-4">
    <p class="text-sm text-slate-400">Showing <span class="text-white font-medium">{len(leads)}</span> of <span class="text-white font-medium">{total_count}</span> leads</p>
    <p class="text-xs text-slate-500">Page {page} of {total_pages}</p>
</div>
<div class="bg-slate-900 border border-slate-800 rounded-xl overflow-hidden card-glow">
    <div class="overflow-x-auto">
        <table class="w-full">
            <thead><tr class="border-b border-slate-800">
                <th class="text-left text-xs font-semibold text-slate-400 uppercase tracking-wider px-6 py-3">Name / Title</th>
                <th class="text-left text-xs font-semibold text-slate-400 uppercase tracking-wider px-4 py-3">Score</th>
                <th class="text-left text-xs font-semibold text-slate-400 uppercase tracking-wider px-4 py-3">Tier</th>
                <th class="text-left text-xs font-semibold text-slate-400 uppercase tracking-wider px-4 py-3">Platform</th>
                <th class="text-left text-xs font-semibold text-slate-400 uppercase tracking-wider px-4 py-3">City</th>
                <th class="text-left text-xs font-semibold text-slate-400 uppercase tracking-wider px-4 py-3">Category</th>
                <th class="text-left text-xs font-semibold text-slate-400 uppercase tracking-wider px-4 py-3">Scraped</th>
                <th class="text-left text-xs font-semibold text-slate-400 uppercase tracking-wider px-4 py-3">Actions</th>
            </tr></thead>
            <tbody class="divide-y divide-slate-800/50">{rows_html}</tbody>
        </table>
    </div>
</div>
{pag_html}'''

        return _render_page("Leads", f"{total_count} leads in database", "leads", content)
    finally:
        db.close()


# ---------------------------------------------------------------------------
# Route: Export CSV
# ---------------------------------------------------------------------------

@app.route("/leads/export")
def leads_export():
    db = get_db()
    try:
        search = request.args.get("search", "").strip()
        tier_filter = request.args.get("tier", "").strip()
        platform_filter = request.args.get("platform", "").strip()

        query = "SELECT * FROM jobs WHERE job_type='raw_scrape'"
        params = []
        if platform_filter:
            query += " AND json_extract(payload, '$.platform')=?"
            params.append(platform_filter)
        if search:
            query += " AND payload LIKE ?"
            params.append(f"%{search}%")
        query += " ORDER BY created_at DESC"

        rows = db.execute(query, params).fetchall()
        leads = [_parse_lead(row) for row in rows]
        if tier_filter:
            leads = [l for l in leads if l["tier"] == tier_filter]

        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow([
            "Name", "Title", "Score", "Tier", "Platform", "City", "State",
            "Category", "Contact Email", "Contact Phone", "Source URL",
            "Agent", "Scraped At",
        ])
        for l in leads:
            writer.writerow([
                l["name"], l["title"], l["score"], l["tier"], l["platform"],
                l["city"], l["state"], l["category"], l["contact_email"],
                l["contact_phone"], l["source_url"], l["agent"], l["scraped_at"],
            ])

        resp = Response(output.getvalue(), mimetype="text/csv")
        resp.headers["Content-Disposition"] = (
            f"attachment; filename=leads_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        )
        return resp
    finally:
        db.close()


# ---------------------------------------------------------------------------
# Route: Lead Detail (/leads/<id>)
# ---------------------------------------------------------------------------

@app.route("/leads/<lead_id>")
def lead_detail(lead_id):
    db = get_db()
    try:
        row = db.execute("SELECT * FROM jobs WHERE id=?", (lead_id,)).fetchone()
        if not row:
            return _render_page("Not Found", "", "leads", '<p class="text-slate-400 text-center py-12">Lead not found.</p>'), 404

        lead = _parse_lead(row)

        # Score percentages for bars and radar
        cf_pct = int(lead["score_career_fit"] / 35 * 100) if lead["score_career_fit"] else 0
        mo_pct = int(lead["score_motivation"] / 25 * 100) if lead["score_motivation"] else 0
        ps_pct = int(lead["score_people_skills"] / 20 * 100) if lead["score_people_skills"] else 0
        dm_pct = int(lead["score_demographics"] / 10 * 100) if lead["score_demographics"] else 0
        dq_pct = int(lead["score_data_quality"] / 10 * 100) if lead["score_data_quality"] else 0

        tier_class = {"A": "bg-tier-a tier-a", "B": "bg-tier-b tier-b", "C": "bg-tier-c tier-c", "D": "bg-tier-d tier-d"}.get(lead["tier"], "bg-tier-d tier-d")
        safe_name = (lead["name"] or lead["title"] or "Unnamed Lead").replace("<", "&lt;").replace(">", "&gt;")
        safe_title = (lead["title"] or "").replace("<", "&lt;").replace(">", "&gt;")
        safe_desc = (lead["source_post_text"] or "").replace("<", "&lt;").replace(">", "&gt;")
        safe_url = (lead["source_url"] or "").replace("<", "&lt;").replace(">", "&gt;")

        # Signals tags
        signals_html = ""
        if lead["recruiting_signals"]:
            tags = "".join(f'<span class="bg-blue-500/10 text-blue-400 border border-blue-500/30 px-3 py-1 rounded-full text-xs font-medium">{s}</span>' for s in lead["recruiting_signals"])
            signals_html = f'''
            <div class="bg-slate-900 border border-slate-800 rounded-xl p-6 card-glow">
                <h4 class="text-sm font-semibold text-slate-300 uppercase tracking-wider mb-4">Recruiting Signals</h4>
                <div class="flex flex-wrap gap-2">{tags}</div>
            </div>'''

        # Sentiment display
        if lead["sentiment_score"] is not None:
            sv = lead["sentiment_score"]
            s_color = "text-green-400" if sv > 0.3 else ("text-red-400" if sv < -0.3 else "text-yellow-400")
            s_label = "Positive" if sv > 0.3 else ("Negative" if sv < -0.3 else "Neutral")
            sentiment_html = f'<p class="text-4xl font-bold {s_color}">{sv:.2f}</p><p class="text-xs text-slate-500 mt-1">{s_label} sentiment</p>'
        else:
            sentiment_html = '<p class="text-sm text-slate-500 text-center py-4">No sentiment data available.</p>'

        enriched_dot = '<div class="w-3 h-3 rounded-full bg-green-500"></div><span class="text-sm text-slate-300">Enriched</span>' if lead["enriched"] else '<div class="w-3 h-3 rounded-full bg-slate-600"></div><span class="text-sm text-slate-500">Not Enriched</span>'
        compliance_dot = '<div class="w-3 h-3 rounded-full bg-green-500"></div><span class="text-sm text-slate-300">Compliance Cleared</span>' if lead["compliance_cleared"] else '<div class="w-3 h-3 rounded-full bg-slate-600"></div><span class="text-sm text-slate-500">Pending Compliance</span>'

        source_url_html = f'<div class="mb-4"><p class="text-xs text-slate-500 mb-1">Source URL</p><a href="{safe_url}" target="_blank" rel="noopener" class="text-blue-400 hover:text-blue-300 text-sm break-all">{safe_url}</a></div>' if safe_url else ""
        post_text_html = f'<div><p class="text-xs text-slate-500 mb-1">Original Post Text</p><div class="bg-slate-800/50 rounded-lg p-4 text-sm text-slate-300 whitespace-pre-wrap max-h-60 overflow-y-auto">{safe_desc}</div></div>' if safe_desc else ""

        content = f'''
<a href="/leads" class="inline-flex items-center text-sm text-slate-400 hover:text-blue-400 mb-6 transition-colors">
    <svg class="w-4 h-4 mr-1" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15 19l-7-7 7-7"/></svg>
    Back to Leads
</a>
<div class="grid grid-cols-1 lg:grid-cols-3 gap-6">
    <div class="lg:col-span-2 space-y-6">
        <div class="bg-slate-900 border border-slate-800 rounded-xl p-6 card-glow">
            <h3 class="text-2xl font-bold text-white">{safe_name}</h3>
            {"<p class='text-slate-400 mt-1'>" + lead["current_role"] + "</p>" if lead["current_role"] else ""}
            <div class="flex items-center space-x-4 mt-3">
                <span class="inline-flex items-center px-3 py-1 rounded-full text-sm font-bold border {tier_class}">{lead["tier"]}-Tier &middot; {lead["score"]}/100</span>
                <span class="text-sm text-slate-500">{lead["platform"].title()}</span>
            </div>
        </div>
        <div class="bg-slate-900 border border-slate-800 rounded-xl p-6 card-glow">
            <h4 class="text-sm font-semibold text-slate-300 uppercase tracking-wider mb-4">Lead Details</h4>
            <div class="grid grid-cols-2 gap-4">
                <div class="bg-slate-800/50 rounded-lg p-3"><p class="text-xs text-slate-500 mb-1">Platform</p><p class="text-sm text-slate-200">{lead["platform"].title()}</p></div>
                <div class="bg-slate-800/50 rounded-lg p-3"><p class="text-xs text-slate-500 mb-1">Category</p><p class="text-sm text-slate-200">{lead["category"] or "N/A"}</p></div>
                <div class="bg-slate-800/50 rounded-lg p-3"><p class="text-xs text-slate-500 mb-1">City</p><p class="text-sm text-slate-200">{lead["city"] or "N/A"}{", " + lead["state"] if lead["state"] else ""}</p></div>
                <div class="bg-slate-800/50 rounded-lg p-3"><p class="text-xs text-slate-500 mb-1">Agent</p><p class="text-sm text-slate-200">{lead["agent"] or "N/A"}</p></div>
                <div class="bg-slate-800/50 rounded-lg p-3"><p class="text-xs text-slate-500 mb-1">Contact Email</p><p class="text-sm text-slate-200">{lead["contact_email"] or "N/A"}</p></div>
                <div class="bg-slate-800/50 rounded-lg p-3"><p class="text-xs text-slate-500 mb-1">Contact Phone</p><p class="text-sm text-slate-200">{lead["contact_phone"] or "N/A"}</p></div>
                <div class="bg-slate-800/50 rounded-lg p-3"><p class="text-xs text-slate-500 mb-1">Post ID</p><p class="text-sm text-slate-200">{lead["post_id"] or "N/A"}</p></div>
                <div class="bg-slate-800/50 rounded-lg p-3"><p class="text-xs text-slate-500 mb-1">Scraped At</p><p class="text-sm text-slate-200">{lead["scraped_at"]}</p></div>
            </div>
        </div>
        <div class="bg-slate-900 border border-slate-800 rounded-xl p-6 card-glow">
            <h4 class="text-sm font-semibold text-slate-300 uppercase tracking-wider mb-4">Source Information</h4>
            {source_url_html}
            {post_text_html}
            {"<div class='mt-4'><p class='text-xs text-slate-500 mb-1'>Title</p><p class='text-sm text-slate-200'>" + safe_title + "</p></div>" if safe_title else ""}
        </div>
        {signals_html}
        <div class="bg-slate-900 border border-slate-800 rounded-xl p-6 card-glow">
            <h4 class="text-sm font-semibold text-slate-300 uppercase tracking-wider mb-4">Compliance &amp; Enrichment</h4>
            <div class="grid grid-cols-2 gap-4">
                <div class="flex items-center space-x-3">{enriched_dot}</div>
                <div class="flex items-center space-x-3">{compliance_dot}</div>
            </div>
        </div>
    </div>
    <div class="space-y-6">
        <div class="bg-slate-900 border border-slate-800 rounded-xl p-6 card-glow">
            <h4 class="text-sm font-semibold text-slate-300 uppercase tracking-wider mb-4">Score Breakdown</h4>
            <div style="height:260px"><canvas id="radarChart"></canvas></div>
            <div class="mt-4 space-y-2">
                <div class="flex justify-between text-xs"><span class="text-slate-400">Career Fit</span><span class="text-slate-200 font-medium">{lead["score_career_fit"]}/35</span></div>
                <div class="w-full bg-slate-800 rounded-full h-1.5"><div class="bg-blue-500 h-1.5 rounded-full" style="width:{cf_pct}%"></div></div>
                <div class="flex justify-between text-xs"><span class="text-slate-400">Motivation</span><span class="text-slate-200 font-medium">{lead["score_motivation"]}/25</span></div>
                <div class="w-full bg-slate-800 rounded-full h-1.5"><div class="bg-purple-500 h-1.5 rounded-full" style="width:{mo_pct}%"></div></div>
                <div class="flex justify-between text-xs"><span class="text-slate-400">People Skills</span><span class="text-slate-200 font-medium">{lead["score_people_skills"]}/20</span></div>
                <div class="w-full bg-slate-800 rounded-full h-1.5"><div class="bg-cyan-500 h-1.5 rounded-full" style="width:{ps_pct}%"></div></div>
                <div class="flex justify-between text-xs"><span class="text-slate-400">Demographics</span><span class="text-slate-200 font-medium">{lead["score_demographics"]}/10</span></div>
                <div class="w-full bg-slate-800 rounded-full h-1.5"><div class="bg-amber-500 h-1.5 rounded-full" style="width:{dm_pct}%"></div></div>
                <div class="flex justify-between text-xs"><span class="text-slate-400">Data Quality</span><span class="text-slate-200 font-medium">{lead["score_data_quality"]}/10</span></div>
                <div class="w-full bg-slate-800 rounded-full h-1.5"><div class="bg-green-500 h-1.5 rounded-full" style="width:{dq_pct}%"></div></div>
            </div>
        </div>
        <div class="bg-slate-900 border border-slate-800 rounded-xl p-6 card-glow">
            <h4 class="text-sm font-semibold text-slate-300 uppercase tracking-wider mb-4">Sentiment Analysis</h4>
            <div class="text-center">{sentiment_html}</div>
        </div>
    </div>
</div>
<script>
Chart.defaults.color='#94a3b8';
new Chart(document.getElementById('radarChart'),{{
    type:'radar',
    data:{{ labels:['Career Fit','Motivation','People Skills','Demographics','Data Quality'], datasets:[{{ label:'Score',data:[{cf_pct},{mo_pct},{ps_pct},{dm_pct},{dq_pct}],backgroundColor:'rgba(59,130,246,0.2)',borderColor:'#3b82f6',pointBackgroundColor:'#3b82f6',pointBorderColor:'#1e293b',pointBorderWidth:2 }}] }},
    options:{{ responsive:true,maintainAspectRatio:false,plugins:{{ legend:{{ display:false }} }},scales:{{ r:{{ beginAtZero:true,max:100,ticks:{{ stepSize:25,font:{{ size:9 }},backdropColor:'transparent' }},grid:{{ color:'rgba(51,65,85,0.5)' }},angleLines:{{ color:'rgba(51,65,85,0.5)' }},pointLabels:{{ font:{{ size:10 }} }} }} }} }}
}});
</script>'''

        return _render_page("Lead Detail", safe_name, "leads", content)
    finally:
        db.close()


# ---------------------------------------------------------------------------
# Route: Agents (/agents)
# ---------------------------------------------------------------------------

@app.route("/agents")
def agents_page():
    db = get_db()
    try:
        run_rows = db.execute(
            "SELECT payload, created_at FROM jobs WHERE job_type='agent_run_log' ORDER BY created_at DESC"
        ).fetchall()
        ar_rows = db.execute("SELECT * FROM agent_runs ORDER BY completed_at DESC").fetchall()

        agent_map = {}
        for row in run_rows:
            p = safe_json(row[0])
            name = p.get("agent_name", "unknown")
            if name not in agent_map:
                agent_map[name] = {"name": name, "platform": p.get("platform", "?"), "run_count": 0, "total_found": 0, "total_new": 0, "error_count": 0, "last_status": "unknown", "last_run": "Never", "first": True}
            ag = agent_map[name]
            ag["run_count"] += 1
            ag["total_found"] += p.get("items_found", 0)
            ag["total_new"] += p.get("items_new", 0)
            status = p.get("status", "unknown")
            if status in ("failed", "error"):
                ag["error_count"] += 1
            if ag["first"]:
                ag["last_status"] = status
                ag["last_run"] = time_ago(p.get("completed_at", row[1]))
                ag["first"] = False

        for row in ar_rows:
            name = row["agent_name"]
            if name not in agent_map:
                agent_map[name] = {"name": name, "platform": row["platform"] or "?", "run_count": 0, "total_found": 0, "total_new": 0, "error_count": 0, "last_status": row["status"], "last_run": time_ago(row["completed_at"]), "first": False}
            ag = agent_map[name]
            ag["run_count"] += 1
            ag["total_found"] += row["items_found"]
            ag["total_new"] += row["items_new"]
            if row["status"] in ("failed", "error"):
                ag["error_count"] += 1

        agents = sorted(agent_map.values(), key=lambda a: a["total_found"], reverse=True)
        total_runs = sum(a["run_count"] for a in agents)
        total_items = sum(a["total_found"] for a in agents)
        total_errors = sum(a["error_count"] for a in agents)
        error_rate = (total_errors / total_runs * 100) if total_runs > 0 else 0.0

        er_color = "text-red-400" if error_rate > 10 else ("text-yellow-400" if error_rate > 5 else "text-green-400")

        rows_html = ""
        for ag in agents:
            if ag["last_status"] in ("success", "completed"):
                status_html = '<span class="inline-flex items-center space-x-1.5 text-xs font-medium text-green-400"><div class="w-1.5 h-1.5 rounded-full bg-green-500"></div><span>OK</span></span>'
            elif ag["last_status"] in ("failed", "error"):
                status_html = '<span class="inline-flex items-center space-x-1.5 text-xs font-medium text-red-400"><div class="w-1.5 h-1.5 rounded-full bg-red-500"></div><span>Error</span></span>'
            else:
                status_html = f'<span class="inline-flex items-center space-x-1.5 text-xs font-medium text-yellow-400"><div class="w-1.5 h-1.5 rounded-full bg-yellow-500 pulse-dot"></div><span>{ag["last_status"].title()}</span></span>'

            err_cls = "text-red-400" if ag["error_count"] > 0 else "text-slate-500"
            rows_html += f'''<tr class="hover:bg-slate-800/30 transition-colors">
                <td class="px-6 py-3"><a href="/agents/{ag["name"]}" class="text-sm font-medium text-blue-400 hover:text-blue-300 transition-colors">{ag["name"]}</a></td>
                <td class="px-4 py-3"><span class="text-sm text-slate-400">{ag["platform"].title()}</span></td>
                <td class="px-4 py-3">{status_html}</td>
                <td class="px-4 py-3"><span class="text-sm text-slate-300 font-mono">{ag["run_count"]}</span></td>
                <td class="px-4 py-3"><span class="text-sm text-slate-300 font-mono">{ag["total_found"]}</span></td>
                <td class="px-4 py-3"><span class="text-sm text-slate-300 font-mono">{ag["total_new"]}</span></td>
                <td class="px-4 py-3"><span class="text-sm font-mono {err_cls}">{ag["error_count"]}</span></td>
                <td class="px-4 py-3"><span class="text-xs text-slate-500">{ag["last_run"]}</span></td>
                <td class="px-4 py-3">
                    <div class="w-10 h-5 bg-green-500/30 rounded-full border border-green-500/50 cursor-pointer relative">
                        <div class="w-4 h-4 bg-green-500 rounded-full absolute top-0.5 left-5"></div>
                    </div>
                </td>
            </tr>'''

        if not agents:
            rows_html = '<tr><td colspan="9" class="px-6 py-12 text-center text-slate-500">No agent data recorded yet.</td></tr>'

        content = f'''
<div class="grid grid-cols-1 md:grid-cols-4 gap-4 mb-8">
    <div class="stat-card bg-slate-900 border border-slate-800 rounded-xl p-5 card-glow">
        <p class="text-xs font-medium text-slate-400 uppercase tracking-wider mb-2">Total Agents</p>
        <p class="text-3xl font-bold text-white">{len(agents)}</p>
    </div>
    <div class="stat-card bg-slate-900 border border-slate-800 rounded-xl p-5 card-glow">
        <p class="text-xs font-medium text-slate-400 uppercase tracking-wider mb-2">Total Runs</p>
        <p class="text-3xl font-bold text-white">{total_runs}</p>
    </div>
    <div class="stat-card bg-slate-900 border border-slate-800 rounded-xl p-5 card-glow">
        <p class="text-xs font-medium text-slate-400 uppercase tracking-wider mb-2">Total Items Found</p>
        <p class="text-3xl font-bold text-white">{total_items}</p>
    </div>
    <div class="stat-card bg-slate-900 border border-slate-800 rounded-xl p-5 card-glow">
        <p class="text-xs font-medium text-slate-400 uppercase tracking-wider mb-2">Error Rate</p>
        <p class="text-3xl font-bold {er_color}">{error_rate:.1f}%</p>
    </div>
</div>
<div class="bg-slate-900 border border-slate-800 rounded-xl overflow-hidden card-glow">
    <div class="overflow-x-auto"><table class="w-full">
        <thead><tr class="border-b border-slate-800">
            <th class="text-left text-xs font-semibold text-slate-400 uppercase tracking-wider px-6 py-3">Agent</th>
            <th class="text-left text-xs font-semibold text-slate-400 uppercase tracking-wider px-4 py-3">Platform</th>
            <th class="text-left text-xs font-semibold text-slate-400 uppercase tracking-wider px-4 py-3">Status</th>
            <th class="text-left text-xs font-semibold text-slate-400 uppercase tracking-wider px-4 py-3">Runs</th>
            <th class="text-left text-xs font-semibold text-slate-400 uppercase tracking-wider px-4 py-3">Items Found</th>
            <th class="text-left text-xs font-semibold text-slate-400 uppercase tracking-wider px-4 py-3">Items New</th>
            <th class="text-left text-xs font-semibold text-slate-400 uppercase tracking-wider px-4 py-3">Errors</th>
            <th class="text-left text-xs font-semibold text-slate-400 uppercase tracking-wider px-4 py-3">Last Run</th>
            <th class="text-left text-xs font-semibold text-slate-400 uppercase tracking-wider px-4 py-3">Enabled</th>
        </tr></thead>
        <tbody class="divide-y divide-slate-800/50">{rows_html}</tbody>
    </table></div>
</div>'''

        return _render_page("Agents", "Scraping agent status and performance", "agents", content)
    finally:
        db.close()


# ---------------------------------------------------------------------------
# Route: Agent Detail (/agents/<name>)
# ---------------------------------------------------------------------------

@app.route("/agents/<agent_name>")
def agent_detail(agent_name):
    db = get_db()
    try:
        run_rows = db.execute(
            "SELECT payload, created_at FROM jobs WHERE job_type='agent_run_log' AND json_extract(payload, '$.agent_name')=? ORDER BY created_at DESC",
            (agent_name,),
        ).fetchall()

        runs = []
        total_found = total_new = 0
        for row in run_rows:
            p = safe_json(row[0])
            runs.append({"status": p.get("status", "unknown"), "items_found": p.get("items_found", 0), "items_new": p.get("items_new", 0), "time": fmt_dt(p.get("completed_at", row[1])), "error": p.get("error")})
            total_found += p.get("items_found", 0)
            total_new += p.get("items_new", 0)

        ar_rows = db.execute("SELECT * FROM agent_runs WHERE agent_name=? ORDER BY completed_at DESC", (agent_name,)).fetchall()
        for row in ar_rows:
            runs.append({"status": row["status"], "items_found": row["items_found"], "items_new": row["items_new"], "time": fmt_dt(row["completed_at"]), "error": row["error"]})
            total_found += row["items_found"]
            total_new += row["items_new"]

        rows_html = ""
        for run in runs:
            if run["status"] in ("success", "completed"):
                s_html = '<span class="inline-flex items-center space-x-1.5 text-xs font-medium text-green-400"><div class="w-1.5 h-1.5 rounded-full bg-green-500"></div><span>' + run["status"].title() + '</span></span>'
            else:
                s_html = '<span class="inline-flex items-center space-x-1.5 text-xs font-medium text-red-400"><div class="w-1.5 h-1.5 rounded-full bg-red-500"></div><span>' + run["status"].title() + '</span></span>'
            err = (run["error"] or "-").replace("<", "&lt;")
            rows_html += f'<tr class="hover:bg-slate-800/30 transition-colors"><td class="px-6 py-3">{s_html}</td><td class="px-4 py-3 text-sm text-slate-300 font-mono">{run["items_found"]}</td><td class="px-4 py-3 text-sm text-slate-300 font-mono">{run["items_new"]}</td><td class="px-4 py-3 text-xs text-slate-500">{run["time"]}</td><td class="px-4 py-3 text-xs text-red-400 max-w-xs truncate">{err}</td></tr>'

        content = f'''
<a href="/agents" class="inline-flex items-center text-sm text-slate-400 hover:text-blue-400 mb-6 transition-colors">
    <svg class="w-4 h-4 mr-1" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15 19l-7-7 7-7"/></svg>
    Back to Agents
</a>
<div class="bg-slate-900 border border-slate-800 rounded-xl p-6 card-glow mb-6">
    <div class="flex items-center justify-between">
        <div><h3 class="text-xl font-bold text-white">{agent_name}</h3><p class="text-sm text-slate-400 mt-1">{len(runs)} recorded runs</p></div>
        <div class="flex items-center space-x-4 text-sm">
            <span class="text-slate-400">Total found: <span class="text-white font-bold">{total_found}</span></span>
            <span class="text-slate-400">Total new: <span class="text-white font-bold">{total_new}</span></span>
        </div>
    </div>
</div>
<div class="bg-slate-900 border border-slate-800 rounded-xl overflow-hidden card-glow">
    <div class="overflow-x-auto"><table class="w-full">
        <thead><tr class="border-b border-slate-800">
            <th class="text-left text-xs font-semibold text-slate-400 uppercase tracking-wider px-6 py-3">Status</th>
            <th class="text-left text-xs font-semibold text-slate-400 uppercase tracking-wider px-4 py-3">Items Found</th>
            <th class="text-left text-xs font-semibold text-slate-400 uppercase tracking-wider px-4 py-3">Items New</th>
            <th class="text-left text-xs font-semibold text-slate-400 uppercase tracking-wider px-4 py-3">Time</th>
            <th class="text-left text-xs font-semibold text-slate-400 uppercase tracking-wider px-4 py-3">Error</th>
        </tr></thead>
        <tbody class="divide-y divide-slate-800/50">{rows_html}</tbody>
    </table></div>
</div>'''

        return _render_page("Agent Detail", agent_name, "agents", content)
    finally:
        db.close()


# ---------------------------------------------------------------------------
# Route: Analytics (/analytics)
# ---------------------------------------------------------------------------

@app.route("/analytics")
def analytics():
    db = get_db()
    try:
        rows = db.execute(
            "SELECT payload, created_at FROM jobs WHERE job_type='raw_scrape' ORDER BY created_at ASC"
        ).fetchall()

        daily_map = {}
        location_map = {}
        score_bins = [0] * 10
        platform_total = {}
        freshness = [0, 0, 0, 0]  # <7, 7-14, 14-30, 30+
        category_map = {}
        now = datetime.now(timezone.utc)

        for row in rows:
            p = safe_json(row[0])
            data = p.get("data", {})

            scraped = p.get("scraped_at", row[1])
            if scraped:
                try:
                    day = scraped[:10]
                    daily_map[day] = daily_map.get(day, 0) + 1
                except (IndexError, TypeError):
                    pass

            state = data.get("location_state", "")
            city = data.get("location_city", "")
            loc = state or city or "Unknown"
            location_map[loc] = location_map.get(loc, 0) + 1

            score = data.get("total_score", 0)
            bucket = min(score // 10, 9)
            score_bins[bucket] += 1

            plat = p.get("platform", data.get("platform", "unknown"))
            platform_total[plat] = platform_total.get(plat, 0) + 1

            try:
                dt = datetime.fromisoformat(scraped.replace("Z", "+00:00"))
                diff = (now - dt).days
                if diff < 7:
                    freshness[0] += 1
                elif diff < 14:
                    freshness[1] += 1
                elif diff < 30:
                    freshness[2] += 1
                else:
                    freshness[3] += 1
            except (ValueError, AttributeError, TypeError):
                freshness[3] += 1

            cat = data.get("category", "uncategorized") or "uncategorized"
            category_map[cat] = category_map.get(cat, 0) + 1

        # Volume chart
        vol_labels = []
        vol_data = []
        for i in range(29, -1, -1):
            day = (now - timedelta(days=i)).strftime("%Y-%m-%d")
            vol_labels.append((now - timedelta(days=i)).strftime("%b %d"))
            vol_data.append(daily_map.get(day, 0))

        # Top 10 locations
        sorted_locs = sorted(location_map.items(), key=lambda x: x[1], reverse=True)[:10]
        loc_labels = json.dumps([l[0] for l in sorted_locs])
        loc_counts = json.dumps([l[1] for l in sorted_locs])

        # Score bins
        score_labels = json.dumps([f"{i*10}-{i*10+9}" for i in range(10)])
        score_counts = json.dumps(score_bins)

        # Platform comparison
        plat_labels = json.dumps([k.title() for k in platform_total.keys()])
        plat_total_json = json.dumps(list(platform_total.values()))

        run_rows = db.execute("SELECT payload FROM jobs WHERE job_type='agent_run_log'").fetchall()
        plat_run_map = {}
        for r in run_rows:
            rp = safe_json(r[0])
            pk = rp.get("platform", "unknown")
            plat_run_map[pk] = plat_run_map.get(pk, 0) + 1
        plat_runs_json = json.dumps([plat_run_map.get(k, 0) for k in platform_total.keys()])

        freshness_json = json.dumps(freshness)

        sorted_cats = sorted(category_map.items(), key=lambda x: x[1], reverse=True)[:8]
        cat_labels = json.dumps([c[0].title() for c in sorted_cats])
        cat_counts = json.dumps([c[1] for c in sorted_cats])

        vol_labels_json = json.dumps(vol_labels)
        vol_data_json = json.dumps(vol_data)

        # Pipeline stats
        total_raw = len(rows)
        status_counts = db.execute("SELECT status, COUNT(*) FROM jobs GROUP BY status").fetchall()
        status_map = {r[0]: r[1] for r in status_counts}
        pending_count = status_map.get("pending", 0)
        done_count = status_map.get("done", 0)
        failed_count = status_map.get("failed", 0)
        unique_agents = db.execute("SELECT COUNT(DISTINCT json_extract(payload, '$.agent')) FROM jobs WHERE job_type='raw_scrape'").fetchone()[0]
        cd_count = db.execute("SELECT COUNT(*) FROM change_detection").fetchone()[0]
        log_count = len(list(LOGS_DIR.glob("*.log"))) if LOGS_DIR.exists() else 0

        content = f'''
<div class="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-8">
    <div class="bg-slate-900 border border-slate-800 rounded-xl p-6 card-glow">
        <h3 class="text-sm font-semibold text-slate-300 mb-4 uppercase tracking-wider">Scraping Volume Over Time</h3>
        <div style="height:300px"><canvas id="volumeChart"></canvas></div>
    </div>
    <div class="bg-slate-900 border border-slate-800 rounded-xl p-6 card-glow">
        <h3 class="text-sm font-semibold text-slate-300 mb-4 uppercase tracking-wider">Top Locations</h3>
        <div style="height:300px"><canvas id="zipChart"></canvas></div>
    </div>
</div>
<div class="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-8">
    <div class="bg-slate-900 border border-slate-800 rounded-xl p-6 card-glow">
        <h3 class="text-sm font-semibold text-slate-300 mb-4 uppercase tracking-wider">Lead Quality Distribution</h3>
        <div style="height:300px"><canvas id="qualityChart"></canvas></div>
    </div>
    <div class="bg-slate-900 border border-slate-800 rounded-xl p-6 card-glow">
        <h3 class="text-sm font-semibold text-slate-300 mb-4 uppercase tracking-wider">Platform Performance</h3>
        <div style="height:300px"><canvas id="platformChart"></canvas></div>
    </div>
</div>
<div class="grid grid-cols-1 lg:grid-cols-3 gap-6 mb-8">
    <div class="bg-slate-900 border border-slate-800 rounded-xl p-6 card-glow">
        <h3 class="text-sm font-semibold text-slate-300 mb-4 uppercase tracking-wider">Lead Freshness</h3>
        <div style="height:280px"><canvas id="freshnessChart"></canvas></div>
    </div>
    <div class="bg-slate-900 border border-slate-800 rounded-xl p-6 card-glow">
        <h3 class="text-sm font-semibold text-slate-300 mb-4 uppercase tracking-wider">Top Categories</h3>
        <div style="height:280px"><canvas id="categoryChart"></canvas></div>
    </div>
    <div class="bg-slate-900 border border-slate-800 rounded-xl p-6 card-glow">
        <h3 class="text-sm font-semibold text-slate-300 mb-4 uppercase tracking-wider">Pipeline Summary</h3>
        <div class="space-y-4 mt-4">
            <div class="flex items-center justify-between"><span class="text-sm text-slate-400">Total Raw Scrapes</span><span class="text-lg font-bold text-white">{total_raw}</span></div>
            <div class="flex items-center justify-between"><span class="text-sm text-slate-400">Pending Processing</span><span class="text-lg font-bold text-yellow-400">{pending_count}</span></div>
            <div class="flex items-center justify-between"><span class="text-sm text-slate-400">Completed Jobs</span><span class="text-lg font-bold text-green-400">{done_count}</span></div>
            <div class="flex items-center justify-between"><span class="text-sm text-slate-400">Failed Jobs</span><span class="text-lg font-bold text-red-400">{failed_count}</span></div>
            <div class="flex items-center justify-between"><span class="text-sm text-slate-400">Unique Agents</span><span class="text-lg font-bold text-blue-400">{unique_agents}</span></div>
            <div class="flex items-center justify-between"><span class="text-sm text-slate-400">Change Detection URLs</span><span class="text-lg font-bold text-purple-400">{cd_count}</span></div>
            <div class="flex items-center justify-between"><span class="text-sm text-slate-400">Log Files</span><span class="text-lg font-bold text-cyan-400">{log_count}</span></div>
        </div>
    </div>
</div>
<script>
Chart.defaults.color='#94a3b8';
Chart.defaults.borderColor='rgba(51,65,85,0.5)';
new Chart(document.getElementById('volumeChart'),{{type:'line',data:{{labels:{vol_labels_json},datasets:[{{label:'Scrapes',data:{vol_data_json},borderColor:'#8b5cf6',backgroundColor:'rgba(139,92,246,0.1)',fill:true,tension:0.4,pointRadius:2}}]}},options:{{responsive:true,maintainAspectRatio:false,plugins:{{legend:{{display:false}}}},scales:{{x:{{grid:{{display:false}},ticks:{{font:{{size:10}}}}}},y:{{beginAtZero:true,grid:{{color:'rgba(51,65,85,0.3)'}}}}}}}}}});
new Chart(document.getElementById('zipChart'),{{type:'bar',data:{{labels:{loc_labels},datasets:[{{label:'Leads',data:{loc_counts},backgroundColor:'rgba(6,182,212,0.6)',borderColor:'#06b6d4',borderWidth:1,borderRadius:4}}]}},options:{{responsive:true,maintainAspectRatio:false,indexAxis:'y',plugins:{{legend:{{display:false}}}},scales:{{x:{{beginAtZero:true,grid:{{color:'rgba(51,65,85,0.3)'}}}},y:{{grid:{{display:false}},ticks:{{font:{{size:10}}}}}}}}}}}});
new Chart(document.getElementById('qualityChart'),{{type:'bar',data:{{labels:{score_labels},datasets:[{{label:'Leads',data:{score_counts},backgroundColor:['rgba(239,68,68,0.6)','rgba(239,68,68,0.5)','rgba(234,179,8,0.5)','rgba(234,179,8,0.6)','rgba(59,130,246,0.5)','rgba(59,130,246,0.6)','rgba(34,197,94,0.5)','rgba(34,197,94,0.6)','rgba(34,197,94,0.7)','rgba(34,197,94,0.8)'],borderRadius:4}}]}},options:{{responsive:true,maintainAspectRatio:false,plugins:{{legend:{{display:false}}}},scales:{{x:{{grid:{{display:false}},ticks:{{font:{{size:10}}}}}},y:{{beginAtZero:true,grid:{{color:'rgba(51,65,85,0.3)'}}}}}}}}}});
new Chart(document.getElementById('platformChart'),{{type:'bar',data:{{labels:{plat_labels},datasets:[{{label:'Total Scraped',data:{plat_total_json},backgroundColor:'rgba(59,130,246,0.6)',borderColor:'#3b82f6',borderWidth:1,borderRadius:4}},{{label:'Agent Runs',data:{plat_runs_json},backgroundColor:'rgba(139,92,246,0.6)',borderColor:'#8b5cf6',borderWidth:1,borderRadius:4}}]}},options:{{responsive:true,maintainAspectRatio:false,plugins:{{legend:{{position:'bottom',labels:{{boxWidth:12,font:{{size:11}}}}}}}},scales:{{x:{{grid:{{display:false}}}},y:{{beginAtZero:true,grid:{{color:'rgba(51,65,85,0.3)'}}}}}}}}}});
new Chart(document.getElementById('freshnessChart'),{{type:'doughnut',data:{{labels:['< 7 days','7-14 days','14-30 days','30+ days'],datasets:[{{data:{freshness_json},backgroundColor:['rgba(34,197,94,0.7)','rgba(59,130,246,0.7)','rgba(234,179,8,0.7)','rgba(239,68,68,0.7)'],borderColor:'#0f172a',borderWidth:2}}]}},options:{{responsive:true,maintainAspectRatio:false,plugins:{{legend:{{position:'bottom',labels:{{boxWidth:12,padding:10,font:{{size:11}}}}}}}}}}}});
new Chart(document.getElementById('categoryChart'),{{type:'doughnut',data:{{labels:{cat_labels},datasets:[{{data:{cat_counts},backgroundColor:['#3b82f6','#8b5cf6','#06b6d4','#f59e0b','#ef4444','#22c55e','#ec4899','#14b8a6','#f97316','#6366f1'],borderColor:'#0f172a',borderWidth:2}}]}},options:{{responsive:true,maintainAspectRatio:false,plugins:{{legend:{{position:'bottom',labels:{{boxWidth:12,padding:10,font:{{size:11}}}}}}}}}}}});
</script>'''

        return _render_page("Analytics", "Deep dive into scraping and lead quality metrics", "analytics", content)
    finally:
        db.close()


# ---------------------------------------------------------------------------
# API endpoint
# ---------------------------------------------------------------------------

@app.route("/api/stats")
def api_stats():
    db = get_db()
    try:
        total = db.execute("SELECT COUNT(*) FROM jobs WHERE job_type='raw_scrape'").fetchone()[0]
        today_start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
        new_today = db.execute("SELECT COUNT(*) FROM jobs WHERE job_type='raw_scrape' AND created_at >= ?", (today_start,)).fetchone()[0]
        pending = db.execute("SELECT COUNT(*) FROM jobs WHERE status='pending'").fetchone()[0]
        return {"total_leads": total, "new_today": new_today, "pending": pending}
    finally:
        db.close()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    PORT = 8080
    print(f"\n  Lead Gen Dashboard")
    print(f"  Database: {DB_PATH}")
    print(f"  URL: http://localhost:{PORT}\n")
    app.run(host="0.0.0.0", port=PORT, debug=True)
