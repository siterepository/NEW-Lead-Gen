"""
NEW Lead Gen Dashboard - Full-featured Flask web dashboard.

Visualizes scraping activity, leads, scoring, and agent status.
Reads from the SQLite database at data/leadgen.db.

Run with:  python3 -m leadgen.dashboard.app
"""

import asyncio
import csv
import io
import json
import os
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

from flask import Flask, Response, jsonify, render_template_string, request

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


def _esc(text):
    """HTML-escape a string."""
    if not text:
        return ""
    return str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")


# ---------------------------------------------------------------------------
# Scoring dimension explanations - maps keywords to readable reasons
# ---------------------------------------------------------------------------

CAREER_FIT_CATEGORIES = {
    "sales": (["sales", "business development", "account executive", "account manager", "sales manager", "sales rep", "bdr", "sdr", "closer", "revenue", "quota", "outside sales", "inside sales"], 10, "Sales background"),
    "entrepreneurial": (["entrepreneur", "founder", "co-founder", "owner", "self-employed", "freelancer", "freelance", "startup", "business owner", "consultant", "solopreneur"], 8, "Entrepreneurial experience"),
    "real_estate": (["real estate agent", "real estate broker", "realtor", "real estate", "realty", "property manager", "keller williams", "coldwell banker"], 7, "Real estate background"),
    "insurance": (["insurance agent", "insurance broker", "insurance", "underwriter", "state farm", "allstate", "life insurance"], 7, "Insurance industry"),
    "military": (["veteran", "military", "army", "navy", "air force", "marine", "coast guard", "national guard", "transitioning military"], 7, "Military veteran"),
    "teaching": (["teacher", "professor", "instructor", "educator", "coach", "coaching", "mentor", "mentoring"], 6, "Education/coaching"),
    "leadership": (["manager", "director", "vp", "vice president", "team lead", "supervisor", "executive", "general manager"], 5, "Leadership role"),
    "athletics": (["athlete", "athletics", "collegiate athlete", "competitive", "varsity", "captain", "personal trainer"], 4, "Athletic background"),
    "customer_facing": (["retail manager", "hospitality", "restaurant manager", "customer service", "customer success", "bartender", "store manager"], 3, "Customer-facing"),
}

MOTIVATION_GROUPS = {
    "job_seeking": (["looking for work", "open to opportunities", "job search", "seeking employment", "available immediately", "open to work", "actively looking", "#opentowork", "job hunting", "between jobs"], 10, "Actively seeking work"),
    "career_change": (["career change", "career pivot", "career transition", "new chapter", "fresh start", "reinvent myself", "switching careers", "new direction"], 8, "Career change signals"),
    "unemployed": (["laid off", "layoff", "downsized", "restructured", "recently unemployed"], 7, "Recently unemployed/laid off"),
    "dissatisfied": (["burned out", "burnout", "need a change", "underpaid", "undervalued", "toxic workplace", "hate my job", "dead end", "frustrated", "overworked"], 6, "Job dissatisfaction"),
    "entrepreneurial_aspiration": (["passive income", "financial freedom", "be my own boss", "side hustle", "wealth building", "residual income", "time freedom"], 5, "Entrepreneurial aspiration"),
    "returning": (["returning to work", "back to work", "re-entering workforce", "career comeback", "stay at home"], 5, "Returning to workforce"),
}

PEOPLE_SKILLS_GROUPS = {
    "networker": (["networking", "networker", "connector", "community builder", "relationship builder", "500+ connections"], 6, "Active networker"),
    "volunteer": (["volunteer", "community service", "nonprofit", "charity", "board member", "rotary"], 5, "Community involvement"),
    "coaching": (["coach", "coaching", "mentor", "mentoring", "life coach"], 5, "Coaching/mentoring"),
    "speaking": (["public speaking", "speaker", "keynote", "toastmasters", "presenter"], 4, "Public speaking"),
    "social_media": (["influencer", "content creator", "blogger", "podcast", "thought leader"], 3, "Social media presence"),
    "team_lead": (["team leader", "led a team", "managed a team", "built a team", "team captain", "leadership"], 3, "Team leadership"),
}


def _explain_score(text):
    """Generate human-readable explanations for each scoring dimension."""
    if not text:
        return {"career_fit": [], "motivation": [], "people_skills": [], "demographics": [], "data_quality": []}
    text_lower = text.lower()
    explanations = {"career_fit": [], "motivation": [], "people_skills": [], "demographics": [], "data_quality": []}

    for cat, (keywords, pts, label) in CAREER_FIT_CATEGORIES.items():
        for kw in keywords:
            if kw in text_lower:
                explanations["career_fit"].append(f"{label} (+{pts}): matched '{kw}'")
                break

    for grp, (keywords, pts, label) in MOTIVATION_GROUPS.items():
        for kw in keywords:
            if kw in text_lower:
                explanations["motivation"].append(f"{label} (+{pts}): matched '{kw}'")
                break

    for grp, (keywords, pts, label) in PEOPLE_SKILLS_GROUPS.items():
        for kw in keywords:
            if kw in text_lower:
                explanations["people_skills"].append(f"{label} (+{pts}): matched '{kw}'")
                break

    # Demographics
    for kw in ["utah", "ut"]:
        if kw in text_lower:
            explanations["demographics"].append("Utah resident (+3)")
            break
    for kw in ["bachelor", "master", "mba", "phd", "degree", "university", "college"]:
        if kw in text_lower:
            explanations["demographics"].append(f"College educated (+2): matched '{kw}'")
            break

    # Data quality hints
    if "@" in text_lower:
        explanations["data_quality"].append("Email available (+3)")
    if "linkedin.com" in text_lower:
        explanations["data_quality"].append("LinkedIn URL available (+2)")

    return explanations


# ---------------------------------------------------------------------------
# Layout wrapper
# ---------------------------------------------------------------------------

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
.chip { cursor: pointer; user-select: none; transition: all 0.15s; }
.chip:hover { opacity: 0.85; }
.chip.active { ring: 2px; box-shadow: 0 0 0 2px rgba(59,130,246,0.5); }
.score-bar { transition: width 0.6s ease; }
.modal-backdrop { background: rgba(0,0,0,0.6); backdrop-filter: blur(4px); }
.enhance-spin { animation: spin 1s linear infinite; }
@keyframes spin { from { transform: rotate(0deg); } to { transform: rotate(360deg); } }
</style>
</head>
<body class="bg-slate-950 text-slate-200 min-h-screen">
<div class="flex min-h-screen">
<!-- Sidebar -->
<aside class="w-64 bg-slate-900 border-r border-slate-800 fixed h-full z-20 flex flex-col">
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

<!-- Enhance Modal (global, used on leads page) -->
<div id="enhanceModal" class="fixed inset-0 z-50 hidden">
    <div class="modal-backdrop absolute inset-0" onclick="closeEnhanceModal()"></div>
    <div class="relative z-10 max-w-2xl mx-auto mt-20 bg-slate-900 border border-slate-700 rounded-2xl shadow-2xl overflow-hidden">
        <div class="flex items-center justify-between px-6 py-4 border-b border-slate-800">
            <h3 class="text-lg font-semibold text-white" id="enhanceModalTitle">Enhance Lead</h3>
            <button onclick="closeEnhanceModal()" class="text-slate-400 hover:text-white transition-colors">
                <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"/></svg>
            </button>
        </div>
        <div class="p-6">
            <div id="enhanceCurrentData" class="mb-4"></div>
            <button id="enhanceNowBtn" onclick="runEnhance()" class="w-full bg-gradient-to-r from-blue-600 to-purple-600 hover:from-blue-700 hover:to-purple-700 text-white font-bold py-3 px-6 rounded-xl transition-all text-lg mb-4">
                ENHANCE NOW
            </button>
            <div id="enhanceResults" class="hidden">
                <h4 class="text-sm font-semibold text-slate-300 uppercase tracking-wider mb-3">Search Results</h4>
                <div id="enhanceResultsList" class="space-y-3 max-h-80 overflow-y-auto"></div>
                <button id="enhanceSaveBtn" onclick="saveEnhanceData()" class="mt-4 w-full bg-green-600 hover:bg-green-700 text-white font-semibold py-2.5 px-4 rounded-lg transition-colors hidden">
                    Save Found Data to Lead
                </button>
            </div>
            <div id="enhanceLoading" class="hidden text-center py-8">
                <svg class="w-8 h-8 text-blue-400 mx-auto enhance-spin" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15"/></svg>
                <p class="text-sm text-slate-400 mt-3">Searching across multiple providers...</p>
            </div>
            <div id="enhanceError" class="hidden bg-red-500/10 border border-red-500/30 rounded-lg p-4 mt-4">
                <p class="text-sm text-red-400" id="enhanceErrorMsg"></p>
            </div>
        </div>
    </div>
</div>

<script>
// ---- Enhance Modal Logic ----
let currentEnhanceLeadId = null;
let enhanceFoundData = {};

function openEnhanceModal(leadId, leadName, leadData) {
    currentEnhanceLeadId = leadId;
    enhanceFoundData = {};
    document.getElementById('enhanceModalTitle').textContent = 'Enhance: ' + leadName;
    document.getElementById('enhanceResults').classList.add('hidden');
    document.getElementById('enhanceLoading').classList.add('hidden');
    document.getElementById('enhanceError').classList.add('hidden');
    document.getElementById('enhanceSaveBtn').classList.add('hidden');
    document.getElementById('enhanceNowBtn').disabled = false;
    document.getElementById('enhanceNowBtn').textContent = 'ENHANCE NOW';

    let html = '<div class="grid grid-cols-2 gap-3">';
    if (leadData.name) html += '<div class="bg-slate-800/50 rounded-lg p-3"><p class="text-xs text-slate-500">Name</p><p class="text-sm text-slate-200">' + leadData.name + '</p></div>';
    if (leadData.platform) html += '<div class="bg-slate-800/50 rounded-lg p-3"><p class="text-xs text-slate-500">Platform</p><p class="text-sm text-slate-200">' + leadData.platform + '</p></div>';
    if (leadData.city) html += '<div class="bg-slate-800/50 rounded-lg p-3"><p class="text-xs text-slate-500">Location</p><p class="text-sm text-slate-200">' + leadData.city + '</p></div>';
    if (leadData.score) html += '<div class="bg-slate-800/50 rounded-lg p-3"><p class="text-xs text-slate-500">Score</p><p class="text-sm text-slate-200">' + leadData.score + '/140</p></div>';
    html += '</div>';
    document.getElementById('enhanceCurrentData').innerHTML = html;

    document.getElementById('enhanceModal').classList.remove('hidden');
}

function closeEnhanceModal() {
    document.getElementById('enhanceModal').classList.add('hidden');
    currentEnhanceLeadId = null;
}

async function runEnhance() {
    if (!currentEnhanceLeadId) return;
    document.getElementById('enhanceNowBtn').disabled = true;
    document.getElementById('enhanceNowBtn').textContent = 'Searching...';
    document.getElementById('enhanceLoading').classList.remove('hidden');
    document.getElementById('enhanceResults').classList.add('hidden');
    document.getElementById('enhanceError').classList.add('hidden');

    try {
        const resp = await fetch('/api/enhance/' + currentEnhanceLeadId, { method: 'POST' });
        const data = await resp.json();
        document.getElementById('enhanceLoading').classList.add('hidden');

        if (data.error) {
            document.getElementById('enhanceError').classList.remove('hidden');
            document.getElementById('enhanceErrorMsg').textContent = data.error;
            document.getElementById('enhanceNowBtn').disabled = false;
            document.getElementById('enhanceNowBtn').textContent = 'RETRY ENHANCE';
            return;
        }

        enhanceFoundData = data.found_data || {};
        let resultsHtml = '';
        const searches = data.searches || [];
        searches.forEach(function(search) {
            resultsHtml += '<div class="bg-slate-800/50 rounded-lg p-3 mb-2">';
            resultsHtml += '<p class="text-xs text-slate-500 mb-2">' + (search.query || 'Search') + '</p>';
            (search.results || []).forEach(function(r) {
                resultsHtml += '<a href="' + r.url + '" target="_blank" rel="noopener" class="block hover:bg-slate-700/50 rounded p-2 mb-1 transition-colors">';
                resultsHtml += '<p class="text-sm text-blue-400 font-medium">' + r.title + '</p>';
                resultsHtml += '<p class="text-xs text-slate-500 truncate">' + r.url + '</p>';
                if (r.snippet) resultsHtml += '<p class="text-xs text-slate-400 mt-1">' + r.snippet.substring(0, 200) + '</p>';
                resultsHtml += '</a>';
            });
            if (!search.results || search.results.length === 0) {
                resultsHtml += '<p class="text-xs text-slate-500 italic">No results found</p>';
            }
            resultsHtml += '</div>';
        });

        document.getElementById('enhanceResultsList').innerHTML = resultsHtml;
        document.getElementById('enhanceResults').classList.remove('hidden');
        document.getElementById('enhanceNowBtn').textContent = 'ENHANCE COMPLETE';

        if (enhanceFoundData.linkedin_url || enhanceFoundData.email || enhanceFoundData.phone) {
            document.getElementById('enhanceSaveBtn').classList.remove('hidden');
        }
    } catch (e) {
        document.getElementById('enhanceLoading').classList.add('hidden');
        document.getElementById('enhanceError').classList.remove('hidden');
        document.getElementById('enhanceErrorMsg').textContent = 'Network error: ' + e.message;
        document.getElementById('enhanceNowBtn').disabled = false;
        document.getElementById('enhanceNowBtn').textContent = 'RETRY ENHANCE';
    }
}

async function saveEnhanceData() {
    if (!currentEnhanceLeadId || !enhanceFoundData) return;
    try {
        const resp = await fetch('/api/enhance/' + currentEnhanceLeadId + '/save', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(enhanceFoundData)
        });
        const data = await resp.json();
        if (data.success) {
            document.getElementById('enhanceSaveBtn').textContent = 'Saved!';
            document.getElementById('enhanceSaveBtn').disabled = true;
            document.getElementById('enhanceSaveBtn').classList.remove('bg-green-600', 'hover:bg-green-700');
            document.getElementById('enhanceSaveBtn').classList.add('bg-slate-600');
            setTimeout(function() { location.reload(); }, 1000);
        }
    } catch (e) {
        alert('Failed to save: ' + e.message);
    }
}
</script>
</body>
</html>"""


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


# ---------------------------------------------------------------------------
# Platform icons/badges
# ---------------------------------------------------------------------------

PLATFORM_ICONS = {
    "linkedin": ('<svg class="w-4 h-4 inline" viewBox="0 0 24 24" fill="#0A66C2"><path d="M20.447 20.452h-3.554v-5.569c0-1.328-.027-3.037-1.852-3.037-1.853 0-2.136 1.445-2.136 2.939v5.667H9.351V9h3.414v1.561h.046c.477-.9 1.637-1.85 3.37-1.85 3.601 0 4.267 2.37 4.267 5.455v6.286zM5.337 7.433a2.062 2.062 0 01-2.063-2.065 2.064 2.064 0 112.063 2.065zm1.782 13.019H3.555V9h3.564v11.452zM22.225 0H1.771C.792 0 0 .774 0 1.729v20.542C0 23.227.792 24 1.771 24h20.451C23.2 24 24 23.227 24 22.271V1.729C24 .774 23.2 0 22.222 0h.003z"/></svg>', 'bg-blue-500/20 text-blue-400 border-blue-500/40'),
    "craigslist": ('<svg class="w-4 h-4 inline" viewBox="0 0 24 24" fill="#8b5cf6"><path d="M12 0C5.373 0 0 5.373 0 12s5.373 12 12 12 12-5.373 12-12S18.627 0 12 0zm0 3a9 9 0 110 18 9 9 0 010-18zm-1 4v4H7v2h4v4h2v-4h4v-2h-4V7h-2z"/></svg>', 'bg-purple-500/20 text-purple-400 border-purple-500/40'),
    "reddit": ('<svg class="w-4 h-4 inline" viewBox="0 0 24 24" fill="#FF4500"><path d="M12 0A12 12 0 000 12a12 12 0 0012 12 12 12 0 0012-12A12 12 0 0012 0zm5.01 4.744c.688 0 1.25.561 1.25 1.249a1.25 1.25 0 01-2.498.056l-2.597-.547-.8 3.747c1.824.07 3.48.632 4.674 1.488.308-.309.73-.491 1.207-.491.968 0 1.754.786 1.754 1.754 0 .716-.435 1.333-1.01 1.614a3.111 3.111 0 01.042.52c0 2.694-3.13 4.87-7.004 4.87-3.874 0-7.004-2.176-7.004-4.87 0-.183.015-.366.043-.534A1.748 1.748 0 014.028 12c0-.968.786-1.754 1.754-1.754.463 0 .898.196 1.207.49 1.207-.883 2.878-1.43 4.744-1.487l.885-4.182a.342.342 0 01.14-.197.35.35 0 01.238-.042l2.906.617a1.214 1.214 0 011.108-.701zM9.25 12C8.561 12 8 12.562 8 13.25c0 .687.561 1.248 1.25 1.248.687 0 1.248-.561 1.248-1.249 0-.688-.561-1.249-1.249-1.249zm5.5 0c-.687 0-1.248.561-1.248 1.25 0 .687.561 1.248 1.249 1.248.688 0 1.249-.561 1.249-1.249 0-.687-.562-1.249-1.25-1.249zm-5.466 3.99a.327.327 0 00-.231.094.33.33 0 000 .463c.842.842 2.484.913 2.961.913.477 0 2.105-.056 2.961-.913a.361.361 0 000-.462.342.342 0 00-.465 0c-.533.533-1.684.73-2.512.73-.828 0-1.979-.196-2.512-.73a.326.326 0 00-.232-.095z"/></svg>', 'bg-orange-500/20 text-orange-400 border-orange-500/40'),
    "search": ('<svg class="w-4 h-4 inline" fill="none" stroke="#22c55e" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z"/></svg>', 'bg-green-500/20 text-green-400 border-green-500/40'),
    "ksl": ('<svg class="w-4 h-4 inline" viewBox="0 0 24 24" fill="#f59e0b"><rect width="24" height="24" rx="4"/><text x="4" y="17" font-size="12" font-weight="bold" fill="#0f172a">KSL</text></svg>', 'bg-amber-500/20 text-amber-400 border-amber-500/40'),
    "facebook": ('<svg class="w-4 h-4 inline" viewBox="0 0 24 24" fill="#1877F2"><path d="M24 12.073c0-6.627-5.373-12-12-12s-12 5.373-12 12c0 5.99 4.388 10.954 10.125 11.854v-8.385H7.078v-3.47h3.047V9.43c0-3.007 1.792-4.669 4.533-4.669 1.312 0 2.686.235 2.686.235v2.953H15.83c-1.491 0-1.956.925-1.956 1.874v2.25h3.328l-.532 3.47h-2.796v8.385C19.612 23.027 24 18.062 24 12.073z"/></svg>', 'bg-blue-600/20 text-blue-400 border-blue-500/40'),
}


def _platform_badge(platform):
    """Return HTML for a platform icon badge."""
    p = (platform or "unknown").lower()
    icon, classes = PLATFORM_ICONS.get(p, (
        '<svg class="w-4 h-4 inline" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M3.055 11H5a2 2 0 012 2v1a2 2 0 002 2 2 2 0 012 2v2.945"/></svg>',
        'bg-slate-500/20 text-slate-400 border-slate-500/40'
    ))
    return f'<span class="inline-flex items-center space-x-1.5 px-2.5 py-1 rounded-full text-xs font-medium border {classes}">{icon}<span>{_esc(platform).title()}</span></span>'


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

    # Build combined text for score explanation
    combined_text = " ".join(filter(None, [
        data.get("title", ""), data.get("description", ""),
        data.get("source_post_text", ""), data.get("current_role", ""),
        data.get("contact_email", ""), data.get("linkedin_url", ""),
        data.get("location_state", ""), data.get("education", ""),
        " ".join(data.get("recruiting_signals", [])),
        " ".join(data.get("career_history", [])),
    ]))

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
        "scraped_at_raw": p.get("scraped_at", row["created_at"]),
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
        "score_nwm_connection": data.get("score_nwm_connection", 0),
        "has_nwm_mutual_connection": data.get("has_nwm_mutual_connection", False),
        "nwm_mutual_names": data.get("nwm_mutual_names", []),
        "sentiment_score": data.get("sentiment_score"),
        "enriched": data.get("enriched", False),
        "enhanced": data.get("enhanced", False),
        "compliance_cleared": data.get("compliance_cleared", False),
        "source_post_text": data.get("source_post_text", data.get("description", "")),
        "relevance_reason": data.get("_relevance_reason", ""),
        "relevance_score": data.get("_relevance_score", 0),
        "linkedin_url": data.get("linkedin_url", ""),
        "education": data.get("education", ""),
        "score_explanations": _explain_score(combined_text),
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
        enhanced_count = 0
        not_enhanced_a_tier = 0
        nwm_connected = 0

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
            is_enhanced = data.get("enhanced", False)
            has_nwm = data.get("has_nwm_mutual_connection", False)

            if is_enhanced:
                enhanced_count += 1
            if has_nwm:
                nwm_connected += 1

            if score >= 75:
                tier_a += 1
                if not is_enhanced:
                    not_enhanced_a_tier += 1
            elif score >= 50:
                tier_b += 1
            elif score >= 25:
                tier_c += 1
            else:
                tier_d += 1

        ab_tier = tier_a + tier_b
        quality_pct = int(ab_tier / total_leads * 100) if total_leads > 0 else 0
        pending_enhance = not_enhanced_a_tier

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
        try:
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
        except Exception:
            pass
        recent_runs = recent_runs[:20]

        # Top 5 newest A-tier leads for carousel
        top_a_rows = db.execute(
            "SELECT * FROM jobs WHERE job_type='raw_scrape' ORDER BY created_at DESC LIMIT 200"
        ).fetchall()
        top_a_leads = []
        for row in top_a_rows:
            lead = _parse_lead(row)
            if lead["tier"] == "A":
                top_a_leads.append(lead)
            if len(top_a_leads) >= 5:
                break

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

        # Quality gauge color
        if quality_pct >= 50:
            gauge_color = "text-green-400"
        elif quality_pct >= 25:
            gauge_color = "text-blue-400"
        else:
            gauge_color = "text-yellow-400"

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
                        <p class="text-sm font-medium text-slate-200">{_esc(run["agent_name"])}</p>
                        <p class="text-xs text-slate-500">{_esc(run["platform"])} &middot; {run["items_found"]} found, {run["items_new"]} new</p>
                    </div>
                </div>
                <span class="text-xs text-slate-500">{run["time_ago"]}</span>
            </div>'''

        if not recent_runs:
            activity_html = '<p class="text-sm text-slate-500 text-center py-8">No agent runs recorded yet.</p>'

        # Top A-tier carousel
        carousel_html = ""
        if top_a_leads:
            for lead in top_a_leads:
                reason = _esc(lead["relevance_reason"]) if lead["relevance_reason"] else "High scoring lead"
                carousel_html += f'''<div class="min-w-[280px] bg-slate-800/50 border border-green-500/20 rounded-xl p-4 snap-start">
                    <div class="flex items-center justify-between mb-2">
                        <span class="inline-flex items-center px-2 py-0.5 rounded-full text-xs font-bold border bg-tier-a tier-a">A</span>
                        {_platform_badge(lead["platform"])}
                    </div>
                    <p class="text-sm font-semibold text-white truncate">{_esc(lead["name"] or lead["title"] or "Unnamed")}</p>
                    <p class="text-xs text-slate-400 mt-1 truncate">{_esc(lead["current_role"] or lead["title"])}</p>
                    <p class="text-xs text-green-400/80 mt-2 line-clamp-2">{reason}</p>
                    <div class="flex items-center justify-between mt-3">
                        <span class="text-sm font-bold text-green-400">{lead["score"]}/140</span>
                        <a href="/leads/{lead["id"]}" class="text-xs text-blue-400 hover:text-blue-300">View &rarr;</a>
                    </div>
                </div>'''
        else:
            carousel_html = '<p class="text-sm text-slate-500 py-4">No A-tier leads yet.</p>'

        # Auto-enhance banner
        enhance_banner = ""
        if pending_enhance > 0:
            enhance_banner = f'''
            <div class="bg-gradient-to-r from-blue-900/40 to-purple-900/40 border border-blue-500/30 rounded-xl p-4 mb-8 flex items-center justify-between">
                <div class="flex items-center space-x-3">
                    <div class="w-10 h-10 rounded-lg bg-blue-500/20 flex items-center justify-center">
                        <svg class="w-5 h-5 text-blue-400" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 10V3L4 14h7v7l9-11h-7z"/></svg>
                    </div>
                    <div>
                        <p class="text-sm font-semibold text-white">{pending_enhance} A-tier lead{"s" if pending_enhance != 1 else ""} not yet enhanced</p>
                        <p class="text-xs text-slate-400">Run auto-enhance to find LinkedIn profiles, emails, and phone numbers</p>
                    </div>
                </div>
                <button onclick="runAutoEnhance()" id="autoEnhanceBtn" class="bg-blue-600 hover:bg-blue-700 text-white text-sm font-semibold px-5 py-2.5 rounded-lg transition-colors flex items-center space-x-2">
                    <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 10V3L4 14h7v7l9-11h-7z"/></svg>
                    <span>Auto-Enhance A-Tier</span>
                </button>
            </div>'''

        content = f'''
{enhance_banner}
<!-- Stat Cards -->
<div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 xl:grid-cols-8 gap-4 mb-8">
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
            <span class="text-xs font-medium text-slate-400 uppercase tracking-wider">Lead Quality</span>
            <div class="w-8 h-8 rounded-lg bg-cyan-500/20 flex items-center justify-center">
                <svg class="w-4 h-4 text-cyan-400" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z"/></svg>
            </div>
        </div>
        <p class="text-3xl font-bold {gauge_color}">{quality_pct}%</p>
        <p class="text-xs text-slate-500 mt-1">A+B tier rate</p>
    </div>
    <div class="stat-card bg-slate-900 border border-slate-800 rounded-xl p-5 card-glow">
        <div class="flex items-center justify-between mb-3">
            <span class="text-xs font-medium text-slate-400 uppercase tracking-wider">Enhanced</span>
            <div class="w-8 h-8 rounded-lg bg-purple-500/20 flex items-center justify-center">
                <svg class="w-4 h-4 text-purple-400" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 10V3L4 14h7v7l9-11h-7z"/></svg>
            </div>
        </div>
        <p class="text-3xl font-bold text-purple-400">{enhanced_count}</p>
        <p class="text-xs text-slate-500 mt-1">{pending_enhance} pending</p>
    </div>
    <div class="stat-card bg-slate-900 border border-slate-800 rounded-xl p-5 card-glow">
        <div class="flex items-center justify-between mb-3">
            <span class="text-xs font-medium text-slate-400 uppercase tracking-wider">NWM Connected</span>
            <div class="w-8 h-8 rounded-lg bg-amber-500/20 flex items-center justify-center">
                <svg class="w-4 h-4 text-amber-400" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13.828 10.172a4 4 0 00-5.656 0l-4 4a4 4 0 105.656 5.656l1.102-1.101"/></svg>
            </div>
        </div>
        <p class="text-3xl font-bold text-amber-400">{nwm_connected}</p>
        <p class="text-xs text-slate-500 mt-1">+40 boost leads</p>
    </div>
    <div class="stat-card bg-slate-900 border border-slate-800 rounded-xl p-5 card-glow">
        <div class="flex items-center justify-between mb-3">
            <span class="text-xs font-medium text-slate-400 uppercase tracking-wider">Active Agents</span>
            <div class="w-8 h-8 rounded-lg bg-pink-500/20 flex items-center justify-center">
                <svg class="w-4 h-4 text-pink-400" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9.75 17L9 20l-1 1h8l-1-1-.75-3M3 13h18M5 17h14a2 2 0 002-2V5a2 2 0 00-2-2H5a2 2 0 00-2 2v10a2 2 0 002 2z"/></svg>
            </div>
        </div>
        <p class="text-3xl font-bold text-white">{active_agents}</p>
        <p class="text-xs text-slate-500 mt-1">Last: {last_scrape}</p>
    </div>
</div>

<!-- Top A-Tier Carousel -->
<div class="bg-slate-900 border border-slate-800 rounded-xl p-6 card-glow mb-8">
    <h3 class="text-sm font-semibold text-slate-300 mb-4 uppercase tracking-wider">Newest A-Tier Leads</h3>
    <div class="flex space-x-4 overflow-x-auto pb-2 snap-x snap-mandatory">{carousel_html}</div>
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

async function runAutoEnhance() {{
    const btn = document.getElementById('autoEnhanceBtn');
    btn.disabled = true;
    btn.innerHTML = '<svg class="w-4 h-4 enhance-spin" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15"/></svg><span>Enhancing...</span>';
    try {{
        const resp = await fetch('/api/auto-enhance', {{ method: 'POST' }});
        const data = await resp.json();
        btn.innerHTML = '<span>Done! ' + (data.enhanced_count || 0) + ' enhanced</span>';
        setTimeout(function() {{ location.reload(); }}, 2000);
    }} catch (e) {{
        btn.innerHTML = '<span>Error: ' + e.message + '</span>';
        btn.disabled = false;
    }}
}}
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
        page = int(request.args.get("page", 1))
        per_page = 25

        platform_rows = db.execute(
            "SELECT DISTINCT json_extract(payload, '$.platform') as p FROM jobs WHERE job_type='raw_scrape' AND p IS NOT NULL"
        ).fetchall()
        platforms = sorted(set(r[0] for r in platform_rows if r[0]))

        all_rows = db.execute(
            "SELECT * FROM jobs WHERE job_type='raw_scrape' ORDER BY created_at DESC"
        ).fetchall()
        all_leads = [_parse_lead(row) for row in all_rows]

        total_count = len(all_leads)
        total_pages = max(1, (total_count + per_page - 1) // per_page)
        page = min(page, total_pages)
        start = (page - 1) * per_page
        leads = all_leads[start:start + per_page]

        # Build platform filter chips
        plat_chips = ""
        for p in platforms:
            plat_chips += f'<button type="button" data-filter-platform="{_esc(p)}" class="chip inline-flex items-center space-x-1.5 px-3 py-1.5 rounded-full text-xs font-medium border border-slate-600 bg-slate-800 text-slate-300 hover:bg-slate-700">{_platform_badge(p)}</button>'

        # Build table rows HTML with all lead data as JSON for JS filtering
        leads_json = json.dumps([{
            "id": l["id"], "name": l["name"], "title": l["title"], "score": l["score"],
            "tier": l["tier"], "platform": l["platform"], "city": l["city"],
            "state": l["state"], "source_url": l["source_url"],
            "relevance_reason": l["relevance_reason"], "scraped_at": l["scraped_at"],
            "scraped_at_raw": l["scraped_at_raw"] or "",
            "description": (l["description"] or "")[:200],
            "current_role": l["current_role"], "has_nwm": l["has_nwm_mutual_connection"],
            "enhanced": l["enhanced"], "contact_email": l["contact_email"],
            "contact_phone": l["contact_phone"],
            "score_career_fit": l["score_career_fit"], "score_motivation": l["score_motivation"],
            "score_people_skills": l["score_people_skills"], "score_demographics": l["score_demographics"],
            "score_data_quality": l["score_data_quality"], "score_nwm_connection": l["score_nwm_connection"],
        } for l in all_leads], default=str)

        content = f'''
<!-- Search & Filter Bar -->
<div class="bg-slate-900 border border-slate-800 rounded-xl p-5 mb-6 card-glow">
    <div class="flex flex-col space-y-4">
        <!-- Search -->
        <div class="relative">
            <svg class="absolute left-3 top-1/2 transform -translate-y-1/2 w-5 h-5 text-slate-500" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z"/></svg>
            <input type="text" id="searchInput" placeholder="Search leads by name, title, city, URL, description..."
                   class="w-full bg-slate-800 border border-slate-700 rounded-lg pl-10 pr-4 py-2.5 text-sm text-slate-200 placeholder-slate-500 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent">
        </div>

        <!-- Filter chips row -->
        <div class="flex flex-wrap items-center gap-3">
            <!-- Tier filters -->
            <span class="text-xs text-slate-500 font-medium uppercase">Tier:</span>
            <button type="button" data-filter-tier="A" class="chip inline-flex items-center px-3 py-1.5 rounded-full text-xs font-bold border border-green-500/40 bg-green-500/10 text-green-400 hover:bg-green-500/20">A</button>
            <button type="button" data-filter-tier="B" class="chip inline-flex items-center px-3 py-1.5 rounded-full text-xs font-bold border border-blue-500/40 bg-blue-500/10 text-blue-400 hover:bg-blue-500/20">B</button>
            <button type="button" data-filter-tier="C" class="chip inline-flex items-center px-3 py-1.5 rounded-full text-xs font-bold border border-yellow-500/40 bg-yellow-500/10 text-yellow-400 hover:bg-yellow-500/20">C</button>
            <button type="button" data-filter-tier="D" class="chip inline-flex items-center px-3 py-1.5 rounded-full text-xs font-bold border border-red-500/40 bg-red-500/10 text-red-400 hover:bg-red-500/20">D</button>

            <span class="text-slate-700">|</span>

            <!-- Platform filters -->
            <span class="text-xs text-slate-500 font-medium uppercase">Platform:</span>
            {plat_chips}

            <span class="text-slate-700">|</span>

            <!-- NWM filter -->
            <span class="text-xs text-slate-500 font-medium uppercase">NWM:</span>
            <button type="button" data-filter-nwm="yes" class="chip inline-flex items-center px-3 py-1.5 rounded-full text-xs font-medium border border-amber-500/40 bg-amber-500/10 text-amber-400 hover:bg-amber-500/20">Connected</button>
            <button type="button" data-filter-nwm="no" class="chip inline-flex items-center px-3 py-1.5 rounded-full text-xs font-medium border border-slate-600 bg-slate-800 text-slate-400 hover:bg-slate-700">Not Connected</button>

            <span class="text-slate-700">|</span>

            <!-- Date range -->
            <span class="text-xs text-slate-500 font-medium uppercase">Date:</span>
            <button type="button" data-filter-date="1" class="chip inline-flex items-center px-3 py-1.5 rounded-full text-xs font-medium border border-slate-600 bg-slate-800 text-slate-400 hover:bg-slate-700">24h</button>
            <button type="button" data-filter-date="7" class="chip inline-flex items-center px-3 py-1.5 rounded-full text-xs font-medium border border-slate-600 bg-slate-800 text-slate-400 hover:bg-slate-700">7d</button>
            <button type="button" data-filter-date="30" class="chip inline-flex items-center px-3 py-1.5 rounded-full text-xs font-medium border border-slate-600 bg-slate-800 text-slate-400 hover:bg-slate-700">30d</button>
            <button type="button" data-filter-date="0" class="chip inline-flex items-center px-3 py-1.5 rounded-full text-xs font-medium border border-slate-600 bg-slate-800 text-slate-400 hover:bg-slate-700">All</button>

            <span class="text-slate-700">|</span>

            <!-- Score range -->
            <span class="text-xs text-slate-500 font-medium uppercase">Score:</span>
            <input type="number" id="scoreMin" min="0" max="140" placeholder="Min" class="w-16 bg-slate-800 border border-slate-700 rounded-lg px-2 py-1.5 text-xs text-slate-200 focus:outline-none focus:ring-1 focus:ring-blue-500">
            <span class="text-xs text-slate-500">-</span>
            <input type="number" id="scoreMax" min="0" max="140" placeholder="Max" class="w-16 bg-slate-800 border border-slate-700 rounded-lg px-2 py-1.5 text-xs text-slate-200 focus:outline-none focus:ring-1 focus:ring-blue-500">
        </div>

        <!-- Sort + actions row -->
        <div class="flex items-center justify-between">
            <div class="flex items-center space-x-3">
                <span class="text-xs text-slate-500 font-medium uppercase">Sort:</span>
                <select id="sortSelect" class="bg-slate-800 border border-slate-700 rounded-lg px-3 py-1.5 text-xs text-slate-200 focus:outline-none focus:ring-1 focus:ring-blue-500">
                    <option value="score_desc">Score (High to Low)</option>
                    <option value="score_asc">Score (Low to High)</option>
                    <option value="date_desc" selected>Date (Newest)</option>
                    <option value="date_asc">Date (Oldest)</option>
                    <option value="name_asc">Name (A-Z)</option>
                    <option value="name_desc">Name (Z-A)</option>
                    <option value="platform_asc">Platform (A-Z)</option>
                </select>
                <button onclick="clearAllFilters()" class="text-xs text-slate-400 hover:text-blue-400 transition-colors">Clear All</button>
            </div>
            <div class="flex items-center space-x-3">
                <span id="resultCount" class="text-sm text-slate-400"></span>
                <a href="/leads/export" class="bg-slate-800 hover:bg-slate-700 text-slate-300 text-sm font-medium px-4 py-2 rounded-lg transition-colors flex items-center space-x-2">
                    <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 10v6m0 0l-3-3m3 3l3-3m2 8H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z"/></svg>
                    <span>Export CSV</span>
                </a>
            </div>
        </div>
    </div>
</div>

<!-- Leads Table -->
<div class="bg-slate-900 border border-slate-800 rounded-xl overflow-hidden card-glow">
    <div class="overflow-x-auto">
        <table class="w-full" id="leadsTable">
            <thead><tr class="border-b border-slate-800">
                <th class="text-left text-xs font-semibold text-slate-400 uppercase tracking-wider px-6 py-3">Lead</th>
                <th class="text-left text-xs font-semibold text-slate-400 uppercase tracking-wider px-4 py-3">Score</th>
                <th class="text-left text-xs font-semibold text-slate-400 uppercase tracking-wider px-4 py-3">Tier</th>
                <th class="text-left text-xs font-semibold text-slate-400 uppercase tracking-wider px-4 py-3">Source</th>
                <th class="text-left text-xs font-semibold text-slate-400 uppercase tracking-wider px-4 py-3">Relevance</th>
                <th class="text-left text-xs font-semibold text-slate-400 uppercase tracking-wider px-4 py-3">City</th>
                <th class="text-left text-xs font-semibold text-slate-400 uppercase tracking-wider px-4 py-3">Date</th>
                <th class="text-left text-xs font-semibold text-slate-400 uppercase tracking-wider px-4 py-3">Actions</th>
            </tr></thead>
            <tbody id="leadsBody" class="divide-y divide-slate-800/50"></tbody>
        </table>
    </div>
</div>

<!-- Pagination -->
<div id="pagination" class="flex items-center justify-center space-x-2 mt-6"></div>

<script>
const ALL_LEADS = {leads_json};
const PER_PAGE = 25;
let currentPage = 1;
let activeFilters = {{ tiers: new Set(), platforms: new Set(), nwm: null, dateDays: 0, scoreMin: null, scoreMax: null }};

const tierColors = {{ A: 'bg-tier-a tier-a', B: 'bg-tier-b tier-b', C: 'bg-tier-c tier-c', D: 'bg-tier-d tier-d' }};
const tierTextColors = {{ A: 'tier-a', B: 'tier-b', C: 'tier-c', D: 'tier-d' }};

function escHtml(s) {{ return s ? String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;') : ''; }}

function platformBadgeHtml(platform) {{
    const p = (platform || 'unknown').toLowerCase();
    const colorMap = {{ linkedin: 'border-blue-500/40 bg-blue-500/20 text-blue-400', craigslist: 'border-purple-500/40 bg-purple-500/20 text-purple-400', reddit: 'border-orange-500/40 bg-orange-500/20 text-orange-400', search: 'border-green-500/40 bg-green-500/20 text-green-400', ksl: 'border-amber-500/40 bg-amber-500/20 text-amber-400', facebook: 'border-blue-500/40 bg-blue-600/20 text-blue-400' }};
    const cls = colorMap[p] || 'border-slate-500/40 bg-slate-500/20 text-slate-400';
    return '<span class="inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium border ' + cls + '">' + escHtml(platform).charAt(0).toUpperCase() + escHtml(platform).slice(1) + '</span>';
}}

function scoreBarHtml(value, max, color) {{
    const pct = max > 0 ? Math.round(value / max * 100) : 0;
    return '<div class="flex items-center space-x-1"><div class="w-16 bg-slate-800 rounded-full h-1"><div class="' + color + ' h-1 rounded-full score-bar" style="width:' + pct + '%"></div></div><span class="text-xs text-slate-500 w-8">' + value + '/' + max + '</span></div>';
}}

function getFilteredLeads() {{
    const search = (document.getElementById('searchInput').value || '').toLowerCase();
    const sortVal = document.getElementById('sortSelect').value;
    const now = new Date();

    let filtered = ALL_LEADS.filter(function(l) {{
        // Text search
        if (search) {{
            const haystack = [l.name, l.title, l.description, l.source_url, l.city, l.state, l.current_role, l.relevance_reason, l.contact_email].join(' ').toLowerCase();
            if (haystack.indexOf(search) === -1) return false;
        }}
        // Tier filter
        if (activeFilters.tiers.size > 0 && !activeFilters.tiers.has(l.tier)) return false;
        // Platform filter
        if (activeFilters.platforms.size > 0 && !activeFilters.platforms.has(l.platform)) return false;
        // NWM filter
        if (activeFilters.nwm === 'yes' && !l.has_nwm) return false;
        if (activeFilters.nwm === 'no' && l.has_nwm) return false;
        // Date filter
        if (activeFilters.dateDays > 0 && l.scraped_at_raw) {{
            try {{
                const ld = new Date(l.scraped_at_raw);
                const diffDays = (now - ld) / (1000 * 60 * 60 * 24);
                if (diffDays > activeFilters.dateDays) return false;
            }} catch(e) {{}}
        }}
        // Score range
        if (activeFilters.scoreMin !== null && l.score < activeFilters.scoreMin) return false;
        if (activeFilters.scoreMax !== null && l.score > activeFilters.scoreMax) return false;
        return true;
    }});

    // Sort
    filtered.sort(function(a, b) {{
        switch(sortVal) {{
            case 'score_desc': return b.score - a.score;
            case 'score_asc': return a.score - b.score;
            case 'date_desc': return (b.scraped_at_raw || '').localeCompare(a.scraped_at_raw || '');
            case 'date_asc': return (a.scraped_at_raw || '').localeCompare(b.scraped_at_raw || '');
            case 'name_asc': return (a.name || '').localeCompare(b.name || '');
            case 'name_desc': return (b.name || '').localeCompare(a.name || '');
            case 'platform_asc': return (a.platform || '').localeCompare(b.platform || '');
            default: return 0;
        }}
    }});

    return filtered;
}}

function renderLeads() {{
    const filtered = getFilteredLeads();
    const totalPages = Math.max(1, Math.ceil(filtered.length / PER_PAGE));
    currentPage = Math.min(currentPage, totalPages);
    const start = (currentPage - 1) * PER_PAGE;
    const pageLeads = filtered.slice(start, start + PER_PAGE);

    document.getElementById('resultCount').textContent = 'Showing ' + pageLeads.length + ' of ' + filtered.length + ' leads (page ' + currentPage + '/' + totalPages + ')';

    let html = '';
    pageLeads.forEach(function(l) {{
        const displayName = escHtml(l.name || l.title || 'Unnamed');
        const subTitle = l.current_role ? '<p class="text-xs text-slate-500 truncate max-w-[200px]">' + escHtml(l.current_role) + '</p>' : (l.title && l.name ? '<p class="text-xs text-slate-500 truncate max-w-[200px]">' + escHtml(l.title) + '</p>' : '');
        const tc = tierColors[l.tier] || tierColors['D'];
        const nwmBadge = l.has_nwm ? '<span class="inline-flex items-center px-1.5 py-0.5 rounded text-xs font-medium bg-amber-500/10 text-amber-400 border border-amber-500/30 ml-1" title="NWM Connection +40">NWM</span>' : '';

        const sourceLink = l.source_url ? '<a href="' + escHtml(l.source_url) + '" target="_blank" rel="noopener" class="text-blue-400 hover:text-blue-300 text-xs truncate block max-w-[120px]" title="' + escHtml(l.source_url) + '">View source</a>' : '<span class="text-xs text-slate-600">--</span>';

        const reasonText = l.relevance_reason ? '<span class="text-xs text-emerald-400/80 truncate block max-w-[180px]" title="' + escHtml(l.relevance_reason) + '">' + escHtml(l.relevance_reason) + '</span>' : '<span class="text-xs text-slate-600">--</span>';

        const enhancedBadge = l.enhanced ? '<span class="text-xs text-purple-400">Enhanced</span>' : '';

        const leadDataJson = escHtml(JSON.stringify({{ name: l.name || l.title, platform: l.platform, city: l.city, score: l.score }}));

        html += '<tr class="hover:bg-slate-800/30 transition-colors">';
        html += '<td class="px-6 py-3"><div><p class="text-sm font-medium text-slate-200">' + displayName + '</p>' + subTitle + '</div></td>';
        html += '<td class="px-4 py-3"><div class="flex flex-col space-y-0.5">';
        html += '<span class="text-sm font-mono font-bold ' + (tierTextColors[l.tier] || '') + '">' + l.score + '</span>';
        html += scoreBarHtml(l.score_career_fit, 35, 'bg-blue-500');
        html += scoreBarHtml(l.score_motivation, 25, 'bg-purple-500');
        html += scoreBarHtml(l.score_nwm_connection, 40, 'bg-amber-500');
        html += '</div></td>';
        html += '<td class="px-4 py-3"><span class="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-bold border ' + tc + '">' + l.tier + '</span>' + nwmBadge + '</td>';
        html += '<td class="px-4 py-3"><div class="flex flex-col space-y-1">' + platformBadgeHtml(l.platform) + sourceLink + '</div></td>';
        html += '<td class="px-4 py-3">' + reasonText + '</td>';
        html += '<td class="px-4 py-3"><span class="text-sm text-slate-400">' + escHtml(l.city || '-') + '</span></td>';
        html += '<td class="px-4 py-3"><span class="text-xs text-slate-500">' + escHtml(l.scraped_at) + '</span></td>';
        html += '<td class="px-4 py-3"><div class="flex items-center space-x-2">';
        html += '<a href="/leads/' + l.id + '" class="text-blue-400 hover:text-blue-300 text-sm font-medium transition-colors">View</a>';
        html += '<button onclick=\'openEnhanceModal("' + l.id + '", "' + escHtml(l.name || l.title || "Lead") + '", ' + JSON.stringify({{ name: l.name || l.title, platform: l.platform, city: l.city, score: l.score }}) + ')\' class="text-purple-400 hover:text-purple-300 text-xs font-medium transition-colors">Enhance</button>';
        html += enhancedBadge;
        html += '</div></td>';
        html += '</tr>';
    }});

    if (!pageLeads.length) {{
        html = '<tr><td colspan="8" class="px-6 py-12 text-center text-slate-500">No leads found matching your criteria.</td></tr>';
    }}

    document.getElementById('leadsBody').innerHTML = html;

    // Pagination
    let pagHtml = '';
    if (totalPages > 1) {{
        if (currentPage > 1) pagHtml += '<button onclick="goPage(' + (currentPage - 1) + ')" class="bg-slate-800 hover:bg-slate-700 text-slate-300 text-sm px-3 py-1.5 rounded-lg transition-colors">Prev</button>';
        for (let p = 1; p <= totalPages; p++) {{
            if (p === currentPage) {{
                pagHtml += '<span class="bg-blue-600 text-white text-sm px-3 py-1.5 rounded-lg">' + p + '</span>';
            }} else if (p <= 3 || p > totalPages - 3 || Math.abs(p - currentPage) <= 1) {{
                pagHtml += '<button onclick="goPage(' + p + ')" class="bg-slate-800 hover:bg-slate-700 text-slate-300 text-sm px-3 py-1.5 rounded-lg transition-colors">' + p + '</button>';
            }} else if (p === 4 || p === totalPages - 3) {{
                pagHtml += '<span class="text-slate-500 text-sm">...</span>';
            }}
        }}
        if (currentPage < totalPages) pagHtml += '<button onclick="goPage(' + (currentPage + 1) + ')" class="bg-slate-800 hover:bg-slate-700 text-slate-300 text-sm px-3 py-1.5 rounded-lg transition-colors">Next</button>';
    }}
    document.getElementById('pagination').innerHTML = pagHtml;
}}

function goPage(p) {{ currentPage = p; renderLeads(); window.scrollTo(0, 0); }}

function clearAllFilters() {{
    activeFilters = {{ tiers: new Set(), platforms: new Set(), nwm: null, dateDays: 0, scoreMin: null, scoreMax: null }};
    document.getElementById('searchInput').value = '';
    document.getElementById('scoreMin').value = '';
    document.getElementById('scoreMax').value = '';
    document.querySelectorAll('.chip').forEach(function(c) {{ c.classList.remove('active'); c.style.boxShadow = ''; }});
    currentPage = 1;
    renderLeads();
}}

// Chip click handlers
document.querySelectorAll('[data-filter-tier]').forEach(function(btn) {{
    btn.addEventListener('click', function() {{
        const tier = this.dataset.filterTier;
        if (activeFilters.tiers.has(tier)) {{
            activeFilters.tiers.delete(tier);
            this.style.boxShadow = '';
            this.classList.remove('active');
        }} else {{
            activeFilters.tiers.add(tier);
            this.style.boxShadow = '0 0 0 2px rgba(59,130,246,0.5)';
            this.classList.add('active');
        }}
        currentPage = 1;
        renderLeads();
    }});
}});

document.querySelectorAll('[data-filter-platform]').forEach(function(btn) {{
    btn.addEventListener('click', function() {{
        const plat = this.dataset.filterPlatform;
        if (activeFilters.platforms.has(plat)) {{
            activeFilters.platforms.delete(plat);
            this.style.boxShadow = '';
            this.classList.remove('active');
        }} else {{
            activeFilters.platforms.add(plat);
            this.style.boxShadow = '0 0 0 2px rgba(59,130,246,0.5)';
            this.classList.add('active');
        }}
        currentPage = 1;
        renderLeads();
    }});
}});

document.querySelectorAll('[data-filter-nwm]').forEach(function(btn) {{
    btn.addEventListener('click', function() {{
        const val = this.dataset.filterNwm;
        document.querySelectorAll('[data-filter-nwm]').forEach(function(b) {{ b.style.boxShadow = ''; b.classList.remove('active'); }});
        if (activeFilters.nwm === val) {{
            activeFilters.nwm = null;
        }} else {{
            activeFilters.nwm = val;
            this.style.boxShadow = '0 0 0 2px rgba(59,130,246,0.5)';
            this.classList.add('active');
        }}
        currentPage = 1;
        renderLeads();
    }});
}});

document.querySelectorAll('[data-filter-date]').forEach(function(btn) {{
    btn.addEventListener('click', function() {{
        const days = parseInt(this.dataset.filterDate);
        document.querySelectorAll('[data-filter-date]').forEach(function(b) {{ b.style.boxShadow = ''; b.classList.remove('active'); }});
        if (activeFilters.dateDays === days) {{
            activeFilters.dateDays = 0;
        }} else {{
            activeFilters.dateDays = days;
            this.style.boxShadow = '0 0 0 2px rgba(59,130,246,0.5)';
            this.classList.add('active');
        }}
        currentPage = 1;
        renderLeads();
    }});
}});

document.getElementById('searchInput').addEventListener('input', function() {{ currentPage = 1; renderLeads(); }});
document.getElementById('sortSelect').addEventListener('change', function() {{ currentPage = 1; renderLeads(); }});
document.getElementById('scoreMin').addEventListener('input', function() {{
    activeFilters.scoreMin = this.value ? parseInt(this.value) : null;
    currentPage = 1;
    renderLeads();
}});
document.getElementById('scoreMax').addEventListener('input', function() {{
    activeFilters.scoreMax = this.value ? parseInt(this.value) : null;
    currentPage = 1;
    renderLeads();
}});

// Initial render
renderLeads();
</script>'''

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
        rows = db.execute(
            "SELECT * FROM jobs WHERE job_type='raw_scrape' ORDER BY created_at DESC"
        ).fetchall()
        leads = [_parse_lead(row) for row in rows]

        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow([
            "Name", "Title", "Score", "Tier", "Platform", "City", "State",
            "Category", "Contact Email", "Contact Phone", "Source URL",
            "Relevance Reason", "NWM Connection", "Agent", "Scraped At",
        ])
        for l in leads:
            writer.writerow([
                l["name"], l["title"], l["score"], l["tier"], l["platform"],
                l["city"], l["state"], l["category"], l["contact_email"],
                l["contact_phone"], l["source_url"], l["relevance_reason"],
                "Yes" if l["has_nwm_mutual_connection"] else "No",
                l["agent"], l["scraped_at"],
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

        # Score percentages for bars
        cf_pct = int(lead["score_career_fit"] / 35 * 100) if lead["score_career_fit"] else 0
        mo_pct = int(lead["score_motivation"] / 25 * 100) if lead["score_motivation"] else 0
        ps_pct = int(lead["score_people_skills"] / 20 * 100) if lead["score_people_skills"] else 0
        dm_pct = int(lead["score_demographics"] / 10 * 100) if lead["score_demographics"] else 0
        dq_pct = int(lead["score_data_quality"] / 10 * 100) if lead["score_data_quality"] else 0
        nwm_pct = int(lead["score_nwm_connection"] / 40 * 100) if lead["score_nwm_connection"] else 0

        tier_class = {"A": "bg-tier-a tier-a", "B": "bg-tier-b tier-b", "C": "bg-tier-c tier-c", "D": "bg-tier-d tier-d"}.get(lead["tier"], "bg-tier-d tier-d")
        safe_name = _esc(lead["name"] or lead["title"] or "Unnamed Lead")
        safe_title = _esc(lead["title"] or "")
        safe_desc = _esc(lead["source_post_text"] or "")
        safe_url = _esc(lead["source_url"] or "")

        # Score explanations
        explanations = lead["score_explanations"]

        def _explain_html(dimension, label):
            items = explanations.get(dimension, [])
            if not items:
                return '<p class="text-xs text-slate-600 italic">No signals detected</p>'
            return "".join(f'<p class="text-xs text-slate-400">{_esc(item)}</p>' for item in items)

        # Relevance reason display
        relevance_html = ""
        if lead["relevance_reason"]:
            relevance_html = f'''
            <div class="bg-gradient-to-r from-emerald-900/20 to-green-900/20 border border-emerald-500/30 rounded-xl p-4 mb-6">
                <div class="flex items-center space-x-2 mb-1">
                    <svg class="w-4 h-4 text-emerald-400" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z"/></svg>
                    <span class="text-xs font-semibold text-emerald-400 uppercase">Relevance Reason</span>
                </div>
                <p class="text-sm text-emerald-300">{_esc(lead["relevance_reason"])}</p>
            </div>'''

        # NWM connection display
        nwm_html = ""
        if lead["has_nwm_mutual_connection"]:
            nwm_names = ", ".join(lead["nwm_mutual_names"]) if lead["nwm_mutual_names"] else "Detected"
            nwm_html = f'''
            <div class="bg-gradient-to-r from-amber-900/20 to-yellow-900/20 border border-amber-500/30 rounded-xl p-4 mb-6">
                <div class="flex items-center space-x-2 mb-1">
                    <svg class="w-4 h-4 text-amber-400" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13.828 10.172a4 4 0 00-5.656 0l-4 4a4 4 0 105.656 5.656l1.102-1.101"/></svg>
                    <span class="text-xs font-semibold text-amber-400 uppercase">NWM Mutual Connection (+40 boost)</span>
                </div>
                <p class="text-sm text-amber-300">{_esc(nwm_names)}</p>
            </div>'''

        # Signals tags
        signals_html = ""
        if lead["recruiting_signals"]:
            tags = "".join(f'<span class="bg-blue-500/10 text-blue-400 border border-blue-500/30 px-3 py-1 rounded-full text-xs font-medium">{_esc(s)}</span>' for s in lead["recruiting_signals"])
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
        enhanced_dot = '<div class="w-3 h-3 rounded-full bg-purple-500"></div><span class="text-sm text-slate-300">Enhanced</span>' if lead["enhanced"] else '<div class="w-3 h-3 rounded-full bg-slate-600"></div><span class="text-sm text-slate-500">Not Enhanced</span>'
        compliance_dot = '<div class="w-3 h-3 rounded-full bg-green-500"></div><span class="text-sm text-slate-300">Compliance Cleared</span>' if lead["compliance_cleared"] else '<div class="w-3 h-3 rounded-full bg-slate-600"></div><span class="text-sm text-slate-500">Pending Compliance</span>'

        source_url_html = f'<div class="mb-4"><p class="text-xs text-slate-500 mb-1">Source URL</p><a href="{safe_url}" target="_blank" rel="noopener" class="text-blue-400 hover:text-blue-300 text-sm break-all">{safe_url}</a></div>' if safe_url else ""
        post_text_html = f'<div><p class="text-xs text-slate-500 mb-1">Original Post Text</p><div class="bg-slate-800/50 rounded-lg p-4 text-sm text-slate-300 whitespace-pre-wrap max-h-60 overflow-y-auto">{safe_desc}</div></div>' if safe_desc else ""

        lead_data_json = json.dumps({"name": lead["name"] or lead["title"], "platform": lead["platform"], "city": lead["city"], "score": lead["score"]})

        content = f'''
<div class="flex items-center justify-between mb-6">
    <a href="/leads" class="inline-flex items-center text-sm text-slate-400 hover:text-blue-400 transition-colors">
        <svg class="w-4 h-4 mr-1" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15 19l-7-7 7-7"/></svg>
        Back to Leads
    </a>
    <button onclick='openEnhanceModal("{lead_id}", "{_esc(lead["name"] or lead["title"] or "Lead")}", {lead_data_json})' class="bg-gradient-to-r from-blue-600 to-purple-600 hover:from-blue-700 hover:to-purple-700 text-white text-sm font-semibold px-5 py-2.5 rounded-lg transition-all flex items-center space-x-2">
        <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 10V3L4 14h7v7l9-11h-7z"/></svg>
        <span>Enhance Lead</span>
    </button>
</div>

{relevance_html}
{nwm_html}

<div class="grid grid-cols-1 lg:grid-cols-3 gap-6">
    <div class="lg:col-span-2 space-y-6">
        <div class="bg-slate-900 border border-slate-800 rounded-xl p-6 card-glow">
            <div class="flex items-center justify-between">
                <div>
                    <h3 class="text-2xl font-bold text-white">{safe_name}</h3>
                    {"<p class='text-slate-400 mt-1'>" + _esc(lead["current_role"]) + "</p>" if lead["current_role"] else ""}
                </div>
                {_platform_badge(lead["platform"])}
            </div>
            <div class="flex items-center space-x-4 mt-3">
                <span class="inline-flex items-center px-3 py-1 rounded-full text-sm font-bold border {tier_class}">{lead["tier"]}-Tier &middot; {lead["score"]}/140</span>
                {"<span class='inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-amber-500/10 text-amber-400 border border-amber-500/30'>NWM Connected +40</span>" if lead["has_nwm_mutual_connection"] else ""}
            </div>
        </div>
        <div class="bg-slate-900 border border-slate-800 rounded-xl p-6 card-glow">
            <h4 class="text-sm font-semibold text-slate-300 uppercase tracking-wider mb-4">Lead Details</h4>
            <div class="grid grid-cols-2 gap-4">
                <div class="bg-slate-800/50 rounded-lg p-3"><p class="text-xs text-slate-500 mb-1">Platform</p><p class="text-sm text-slate-200">{_esc(lead["platform"]).title()}</p></div>
                <div class="bg-slate-800/50 rounded-lg p-3"><p class="text-xs text-slate-500 mb-1">Category</p><p class="text-sm text-slate-200">{_esc(lead["category"]) or "N/A"}</p></div>
                <div class="bg-slate-800/50 rounded-lg p-3"><p class="text-xs text-slate-500 mb-1">City</p><p class="text-sm text-slate-200">{_esc(lead["city"]) or "N/A"}{", " + _esc(lead["state"]) if lead["state"] else ""}</p></div>
                <div class="bg-slate-800/50 rounded-lg p-3"><p class="text-xs text-slate-500 mb-1">Agent</p><p class="text-sm text-slate-200">{_esc(lead["agent"]) or "N/A"}</p></div>
                <div class="bg-slate-800/50 rounded-lg p-3"><p class="text-xs text-slate-500 mb-1">Contact Email</p><p class="text-sm text-slate-200">{_esc(lead["contact_email"]) or "N/A"}</p></div>
                <div class="bg-slate-800/50 rounded-lg p-3"><p class="text-xs text-slate-500 mb-1">Contact Phone</p><p class="text-sm text-slate-200">{_esc(lead["contact_phone"]) or "N/A"}</p></div>
                <div class="bg-slate-800/50 rounded-lg p-3"><p class="text-xs text-slate-500 mb-1">LinkedIn</p><p class="text-sm text-slate-200">{('<a href="' + _esc(lead["linkedin_url"]) + '" target="_blank" class="text-blue-400 hover:text-blue-300">' + _esc(lead["linkedin_url"])[:50] + '</a>') if lead["linkedin_url"] else "N/A"}</p></div>
                <div class="bg-slate-800/50 rounded-lg p-3"><p class="text-xs text-slate-500 mb-1">Scraped At</p><p class="text-sm text-slate-200">{_esc(lead["scraped_at"])}</p></div>
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
            <h4 class="text-sm font-semibold text-slate-300 uppercase tracking-wider mb-4">Status</h4>
            <div class="grid grid-cols-3 gap-4">
                <div class="flex items-center space-x-3">{enriched_dot}</div>
                <div class="flex items-center space-x-3">{enhanced_dot}</div>
                <div class="flex items-center space-x-3">{compliance_dot}</div>
            </div>
        </div>
    </div>
    <div class="space-y-6">
        <!-- Score Breakdown -->
        <div class="bg-slate-900 border border-slate-800 rounded-xl p-6 card-glow">
            <h4 class="text-sm font-semibold text-slate-300 uppercase tracking-wider mb-4">Score Breakdown</h4>
            <div style="height:260px"><canvas id="radarChart"></canvas></div>
            <div class="mt-4 space-y-3">
                <div>
                    <div class="flex justify-between text-xs"><span class="text-slate-400">Career Fit</span><span class="text-slate-200 font-medium">{lead["score_career_fit"]}/35</span></div>
                    <div class="w-full bg-slate-800 rounded-full h-1.5 mt-0.5"><div class="bg-blue-500 h-1.5 rounded-full score-bar" style="width:{cf_pct}%"></div></div>
                    <div class="mt-1">{_explain_html("career_fit", "Career Fit")}</div>
                </div>
                <div>
                    <div class="flex justify-between text-xs"><span class="text-slate-400">Motivation</span><span class="text-slate-200 font-medium">{lead["score_motivation"]}/25</span></div>
                    <div class="w-full bg-slate-800 rounded-full h-1.5 mt-0.5"><div class="bg-purple-500 h-1.5 rounded-full score-bar" style="width:{mo_pct}%"></div></div>
                    <div class="mt-1">{_explain_html("motivation", "Motivation")}</div>
                </div>
                <div>
                    <div class="flex justify-between text-xs"><span class="text-slate-400">People Skills</span><span class="text-slate-200 font-medium">{lead["score_people_skills"]}/20</span></div>
                    <div class="w-full bg-slate-800 rounded-full h-1.5 mt-0.5"><div class="bg-cyan-500 h-1.5 rounded-full score-bar" style="width:{ps_pct}%"></div></div>
                    <div class="mt-1">{_explain_html("people_skills", "People Skills")}</div>
                </div>
                <div>
                    <div class="flex justify-between text-xs"><span class="text-slate-400">Demographics</span><span class="text-slate-200 font-medium">{lead["score_demographics"]}/10</span></div>
                    <div class="w-full bg-slate-800 rounded-full h-1.5 mt-0.5"><div class="bg-amber-500 h-1.5 rounded-full score-bar" style="width:{dm_pct}%"></div></div>
                    <div class="mt-1">{_explain_html("demographics", "Demographics")}</div>
                </div>
                <div>
                    <div class="flex justify-between text-xs"><span class="text-slate-400">Data Quality</span><span class="text-slate-200 font-medium">{lead["score_data_quality"]}/10</span></div>
                    <div class="w-full bg-slate-800 rounded-full h-1.5 mt-0.5"><div class="bg-green-500 h-1.5 rounded-full score-bar" style="width:{dq_pct}%"></div></div>
                    <div class="mt-1">{_explain_html("data_quality", "Data Quality")}</div>
                </div>
                <div>
                    <div class="flex justify-between text-xs"><span class="text-amber-400">NWM Connection</span><span class="text-amber-300 font-medium">{lead["score_nwm_connection"]}/40</span></div>
                    <div class="w-full bg-slate-800 rounded-full h-1.5 mt-0.5"><div class="bg-amber-500 h-1.5 rounded-full score-bar" style="width:{nwm_pct}%"></div></div>
                    <p class="text-xs text-slate-500 mt-1">{"Mutual connection detected" if lead["has_nwm_mutual_connection"] else "No NWM connection found"}</p>
                </div>
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
    data:{{ labels:['Career Fit','Motivation','People Skills','Demographics','Data Quality','NWM Connection'], datasets:[{{ label:'Score',data:[{cf_pct},{mo_pct},{ps_pct},{dm_pct},{dq_pct},{nwm_pct}],backgroundColor:'rgba(59,130,246,0.2)',borderColor:'#3b82f6',pointBackgroundColor:'#3b82f6',pointBorderColor:'#1e293b',pointBorderWidth:2 }}] }},
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
        try:
            ar_rows = db.execute("SELECT * FROM agent_runs ORDER BY completed_at DESC").fetchall()
        except Exception:
            ar_rows = []

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
                status_html = f'<span class="inline-flex items-center space-x-1.5 text-xs font-medium text-yellow-400"><div class="w-1.5 h-1.5 rounded-full bg-yellow-500 pulse-dot"></div><span>{_esc(ag["last_status"]).title()}</span></span>'

            err_cls = "text-red-400" if ag["error_count"] > 0 else "text-slate-500"
            rows_html += f'''<tr class="hover:bg-slate-800/30 transition-colors">
                <td class="px-6 py-3"><a href="/agents/{_esc(ag["name"])}" class="text-sm font-medium text-blue-400 hover:text-blue-300 transition-colors">{_esc(ag["name"])}</a></td>
                <td class="px-4 py-3"><span class="text-sm text-slate-400">{_esc(ag["platform"]).title()}</span></td>
                <td class="px-4 py-3">{status_html}</td>
                <td class="px-4 py-3"><span class="text-sm text-slate-300 font-mono">{ag["run_count"]}</span></td>
                <td class="px-4 py-3"><span class="text-sm text-slate-300 font-mono">{ag["total_found"]}</span></td>
                <td class="px-4 py-3"><span class="text-sm text-slate-300 font-mono">{ag["total_new"]}</span></td>
                <td class="px-4 py-3"><span class="text-sm font-mono {err_cls}">{ag["error_count"]}</span></td>
                <td class="px-4 py-3"><span class="text-xs text-slate-500">{ag["last_run"]}</span></td>
            </tr>'''

        if not agents:
            rows_html = '<tr><td colspan="8" class="px-6 py-12 text-center text-slate-500">No agent data recorded yet.</td></tr>'

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

        try:
            ar_rows = db.execute("SELECT * FROM agent_runs WHERE agent_name=? ORDER BY completed_at DESC", (agent_name,)).fetchall()
            for row in ar_rows:
                runs.append({"status": row["status"], "items_found": row["items_found"], "items_new": row["items_new"], "time": fmt_dt(row["completed_at"]), "error": row["error"]})
                total_found += row["items_found"]
                total_new += row["items_new"]
        except Exception:
            pass

        rows_html = ""
        for run in runs:
            if run["status"] in ("success", "completed"):
                s_html = '<span class="inline-flex items-center space-x-1.5 text-xs font-medium text-green-400"><div class="w-1.5 h-1.5 rounded-full bg-green-500"></div><span>' + _esc(run["status"]).title() + '</span></span>'
            else:
                s_html = '<span class="inline-flex items-center space-x-1.5 text-xs font-medium text-red-400"><div class="w-1.5 h-1.5 rounded-full bg-red-500"></div><span>' + _esc(run["status"]).title() + '</span></span>'
            err = _esc(run["error"] or "-")
            rows_html += f'<tr class="hover:bg-slate-800/30 transition-colors"><td class="px-6 py-3">{s_html}</td><td class="px-4 py-3 text-sm text-slate-300 font-mono">{run["items_found"]}</td><td class="px-4 py-3 text-sm text-slate-300 font-mono">{run["items_new"]}</td><td class="px-4 py-3 text-xs text-slate-500">{run["time"]}</td><td class="px-4 py-3 text-xs text-red-400 max-w-xs truncate">{err}</td></tr>'

        content = f'''
<a href="/agents" class="inline-flex items-center text-sm text-slate-400 hover:text-blue-400 mb-6 transition-colors">
    <svg class="w-4 h-4 mr-1" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15 19l-7-7 7-7"/></svg>
    Back to Agents
</a>
<div class="bg-slate-900 border border-slate-800 rounded-xl p-6 card-glow mb-6">
    <div class="flex items-center justify-between">
        <div><h3 class="text-xl font-bold text-white">{_esc(agent_name)}</h3><p class="text-sm text-slate-400 mt-1">{len(runs)} recorded runs</p></div>
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

        return _render_page("Agent Detail", _esc(agent_name), "agents", content)
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
        freshness = [0, 0, 0, 0]
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

        vol_labels = []
        vol_data = []
        for i in range(29, -1, -1):
            day = (now - timedelta(days=i)).strftime("%Y-%m-%d")
            vol_labels.append((now - timedelta(days=i)).strftime("%b %d"))
            vol_data.append(daily_map.get(day, 0))

        sorted_locs = sorted(location_map.items(), key=lambda x: x[1], reverse=True)[:10]
        loc_labels = json.dumps([l[0] for l in sorted_locs])
        loc_counts = json.dumps([l[1] for l in sorted_locs])

        score_labels = json.dumps([f"{i*10}-{i*10+9}" for i in range(10)])
        score_counts = json.dumps(score_bins)

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

        total_raw = len(rows)
        status_counts = db.execute("SELECT status, COUNT(*) FROM jobs GROUP BY status").fetchall()
        status_map = {r[0]: r[1] for r in status_counts}
        pending_count = status_map.get("pending", 0)
        done_count = status_map.get("done", 0)
        failed_count = status_map.get("failed", 0)
        unique_agents = db.execute("SELECT COUNT(DISTINCT json_extract(payload, '$.agent')) FROM jobs WHERE job_type='raw_scrape'").fetchone()[0]
        try:
            cd_count = db.execute("SELECT COUNT(*) FROM change_detection").fetchone()[0]
        except Exception:
            cd_count = 0
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
# API: Stats
# ---------------------------------------------------------------------------

@app.route("/api/stats")
def api_stats():
    db = get_db()
    try:
        total = db.execute("SELECT COUNT(*) FROM jobs WHERE job_type='raw_scrape'").fetchone()[0]
        today_start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
        new_today = db.execute("SELECT COUNT(*) FROM jobs WHERE job_type='raw_scrape' AND created_at >= ?", (today_start,)).fetchone()[0]
        pending = db.execute("SELECT COUNT(*) FROM jobs WHERE status='pending'").fetchone()[0]
        return jsonify({"total_leads": total, "new_today": new_today, "pending": pending})
    finally:
        db.close()


# ---------------------------------------------------------------------------
# API: Enhance a single lead
# ---------------------------------------------------------------------------

@app.route("/api/enhance/<lead_id>", methods=["POST"])
def api_enhance_lead(lead_id):
    """Search for additional info about a lead via Serper.dev."""
    db = get_db()
    try:
        row = db.execute("SELECT * FROM jobs WHERE id=?", (lead_id,)).fetchone()
        if not row:
            return jsonify({"error": "Lead not found"}), 404

        lead = _parse_lead(row)
        name = lead["name"] or lead["title"] or ""
        city = lead["city"] or "Utah"
        platform = lead["platform"] or ""

        if not name:
            return jsonify({"error": "Lead has no name to search for"}), 400

        # Use Serper.dev API for searches
        serper_key = os.environ.get("SERPER_API_KEY", "")
        if not serper_key:
            return jsonify({"error": "SERPER_API_KEY not set. Set it in your environment to enable enhance."}), 400

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
                    # Auto-detect LinkedIn URL
                    if "linkedin.com/in/" in url and not found_data.get("linkedin_url"):
                        found_data["linkedin_url"] = url

                searches.append({"query": query, "results": results})
            except Exception as e:
                searches.append({"query": query, "results": [], "error": str(e)})

        # Mark as enhanced in the database
        payload = safe_json(row["payload"])
        payload_data = payload.get("data", {})
        payload_data["enhanced"] = True
        if found_data.get("linkedin_url"):
            payload_data["linkedin_url"] = found_data["linkedin_url"]
        payload["data"] = payload_data
        db.execute("UPDATE jobs SET payload=? WHERE id=?", (json.dumps(payload), lead_id))
        db.commit()

        return jsonify({
            "success": True,
            "searches": searches,
            "found_data": found_data,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        db.close()


# ---------------------------------------------------------------------------
# API: Save enhanced data to a lead
# ---------------------------------------------------------------------------

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
# API: Auto-enhance A-tier leads
# ---------------------------------------------------------------------------

@app.route("/api/auto-enhance", methods=["POST"])
def api_auto_enhance():
    """Auto-enhance up to 10 A-tier leads that haven't been enhanced yet."""
    db = get_db()
    try:
        rows = db.execute(
            "SELECT * FROM jobs WHERE job_type='raw_scrape' ORDER BY created_at DESC"
        ).fetchall()

        serper_key = os.environ.get("SERPER_API_KEY", "")
        if not serper_key:
            return jsonify({"error": "SERPER_API_KEY not set"}), 400

        import httpx

        candidates = []
        for row in rows:
            p = safe_json(row["payload"])
            data = p.get("data", {})
            score = data.get("total_score", 0)
            enhanced = data.get("enhanced", False)
            if score >= 75 and not enhanced:
                name = data.get("name", "")
                if not name:
                    first = data.get("first_name", "")
                    last = data.get("last_name", "")
                    name = f"{first} {last}".strip()
                if not name:
                    name = data.get("title", "")
                if name:
                    candidates.append({
                        "id": row["id"],
                        "name": name,
                        "city": data.get("location_city", "Utah"),
                    })
            if len(candidates) >= 10:
                break

        enhanced_count = 0
        results = []

        for candidate in candidates:
            try:
                query = f'"{candidate["name"]}" LinkedIn profile "{candidate["city"]}"'
                resp = httpx.post(
                    "https://google.serper.dev/search",
                    json={"q": query, "num": 3, "gl": "us", "hl": "en"},
                    headers={"X-API-KEY": serper_key, "Content-Type": "application/json"},
                    timeout=15.0,
                )
                resp.raise_for_status()
                data = resp.json()

                linkedin_url = ""
                for item in data.get("organic", []):
                    url = item.get("link", "")
                    if "linkedin.com/in/" in url:
                        linkedin_url = url
                        break

                # Update the lead payload
                row = db.execute("SELECT * FROM jobs WHERE id=?", (candidate["id"],)).fetchone()
                if row:
                    payload = safe_json(row["payload"])
                    payload_data = payload.get("data", {})
                    payload_data["enhanced"] = True
                    if linkedin_url:
                        payload_data["linkedin_url"] = linkedin_url
                    payload["data"] = payload_data
                    db.execute("UPDATE jobs SET payload=? WHERE id=?", (json.dumps(payload), candidate["id"]))
                    db.commit()
                    enhanced_count += 1
                    results.append({"id": candidate["id"], "name": candidate["name"], "linkedin_url": linkedin_url})
            except Exception as e:
                results.append({"id": candidate["id"], "name": candidate["name"], "error": str(e)})

        return jsonify({
            "success": True,
            "enhanced_count": enhanced_count,
            "total_candidates": len(candidates),
            "results": results,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500
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
