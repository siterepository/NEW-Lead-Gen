#!/usr/bin/env bash
# ============================================================
# Automated scrape runner - called by macOS LaunchAgent
# ============================================================
# Runs all registered agents, scores leads, and exports CSVs.
# Logs output to data/logs/
#
set -euo pipefail

export PATH="$HOME/Library/Python/3.9/bin:/usr/local/bin:/usr/bin:/bin:$PATH"

PROJECT_DIR="$HOME/NEW Lead Gen"
LOG_DIR="$PROJECT_DIR/data/logs"
DATE=$(date +%Y-%m-%d_%H%M)
LOG_FILE="$LOG_DIR/scrape_$DATE.log"

mkdir -p "$LOG_DIR" "$PROJECT_DIR/data/exports"

cd "$PROJECT_DIR"

echo "=== Lead Gen Scrape Started: $(date) ===" | tee "$LOG_FILE"

# High-relevance Craigslist agents
for agent in craigslist_slc_jobs_wanted craigslist_provo_jobs_wanted craigslist_slc_business craigslist_slc_gigs; do
    echo "--- Running: $agent ---" | tee -a "$LOG_FILE"
    leadgen run --agent "$agent" >> "$LOG_FILE" 2>&1 || echo "  [WARN] $agent had errors" | tee -a "$LOG_FILE"
    sleep 5
done

# Web search agent (new)
echo "--- Running: web_search ---" | tee -a "$LOG_FILE"
leadgen run --agent web_search >> "$LOG_FILE" 2>&1 || echo "  [WARN] web_search had errors" | tee -a "$LOG_FILE"

# Score all leads
echo "--- Scoring leads ---" | tee -a "$LOG_FILE"
leadgen score >> "$LOG_FILE" 2>&1 || true

# Export leads
echo "--- Exporting leads ---" | tee -a "$LOG_FILE"
leadgen export --output "data/exports/leads_$DATE.csv" >> "$LOG_FILE" 2>&1 || true
leadgen export --tier A --output "data/exports/a_tier_$DATE.csv" >> "$LOG_FILE" 2>&1 || true

# Quality report
echo "--- Quality Report ---" | tee -a "$LOG_FILE"
python3 -c "
import sqlite3, json
db = sqlite3.connect('data/leadgen.db')
total = db.execute('SELECT COUNT(*) FROM jobs WHERE job_type=\"raw_scrape\"').fetchone()[0]
# Count items with relevance info
print(f'Total items in queue: {total}')
db.close()
" >> "$LOG_FILE" 2>&1

# Clean old logs (keep last 30 days)
find "$LOG_DIR" -name "scrape_*.log" -mtime +30 -delete 2>/dev/null || true

# Clean old exports (keep last 30 days)
find "$PROJECT_DIR/data/exports" -name "*.csv" -mtime +30 -delete 2>/dev/null || true

echo "=== Scrape Complete: $(date) ===" | tee -a "$LOG_FILE"
echo "Log saved to: $LOG_FILE"
