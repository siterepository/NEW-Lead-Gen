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

# Run all registered KSL agents (most reliable)
for agent in ksl_job_seekers ksl_services_offered ksl_business_for_sale ksl_resume_posts ksl_career_services ksl_gig_workers ksl_professional_services ksl_coaching_consulting; do
    echo "--- Running: $agent ---" | tee -a "$LOG_FILE"
    leadgen run --agent "$agent" >> "$LOG_FILE" 2>&1 || echo "  [WARN] $agent had errors" | tee -a "$LOG_FILE"
    sleep 2
done

# Run Craigslist agents
for agent in craigslist_slc_jobs_wanted craigslist_provo_jobs_wanted craigslist_slc_resumes craigslist_slc_gigs craigslist_slc_business; do
    echo "--- Running: $agent ---" | tee -a "$LOG_FILE"
    leadgen run --agent "$agent" >> "$LOG_FILE" 2>&1 || echo "  [WARN] $agent had errors" | tee -a "$LOG_FILE"
    sleep 3
done

# Score all leads
echo "--- Scoring leads ---" | tee -a "$LOG_FILE"
leadgen score >> "$LOG_FILE" 2>&1 || true

# Export leads
echo "--- Exporting leads ---" | tee -a "$LOG_FILE"
leadgen export --output "data/exports/leads_$DATE.csv" >> "$LOG_FILE" 2>&1 || true
leadgen export --tier A --output "data/exports/a_tier_$DATE.csv" >> "$LOG_FILE" 2>&1 || true

# Clean old logs (keep last 30 days)
find "$LOG_DIR" -name "scrape_*.log" -mtime +30 -delete 2>/dev/null || true

# Clean old exports (keep last 30 days)
find "$PROJECT_DIR/data/exports" -name "*.csv" -mtime +30 -delete 2>/dev/null || true

echo "=== Scrape Complete: $(date) ===" | tee -a "$LOG_FILE"
echo "Log saved to: $LOG_FILE"
