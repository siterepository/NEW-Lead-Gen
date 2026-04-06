#!/usr/bin/env bash
# ============================================================
# Lead Gen Service Manager
# ============================================================
#
# Usage:
#   ./scripts/service.sh start    # Start automated scraping (every 2 hours)
#   ./scripts/service.sh stop     # Stop automated scraping
#   ./scripts/service.sh status   # Check if running + show last scrape
#   ./scripts/service.sh run      # Run one scrape NOW (manual)
#   ./scripts/service.sh logs     # Show latest scrape log
#   ./scripts/service.sh exports  # List recent CSV exports
#
set -euo pipefail

PLIST="$HOME/Library/LaunchAgents/com.leadgen.scrape.plist"
PROJECT_DIR="$HOME/NEW Lead Gen"
LOG_DIR="$PROJECT_DIR/data/logs"

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

case "${1:-}" in
    start)
        mkdir -p "$LOG_DIR"
        launchctl load "$PLIST" 2>/dev/null && \
            echo -e "${GREEN}Lead Gen scraper STARTED${NC}" && \
            echo "  Schedule: every 2 hours" && \
            echo "  Logs: $LOG_DIR/" && \
            echo "  Exports: $PROJECT_DIR/data/exports/" || \
            echo -e "${YELLOW}Already running (or use 'stop' first)${NC}"
        ;;

    stop)
        launchctl unload "$PLIST" 2>/dev/null && \
            echo -e "${RED}Lead Gen scraper STOPPED${NC}" || \
            echo -e "${YELLOW}Not currently running${NC}"
        ;;

    status)
        echo -e "${GREEN}Lead Gen Service Status${NC}"
        echo "─────────────────────────────────────────"

        if launchctl list | grep -q "com.leadgen.scrape"; then
            echo -e "Service:  ${GREEN}RUNNING${NC} (every 2 hours)"
        else
            echo -e "Service:  ${RED}STOPPED${NC}"
        fi

        # Last scrape log
        LATEST_LOG=$(ls -t "$LOG_DIR"/scrape_*.log 2>/dev/null | head -1)
        if [ -n "$LATEST_LOG" ]; then
            echo -e "Last run: $(basename "$LATEST_LOG" | sed 's/scrape_//;s/.log//;s/_/ /')"
            ITEMS=$(grep -c "Running:" "$LATEST_LOG" 2>/dev/null || echo "0")
            echo -e "Agents:   $ITEMS ran"
        else
            echo "Last run: never"
        fi

        # Export count
        EXPORT_COUNT=$(ls "$PROJECT_DIR/data/exports/"*.csv 2>/dev/null | wc -l | tr -d ' ')
        echo -e "Exports:  $EXPORT_COUNT CSV files"

        # DB size
        if [ -f "$PROJECT_DIR/data/leadgen.db" ]; then
            DB_SIZE=$(du -h "$PROJECT_DIR/data/leadgen.db" | cut -f1)
            echo -e "Database: $DB_SIZE"
        fi
        ;;

    run)
        echo -e "${YELLOW}Running manual scrape now...${NC}"
        "$PROJECT_DIR/scripts/run_scrape.sh"
        ;;

    logs)
        LATEST_LOG=$(ls -t "$LOG_DIR"/scrape_*.log 2>/dev/null | head -1)
        if [ -n "$LATEST_LOG" ]; then
            echo -e "${GREEN}Latest log: $LATEST_LOG${NC}"
            echo "─────────────────────────────────────────"
            cat "$LATEST_LOG"
        else
            echo "No logs found yet. Run './scripts/service.sh run' first."
        fi
        ;;

    exports)
        echo -e "${GREEN}Recent Exports${NC}"
        echo "─────────────────────────────────────────"
        ls -lh "$PROJECT_DIR/data/exports/"*.csv 2>/dev/null | tail -20 || echo "No exports yet."
        ;;

    *)
        echo "Lead Gen Service Manager"
        echo ""
        echo "Usage:"
        echo "  ./scripts/service.sh start    Start automated scraping (every 2 hours)"
        echo "  ./scripts/service.sh stop     Stop automated scraping"
        echo "  ./scripts/service.sh status   Check service status"
        echo "  ./scripts/service.sh run      Run one scrape NOW"
        echo "  ./scripts/service.sh logs     Show latest scrape log"
        echo "  ./scripts/service.sh exports  List recent CSV exports"
        ;;
esac
