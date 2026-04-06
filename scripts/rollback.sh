#!/usr/bin/env bash
# ============================================================
# ROLLBACK SCRIPT - Restore project to last known working state
# ============================================================
#
# Usage:
#   ./scripts/rollback.sh              # Show available restore points
#   ./scripts/rollback.sh latest       # Rollback to most recent working tag
#   ./scripts/rollback.sh v0.1.0       # Rollback to specific tag
#   ./scripts/rollback.sh --save       # Save current state as new restore point BEFORE making changes
#   ./scripts/rollback.sh --status     # Show current state vs last known good
#
# Rules:
#   1. Always run --save BEFORE making risky changes
#   2. Tags prefixed with "working-" are auto-created restore points
#   3. The script creates a backup branch before rollback (rollback-backup-TIMESTAMP)
#   4. .env and data/ are NEVER rolled back (secrets + scraped data preserved)
#
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_DIR"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# ---- Functions ----

show_restore_points() {
    echo -e "${GREEN}Available restore points:${NC}"
    echo "─────────────────────────────────────────────────────"
    git tag -l -n1 --sort=-creatordate | head -20
    echo ""
    echo -e "Current HEAD: ${YELLOW}$(git log --oneline -1)${NC}"
    echo -e "Current branch: ${YELLOW}$(git branch --show-current)${NC}"
}

save_restore_point() {
    # Run tests first to verify it's actually working
    echo -e "${YELLOW}Verifying project health before saving...${NC}"

    TEST_RESULT=$(cd "$PROJECT_DIR" && python3 -m pytest tests/ -q 2>&1 | tail -1)
    if echo "$TEST_RESULT" | grep -q "passed"; then
        echo -e "${GREEN}Tests: $TEST_RESULT${NC}"
    else
        echo -e "${RED}Tests failing! Not saving restore point.${NC}"
        echo "$TEST_RESULT"
        exit 1
    fi

    # Generate tag name from date
    TAG="working-$(date +%Y%m%d-%H%M%S)"
    COMMIT=$(git log --oneline -1)

    git tag -a "$TAG" -m "Auto-saved working state: $COMMIT | $TEST_RESULT"

    echo -e "${GREEN}Restore point saved: $TAG${NC}"
    echo -e "  Commit: $COMMIT"
    echo -e "  Tests:  $TEST_RESULT"
}

rollback_to() {
    TARGET="$1"

    # Verify target exists
    if ! git rev-parse "$TARGET" >/dev/null 2>&1; then
        echo -e "${RED}Error: '$TARGET' is not a valid tag or commit${NC}"
        show_restore_points
        exit 1
    fi

    echo -e "${YELLOW}Rolling back to: $TARGET${NC}"
    echo -e "  Target: $(git log --oneline -1 "$TARGET")"
    echo -e "  Current: $(git log --oneline -1)"
    echo ""

    # Create backup branch of current state
    BACKUP="rollback-backup-$(date +%Y%m%d-%H%M%S)"
    git branch "$BACKUP"
    echo -e "${GREEN}Current state backed up to branch: $BACKUP${NC}"

    # Preserve .env and data/
    ENV_BACKUP=""
    if [ -f .env ]; then
        ENV_BACKUP=$(cat .env)
    fi

    DATA_EXISTS=false
    if [ -d data ]; then
        DATA_EXISTS=true
    fi

    # Reset to target
    git checkout "$TARGET" -- .
    git checkout HEAD -- .env 2>/dev/null || true

    # Restore .env if it was overwritten or deleted
    if [ -n "$ENV_BACKUP" ]; then
        echo "$ENV_BACKUP" > .env
    fi

    # Ensure data/ exists
    if [ "$DATA_EXISTS" = true ]; then
        mkdir -p data
    fi

    # Stage and commit the rollback
    git add -A
    git commit -m "Rollback to $TARGET

Backup branch: $BACKUP
Rolled back by: scripts/rollback.sh

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>" 2>/dev/null || echo "(no changes to commit)"

    echo ""
    echo -e "${GREEN}Rollback complete!${NC}"
    echo -e "  Now at: $(git log --oneline -1)"
    echo -e "  Backup: branch '$BACKUP' (use 'git branch -D $BACKUP' to delete)"
    echo -e "  .env:   preserved"
    echo -e "  data/:  preserved"

    # Verify
    echo ""
    echo -e "${YELLOW}Running tests to verify...${NC}"
    python3 -m pytest tests/ -q 2>&1 | tail -3
}

show_status() {
    echo -e "${GREEN}Project Health Check${NC}"
    echo "─────────────────────────────────────────────────────"
    echo -e "Branch:    ${YELLOW}$(git branch --show-current)${NC}"
    echo -e "HEAD:      $(git log --oneline -1)"
    echo -e "Modified:  $(git status --short | wc -l | tr -d ' ') files"
    echo ""

    LATEST_TAG=$(git describe --tags --abbrev=0 2>/dev/null || echo "none")
    if [ "$LATEST_TAG" != "none" ]; then
        COMMITS_SINCE=$(git rev-list "$LATEST_TAG"..HEAD --count)
        echo -e "Last tag:  ${GREEN}$LATEST_TAG${NC} ($COMMITS_SINCE commits ahead)"
    fi

    echo ""
    echo -e "${YELLOW}Tests:${NC}"
    python3 -m pytest tests/ -q 2>&1 | tail -3
}

find_latest_working() {
    git tag -l "working-*" --sort=-creatordate | head -1 || git tag -l "v*-working" --sort=-creatordate | head -1
}

# ---- Main ----

case "${1:-}" in
    "")
        show_restore_points
        ;;
    "--save")
        save_restore_point
        ;;
    "--status")
        show_status
        ;;
    "latest")
        LATEST=$(find_latest_working)
        if [ -z "$LATEST" ]; then
            echo -e "${RED}No working tags found${NC}"
            exit 1
        fi
        rollback_to "$LATEST"
        ;;
    *)
        rollback_to "$1"
        ;;
esac
