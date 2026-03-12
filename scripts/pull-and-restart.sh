#!/bin/bash
# Run from repo root on the Pi. Pulls from origin/main and restarts the bot if there were updates.
# Usage: ./scripts/pull-and-restart.sh
# Or from repo root: bash scripts/pull-and-restart.sh

set -e
REPO_DIR="${REPO_DIR:-$(cd "$(dirname "$0")/.." && pwd)}"
cd "$REPO_DIR"

# Optional toggle file so the bot can disable this cron-driven auto-update.
# If this file exists, the script exits immediately without pulling or restarting.
CRON_DISABLE_FILE="${CRON_DISABLE_FILE:-$REPO_DIR/.pull_cron_disabled}"
if [ -f "$CRON_DISABLE_FILE" ]; then
  echo "Auto-update cron is currently DISABLED (flag file: $CRON_DISABLE_FILE). Exiting without changes."
  exit 0
fi

# Default branch to track (override with env if you use another branch)
BRANCH="${DEPLOY_BRANCH:-main}"

git fetch origin
HEAD_OLD=$(git rev-parse HEAD)
HEAD_NEW=$(git rev-parse "origin/$BRANCH")

if [ "$HEAD_OLD" = "$HEAD_NEW" ]; then
  echo "No changes (already at $HEAD_OLD)."
  exit 0
fi

echo "Pulling updates ($HEAD_OLD -> $HEAD_NEW)..."
git pull origin "$BRANCH"

# Restart the bot: kill existing process, start in background with venv
if command -v pkill >/dev/null 2>&1; then
  pkill -f "python main.py" 2>/dev/null || true
  sleep 1
fi

# Activate venv if present and run bot (nohup so it survives this script)
if [ -d "venv" ]; then
  source venv/bin/activate
fi
nohup python main.py >> bot.log 2>&1 &
echo "Bot restarted (PID $!)."
