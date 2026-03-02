#!/bin/bash
# Restart the bot without pulling. Use after editing files on the Pi or when you don't want to update from git.
# Usage: bash scripts/restart-bot.sh

set -e
REPO_DIR="${REPO_DIR:-$(cd "$(dirname "$0")/.." && pwd)}"
cd "$REPO_DIR"

# Kill existing bot
if command -v pkill >/dev/null 2>&1; then
  pkill -f "python main.py" 2>/dev/null || true
  sleep 1
fi

# Start with venv
if [ -d "venv" ]; then
  source venv/bin/activate
fi
nohup python main.py >> bot.log 2>&1 &
echo "Bot restarted (PID $!)."
