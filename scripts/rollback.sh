#!/bin/bash
# Roll back one commit (undo last deploy) and restart the bot.
# Run multiple times to go back further. Does not pull from remote.
# Usage: bash scripts/rollback.sh

set -e
REPO_DIR="${REPO_DIR:-$(cd "$(dirname "$0")/.." && pwd)}"
cd "$REPO_DIR"

# Check we have a commit to roll back to
if ! git rev-parse HEAD~1 >/dev/null 2>&1; then
  echo "Nothing to roll back (only one commit in history)."
  exit 1
fi

PREV=$(git rev-parse HEAD~1)
echo "Rolling back from $(git rev-parse --short HEAD) to $(git rev-parse --short HEAD~1)..."
git reset --hard HEAD~1

# Kill and restart bot
if command -v pkill >/dev/null 2>&1; then
  pkill -f "python main.py" 2>/dev/null || true
  sleep 1
fi
if [ -d "venv" ]; then
  source venv/bin/activate
fi
nohup python main.py >> bot.log 2>&1 &
echo "Rolled back to $(git rev-parse --short HEAD). Bot restarted (PID $!)."
