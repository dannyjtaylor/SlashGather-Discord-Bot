#!/bin/bash
# Kill the Discord bot process (no restart).
# Usage: bash scripts/kill-bot.sh

set -e
if pkill -f "python main.py" 2>/dev/null; then
  echo "Bot process killed."
else
  echo "No bot process found (already stopped?)."
fi
