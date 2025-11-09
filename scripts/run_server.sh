#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_DIR"

if [[ -d ".venv" ]]; then
  source ".venv/bin/activate"
fi

export FLASK_APP="app/app.py"
export FLASK_ENV=production

exec flask run --host=0.0.0.0 --port=5000
