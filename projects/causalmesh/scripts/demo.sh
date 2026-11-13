#!/usr/bin/env bash
set -euo pipefail
python -m causalmesh demo --transfers 250
python -m causalmesh audit
python -m causalmesh replay
python -m causalmesh stats
echo 'Launch: uvicorn causalmesh.api:app --host 127.0.0.1 --port 8000'
