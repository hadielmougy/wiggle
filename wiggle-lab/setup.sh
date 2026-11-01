#!/usr/bin/env bash
# One-shot setup: venv + deps.
set -euo pipefail
cd "$(dirname "$0")"
python3 -m venv .venv
# shellcheck disable=SC1091
. .venv/bin/activate
pip install -q --disable-pip-version-check -r requirements.txt
echo
echo "Ready. Start the lab with:"
echo "    cd $(pwd) && source .venv/bin/activate && streamlit run app.py"
