#!/usr/bin/env bash
set -euo pipefail

echo "=== Setting up project environment (macOS/Linux) ==="

# Ensure uv is installed
if ! command -v uv >/dev/null 2>&1; then
  echo "Installing uv for current user..."
  python3 -m pip install --user --upgrade uv
  export PATH="$PATH:$(python3 -m site --user-base)/bin"
fi

# Create/refresh virtual environment and install dependencies
uv venv .venv
uv sync

echo "Environment ready. Activate with: source .venv/bin/activate"