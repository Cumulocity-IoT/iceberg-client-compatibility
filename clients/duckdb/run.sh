#!/bin/sh
# Usage, from the repository root: clients/duckdb/run.sh <plan.json> <out.json>
set -eu
. harness/env.sh
export PYTHONDONTWRITEBYTECODE=1  # importing harness.redact must not write into harness/
exec uv run --script clients/duckdb/checks.py "$1" "$2"
