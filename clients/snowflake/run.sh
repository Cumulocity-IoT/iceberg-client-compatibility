#!/usr/bin/env bash
# Usage, from the repository root: clients/snowflake/run.sh <plan.json> <out.json>
set -euo pipefail
. harness/env.sh
export PYTHONDONTWRITEBYTECODE=1  # importing harness.redact must not write into harness/
exec uv run --script "$(dirname "$0")/checks.py" "$1" "$2"
