#!/usr/bin/env bash
# Usage, from the repository root: clients/dremio/run.sh <plan.json> <out.json>
set -euo pipefail
. harness/env.sh
exec uv run --script "$(dirname "$0")/checks.py" "$1" "$2"
