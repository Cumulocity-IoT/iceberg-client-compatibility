#!/usr/bin/env bash
# Usage, from the repository root: clients/spark/run.sh <plan.json> <out.json>
set -euo pipefail
. harness/env.sh
# Spark 4.1 runs on Java 17 or 21 only.
if ! java -version 2>&1 | grep -qE 'version "(17|21)[."]'; then
  mise install -q java@21 >/dev/null
  JAVA_HOME="$(mise where java@21)"
  export JAVA_HOME PATH="$JAVA_HOME/bin:$PATH"
fi
exec uv run --script clients/spark/checks.py "$1" "$2"
