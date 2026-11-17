#!/usr/bin/env bash
# Usage, from the repository root: clients/athena/run.sh <plan.json> <out.json>
#
# Queries the catalog that aws/setup.sh federated into the AWS Glue Data Catalog, as the identity of aws/identity.sh.
set -euo pipefail
. harness/env.sh
. aws/identity.sh
export PYTHONDONTWRITEBYTECODE=1 # importing harness.redact must not write into harness/
exec uv run --script "$(dirname "$0")/checks.py" "$1" "$2"
