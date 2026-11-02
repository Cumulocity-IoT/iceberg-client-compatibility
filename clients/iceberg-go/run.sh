#!/usr/bin/env bash
# Usage, from the repository root: clients/iceberg-go/run.sh <plan.json> <out.json>
set -euo pipefail
. harness/env.sh

dir="$PWD/clients/iceberg-go"
# The Go release that go.mod names; CI installs it with actions/setup-go (go-version-file), locally mise does.
if ! go version >/dev/null 2>&1; then  # also a version-manager shim without a Go set
  version="$(sed -n 's/^go //p' "$dir/go.mod")"
  mise install -q "go@$version" >/dev/null
  PATH="$(mise where "go@$version")/bin:$PATH"
fi
# -mod=mod adds the go.sum entries of a version the release check wrote into go.mod; committed entries still verify.
go -C "$dir" build -mod=mod -o target/iceberg-go-compat .
exec "$dir/target/iceberg-go-compat" "$1" "$2"
