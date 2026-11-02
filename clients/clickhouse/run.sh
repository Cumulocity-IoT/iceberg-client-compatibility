#!/usr/bin/env bash
# Usage, from the repository root: clients/clickhouse/run.sh <plan.json> <out.json>
set -euo pipefail
. harness/env.sh

# The release tag is v<version>-stable; the assets are a bare binary for macOS and a tarball for Linux.
version="26.10.2.103"
dir="$PWD/clients/clickhouse"
export CLICKHOUSE="$dir/target/clickhouse-$version"
if [ ! -x "$CLICKHOUSE" ]; then
  mkdir -p "$dir/target"
  base="https://github.com/ClickHouse/ClickHouse/releases/download/v$version-stable"
  case "$(uname -s)-$(uname -m)" in
    Darwin-arm64) curl -fsSL -o "$CLICKHOUSE.part" "$base/clickhouse-macos-aarch64" ;;
    Linux-x86_64)
      curl -fsSL "$base/clickhouse-common-static-$version-amd64.tgz" \
        | tar -xzO -f - "clickhouse-common-static-$version/usr/bin/clickhouse" > "$CLICKHOUSE.part" ;;
    *) echo "no ClickHouse $version binary for $(uname -s) $(uname -m)" >&2; exit 1 ;;
  esac
  chmod +x "$CLICKHOUSE.part" && mv "$CLICKHOUSE.part" "$CLICKHOUSE"
fi
export PYTHONDONTWRITEBYTECODE=1  # importing harness.redact must not write into harness/
exec uv run --script "$dir/checks.py" "$1" "$2"
