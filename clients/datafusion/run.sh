#!/usr/bin/env bash
# Usage: clients/datafusion/run.sh <plan.json> <out.json>, from the repository root.
set -euo pipefail
dir="$(cd "$(dirname "$0")" && pwd)"
plan="$1" out="$2"
. harness/env.sh
cargo=cargo
"$cargo" --version >/dev/null 2>&1 || cargo="$HOME/.cargo/bin/cargo"  # a version-manager shim without a pinned Rust

build() { "$cargo" build --release --quiet --manifest-path "$dir/Cargo.toml" "$@"; }
binary="$dir/target/release/datafusion-compat"

build
status=0
"$binary" "$plan" "$out" || status=$?
# Exit code 3: the catalog needs the "tls" workaround switch (see Cargo.toml), so build with it and run again.
if [ "$status" -eq 3 ]; then
  build --features tls
  "$binary" "$plan" "$out"
elif [ "$status" -ne 0 ]; then
  exit "$status"
fi
