#!/usr/bin/env bash
# Usage, from the repository root: clients/trino/run.sh <plan.json> <out.json>
# Starts a single-node Trino server under target/, runs checks.py against it over localhost, and stops it.
set -euo pipefail
. harness/env.sh

TRINO_VERSION=484
dir="$PWD/clients/trino"
target="$dir/target"
server="$target/trino-server-core-$TRINO_VERSION"

# Trino runs on Java 25: the runner's preinstalled 25, else one from mise.
if ! java -version 2>&1 | grep -qE 'version "25[."]'; then
  if [ -n "${JAVA_HOME_25_X64:-}" ]; then
    JAVA_HOME="$JAVA_HOME_25_X64"
  else
    mise install -q java@25 >/dev/null
    JAVA_HOME="$(mise where java@25)"
  fi
  export JAVA_HOME PATH="$JAVA_HOME/bin:$PATH"
fi

# The core server and the Iceberg plugin, which together are a third smaller than the full server tarball.
if [ ! -d "$server" ]; then
  url="https://github.com/trinodb/trino/releases/download/$TRINO_VERSION"
  staging="$target/staging"
  rm -rf "$staging" && mkdir -p "$staging"
  curl -fsSL -o "$staging/core.tar.gz" "$url/trino-server-core-$TRINO_VERSION.tar.gz"
  curl -fsSL -o "$staging/iceberg.zip" "$url/trino-iceberg-$TRINO_VERSION.zip"
  tar xzf "$staging/core.tar.gz" -C "$staging"
  unzip -q "$staging/iceberg.zip" -d "$staging/trino-server-core-$TRINO_VERSION/plugin"
  mv "$staging/trino-server-core-$TRINO_VERSION" "$server" && rm -rf "$staging"
fi

# An ephemeral configuration. The catalogs hold no secrets: Trino reads them from ${ENV:...}.
etc="$target/etc" data="$target/data"
rm -rf "$etc" "$data" && mkdir -p "$etc/catalog" "$data"
port="$(python3 -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])')"
cat > "$etc/node.properties" <<EOF
node.environment=compat
node.id=compat
node.data-dir=$data
EOF
cat > "$etc/config.properties" <<EOF
coordinator=true
node-scheduler.include-coordinator=true
http-server.http.port=$port
discovery.uri=http://127.0.0.1:$port
EOF
printf '%s\n' -Xmx2G -XX:+ExitOnOutOfMemoryError -Djdk.attach.allowAttachSelf=true > "$etc/jvm.config"
# The catalog `lake` and its twin with the case-insensitive-names switch; the arguments are connect-level lines.
catalogs() {
  { cat "$dir/example.properties"; printf '%s\n' "$@"; } > "$etc/catalog/lake.properties"
  { cat "$etc/catalog/lake.properties"; echo "iceberg.rest-catalog.case-insensitive-name-matching=true"; } \
    > "$etc/catalog/lake_case_insensitive_names.properties"
}
launcher() { "$server/bin/launcher" --etc-dir "$etc" --data-dir "$data" "$@" >/dev/null; }
started() {
  rm -rf "$data" && mkdir -p "$data"
  launcher start
  for _ in $(seq 180); do
    if curl -fs "http://127.0.0.1:$port/v1/info" 2>/dev/null | grep -q '"starting":false'; then return 0; fi
    launcher status || return 1  # the server exits when a catalog fails to load
    sleep 1
  done
  return 1
}

# A server that does not start is a catalog-login result: written scrubbed, with the cause from the server log.
not_started() {
  python3 -c 'import json, sys; from harness.redact import redact
json.dump({"client": "trino", "version": sys.argv[2], "build": "trino-server-core " + sys.argv[2],
           "checks": {"catalog-login": {"status": "fail", "note": redact("Trino did not start: " + sys.argv[3])[:500]}}},
          open(sys.argv[1], "w"), indent=2)' "$1" "$TRINO_VERSION" "$2"
}
cause() { grep -m1 '^Caused by: ' "$data/var/log/server.log" | cut -c12- | sed -e 's/: \[.*//' -e 's/@[0-9a-f]*//g' || true; }

trap 'launcher stop || true' EXIT
catalogs
if ! started; then
  TRINO_CONNECT_ERROR="$(cause)"
  launcher stop || true
  case "$TRINO_CONNECT_ERROR" in
    *"Unable to load region"*) ;;
    *) not_started "$2" "$TRINO_CONNECT_ERROR"; exit 0 ;;
  esac
  # Workaround s3-region: native S3 builds its client when the catalog loads, and without a region the server
  # does not start, so the switch is needed to connect at all.
  catalogs "s3.region=$(python3 -c 'import json, sys; print(json.load(open(sys.argv[1]))["region"])' "$1")"
  if ! started; then
    not_started "$2" "$(cause) (also with s3.region)"
    exit 0
  fi
  export TRINO_CONNECT_WORKAROUND=s3-region TRINO_CONNECT_ERROR
fi

export TRINO_PORT="$port" PYTHONDONTWRITEBYTECODE=1  # importing harness.redact must not write into harness/
TRINO_JAVA="$(java -XshowSettings:properties -version 2>&1 | awk -F' = ' '/^ *java.version =/ {print $2}')"
export TRINO_JAVA
uv run --script "$dir/checks.py" "$1" "$2"
