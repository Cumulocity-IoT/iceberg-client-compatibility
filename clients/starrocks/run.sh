#!/usr/bin/env bash
# Usage, from the repository root: clients/starrocks/run.sh <plan.json> <out.json>
# Starts the StarRocks all-in-one container (FE and BE), runs checks.py against its MySQL port, and removes it.
set -euo pipefail
. harness/env.sh

image=starrocks/allin1-ubuntu:4.1.7  # published for amd64 and arm64
name="compat-starrocks-$$"
port="$(python3 -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])')"

trap 'docker rm -f "$name" >/dev/null 2>&1 || true' EXIT
# The image defaults to an 8 GB FE heap and a BE limit of 90 % of the host; the caps keep the container under
# 5 GB on a 2-vCPU, 8 GB CI runner. The container gets no secrets: checks.py sends the catalog settings over SQL.
docker run -d --name "$name" --memory 5g -p "127.0.0.1:$port:9030" --entrypoint /usr/bin/tini-static "$image" \
  -- bash -c 'sed -i "s/-Xmx8192m/-Xmx2048m/" starrocks/fe/conf/fe.conf
              echo "mem_limit = 2G" >> starrocks/be/conf/be.conf
              exec ./entrypoint.sh' >/dev/null

# health_check.sh passes once the BE is registered with the FE and both answer /api/health.
for _ in $(seq 300); do
  if docker exec "$name" ./health_check.sh >/dev/null 2>&1; then break; fi
  sleep 1
done
if ! docker exec "$name" ./health_check.sh >/dev/null 2>&1; then
  echo "StarRocks did not become healthy within 300 s; the last log lines:" >&2
  docker logs --tail 40 "$name" 2>&1 | PYTHONDONTWRITEBYTECODE=1 python3 -c \
    'import sys; from harness.redact import redact; sys.stdout.write(redact(sys.stdin.read()))' >&2
  exit 1
fi

export STARROCKS_PORT="$port" PYTHONDONTWRITEBYTECODE=1  # importing harness.redact must not write into harness/
uv run --script clients/starrocks/checks.py "$1" "$2"
