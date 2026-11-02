#!/usr/bin/env bash
# Usage, from the repository root: clients/doris/run.sh <plan.json> <out.json>
#
# Starts one Doris frontend and one backend from the release's Docker images on a private network, runs the checks
# over the MySQL protocol and always removes the stack again. Both images are multi-arch (amd64, arm64).
set -euo pipefail
. harness/env.sh

DORIS="4.1.5"
FE_IMAGE="apache/doris:fe-$DORIS" BE_IMAGE="apache/doris:be-$DORIS"
NET=iceberg-compat-doris FE=iceberg-compat-doris-fe BE=iceberg-compat-doris-be
SUBNET=172.29.80 # fixed, because the images' entrypoints take the FE and BE addresses as IPs
export PYTHONDONTWRITEBYTECODE=1 # importing harness.redact must not write into harness/

cleanup() {
  docker rm -f "$FE" "$BE" >/dev/null 2>&1 || true
  docker network rm "$NET" >/dev/null 2>&1 || true
}
trap cleanup EXIT
cleanup # a stack left over by a killed run

# Logs carry no secrets before the catalog is created; they still pass the redaction the results get.
logs() { docker logs --tail 40 "$1" 2>&1 | python3 -c 'import sys; from harness.redact import redact; print(redact(sys.stdin.read()))'; }
wait_for() { # <seconds> <container to show on timeout> <description> <command...>
  local deadline=$((SECONDS + $1)) container=$2 what=$3
  shift 3
  until "$@" >/dev/null 2>&1; do
    if ((SECONDS > deadline)); then
      echo "doris: $what not ready after ${SECONDS}s" >&2
      logs "$container" >&2
      exit 1
    fi
    sleep 2
  done
}
sql() { docker exec "$FE" mysql -uroot -h127.0.0.1 -P9030 -e "$1"; }

for image in "$FE_IMAGE" "$BE_IMAGE"; do
  docker image inspect "$image" >/dev/null 2>&1 || docker pull -q "$image" >/dev/null
done

# The backend needs vm.max_map_count >= 2000000 on the Docker host (Doris installation guide); a container reads
# the host's value.
MAP_COUNT=$(docker run --rm --entrypoint cat "$BE_IMAGE" /proc/sys/vm/max_map_count)
if ((MAP_COUNT < 2000000)); then
  if [ "$(uname -s)" = Linux ] && sudo -n true 2>/dev/null; then
    sudo sysctl -q -w vm.max_map_count=2000000
  else
    echo "doris: vm.max_map_count is $MAP_COUNT on the Docker host; set it to 2000000 (sysctl -w vm.max_map_count=2000000)" >&2
    exit 1
  fi
fi

docker network create --subnet "$SUBNET.0/24" "$NET" >/dev/null

# Memory stays under 5 GB in all: the images default to an 8 GB frontend heap and a backend sized to the host.
docker run -d --name "$FE" --network "$NET" --ip "$SUBNET.2" --memory 2g -p 127.0.0.1::9030 \
  -e FE_SERVERS="fe1:$SUBNET.2:9010" -e FE_ID=1 --entrypoint bash "$FE_IMAGE" -c '
    sed -i -E "s/-Xmx[0-9]+[mMgG]/-Xmx1280m/g; s/-Xms[0-9]+[mMgG]/-Xms512m/g" /opt/apache-doris/fe/conf/fe.conf
    exec bash init_fe.sh' >/dev/null
wait_for 240 "$FE" frontend sql "SHOW FRONTENDS"

# The backend's entrypoint registers it with the frontend (ALTER SYSTEM ADD BACKEND) once that answers.
docker run -d --name "$BE" --network "$NET" --ip "$SUBNET.3" --memory 3g \
  -e FE_SERVERS="fe1:$SUBNET.2:9010" -e BE_ADDR="$SUBNET.3:9050" --entrypoint bash "$BE_IMAGE" -c '
    conf=/opt/apache-doris/be/conf/be.conf
    sed -i -E "s/-Xmx[0-9]+[mMgG]/-Xmx512m/g" "$conf"
    echo "mem_limit = 2200M" >> "$conf"
    exec bash entry_point.sh' >/dev/null
backend_alive() { sql 'SHOW BACKENDS\G' | grep -q 'Alive: true'; }
wait_for 240 "$BE" backend backend_alive

export DORIS_PORT="$(docker port "$FE" 9030/tcp | head -1 | sed 's/.*://')"
export DORIS_BUILD="Apache Doris $DORIS (Docker $FE_IMAGE, $BE_IMAGE), $(docker exec "$FE" sh -c 'ls /opt/apache-doris/fe/lib' |
  sed -n 's/^iceberg-core-\(.*\)\.jar$/Iceberg Java \1/p')"
uv run --script clients/doris/checks.py "$1" "$2"
