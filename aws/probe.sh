#!/usr/bin/env bash
# Reproduces the results in README.md, read only: what the federated catalog shows against what the catalog holds,
# a mixed-case table by its exact name, and Athena queries on a lowercase and a mixed-case table.
set -euo pipefail
. "$(dirname "$0")/env.sh"
NAMESPACES=${PROBE_NAMESPACES:-cdc_alarm cdc_event cdc_measurement latest_inventory}
MIXED_NAMESPACE=${PROBE_MIXED_NAMESPACE:-cdc_measurement}
MIXED_TABLE=${PROBE_MIXED_TABLE:-c8y_Temperature}
LOWER_TABLE=${PROBE_LOWER_TABLE:-cdc_alarm.alarm}

# The catalog itself, over its REST API; the token stays in this shell.
TOKEN=$(curl -s "$CATALOG_TOKEN_URI" -d grant_type=client_credentials \
  --data-urlencode "client_id=${CATALOG_CREDENTIAL%%:*}" --data-urlencode "client_secret=${CATALOG_CREDENTIAL#*:}" \
  --data-urlencode "scope=$CATALOG_SCOPE" | python3 -c 'import json, sys; print(json.load(sys.stdin)["access_token"])')
PREFIX=$(curl -s -H "Authorization: Bearer $TOKEN" "$CATALOG_URI/v1/config?warehouse=$CATALOG_WAREHOUSE" \
  | python3 -c 'import json, sys; c = json.load(sys.stdin); print(c.get("overrides", {}).get("prefix") or c["defaults"]["prefix"])')
remote_tables() {
  curl -s -H "Authorization: Bearer $TOKEN" "$CATALOG_URI/v1/$PREFIX/namespaces/$1/tables?pageSize=1000" \
    | python3 -c 'import json, sys; print(" ".join(sorted(i["name"] for i in json.load(sys.stdin)["identifiers"])))'
}

step "Namespaces of the federated catalog $ACCOUNT:$CATALOG"
aws glue get-databases --catalog-id "$ACCOUNT:$CATALOG" --query 'DatabaseList[].Name' --output text

step "Tables: in the catalog, and through the federated catalog"
for ns in $NAMESPACES; do
  echo "$ns"
  echo "  catalog:   $(remote_tables "$ns")"
  echo "  federated: $(aws glue get-tables --catalog-id "$ACCOUNT:$CATALOG" --database-name "$ns" --query 'TableList[].Name' --output text | tr '\t' ' ')"
done

step "Glue get-table by exact name: $MIXED_NAMESPACE.$MIXED_TABLE"
aws glue get-table --catalog-id "$ACCOUNT:$CATALOG" --database-name "$MIXED_NAMESPACE" --name "$MIXED_TABLE" \
  --query 'Table.{name: Name, location: StorageDescriptor.Location}' --output json 2>&1 || true

athena() {
  local id state
  if ! id=$(aws athena start-query-execution --work-group "$WORKGROUP" --query-execution-context "Catalog=$CATALOG" \
      --query-string "$1" --query QueryExecutionId --output text 2>&1); then
    echo "rejected: $id"
    return
  fi
  for _ in $(seq 90); do
    state=$(aws athena get-query-execution --query-execution-id "$id" --query QueryExecution.Status.State --output text)
    case $state in SUCCEEDED | FAILED | CANCELLED) break ;; esac
    sleep 2
  done
  if [ "$state" = SUCCEEDED ]; then
    aws athena get-query-results --query-execution-id "$id" --query 'ResultSet.Rows[1].Data[0].VarCharValue' --output text
  else
    echo "$state: $(aws athena get-query-execution --query-execution-id "$id" --query QueryExecution.Status.StateChangeReason --output text)"
  fi
}

step "Athena: SELECT count(*) FROM $LOWER_TABLE"
athena "SELECT count(*) FROM $LOWER_TABLE"

step "Athena: SELECT count(*) FROM $MIXED_NAMESPACE.\"$MIXED_TABLE\""
athena "SELECT count(*) FROM $MIXED_NAMESPACE.\"$MIXED_TABLE\""
