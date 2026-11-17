#!/usr/bin/env bash
# Shows, read only, what AWS Glue federates (README.md): the tables of each namespace in the catalog and in the
# federated catalog, and a mixed-case table looked up by its exact name. The athena and redshift clients query the
# federated tables.
set -euo pipefail
. "$(dirname "$0")/env.sh"
NAMESPACES=${PROBE_NAMESPACES:-cdc_alarm cdc_event cdc_measurement latest_inventory}
MIXED_NAMESPACE=${PROBE_MIXED_NAMESPACE:-cdc_measurement}
MIXED_TABLE=${PROBE_MIXED_TABLE:-c8y_Temperature}

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
