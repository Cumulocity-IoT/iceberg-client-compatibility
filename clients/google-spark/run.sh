#!/usr/bin/env bash
# Usage, from the repository root: clients/google-spark/run.sh <plan.json> <out.json>
#
# Submits the test job as a batch of Google Cloud Managed Service for Apache Spark, waits for it, and fetches its
# result. gcloud runs as whoever it is logged in as: in CI the workflow, through workload identity federation.
set -euo pipefail
. harness/env.sh
: "${GCP_PROJECT:?}" "${GCP_REGION:?}" "${GCP_SERVICE_ACCOUNT:?}" "${GCP_STAGING_BUCKET:?}" "${GCP_BIGLAKE_CATALOG:?}"
: "${GCP_NAMESPACE:?}" "${GCP_CATALOG_SECRET:?}"
: "${GCP_SUBNET:=default}" # needs Private Google Access and Cloud NAT, see README.md

RUNTIME=2.3 # Spark 3.5, Scala 2.13; the runtime brings its own Iceberg and loads it ahead of --jars
ICEBERG_AWS_BUNDLE=1.10.1 # the AWS SDK that S3FileIO needs, which the runtime lacks
here=clients/google-spark
batch="compat-$(date -u +%Y%m%d-%H%M%S)"
result="gs://${GCP_STAGING_BUCKET}/runs/${batch}/out.json"
g=(--project="$GCP_PROJECT" --region="$GCP_REGION")

bundle="gs://${GCP_STAGING_BUCKET}/jars/iceberg-aws-bundle-${ICEBERG_AWS_BUNDLE}.jar"
if ! gcloud storage ls "$bundle" >/dev/null 2>&1; then
  curl -fsSL "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_AWS_BUNDLE}/iceberg-aws-bundle-${ICEBERG_AWS_BUNDLE}.jar" |
    gcloud storage cp - "$bundle"
fi

# The job imports the checks of the Apache Spark client and the scrubbing of the harness as they are.
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
mkdir "$work/harness"
cp harness/__init__.py harness/redact.py "$work/harness/"
cp clients/spark/checks.py "$here/example.py" "$here/gold.py" "$work/"
cp "$1" "$work/plan.json"
(cd "$work" && python3 -m zipfile -c deps.zip checks.py example.py gold.py harness)

# --ttl ends a batch that hangs; the job reads the catalog credential from Secret Manager, never from an argument.
gcloud dataproc batches submit pyspark "$here/checks_job.py" "${g[@]}" --batch="$batch" --version="$RUNTIME" \
  --ttl=30m --subnet="$GCP_SUBNET" --service-account="$GCP_SERVICE_ACCOUNT" --deps-bucket="gs://${GCP_STAGING_BUCKET}" \
  --jars="$bundle" --py-files="$work/deps.zip" --files="$work/plan.json" -- \
  --catalog-uri="$CATALOG_URI" --warehouse="$CATALOG_WAREHOUSE" --token-uri="$CATALOG_TOKEN_URI" \
  --scope="$CATALOG_SCOPE" --secret="$GCP_CATALOG_SECRET" --gcp-project="$GCP_PROJECT" \
  --biglake-catalog="$GCP_BIGLAKE_CATALOG" --namespace="$GCP_NAMESPACE" --out="$result"

runtime=$(gcloud dataproc batches describe "$batch" "${g[@]}" --format='value(runtimeConfig.version)')
gcloud storage cat "$result" | jq --arg runtime "$runtime" '{client, version: ($runtime + "+iceberg." + .iceberg), build, checks}' >"$2"
