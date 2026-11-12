#!/usr/bin/env bash
# One-time setup of the Google Cloud project that runs the tests of Managed Service for Apache Spark. Idempotent:
# each step checks before it creates or grants. Needs gcloud logged in with rights to create service accounts,
# buckets and namespaces and to grant IAM roles in the project.
#
# The catalog credential is not created here: store <clientId>:<clientSecret> of a catalog principal of its own in
# Secret Manager first, as the setup guide (README.md) describes, and name the secret in GCP_CATALOG_SECRET.
set -euo pipefail

: "${GCP_PROJECT:?the Google Cloud project}"
: "${GCP_REGION:=europe-west1}"                   # Dataproc needs a region, not a multi-region
: "${GCP_BIGLAKE_CATALOG:?the BigLake Iceberg catalog that receives the copy}"
: "${GCP_NAMESPACE:=compat_tests}"                # reserved for the tests, replaced on every run
: "${GCP_STAGING_BUCKET:=${GCP_PROJECT}-compat-tests}"
: "${GCP_CATALOG_SECRET:=c8y-catalog-credential}" # the secret's ID in Secret Manager
: "${GITHUB_REPO:=Cumulocity-IoT/iceberg-client-compatibility}"
: "${WIF_POOL:=github-actions}"
SA_ID=compat-tests
SA="${SA_ID}@${GCP_PROJECT}.iam.gserviceaccount.com"
p=(--project="$GCP_PROJECT")
number=$(gcloud projects describe "$GCP_PROJECT" --format='value(projectNumber)')
# IAM accepts a new service account in policies only after up to a minute, so each grant is retried.
retry() {
  for _ in 1 2 3 4 5 6 7 8 9; do "$@" 2>/dev/null && return; sleep 10; done
  "$@"
}

if ! gcloud iam service-accounts describe "$SA" "${p[@]}" >/dev/null 2>&1; then
  gcloud iam service-accounts create "$SA_ID" --display-name="Iceberg client compatibility tests" "${p[@]}"
fi

# dataproc.editor submits batches (the CI identity), dataproc.worker runs them (the job identity); one account is both.
# bigquery.jobUser runs the query on the copy.
for role in roles/dataproc.editor roles/dataproc.worker roles/biglake.viewer roles/bigquery.jobUser; do
  retry gcloud projects add-iam-policy-binding "$GCP_PROJECT" --member="serviceAccount:$SA" --role="$role" \
    --condition=None --quiet >/dev/null
done
# Submitting a batch that runs as the account needs serviceAccountUser on it, here for the account itself.
retry gcloud iam service-accounts add-iam-policy-binding "$SA" "${p[@]}" --member="serviceAccount:$SA" \
  --role=roles/iam.serviceAccountUser --quiet >/dev/null
# GitHub Actions of this repository only, through the organization's workload identity pool.
retry gcloud iam service-accounts add-iam-policy-binding "$SA" "${p[@]}" --role=roles/iam.workloadIdentityUser --quiet \
  --member="principalSet://iam.googleapis.com/projects/${number}/locations/global/workloadIdentityPools/${WIF_POOL}/attribute.repository/${GITHUB_REPO}" \
  >/dev/null
retry gcloud secrets add-iam-policy-binding "$GCP_CATALOG_SECRET" "${p[@]}" --member="serviceAccount:$SA" \
  --role=roles/secretmanager.secretAccessor --quiet >/dev/null

# The job files, the Iceberg AWS bundle and the results of each run.
if ! gcloud storage buckets describe "gs://${GCP_STAGING_BUCKET}" >/dev/null 2>&1; then
  gcloud storage buckets create "gs://${GCP_STAGING_BUCKET}" --location="$GCP_REGION" "${p[@]}" \
    --uniform-bucket-level-access --public-access-prevention
fi
retry gcloud storage buckets add-iam-policy-binding "gs://${GCP_STAGING_BUCKET}" --member="serviceAccount:$SA" \
  --role=roles/storage.objectAdmin >/dev/null

# The namespace that receives the copy; the account may write there and nowhere else in the catalog.
ns=(--catalog="$GCP_BIGLAKE_CATALOG" "${p[@]}")
if ! gcloud biglake iceberg namespaces describe "$GCP_NAMESPACE" "${ns[@]}" >/dev/null 2>&1; then
  gcloud biglake iceberg namespaces create "$GCP_NAMESPACE" "${ns[@]}"
fi
policy=$(mktemp)
trap 'rm -f "$policy"' EXIT
gcloud biglake iceberg namespaces get-iam-policy "$GCP_NAMESPACE" "${ns[@]}" --format=json |
  jq --arg m "serviceAccount:$SA" '
    .bindings = ((.bindings // []) as $b
      | if any($b[]; .role == "roles/biglake.editor") then
          [$b[] | if .role == "roles/biglake.editor" then .members = (.members + [$m] | unique) else . end]
        else $b + [{role: "roles/biglake.editor", members: [$m]}] end)' >"$policy"
gcloud biglake iceberg namespaces set-iam-policy "$GCP_NAMESPACE" "$policy" "${ns[@]}" >/dev/null

echo "GitHub variables for the environment of the workflow:"
echo "  GCP_PROJECT=$GCP_PROJECT"
echo "  GCP_REGION=$GCP_REGION"
echo "  GCP_WIF_PROVIDER=projects/${number}/locations/global/workloadIdentityPools/${WIF_POOL}/providers/github"
echo "  GCP_SERVICE_ACCOUNT=$SA"
echo "  GCP_STAGING_BUCKET=$GCP_STAGING_BUCKET"
echo "  GCP_BIGLAKE_CATALOG=$GCP_BIGLAKE_CATALOG"
echo "  GCP_NAMESPACE=$GCP_NAMESPACE"
echo "  GCP_CATALOG_SECRET=projects/${GCP_PROJECT}/secrets/${GCP_CATALOG_SECRET}"
