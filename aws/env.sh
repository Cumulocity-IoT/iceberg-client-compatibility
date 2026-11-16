# Sourced by the scripts in aws/: the catalog settings from the repository's .env, the AWS side from the
# environment, and the names of the resources the scripts create.
ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
if [ -f "$ROOT/.env" ]; then
  set -a
  . "$ROOT/.env"
  set +a
fi
: "${CATALOG_URI:?}" "${CATALOG_WAREHOUSE:?}" "${CATALOG_TOKEN_URI:?}" "${CATALOG_SCOPE:?}" "${CATALOG_CREDENTIAL:?}"
: "${AWS_PROFILE:?set AWS_PROFILE to the account to set up in}"
export AWS_PROFILE AWS_REGION=${AWS_REGION:-eu-central-1}

TENANT=$CATALOG_WAREHOUSE
SECRET_NAME=compat/$TENANT/catalog-client
ROLE_NAME=compat-glue-federation-$TENANT
CONNECTION=compat-$TENANT-polaris
CATALOG=$TENANT
WORKGROUP=compat
ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
ROLE_ARN=arn:aws:iam::$ACCOUNT:role/$ROLE_NAME
CONNECTION_ARN=arn:aws:glue:$AWS_REGION:$ACCOUNT:connection/$CONNECTION
# Amazon Redshift Serverless reads the federated catalog through resource links in the default Glue catalog, one per
# namespace that the tests read, each with an external schema of the namespace's name on it.
REDSHIFT=compat
REDSHIFT_ROLE_NAME=compat-redshift-federation
REDSHIFT_ROLE_ARN=arn:aws:iam::$ACCOUNT:role/$REDSHIFT_ROLE_NAME
NAMESPACES="cdc_alarm cdc_event cdc_inventory cdc_measurement cdc_rejected latest_inventory view_measurement"
LINK_PREFIX=compat_
# The identity of the GitHub workflow, through OpenID Connect, and the environment whose runs may take it.
GITHUB_REPO=${GITHUB_REPO:-Cumulocity-IoT/iceberg-client-compatibility}
GITHUB_ENVIRONMENT=${GITHUB_ENVIRONMENT:-eu-latest-test}
CI_ROLE_NAME=compat-github-actions
CI_ROLE_ARN=arn:aws:iam::$ACCOUNT:role/$CI_ROLE_NAME
OIDC_PROVIDER_ARN=arn:aws:iam::$ACCOUNT:oidc-provider/token.actions.githubusercontent.com

step() { echo; echo "== $*"; }

# Lets principal $1 find the federated catalog and its namespaces, and read the tables in them.
grant_catalog() {
  local principal=$1 ns
  aws lakeformation grant-permissions --principal DataLakePrincipalIdentifier="$principal" --permissions DESCRIBE \
    --resource "{\"Catalog\": {\"Id\": \"$ACCOUNT:$CATALOG\"}}"
  for ns in $NAMESPACES; do
    aws lakeformation grant-permissions --principal DataLakePrincipalIdentifier="$principal" --permissions DESCRIBE \
      --resource "{\"Database\": {\"CatalogId\": \"$ACCOUNT:$CATALOG\", \"Name\": \"$ns\"}}"
    aws lakeformation grant-permissions --principal DataLakePrincipalIdentifier="$principal" --permissions SELECT DESCRIBE \
      --resource "{\"Table\": {\"CatalogId\": \"$ACCOUNT:$CATALOG\", \"DatabaseName\": \"$ns\", \"TableWildcard\": {}}}"
  done
}

# Runs SQL in the Redshift Serverless database as its administrator, whose password Secrets Manager keeps.
redshift_sql() {
  local secret id state
  secret=$(aws redshift-serverless get-namespace --namespace-name "$REDSHIFT" --query namespace.adminPasswordSecretArn --output text)
  id=$(aws redshift-data execute-statement --workgroup-name "$REDSHIFT" --database "$REDSHIFT" --secret-arn "$secret" \
    --sql "$1" --query Id --output text)
  while state=$(aws redshift-data describe-statement --id "$id" --query Status --output text); do
    case $state in FINISHED) return 0 ;; FAILED | ABORTED) aws redshift-data describe-statement --id "$id" --query Error --output text >&2; return 1 ;; esac
    sleep 2
  done
}
