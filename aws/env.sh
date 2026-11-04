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

step() { echo; echo "== $*"; }
