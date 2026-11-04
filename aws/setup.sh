#!/usr/bin/env bash
# Sets up AWS Glue catalog federation of the tenant's Iceberg catalog and an Athena workgroup (see README.md).
# Skips what already exists. Run it yourself: it stores a secret and adds a Lake Formation administrator.
set -euo pipefail
. "$(dirname "$0")/env.sh"
: "${LAKE_LOCATION:?set LAKE_LOCATION to the tenant prefix of the lake, s3://<bucket>/<tenant>}"
VENDING=${CREDENTIAL_VENDING_MODE:-AWS_VENDED}
LAKE_LOCATION=${LAKE_LOCATION%/}
BUCKET=${LAKE_LOCATION#s3://}
BUCKET=${BUCKET%%/*}
PREFIX=${LAKE_LOCATION#s3://"$BUCKET"/}

step "Secrets Manager secret $SECRET_NAME"
if ! SECRET_ARN=$(aws secretsmanager describe-secret --secret-id "$SECRET_NAME" --query ARN --output text 2>/dev/null); then
  # On standard input, so the client secret never appears in a command line.
  SECRET_ARN=$(python3 -c 'import json, os
print(json.dumps({"USER_MANAGED_CLIENT_APPLICATION_CLIENT_SECRET": os.environ["CATALOG_CREDENTIAL"].split(":", 1)[1]}))' \
    | aws secretsmanager create-secret --name "$SECRET_NAME" --secret-string file:///dev/stdin \
        --description "Client secret of the read-only catalog principal of $TENANT, for Glue catalog federation" \
        --query ARN --output text)
fi
echo "$SECRET_ARN"

step "IAM role $ROLE_NAME"
if ! aws iam get-role --role-name "$ROLE_NAME" >/dev/null 2>&1; then
  aws iam create-role --role-name "$ROLE_NAME" --query Role.Arn --output text \
    --description "Glue catalog federation to the Iceberg catalog of $TENANT" \
    --assume-role-policy-document '{"Version": "2012-10-17", "Statement": [{"Effect": "Allow",
      "Principal": {"Service": ["glue.amazonaws.com", "lakeformation.amazonaws.com"]}, "Action": "sts:AssumeRole"}]}'
fi
aws iam put-role-policy --role-name "$ROLE_NAME" --policy-name federation --policy-document "{
  \"Version\": \"2012-10-17\", \"Statement\": [
    {\"Sid\": \"CatalogClientSecret\", \"Effect\": \"Allow\",
     \"Action\": [\"secretsmanager:GetSecretValue\", \"secretsmanager:DescribeSecret\", \"secretsmanager:PutSecretValue\"],
     \"Resource\": \"$SECRET_ARN\"},
    {\"Sid\": \"LakeRead\", \"Effect\": \"Allow\", \"Action\": \"s3:GetObject\", \"Resource\": \"arn:aws:s3:::$BUCKET/$PREFIX/*\"},
    {\"Sid\": \"LakeList\", \"Effect\": \"Allow\", \"Action\": \"s3:ListBucket\", \"Resource\": \"arn:aws:s3:::$BUCKET\",
     \"Condition\": {\"StringLike\": {\"s3:prefix\": [\"$PREFIX/*\"]}}}]}"
echo "$ROLE_ARN"

step "Lake Formation administrator"
# The caller must administer Lake Formation to create a federated catalog. put-data-lake-settings replaces the
# whole settings object, so the current settings are read and written back with the caller added.
CALLER_ROLE=$(aws sts get-caller-identity --query Arn --output text | cut -d/ -f2)
CALLER_ARN=$(aws iam get-role --role-name "$CALLER_ROLE" --query Role.Arn --output text)
SETTINGS=$(aws lakeformation get-data-lake-settings --query DataLakeSettings --output json | python3 -c 'import json, sys
s = json.load(sys.stdin)
admins = s.setdefault("DataLakeAdmins", [])
if not any(a["DataLakePrincipalIdentifier"] == sys.argv[1] for a in admins):
    admins.append({"DataLakePrincipalIdentifier": sys.argv[1]})
print(json.dumps(s))' "$CALLER_ARN")
aws lakeformation put-data-lake-settings --data-lake-settings "$SETTINGS"
echo "$CALLER_ARN"

step "Glue connection $CONNECTION ($VENDING)"
if ! aws glue get-connection --name "$CONNECTION" >/dev/null 2>&1; then
  CONNECTION=$CONNECTION TENANT=$TENANT ROLE_ARN=$ROLE_ARN VENDING=$VENDING SECRET_ARN=$SECRET_ARN python3 -c 'import json, os
print(json.dumps({
    "Name": os.environ["CONNECTION"],
    "Description": "Iceberg REST catalog of " + os.environ["TENANT"],
    "ConnectionType": "ICEBERGRESTCATALOG",
    "ConnectionProperties": {
        "INSTANCE_URL": os.environ["CATALOG_URI"],
        "ROLE_ARN": os.environ["ROLE_ARN"],
        "CREDENTIAL_VENDING_MODE": os.environ["VENDING"],
        "CATALOG_CASING_FILTER": "LOWERCASE_ONLY"},
    "AuthenticationConfiguration": {
        "AuthenticationType": "OAUTH2",
        "OAuth2Properties": {
            "OAuth2GrantType": "CLIENT_CREDENTIALS",
            "TokenUrl": os.environ["CATALOG_TOKEN_URI"],
            "OAuth2ClientApplication": {"UserManagedClientApplicationClientId": os.environ["CATALOG_CREDENTIAL"].split(":", 1)[0]},
            "TokenUrlParametersMap": {"scope": os.environ["CATALOG_SCOPE"]}},
        "SecretArn": os.environ["SECRET_ARN"]}}))' \
    | aws glue create-connection --connection-input file:///dev/stdin --query CreateConnectionStatus --output text
fi
aws glue get-connection --name "$CONNECTION" --query Connection.Status --output text

step "Lake Formation registration of the connection"
if ! aws lakeformation describe-resource --resource-arn "$CONNECTION_ARN" >/dev/null 2>&1; then
  aws lakeformation register-resource --resource-arn "$CONNECTION_ARN" --role-arn "$ROLE_ARN" \
    --with-federation --with-privileged-access
fi
echo "$CONNECTION_ARN"

step "Federated catalog $CATALOG"
if ! aws glue get-catalog --catalog-id "$CATALOG" >/dev/null 2>&1; then
  aws glue create-catalog --name "$CATALOG" --catalog-input "{
    \"Description\": \"Federated Iceberg catalog of $TENANT\",
    \"FederatedCatalog\": {\"Identifier\": \"$TENANT\", \"ConnectionName\": \"$CONNECTION\"},
    \"CreateTableDefaultPermissions\": [], \"CreateDatabaseDefaultPermissions\": []}"
fi
echo "$ACCOUNT:$CATALOG"

step "Athena workgroup $WORKGROUP"
if ! aws athena get-work-group --work-group "$WORKGROUP" >/dev/null 2>&1; then
  aws athena create-work-group --name "$WORKGROUP" --description "Queries of the federated catalog $CATALOG" \
    --configuration '{"ManagedQueryResultsConfiguration": {"Enabled": true}, "EnforceWorkGroupConfiguration": true,
      "EngineVersion": {"SelectedEngineVersion": "AUTO"}}'
fi
echo "$WORKGROUP"

if [ "$VENDING" = AWS_VENDED ]; then
  step "Bucket-policy statements the owner of $BUCKET adds for AWS_VENDED"
  cat <<EOF
{ "Sid": "GlueFederationRead", "Effect": "Allow", "Principal": { "AWS": "$ROLE_ARN" },
  "Action": "s3:GetObject", "Resource": "arn:aws:s3:::$BUCKET/$PREFIX/*" },
{ "Sid": "GlueFederationList", "Effect": "Allow", "Principal": { "AWS": "$ROLE_ARN" },
  "Action": "s3:ListBucket", "Resource": "arn:aws:s3:::$BUCKET",
  "Condition": { "StringLike": { "s3:prefix": ["$PREFIX/*"] } } }
EOF
fi
