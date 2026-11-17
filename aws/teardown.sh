#!/usr/bin/env bash
# Removes what setup.sh created, in dependency order. The Lake Formation administrator and the GitHub OpenID Connect
# provider stay, since they may serve other resources; remove them in the consoles if they were added only for this.
set -uo pipefail
. "$(dirname "$0")/env.sh"

delete_role() {
  local policy
  for policy in $(aws iam list-role-policies --role-name "$1" --query PolicyNames --output text); do
    aws iam delete-role-policy --role-name "$1" --policy-name "$policy"
  done
  aws iam delete-role --role-name "$1"
}

step "GitHub Actions identity $CI_ROLE_NAME"
delete_role "$CI_ROLE_NAME"
step "Redshift Serverless workgroup and namespace $REDSHIFT"
aws redshift-serverless delete-workgroup --workgroup-name "$REDSHIFT" --query workgroup.status --output text
while aws redshift-serverless get-workgroup --workgroup-name "$REDSHIFT" >/dev/null 2>&1; do sleep 20; done
aws redshift-serverless delete-namespace --namespace-name "$REDSHIFT" --query namespace.status --output text
step "IAM role $REDSHIFT_ROLE_NAME"
delete_role "$REDSHIFT_ROLE_NAME"
step "Glue resource links $LINK_PREFIX<namespace>"
for ns in $NAMESPACES; do aws glue delete-database --name "$LINK_PREFIX$ns"; done
step "Federated catalog $CATALOG"
aws glue delete-catalog --catalog-id "$CATALOG"
step "Lake Formation registration of $CONNECTION"
aws lakeformation deregister-resource --resource-arn "$CONNECTION_ARN"
step "Glue connection $CONNECTION"
aws glue delete-connection --connection-name "$CONNECTION"
step "Athena workgroup $WORKGROUP"
aws athena delete-work-group --work-group "$WORKGROUP" --recursive-delete-option
step "IAM role $ROLE_NAME"
delete_role "$ROLE_NAME"
step "Secrets Manager secret $SECRET_NAME (deleted after seven days)"
aws secretsmanager delete-secret --secret-id "$SECRET_NAME" --recovery-window-in-days 7 --query DeletionDate --output text
