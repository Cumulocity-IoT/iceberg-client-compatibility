#!/usr/bin/env bash
# Removes what setup.sh created, in dependency order. The Lake Formation administrator stays, since it may
# administer other resources; remove it in the Lake Formation console if it was added only for this.
set -uo pipefail
. "$(dirname "$0")/env.sh"

step "Federated catalog $CATALOG"
aws glue delete-catalog --catalog-id "$CATALOG"
step "Lake Formation registration of $CONNECTION"
aws lakeformation deregister-resource --resource-arn "$CONNECTION_ARN"
step "Glue connection $CONNECTION"
aws glue delete-connection --connection-name "$CONNECTION"
step "Athena workgroup $WORKGROUP"
aws athena delete-work-group --work-group "$WORKGROUP" --recursive-delete-option
step "IAM role $ROLE_NAME"
for policy in $(aws iam list-role-policies --role-name "$ROLE_NAME" --query PolicyNames --output text); do
  aws iam delete-role-policy --role-name "$ROLE_NAME" --policy-name "$policy"
done
aws iam delete-role --role-name "$ROLE_NAME"
step "Secrets Manager secret $SECRET_NAME (deleted after seven days)"
aws secretsmanager delete-secret --secret-id "$SECRET_NAME" --recovery-window-in-days 7 --query DeletionDate --output text
