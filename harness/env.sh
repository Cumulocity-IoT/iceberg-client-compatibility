# Sourced by every clients/*/run.sh, from the repository root: loads the settings and isolates the client.
#
# Storage access must come from vended credentials alone, so nothing a developer machine has configured for AWS
# or Azure may reach the client. CI passes the settings as environment variables instead of a .env file.

if [ -f .env ]; then set -a; . ./.env; set +a; fi

unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN AWS_REGION AWS_DEFAULT_REGION AWS_PROFILE \
  AWS_ENDPOINT_URL AZURE_STORAGE_ACCOUNT AZURE_STORAGE_KEY AZURE_STORAGE_SAS_TOKEN \
  AZURE_STORAGE_CONNECTION_STRING AZURE_CLIENT_ID AZURE_CLIENT_SECRET AZURE_TENANT_ID
export AWS_CONFIG_FILE=/dev/null AWS_SHARED_CREDENTIALS_FILE=/dev/null AWS_EC2_METADATA_DISABLED=true
