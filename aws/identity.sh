# Sourced by clients/athena/run.sh and clients/redshift/run.sh after harness/env.sh, which keeps the machine's AWS
# settings away from every client: the AWS identity that calls the platform under test, under names of its own.
#
# In CI the workflow passes the credentials of its role as PLATFORM_AWS_ACCESS_KEY_ID, PLATFORM_AWS_SECRET_ACCESS_KEY
# and PLATFORM_AWS_SESSION_TOKEN. On a developer machine, PLATFORM_AWS_PROFILE names an AWS CLI profile of the account
# that setup.sh set up. Neither identity has access to the lake's bucket, so the data comes with the catalog's
# vended credentials.

if [ -z "${PLATFORM_AWS_ACCESS_KEY_ID:-}" ]; then
  : "${PLATFORM_AWS_PROFILE:?no AWS credentials: in CI, the workflow did not log in to AWS; on a developer machine, set PLATFORM_AWS_PROFILE to the AWS CLI profile of the account that aws/setup.sh set up}"
  eval "$(AWS_CONFIG_FILE="$HOME/.aws/config" AWS_SHARED_CREDENTIALS_FILE="$HOME/.aws/credentials" \
    aws configure export-credentials --profile "$PLATFORM_AWS_PROFILE" --format env-no-export |
    sed -E -n 's/^AWS_(ACCESS_KEY_ID|SECRET_ACCESS_KEY|SESSION_TOKEN)=/export PLATFORM_AWS_\1=/p')"
fi
export PLATFORM_AWS_REGION=${PLATFORM_AWS_REGION:-eu-central-1}
