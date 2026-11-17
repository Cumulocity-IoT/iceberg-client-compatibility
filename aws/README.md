# AWS Glue catalog federation, Amazon Athena, and Amazon Redshift

Amazon Athena and Amazon Redshift read Iceberg tables only through the AWS Glue Data Catalog, which can federate a
remote Iceberg REST catalog such as that of the Cumulocity Streaming Lake Ingestion lakehouse. This folder sets up
that federation for the [compatibility table](../README.md#compatibility), whose footnotes give its limits.

## Setup

[`setup.sh`](setup.sh) creates, in an AWS account:

1. A Secrets Manager secret with the client secret, under the key `USER_MANAGED_CLIENT_APPLICATION_CLIENT_SECRET`,
   and an IAM role that AWS Glue and AWS Lake Formation may assume to read it.
2. An AWS Glue connection of type `ICEBERGRESTCATALOG` to the catalog, with OAuth2 client credentials and
   `CREDENTIAL_VENDING_MODE = SOURCE_VENDED`, registered with Lake Formation (`--with-federation
   --with-privileged-access`), and a federated catalog on it, named after the tenant ID.
3. An Athena workgroup on engine version 3.
4. A Redshift Serverless workgroup. Redshift resolves external schemas only in the default Glue catalog, so each
   namespace gets a Glue resource link there, and an external schema of the namespace's name on that link.
5. An IAM role that the compatibility workflow on GitHub assumes through OpenID Connect.

With `SOURCE_VENDED`, Athena and Redshift read the data with the catalog's vended credentials, so no identity in the
AWS account needs access to the lakehouse's bucket. With `AWS_VENDED`, the default, Lake Formation reads the data
with the role of step 1, which then needs a grant on the bucket.

## Names

The connection property `CATALOG_CASING_FILTER` takes `LOWERCASE_ONLY` (the default) or `UPPERCASE_ONLY`. Glue
therefore federates the main tables, such as `cdc_alarm.alarm` and `latest_inventory.inventory`, but no table with a
mixed-case name such as `c8y_Temperature`, and a query by the exact, quoted name does not find one either.

Columns are federated whatever their case. Glue lists them in lowercase, and Athena and Redshift resolve camelCase
columns and nested fields, such as `creationTime`, in any case. Redshift with `enable_case_sensitive_identifier` on
finds only the lowercase spelling.

Athena rejects DDL, such as `SHOW TABLES`, on a federated catalog; metadata comes from the Glue API.

## Options for mixed-case tables

| Option | Who | What it changes |
|---|---|---|
| A case-preserving casing filter | AWS | Mixed-case tables become reachable, quoted, as in Spark or Trino |
| Lowercase table names in the lakehouse | Lakehouse operator | Makes every table reachable, but renames every fragment table for existing users |
| Direct access without Glue: Amazon EMR and EMR Serverless (Spark, Trino, Flink), AWS Glue ETL jobs with Spark, SageMaker notebooks | Customer | Works today with the clients of the compatibility table, which keep the names' case, but without Athena or Redshift |

## Scripts

All scripts read the catalog settings from the repository's `.env` and take the AWS side from the environment:

| Setting | Meaning |
|---|---|
| `AWS_PROFILE` | the AWS account to set up in, with administrator rights and Lake Formation administration |
| `AWS_REGION` | default `eu-central-1` |
| `CREDENTIAL_VENDING_MODE` | `SOURCE_VENDED` (default) or `AWS_VENDED` |
| `LAKE_LOCATION` | for `AWS_VENDED` only: the tenant's prefix of the lakehouse, `s3://<bucket>/<tenant>`, for the role's read access |
| `GITHUB_REPO`, `GITHUB_ENVIRONMENT`, `GITHUB_SUBJECT_PREFIX` | the repository, the environment, and the repository's OpenID Connect subject prefix, whose workflow runs may assume the CI role; the defaults are those of this repository. `gh api repos/<repo>/actions/oidc/customization/sub` prints the prefix as `sub_claim_prefix` |

* [`setup.sh`](setup.sh) creates everything above and skips what already exists; on an existing connection it
  switches the credential vending mode. For `AWS_VENDED` it prints the bucket-policy statement that the lakehouse's
  owner needs to add, and at the end the variables `AWS_ROLE_ARN` and `AWS_REGION` for the GitHub environment. Run it
  with an administrator profile, because it changes permissions in the account.
* [`probe.sh`](probe.sh) changes nothing. It compares what Glue federates with what the catalog holds, and queries
  lowercase and mixed-case tables in Athena at the tables' current snapshots, whose IDs it prints so that another
  engine can count the same data with `VERSION AS OF <snapshot>`.
* [`identity.sh`](identity.sh) gives the clients `athena` and `redshift` their AWS identity: in CI the workflow's role,
  on a developer machine the AWS CLI profile in `PLATFORM_AWS_PROFILE`, for example
  `PLATFORM_AWS_PROFILE=<profile> python -m harness.run athena redshift`.
* [`teardown.sh`](teardown.sh) removes everything `setup.sh` created, except the Lake Formation administrator and the
  OpenID Connect provider.
