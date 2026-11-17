# AWS Glue catalog federation, Amazon Athena, and Amazon Redshift

Can AWS's own analytics services, such as Amazon Athena and Amazon Redshift, read the Cumulocity Streaming Lake
Ingestion lakehouse? They reach Iceberg tables only through the AWS Glue Data Catalog, which can federate a remote
Iceberg REST catalog. This folder records what that federation does with the lakehouse, as tested on 2026-11-03,
2026-11-10, and 2026-11-16 in `eu-central-1`, and holds the scripts that set it up and reproduce every result below.
Athena and Redshift are also in the [compatibility table](../README.md#compatibility), tested every week.

## The lakehouse, as federation sees it

* An Apache Polaris Iceberg REST catalog, one catalog per tenant, logged in to with OAuth2 client credentials, scope
  `PRINCIPAL_ROLE:ALL`, and a read-only principal.
* The catalog vends short-lived, table-scoped storage credentials (`X-Iceberg-Access-Delegation:
  vended-credentials`). That is how every engine in the compatibility table reads the data; none of them needs its
  own access to the bucket.
* Table names keep the case of the data: Cumulocity fragment names such as `c8y_Temperature`, and camelCase column
  names such as `eventType`. Main tables (`cdc_alarm.alarm`, `cdc_event.event`, `cdc_inventory.inventory`,
  `cdc_operation.operation`, `latest_inventory.inventory`) have lowercase names; every fragment table of the data,
  measurements included, has a mixed-case one.
* Iceberg format v2. The latest tables are upserted with equality deletes.

## What was set up

In a separate AWS account, with [`setup.sh`](setup.sh):

1. A Secrets Manager secret with the client secret, under the key `USER_MANAGED_CLIENT_APPLICATION_CLIENT_SECRET`.
2. An IAM role that AWS Glue and AWS Lake Formation may assume, allowed to read that secret. With `AWS_VENDED` only,
   it may also read the tenant's prefix of the lakehouse.
3. An AWS Glue connection of type `ICEBERGRESTCATALOG` to the catalog, with OAuth2 client credentials (token URL and
   scope), `CREDENTIAL_VENDING_MODE = SOURCE_VENDED` and `CATALOG_CASING_FILTER = LOWERCASE_ONLY`.
4. The connection registered with Lake Formation (`--with-federation --with-privileged-access`), and a federated
   catalog on it whose identifier is the tenant's catalog.
5. An Athena workgroup with managed query results, on Athena engine version 3.
6. A Redshift Serverless namespace and workgroup at 8 RPU, the smallest capacity. Redshift resolves external schemas
   only in the default Glue catalog, so each namespace that the tests read gets a Glue resource link there, and an
   external schema of the namespace's name on that link. The namespace's IAM role may read Glue's metadata and call
   Lake Formation, and nothing else.
7. An IAM role for the compatibility run on GitHub, which the workflow assumes through OpenID Connect, with the same
   rights as the Redshift role plus queries in the two workgroups.

No IAM identity in the account has access to the lakehouse's bucket. With `SOURCE_VENDED`, the data comes only with
the catalog's vended credentials.

## Results

| What | Result | Evidence |
|---|---|---|
| Metadata federation with OAuth2 client credentials | Works | `aws glue get-databases --catalog-id <account>:<catalog>` lists all 15 namespaces of the tenant catalog |
| Data access with the catalog's vended credentials (`CREDENTIAL_VENDING_MODE = SOURCE_VENDED`) | Works since 2026-11-10, in Athena and Redshift | On 2026-11-03, `create-connection` refused the property: `Requester is not allowed to create a connection with the CREDENTIAL_VENDING_MODE = SOURCE_VENDED property`. AWS [announced](https://aws.amazon.com/about-aws/whats-new/2026/11/glue-catalog-vended-credentials-iceberg/) vended credentials for federated catalogs on 2026-11-06 and [documents](https://docs.aws.amazon.com/lake-formation/latest/dg/catalog-federation.html) the property ("IAM role needs Amazon S3 access to table data: No"). On 2026-11-16, the account's administrator got `403 Forbidden` on the table files, and neither the connection's role nor the Redshift role had S3 permissions; Athena and Redshift both read `cdc_alarm.alarm` (1,402 rows) and `cdc_inventory.inventory` |
| Data access with Lake Formation's credentials (`AWS_VENDED`, the default) | Needs a grant on the lakehouse's bucket | Athena `SELECT count(*) FROM cdc_alarm.alarm`: `PERMISSION_DENIED: User: …assumed-role/<role>/AWSLF-… is not authorized to perform: s3:GetObject on resource: "arn:aws:s3:::<bucket>/<tenant>/cdc_alarm/alarm/metadata/….metadata.json" because no resource-based policy allows the s3:GetObject action` |
| Tables with mixed-case names | Not federated | `CATALOG_CASING_FILTER` accepts `LOWERCASE_ONLY` (the default, also when omitted) or `UPPERCASE_ONLY`. `cdc_event` lists `compat_lowercase` and `event` only, of six tables; `cdc_measurement` lists nothing; `latest_inventory` lists `inventory` only, of 17 tables |
| A mixed-case table by its exact name | Not found | `aws glue get-table … --name c8y_Temperature`: `EntityNotFoundException`, `FromFederationSource: True`; Athena `SELECT … FROM cdc_measurement."c8y_Temperature"`: `TABLE_NOT_FOUND: … Table '<catalog>.cdc_measurement.c8y_temperature' does not exist`; Redshift: `EntityNotFoundException from glue - Table does not exist: cdc_measurement.c8y_temperature` |
| Columns with mixed-case names | Work, in lowercase | Glue lists every column and nested field in lowercase. Athena and Redshift resolve the names in any case: at one snapshot of `cdc_inventory.inventory`, `count(creationTime)` equals the row count, its minimum and maximum match PyIceberg to the millisecond, and so do the counts of the nested fields `templateParameters[].autoSelect` and `.typeDisplayName.semanticType`. Redshift with `enable_case_sensitive_identifier` on finds only the lowercase spelling |
| Equality deletes (latest tables) | Applied by Athena and Redshift | `latest_inventory.inventory`: 5,002 rows and 5,002 distinct ids in Athena, Redshift, and Apache Spark, from data files with about 6,000 rows and 135 delete files |
| Long strings | Truncated by Redshift | Redshift reads an Iceberg string as `varchar(16384)` and [truncates](https://docs.aws.amazon.com/redshift/latest/dg/querying-iceberg-supported-data-types.html) longer strings: a string of 32,768 bytes comes back with 16,382. Athena returns it whole |
| Iceberg views | Not federated | Glue federates tables only, also no view with a lowercase name such as `view_alarm.alarm` |

Athena rejects DDL such as `SHOW TABLES` on a federated catalog (`DDL queries are not supported for this catalog`);
metadata comes from the Glue API.

## What it means today

Through Glue federation with `SOURCE_VENDED`, Athena and Redshift read the tenant's tables that have lowercase names
with the catalog's vended credentials, as every other engine does: no bucket grants, and the catalog keeps enforcing
tenant isolation. Their camelCase columns and the equality deletes of the latest inventory work, with the same results
as Apache Spark. These are the main tables: alarms, events, operations, and the inventory with its history.

The fragment tables, which hold every measurement, stay out of reach: the casing filter federates only names that are
all lowercase or all uppercase. Glue does not federate the views either. In the compatibility table, the checks on
these tables run again on tables with lowercase names that hold the same data, with the `lowercase-tables` workaround,
so the cells show what Athena and Redshift do with the data once Glue lists its table.

AWS services that talk to the Iceberg REST catalog directly instead of through Glue are not affected: Amazon EMR and
EMR Serverless (Spark, Trino, Flink), AWS Glue ETL jobs with Spark, and SageMaker notebooks with any client of the
compatibility table. They keep the names' case and read with the catalog's vended credentials.

## Options

| Option | Who | What it changes |
|---|---|---|
| A case-preserving casing filter | AWS | Mixed-case tables become reachable, quoted, as in Spark or Trino. Their columns already work |
| Lowercase table names in the lakehouse | Lakehouse operator | Makes every table reachable, but renames every fragment table for existing users |
| Direct access without Glue (EMR, EMR Serverless, Glue ETL Spark, SageMaker) | Customer | Works today, without Athena or Redshift |

## Question for AWS

Is a case-preserving mode planned for federated catalogs, so that tables with mixed-case names are federated?

## Scripts

All scripts read the catalog settings from the repository's `.env` (`CATALOG_URI`, `CATALOG_WAREHOUSE`,
`CATALOG_TOKEN_URI`, `CATALOG_SCOPE`, `CATALOG_CREDENTIAL`) and take the AWS side from the environment:

| Setting | Meaning |
|---|---|
| `AWS_PROFILE` | the AWS account to set up in, with administrator rights and Lake Formation administration |
| `AWS_REGION` | default `eu-central-1` |
| `CREDENTIAL_VENDING_MODE` | `SOURCE_VENDED` (default) or `AWS_VENDED` |
| `LAKE_LOCATION` | for `AWS_VENDED` only: the tenant's prefix of the lakehouse, `s3://<bucket>/<tenant>`, for the role's read access |
| `GITHUB_REPO`, `GITHUB_ENVIRONMENT` | the repository and the environment whose workflow runs may assume the CI role; default `Cumulocity-IoT/iceberg-client-compatibility` and `eu-latest-test` |
| `GITHUB_SUBJECT_PREFIX` | the repository as its OpenID Connect tokens name it, which `gh api repos/<repo>/actions/oidc/customization/sub` prints as `sub_claim_prefix`; default that of `Cumulocity-IoT/iceberg-client-compatibility`, which uses GitHub's immutable subject claims with the IDs of the owner and the repository |

* [`setup.sh`](setup.sh) creates everything above and skips what already exists. On an existing connection it switches
  the credential vending mode, which keeps the registration and the federated catalog. For `AWS_VENDED` it prints the
  bucket-policy statement the lakehouse's owner needs to add. At the end it prints the variables `AWS_ROLE_ARN` and
  `AWS_REGION` that the GitHub environment needs. Creating the secret, the Lake Formation administrator, and the
  OpenID Connect identity change permissions in the account; run it yourself.
* [`probe.sh`](probe.sh) reads, and reproduces the results of Glue and Athena above. It counts at the tables' current
  snapshots and prints their IDs, so another engine can count the same data with `VERSION AS OF <snapshot>`.
* [`identity.sh`](identity.sh) gives the clients `athena` and `redshift` their AWS identity: in CI the workflow's role,
  on a developer machine the AWS CLI profile in `PLATFORM_AWS_PROFILE`, for example
  `PLATFORM_AWS_PROFILE=<profile> python -m harness.run athena redshift`.
* [`teardown.sh`](teardown.sh) removes everything `setup.sh` created, except the Lake Formation administrator and the
  OpenID Connect provider.
