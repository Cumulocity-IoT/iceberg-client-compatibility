# AWS Glue catalog federation and Amazon Athena

Can AWS's own analytics services, such as Amazon Athena, Amazon Redshift and Amazon SageMaker, read the Cumulocity
Streaming Lake Ingestion lake? They reach Iceberg tables only through the AWS Glue Data Catalog, which can federate a
remote Iceberg REST catalog. This folder records what that federation does with the lake, as tested on 2026-11-03 and
2026-11-10 in `eu-central-1`, and the scripts that set it up and reproduce every result below.

## The lake, as federation sees it

* An Apache Polaris Iceberg REST catalog, one catalog per tenant, logged in to with OAuth2 client credentials, scope
  `PRINCIPAL_ROLE:ALL`, and a read-only principal.
* The catalog vends short-lived, table-scoped storage credentials (`X-Iceberg-Access-Delegation:
  vended-credentials`). That is how every engine in the [compatibility table](../README.md#compatibility) reads the
  data; none of them needs its own access to the bucket.
* Table names keep the case of the data: Cumulocity fragment names such as `c8y_Temperature`, and camelCase column
  names such as `eventType`. Main tables (`cdc_alarm.alarm`, `cdc_event.event`, `cdc_inventory.inventory`,
  `cdc_operation.operation`, `latest_inventory.inventory`) have lowercase names; every fragment table, measurements
  included, has a mixed-case one.
* Iceberg format v2. The latest tables are upserted with equality deletes.

## What was set up

In a separate AWS account, with [`setup.sh`](setup.sh):

1. A Secrets Manager secret with the client secret, under the key `USER_MANAGED_CLIENT_APPLICATION_CLIENT_SECRET`.
2. An IAM role that AWS Glue and AWS Lake Formation may assume, allowed to read that secret. With `AWS_VENDED` only,
   it may also read the tenant's prefix of the lake.
3. An AWS Glue connection of type `ICEBERGRESTCATALOG` to the catalog, with OAuth2 client credentials (token URL and
   scope), `CREDENTIAL_VENDING_MODE = SOURCE_VENDED` and `CATALOG_CASING_FILTER = LOWERCASE_ONLY`.
4. The connection registered with Lake Formation (`--with-federation --with-privileged-access`), and a federated
   catalog on it whose identifier is the tenant's catalog.
5. An Athena workgroup with managed query results, on Athena engine version 3.

## Results

| What | Result | Evidence |
|---|---|---|
| Metadata federation with OAuth2 client credentials | Works | `aws glue get-databases --catalog-id <account>:<catalog>` lists all 15 namespaces of the tenant catalog |
| Data access with the catalog's vended credentials (`CREDENTIAL_VENDING_MODE = SOURCE_VENDED`) | Works since 2026-11-10 | On 2026-11-03, `create-connection` refused the property: `Requester is not allowed to create a connection with the CREDENTIAL_VENDING_MODE = SOURCE_VENDED property`. AWS [announced](https://aws.amazon.com/about-aws/whats-new/2026/11/glue-catalog-vended-credentials-iceberg/) vended credentials for federated catalogs on 2026-11-06 and [documents](https://docs.aws.amazon.com/lake-formation/latest/dg/catalog-federation.html) the property. On 2026-11-10, `update-connection` accepted it, and Athena `SELECT count(*) FROM cdc_inventory.inventory` returned 3,798,280 rows with no grant on the lake's bucket |
| Data access with Lake Formation's credentials (`AWS_VENDED`, the default) | Needs a grant on the lake's bucket | Athena `SELECT count(*) FROM cdc_alarm.alarm`: `PERMISSION_DENIED: User: …assumed-role/<role>/AWSLF-… is not authorized to perform: s3:GetObject on resource: "arn:aws:s3:::<bucket>/<tenant>/cdc_alarm/alarm/metadata/….metadata.json" because no resource-based policy allows the s3:GetObject action` |
| Tables with mixed-case names | Not federated | `CATALOG_CASING_FILTER` accepts `LOWERCASE_ONLY` (the default, also when omitted) or `UPPERCASE_ONLY`. `cdc_event` lists `event` only, of `c8y_Position`, `compat_Fixture`, `compat_x002EEscaped`, `event`, `jobStatus`; `cdc_measurement` lists nothing; `latest_inventory` lists `inventory` only, of 17 tables |
| A mixed-case table by its exact name | Not found | `aws glue get-table … --name c8y_Temperature`: `EntityNotFoundException`, `FromFederationSource: True`; Athena `SELECT … FROM cdc_measurement."c8y_Temperature"`: `TABLE_NOT_FOUND: … Table '<catalog>.cdc_measurement.c8y_temperature' does not exist` |
| Columns with mixed-case names | Work | Athena lowercases the names and resolves them: 23 of the 31 columns of `cdc_inventory.inventory` have capital letters, and at one snapshot `count(lastupdated)`, `count(eventtype)` and `count(creationtime)` each equal the 3,798,358 rows. Apache Spark 4.1 with Iceberg 1.12.0 returns the same four counts and the same latest `lastUpdated`, 2026-11-10 12:54:15.454 UTC, at the same snapshot |
| Equality deletes (latest tables) | Applied by Athena | `latest_inventory.inventory` at one snapshot: 5,002 rows and 5,002 distinct ids in Athena, the same as in Apache Spark. Redshift is untested; it documents no support for equality deletes |
| Iceberg views | Not federated | Glue federates tables only |

Athena rejects DDL such as `SHOW TABLES` on a federated catalog (`DDL queries are not supported for this catalog`);
metadata comes from the Glue API.

## What it means today

Through Glue federation with `SOURCE_VENDED`, Athena reads the tenant's tables that have lowercase names with the
catalog's vended credentials, as every other engine does: no bucket grants, and the catalog keeps enforcing tenant
isolation. Their camelCase columns and the equality deletes of the latest inventory work, with the same results as
Apache Spark. These are the main tables: alarms, events, operations, and the inventory with its history.

The fragment tables, which hold every measurement, stay out of reach: the casing filter federates only names that are
all lowercase or all uppercase. Glue does not federate the views either.

AWS services that talk to the Iceberg REST catalog directly instead of through Glue are not affected: Amazon EMR and
EMR Serverless (Spark, Trino, Flink), AWS Glue ETL jobs with Spark, and SageMaker notebooks with any client of the
compatibility table. They keep the names' case and read with the catalog's vended credentials.

## Options

| Option | Who | What it changes |
|---|---|---|
| A case-preserving casing filter | AWS | Mixed-case tables become reachable, quoted, as in Spark or Trino. Their columns already work |
| Lowercase table names in the lake | Lake operator | Makes every table reachable, but renames every fragment table for existing users |
| Direct access without Glue (EMR, EMR Serverless, Glue ETL Spark, SageMaker) | Customer | Works today, without Athena or Redshift |

## Questions for AWS

1. Is a case-preserving mode for federated catalogs planned, so that tables with mixed-case names are federated?
2. Do Redshift's queries on federated tables apply Iceberg v2 equality deletes?

## Scripts

All three read the catalog settings from the repository's `.env` (`CATALOG_URI`, `CATALOG_WAREHOUSE`,
`CATALOG_TOKEN_URI`, `CATALOG_SCOPE`, `CATALOG_CREDENTIAL`) and take the AWS side from the environment:

| Setting | Meaning |
|---|---|
| `AWS_PROFILE` | the AWS account to set up in, with administrator rights and Lake Formation administration |
| `AWS_REGION` | default `eu-central-1` |
| `CREDENTIAL_VENDING_MODE` | `SOURCE_VENDED` (default) or `AWS_VENDED` |
| `LAKE_LOCATION` | for `AWS_VENDED` only: the tenant's prefix of the lake, `s3://<bucket>/<tenant>`, for the role's read access |

* [`setup.sh`](setup.sh) creates everything above and skips what already exists. On an existing connection it switches
  the credential vending mode, which keeps the registration and the federated catalog. For `AWS_VENDED` it prints the
  bucket-policy statement the lake's owner needs to add. Creating the secret and adding the Lake Formation
  administrator change permissions in the account; run it yourself.
* [`probe.sh`](probe.sh) reads, and reproduces the results above. It counts at the tables' current snapshots and
  prints their IDs, so another engine can count the same data with `VERSION AS OF <snapshot>`.
* [`teardown.sh`](teardown.sh) removes everything `setup.sh` created, except the Lake Formation administrator.
