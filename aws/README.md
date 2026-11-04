# AWS Glue catalog federation and Amazon Athena

Can AWS's own analytics services, such as Amazon Athena, Amazon Redshift and Amazon SageMaker, read the Cumulocity
Streaming Lake Ingestion lake? They reach Iceberg tables only through the AWS Glue Data Catalog, which can federate a
remote Iceberg REST catalog. This folder records what that federation does with the lake, as tested on 2026-11-03 in
`eu-central-1`, and the scripts that set it up and reproduce every result below.

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
2. An IAM role that AWS Glue and AWS Lake Formation may assume, allowed to read that secret and the tenant's prefix of
   the lake.
3. An AWS Glue connection of type `ICEBERGRESTCATALOG` to the catalog, with OAuth2 client credentials (token URL and
   scope).
4. The connection registered with Lake Formation (`--with-federation --with-privileged-access`), and a federated
   catalog on it whose identifier is the tenant's catalog.
5. An Athena workgroup with managed query results.

## Results

| What | Result | Evidence |
|---|---|---|
| Metadata federation with OAuth2 client credentials | Works | `aws glue get-databases --catalog-id <account>:<catalog>` lists all 15 namespaces of the tenant catalog |
| Data access with the catalog's vended credentials (`CREDENTIAL_VENDING_MODE = SOURCE_VENDED`) | Refused | `create-connection`: `Requester is not allowed to create a connection with the CREDENTIAL_VENDING_MODE = SOURCE_VENDED property`, from an administrator role. The connection type lists the value (`describe-connection-type ICEBERGRESTCATALOG`), no documentation mentions it |
| Data access with Lake Formation's credentials (`AWS_VENDED`, the default) | Needs a grant on the lake's bucket | Athena `SELECT count(*) FROM cdc_alarm.alarm`: `PERMISSION_DENIED: User: …assumed-role/<role>/AWSLF-… is not authorized to perform: s3:GetObject on resource: "arn:aws:s3:::<bucket>/<tenant>/cdc_alarm/alarm/metadata/….metadata.json" because no resource-based policy allows the s3:GetObject action` |
| Tables with mixed-case names | Not federated | `CATALOG_CASING_FILTER` accepts `LOWERCASE_ONLY` (the default, also when omitted) or `UPPERCASE_ONLY`. `cdc_event` lists `event` only, of `c8y_Position`, `compat_Fixture`, `compat_x002EEscaped`, `event`, `jobStatus`; `cdc_measurement` lists nothing |
| A mixed-case table by its exact name | Not found | `aws glue get-table … --name c8y_Temperature`: `EntityNotFoundException`, `FromFederationSource: True`; Athena `SELECT … FROM cdc_measurement."c8y_Temperature"`: `TABLE_NOT_FOUND: … Table '<catalog>.cdc_measurement.c8y_temperature' does not exist` |
| Columns with mixed-case names | Not verified | Needs data access. AWS documents that a table definition with capital letters isn't supported by Lake Formation or the Glue Data Catalog ([S3 Tables naming rules](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables-buckets-naming.html)) |
| Iceberg views | Not federated | Glue federates tables only |
| Equality deletes (latest tables) | Not verified | Needs data access. Redshift documents no support for equality deletes |

Athena rejects DDL such as `SHOW TABLES` on a federated catalog (`DDL queries are not supported for this catalog`);
metadata comes from the Glue API.

## What it means today

Through Glue federation, AWS services can reach only the tenant's main tables, and only once the lake's owner grants
the customer's Lake Formation role read access to the tenant's prefix. The fragment tables, which hold every
measurement, are out of reach, and so are the views.

AWS services that talk to the Iceberg REST catalog directly instead of through Glue are not affected: Amazon EMR and
EMR Serverless (Spark, Trino, Flink), AWS Glue ETL jobs with Spark, and SageMaker notebooks with any client of the
compatibility table. They keep the names' case and read with the catalog's vended credentials.

## Options

| Option | Who | What it changes |
|---|---|---|
| Credential vending from the remote catalog (`SOURCE_VENDED`) | AWS | Federation reads with the catalog's table-scoped credentials, as every other engine does. No bucket grants, and the catalog keeps enforcing tenant isolation |
| A case-preserving casing filter | AWS | Mixed-case tables and columns become reachable, quoted, as in Spark or Trino |
| A grant per customer on the lake's bucket | Lake operator | Works today with `AWS_VENDED`, at the cost of one bucket-policy statement per customer role and tenant prefix. Natural where the customer owns the bucket |
| Lowercase table and column names in the lake | Lake operator | Makes every table reachable, but renames everything for existing users |
| Direct access without Glue (EMR, EMR Serverless, Glue ETL Spark, SageMaker) | Customer | Works today, without Athena or Redshift |

## Questions for AWS

1. Is `CREDENTIAL_VENDING_MODE = SOURCE_VENDED` for `ICEBERGRESTCATALOG` connections available, and how is an account
   enabled for it?
2. Is a case-preserving mode for federated catalogs planned, for table and column names?
3. Do federated tables with mixed-case column names work in Athena and Redshift once the table itself is reachable?
4. Do Athena and Redshift apply Iceberg v2 equality deletes on federated tables?

## Scripts

All three read the catalog settings from the repository's `.env` (`CATALOG_URI`, `CATALOG_WAREHOUSE`,
`CATALOG_TOKEN_URI`, `CATALOG_SCOPE`, `CATALOG_CREDENTIAL`) and take the AWS side from the environment:

| Setting | Meaning |
|---|---|
| `AWS_PROFILE` | the AWS account to set up in, with administrator rights and Lake Formation administration |
| `AWS_REGION` | default `eu-central-1` |
| `LAKE_LOCATION` | the tenant's prefix of the lake, `s3://<bucket>/<tenant>`, for the role's read access |
| `CREDENTIAL_VENDING_MODE` | `AWS_VENDED` (default) or `SOURCE_VENDED` |

* [`setup.sh`](setup.sh) creates everything above, skips what already exists, and prints the bucket-policy statement
  the lake's owner needs to add for `AWS_VENDED`. Creating the secret and adding the Lake Formation administrator change
  permissions in the account; run it yourself.
* [`probe.sh`](probe.sh) reads, and reproduces the results above.
* [`teardown.sh`](teardown.sh) removes everything `setup.sh` created, except the Lake Formation administrator.
