# Iceberg client compatibility

This repository serves two purposes for the managed lakehouse that Cumulocity Streaming Lake Ingestion writes for
your tenant:

* **Examples:** for each Apache Iceberg client, code that connects the client to the lakehouse and reads data. Use
  it as the starting point for your own connection. The Example column of the table links to it.
* **Compatibility:** a table of which clients can read the lakehouse, and which features of the lakehouse they
  handle. The tests run the same connection code as the examples, so the example of a client that is listed as
  working has worked in the last run.

The table below is generated from test runs against a Cumulocity tenant. Each run connects the way a customer
does: through the tenant's Iceberg catalog, with a read-only catalog principal and storage credentials that the
catalog vends. The tests only read the lakehouse; Streaming Lake Ingestion is a managed lakehouse. The Spark
services write a copy, but only into their platform's own storage.

Each column of the table is one thing your client has to handle to read this lakehouse, from logging in to the
catalog to reading tables that are updated with delete files. What each column means is listed below the table.

## Compatibility

<!-- BEGIN GENERATED TABLE -->
### Client libraries

| Client | Example | Version | Tested | Login | S3 | CDC tables | Latest tables | Schema evolution | Incremental read | Decimal | Timestamps | Nesting | Large values | Identifiers |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| [Iceberg Go](https://go.iceberg.apache.org) | [🔗](clients/iceberg-go/example.go "Example for Iceberg Go") | 0.7.0 | 2026-11-16 | ✅ | ✅ | ✅ | ✅ | ⚠️[^1] | ✅ | ⚠️[^1] | ⚠️[^1] | ✅ | ⚠️[^1] | ⚠️[^1] |
| [Iceberg Java](https://iceberg.apache.org) | [🔗](clients/iceberg-java/src/main/java/Example.java "Example for Iceberg Java") | 1.12.0 | 2026-11-16 | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| [Iceberg Rust](https://rust.iceberg.apache.org) | [🔗](clients/iceberg-rust/src/example.rs "Example for Iceberg Rust") | 0.11.0 | 2026-11-17 | ⚠️[^2] | ✅ | ✅ | ✅ | ✅ | ➖[^3] | ✅ | ✅ | ✅ | ✅ | ⚠️[^4] |
| [PyIceberg](https://py.iceberg.apache.org) | [🔗](clients/pyiceberg/example.py "Example for PyIceberg") | 0.12.0 | 2026-11-17 | ✅ | ✅ | ✅ | ➖[^5] | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |

### Engines

| Client | Example | Version | Tested | Login | S3 | CDC tables | Latest tables | Schema evolution | Incremental read | Decimal | Timestamps | Nesting | Large values | Identifiers | Views |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| [Apache DataFusion](https://datafusion.apache.org) | [🔗](clients/datafusion/src/example.rs "Example for Apache DataFusion") | 54.1.0 | 2026-11-17 | ⚠️[^2] | ✅ | ✅ | ✅ | ✅ | ➖[^3] | ✅ | ✅ | ✅ | ✅ | ✅ | ➖[^6] |
| [Apache Doris](https://doris.apache.org) | [🔗](clients/doris/example.py "Example for Apache Doris") | 4.1.5 | 2026-11-17 | ✅ | ✅ | ✅ | ✅ | ✅ | ➖[^3] | ✅ | ✅ | ✅ | ✅ | ✅ | ❌[^7] |
| [Apache Flink](https://flink.apache.org) | [🔗](clients/flink/example.py "Example for Apache Flink") | 2.3.0 | 2026-11-16 | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌[^8] | ✅ | ✅ | ✅ | ❌[^9] |
| [Apache Spark](https://spark.apache.org) | [🔗](clients/spark/example.py "Example for Apache Spark") | 4.1.4 | 2026-11-16 | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| [ClickHouse](https://clickhouse.com) | [🔗](clients/clickhouse/example.py "Example for ClickHouse") | 26.10.4.207 | 2026-11-17 | ✅ | ✅ | ✅ | ✅ | ✅ | ➖[^3] | ✅ | ✅ | ✅ | ✅ | ✅ | ➖[^6] |
| [Daft](https://www.daft.ai) | [🔗](clients/daft/example.py "Example for Daft") | 0.7.26 | 2026-11-17 | ✅ | ✅ | ❌[^10] | ➖[^5] | ✅ | ➖[^3] | ✅ | ✅ | ✅ | ✅ | ✅ | ➖[^6] |
| [DuckDB](https://duckdb.org) | [🔗](clients/duckdb/example.py "Example for DuckDB") | 2.0.0 | 2026-11-17 | ✅ | ✅ | ✅ | ✅ | ✅ | ➖[^3] | ✅ | ✅ | ✅ | ✅ | ✅ | ❌[^11] |
| [Polars](https://pola.rs) | [🔗](clients/polars/example.py "Example for Polars") | 2.1.0 | 2026-11-17 | ✅ | ✅ | ✅ | ➖[^5] | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ➖[^6] |
| [StarRocks](https://www.starrocks.io) | [🔗](clients/starrocks/example.py "Example for StarRocks") | 4.1.7 | 2026-11-17 | ✅ | ✅ | ✅ | ✅ | ✅ | ➖[^3] | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| [Trino](https://trino.io) | [🔗](clients/trino/example.properties "Example for Trino") | 484 | 2026-11-16 | ⚠️[^12] | ⚠️[^13] | ⚠️[^13] | ✅ | ⚠️[^13] | ⚠️[^13] | ⚠️[^13] | ⚠️[^13] | ⚠️[^13] | ⚠️[^13] | ⚠️[^13] | ❌[^14] |

### Products

| Client | Example | Version | Tested | Login | S3 | CDC tables | Latest tables | Schema evolution | Incremental read | Decimal | Timestamps | Nesting | Large values | Identifiers | Views |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| [Amazon Athena](https://aws.amazon.com/athena/) | [🔗](clients/athena/example.py "Example for Amazon Athena") | 3 | 2026-11-17 | ✅ | ⚠️[^15] | ⚠️[^15] | ✅ | ⚠️[^15] | ➖[^3] | ⚠️[^15] | ⚠️[^15] | ⚠️[^15] | ⚠️[^15] | ❌[^15] | ➖[^15] |
| [Amazon Redshift](https://aws.amazon.com/redshift/) | [🔗](clients/redshift/example.py "Example for Amazon Redshift") | 1.0.539508 | 2026-11-17 | ✅ | ⚠️[^15] | ⚠️[^15] | ✅ | ⚠️[^15] | ➖[^3] | ⚠️[^15] | ⚠️[^15] | ⚠️[^15] | ❌[^16] | ❌[^15] | ➖[^15] |
| [Dremio Enterprise](https://www.dremio.com) | [🔗](clients/dremio/example.py "Example for Dremio Enterprise") | 26.1.12 | 2026-11-17 | ✅ | ✅ | ✅ | ✅ | ✅ | ➖[^3] | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| [Snowflake](https://www.snowflake.com) | [🔗](clients/snowflake/example.py "Example for Snowflake") | 10.42.101 | 2026-11-17 | ✅ | ✅ | ✅ | ➖[^17] | ✅ | ➖[^3] | ✅ | ✅ | ✅ | ✅ | ✅ | ➖[^6] |

### Spark services that copy into your platform

These services run a Spark job in your account on a data platform. The job reads the lakehouse through the catalog and writes a copy, such as hourly averages per device, into the platform's storage, where the platform's SQL engine and business intelligence tools query it. The copy is as current as the job's last run, and your cloud provider bills the job's compute, storage, and network traffic. The platform chooses the Spark version and, unless the setup guide adds JAR files, the Iceberg version, so a result here can differ from the Apache Spark row under Engines. Each service's folder holds its setup guide, `README.md`, next to the example.

| Client | Example | Version | Tested | Login | S3 | CDC tables | Latest tables | Schema evolution | Incremental read | Decimal | Timestamps | Nesting | Large values | Identifiers | Views | Copy |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| [Google Cloud Managed Service for Apache Spark](https://cloud.google.com/products/managed-service-for-apache-spark) | [🔗](clients/google-spark/example.py "Example for Google Cloud Managed Service for Apache Spark") | 2.3.43+iceberg.1.9.0 | 2026-11-16 | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |

### What the columns mean

| Column | Meaning |
|---|---|
| Login | Logs in to the catalog with the client ID and secret you issue for your tenant (OAuth2 client credentials) and lists the namespaces. |
| S3 | Reads data files on Amazon S3 with the short-lived storage credentials the catalog hands out, with no storage keys or region of your own. |
| CDC tables | Reads the change data capture tables (`cdc_*`), which record every change. Filters by time, by device and with NOT return counts that add up, also across a month boundary, where the data is partitioned. |
| Latest tables | Reads the `latest_inventory` tables, which hold the current state of each device. They are updated with equality and position deletes; a client that cannot apply them cannot read these tables, or returns outdated rows. |
| Schema evolution | Reads columns that were added later: older rows return NULL, newer rows their value. |
| Incremental read | Reads only the rows appended since a given snapshot, skipping compactions in between. |
| Decimal | Returns numbers, which the lakehouse stores as `decimal(38,9)`, exactly, without rounding them through floating point. |
| Timestamps | Returns timestamps as the same instant, to the millisecond, whatever the time zone of the session. |
| Nesting | Reads structured values: fields of objects nested up to 16 levels deep, and lists of objects. |
| Large values | Reads strings of up to 32,768 bytes, binary values, and tables with many columns. |
| Identifiers | Finds tables and columns under their exact names: camelCase, escaped special characters such as `compat_x002EEscaped`, dashes, and SQL reserved words. |
| Views | Runs the views in the `view_*` namespaces, which turn nested fields into plain columns, and gets the same rows as from their base tables. |
| Copy | Writes hourly averages of the measurement table into the platform's storage with the setup guide's job, and the platform's SQL engine reads back the same hours and measurement counts. |

[^1]: Iceberg Go 0.7.0, Schema evolution, Decimal, Timestamps, Large values, Identifiers: needs the `filter-after-scan` workaround: a row filter combined with a projection that leaves out a list-of-structs column ahead of the filtered column is applied to the next column (pruneParquetSchema.Struct in table/internal), here giving no row; the workaround plans the files with the filter, reads them without it and filters the rows after the scan. A fix is sketched in https://github.com/Cumulocity-IoT/iceberg-go-fixes/pull/1.
[^2]: Iceberg Rust 0.11.0 and Apache DataFusion 54.1.0: needs the `tls` workaround: iceberg-catalog-rest, the REST catalog client of Iceberg Rust, which Apache DataFusion reads Iceberg with, builds reqwest without a TLS backend, so an https catalog needs a direct reqwest 0.12 dependency with rustls-tls (apache/iceberg-rust#2888).
[^3]: Incremental read: each of these clients reads an Iceberg table at one snapshot only, such as with time travel, and has no read of the rows appended between two snapshots: Iceberg Rust 0.11.0 (apache/iceberg-rust#2152); Apache DataFusion 54.1.0 (built on Iceberg Rust, apache/iceberg-rust#2152); Apache Doris 4.1.5 (doris.apache.org/docs/lakehouse/catalogs/iceberg-catalog; Doris ignores @incr on an Iceberg table); ClickHouse 26.10.4.207 (clickhouse.com/docs/engines/table-engines/integrations/iceberg); Daft 0.7.26 (Eventual-Inc/Daft#4139); DuckDB 2.0.0 (duckdb/duckdb-iceberg#1355); StarRocks 4.1.7 (docs.starrocks.io/docs/data_source/catalog/iceberg/iceberg_timetravel); Amazon Athena 3 (docs.aws.amazon.com/athena/latest/ug/querying-iceberg-time-travel-and-version-travel-queries.html); Amazon Redshift 1.0.539508 (docs.aws.amazon.com/redshift/latest/dg/querying-iceberg.html); Dremio Enterprise 26.1.12 (docs.dremio.com/current/reference/sql/commands/SELECT-statements); Snowflake 10.42.101 (the CHANGES query fails with error 091947).
[^4]: Iceberg Rust 0.11.0, Identifiers: needs the `schema-order` workaround: Iceberg Rust relabels the selected columns by position (RecordBatchTransformer::compare_schemas) when a Parquet field name differs from its Iceberg name, as Iceberg Java writes dash-name as dash_x2Dname, so a selection in another order than the file's returns values under other columns' names; the workaround selects the columns in the table's order. A fix is sketched in https://github.com/Cumulocity-IoT/iceberg-rust-fixes/pull/1.
[^5]: PyIceberg 0.12.0, Daft 0.7.26, and Polars 2.1.0: PyIceberg raises on equality delete files and cannot read the table (apache/iceberg-python#1210). Daft plans its Iceberg reads with PyIceberg (Eventual-Inc/Daft#2458), and Polars falls back to PyIceberg's reader for the equality delete files its own planner rejects.
[^6]: Views: each of these clients loads only tables from the catalog, not Iceberg views: Apache DataFusion 54.1.0 (built on Iceberg Rust, apache/iceberg-rust#55); ClickHouse 26.10.4.207 (src/Databases/DataLake/RestCatalog.cpp); Daft 0.7.26 (docs.daft.ai/en/stable/connectors/custom-catalogs); Polars 2.1.0 (docs.pola.rs/api/python/stable/reference/api/polars.scan_iceberg.html); Snowflake 10.42.101 (docs.snowflake.com/en/user-guide/tables-iceberg-catalog-linked-database).
[^7]: Apache Doris 4.1.5, Views: Doris 4.1.5 raises "Cannot get engine-name for view" because the view versions record no engine-name, which the Iceberg view spec makes optional; apache/doris#69829 (merged to master on 2026-11-02) loads such views.
[^8]: Apache Flink 2.3.0, Timestamps: collect() raises a PickleException on TIMESTAMP_LTZ values (FLINK-38418), and to_pandas() returns 2026-11-02T10:11:12.345+05:30 for the instant 2026-11-02T10:11:12.345Z.
[^9]: Apache Flink 2.3.0, Views: Iceberg's Flink catalog refuses the view: Cannot read view view_measurement.c8y_Temperature: its default-namespace 'cdc_measurement' does not match the view's namespace 'view_measurement', and Flink resolves references against the view's own namespace.
[^10]: Daft 0.7.26, CDC tables: NOT (source = S) counts 0 of the 69120 rows of other devices, which matches Eventual-Inc/Daft#7657, a comparison under NOT rewritten onto the source bucket partition.
[^11]: DuckDB 2.0.0, Views: DuckDB runs only an Iceberg view's 'duckdb' SQL representation and these views carry DremioSQL, spark, trino and starrocks (Binder Error: Cannot query Iceberg view '"c8y_Temperature"': no SQL representation with dialect 'duckdb').
[^12]: Trino 484, Login: needs the `s3-region` workaround: Without it the catalog does not load and the server does not start: SdkClientException: Unable to load region from any of the providers in the chain DefaultAwsRegionProviderChain.
[^13]: Trino 484, S3, CDC tables, Schema evolution, Incremental read, Decimal, Timestamps, Nesting, Large values, Identifiers: needs the `case-insensitive-names` workaround: Without it: TABLE_NOT_FOUND: line 1:15: Table 'lake.cdc_measurement.c8y_temperature' does not exist
[^14]: Trino 484, Views: With the case-insensitive-names switch: INVALID_VIEW: line 1:22: Failed analyzing stored view 'lake_case_insensitive_names.view_measurement.c8y_temperature': line 10:6: Schema must be specified when session schema is not set A fix is sketched in https://github.com/Cumulocity-IoT/trino-fixes/pull/1.
[^15]: Amazon Athena 3 and Amazon Redshift 1.0.539508: AWS Glue catalog federation, through which both read the catalog, leaves out the tables whose names mix lowercase and uppercase letters, as its connection property CATALOG_CASING_FILTER takes LOWERCASE_ONLY or UPPERCASE_ONLY, and federates no views (docs.aws.amazon.com/lake-formation/latest/dg/catalog-federation.html). The checks marked ⚠️ read the same data from tables whose names have no capitals, with the `lowercase-tables` workaround. [`aws/`](aws) describes the federation.
[^16]: Amazon Redshift 1.0.539508, Large values: With the lowercase-tables switch: Expected 16384 chars and 32768 bytes of 'ä', got 8191 chars, 16382 bytes of ['ä']. Redshift reads an Iceberg string as varchar(16384) and truncates longer strings (docs.aws.amazon.com/redshift/latest/dg/querying-iceberg-supported-data-types.html).
[^17]: Snowflake 10.42.101, Latest tables: A catalog-linked database does not read equality delete files ("Row-level deletes with equality delete files aren't supported", docs.snowflake.com/en/user-guide/tables-iceberg-catalog-linked-database); the query fails with ProgrammingError: 091968 (0A000): Equality deletes on Iceberg tables are not supported.
<!-- END GENERATED TABLE -->

✅ works · ⚠️ works with a workaround, see the footnote · ❌ fails · ➖ not supported by the client ·
❔ inconclusive · empty: not tested.

The latest version of each client is re-tested every week. Older versions keep their last result and its date,
and are not re-tested; an older version is listed only where one of its cells differs from the newer version's.
A cell changes to a worse status only when a second run of the client, at the end of the same test run, confirms it.

The table is for information only and is not a statement of support; see [Disclaimer](#disclaimer).

Amazon Athena and Amazon Redshift read the lakehouse through AWS Glue catalog federation, which [`aws/`](aws) sets up
and describes.

## Connecting a client

Each folder in [`clients/`](clients) holds the example for that client, linked from the Example column of the
table, and a `README.md` where the client needs more than these catalog settings, such as a data source. Every client
uses the same catalog settings:

| Setting | Value |
|---|---|
| Catalog type | Iceberg REST |
| URI | `https://iceberg.<environment-domain>/api/catalog` |
| Warehouse | `<your-tenant-id>` |
| Credential | `<clientId>:<clientSecret>`, issued with the principals API of your tenant |
| OAuth2 token endpoint | `https://iceberg.<environment-domain>/api/catalog/v1/oauth/tokens` |
| Scope | `PRINCIPAL_ROLE:ALL` |
| Header `X-Iceberg-Access-Delegation` | `vended-credentials` |

## Running the tests

Copy `.env.example` to `.env` and fill it in; `.env` is ignored by git. The harness needs Python 3.11 or later.

```shell
python -m unittest harness.test_harness   # self-test of the harness
python -m harness.render                  # regenerate the table from results/
```

## Disclaimer

This compatibility information is for informational purposes only. It records the results of automated
tests that we run against a Cumulocity test tenant, for the client versions and on the dates shown in the table.

It is not a commitment, warranty, or certification. Cumulocity does not guarantee that a listed client works with
Cumulocity Streaming Lake Ingestion, that it keeps working with later releases of either, or that we test any
particular client or version. Listing a client is not an endorsement of, partnership with, or statement of
support for that product.

The tests cover only the checks described under "What the columns mean". They do not cover other features,
configurations, data, environments, performance, or scale, so a client can behave differently in your setup than
it does here. A ✅ means that the check passed in the run on the date shown. Test a client in your own environment
before you rely on it.

Workarounds and example code are provided as is, without support.

Product and company names are trademarks or registered trademarks of their respective owners.
