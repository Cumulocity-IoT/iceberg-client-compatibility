# Iceberg client compatibility

Which Apache Iceberg clients can read a lake that Cumulocity Streaming Lake Ingestion writes, and how to
connect each of them.

The table below is generated from test runs against a Cumulocity tenant. Each run connects the way a customer
does: through the tenant's Iceberg catalog, with a read-only catalog principal and storage credentials that the
catalog vends. The tests only read; Streaming Lake Ingestion is a managed lake.

Each column of the table is one thing your client has to handle to read this lake, from logging in to the catalog
to reading tables that are updated with delete files. What each column means is listed below the table.

## Compatibility

<!-- BEGIN GENERATED TABLE -->
### Client libraries

| Client | Version | Tested | Login | S3 | CDC tables | Latest tables | Schema evolution | Incremental read | Decimal | Timestamps | Nesting | Large values | Identifiers |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| [Iceberg Go](https://go.iceberg.apache.org) | 0.7.0 | 2026-11-09 | ✅ | ✅ | ✅ | ✅ | ⚠️[^1] | ✅ | ⚠️[^1] | ⚠️[^1] | ✅ | ⚠️[^1] | ⚠️[^1] |
| [Iceberg Java](https://iceberg.apache.org) | 1.12.0 | 2026-11-09 | ✅ | ✅ | ✅ | ✅ | ✅ | ❔[^2] | ✅ | ✅ | ✅ | ✅ | ✅ |
| [Iceberg Rust](https://rust.iceberg.apache.org) | 0.11.0 | 2026-11-09 | ⚠️[^3] | ✅ | ✅ | ✅ | ✅ | ➖[^4] | ✅ | ✅ | ✅ | ✅ | ⚠️[^5] |
| [PyIceberg](https://py.iceberg.apache.org) | 0.12.0 | 2026-11-09 | ✅ | ✅ | ✅ | ➖[^6] | ✅ | ❔[^7] | ✅ | ✅ | ✅ | ✅ | ✅ |

### Engines

| Client | Version | Tested | Login | S3 | CDC tables | Latest tables | Schema evolution | Incremental read | Decimal | Timestamps | Nesting | Large values | Identifiers | Views |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| [Apache DataFusion](https://datafusion.apache.org) | 54.1.0 | 2026-11-09 | ⚠️[^8] | ✅ | ✅ | ✅ | ✅ | ➖[^9] | ✅ | ✅ | ✅ | ✅ | ✅ | ➖[^10] |
| [Apache Doris](https://doris.apache.org) | 4.1.5 | 2026-11-09 | ✅ | ✅ | ✅ | ✅ | ✅ | ❔[^11] | ✅ | ✅[^12] | ✅ | ✅ | ✅ | ❌[^13] |
| [Apache Flink](https://flink.apache.org) | 2.3.0 | 2026-11-09 | ✅ | ✅ | ✅ | ✅ | ✅ | ❔[^14] | ✅ | ❌[^15] | ✅ | ✅ | ✅ | ❌[^16] |
| [Apache Spark](https://spark.apache.org) | 4.1.4 | 2026-11-09 | ✅ | ✅ | ✅ | ✅ | ✅ | ❔[^17] | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| [ClickHouse](https://clickhouse.com) | 26.10.2.103 | 2026-11-09 | ✅ | ✅ | ✅ | ✅ | ✅ | ➖[^18] | ✅ | ✅ | ✅ | ✅ | ✅ | ➖[^19] |
| [Daft](https://www.daft.ai) | 0.7.26 | 2026-11-09 | ✅ | ✅ | ❌[^20] | ➖[^21] | ✅ | ➖[^22] | ✅ | ✅ | ✅ | ✅ | ✅ | ➖[^23] |
| [DuckDB](https://duckdb.org) | 2.0.0 | 2026-11-09 | ✅ | ✅ | ✅ | ✅ | ✅ | ❔[^24] | ✅ | ✅ | ✅ | ✅ | ✅ | ❌[^25] |
| [Polars](https://pola.rs) | 2.1.0 | 2026-11-09 | ✅ | ✅ | ✅ | ➖[^26] | ✅ | ❔[^27] | ✅ | ✅ | ✅ | ✅ | ✅ | ➖[^28] |
| [StarRocks](https://www.starrocks.io) | 4.1.7 | 2026-11-09 | ✅ | ✅ | ✅ | ✅ | ✅ | ➖[^29] | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| [Trino](https://trino.io) | 484 | 2026-11-09 | ⚠️[^30] | ⚠️[^31] | ⚠️[^31] | ✅ | ⚠️[^31] | ❔[^32] | ⚠️[^31] | ⚠️[^31] | ⚠️[^31] | ⚠️[^31] | ⚠️[^31] | ❌[^33] |

### Products

| Client | Version | Tested | Login | S3 | CDC tables | Latest tables | Schema evolution | Incremental read | Decimal | Timestamps | Nesting | Large values | Identifiers | Views |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| [Dremio Enterprise](https://www.dremio.com) | 26.1.12 | 2026-11-09 | ✅[^34] | ✅[^34] | ❌[^35] | ❌[^35] | ❌[^35] | ➖[^36] | ❌[^35] | ❌[^35] | ❌[^35] | ❌[^35] | ❌[^35] | ❌[^35] |
| [Snowflake](https://www.snowflake.com) | 10.40.103 | 2026-11-09 | ✅[^37] | ✅[^37] | ✅ | ➖[^38] | ✅ | ➖[^39] | ✅ | ✅ | ✅ | ✅ | ✅ | ➖[^40] |

### What the columns mean

| Column | Meaning |
|---|---|
| Login | Logs in to the catalog with the client ID and secret you issue for your tenant (OAuth2 client credentials) and lists the namespaces. |
| S3 | Reads data files on Amazon S3 with the short-lived storage credentials the catalog hands out, with no storage keys or region of your own. |
| CDC tables | Reads the change data capture tables (`cdc_*`), which record every change. Filters by time, by device and with NOT return counts that add up, also across a month boundary, where the data is partitioned. |
| Latest tables | Reads the `latest_inventory` tables, which hold the current state of each device. They are updated with equality and position deletes; a client that cannot apply them cannot read these tables, or returns outdated rows. |
| Schema evolution | Reads columns that were added later: older rows return NULL, newer rows their value. |
| Incremental read | Reads only the rows appended since a given snapshot, skipping compactions in between. |
| Decimal | Returns numbers, which the lake stores as `decimal(38,9)`, exactly, without rounding them through floating point. |
| Timestamps | Returns timestamps as the same instant, to the millisecond, whatever the time zone of the session. |
| Nesting | Reads structured values: fields of objects nested up to 16 levels deep, and lists of objects. |
| Large values | Reads strings of up to 32,768 bytes, binary values, and tables with many columns. |
| Identifiers | Finds tables and columns under their exact names: camelCase, escaped special characters such as `compat_x002EEscaped`, dashes, and SQL reserved words. |
| Views | Runs the views in the `view_*` namespaces, which turn nested fields into plain columns, and gets the same rows as from their base tables. |

[^1]: Iceberg Go 0.7.0, Schema evolution, Decimal, Timestamps, Large values, Identifiers: needs the `filter-after-scan` workaround: a row filter combined with a projection that leaves out a list-of-structs column ahead of the filtered column is applied to the next column (pruneParquetSchema.Struct in table/internal), here giving no row; the workaround plans the files with the filter, reads them without it and filters the rows after the scan. A fix is sketched in https://github.com/Cumulocity-IoT/iceberg-go-fixes/pull/1.
[^2]: Iceberg Java 1.12.0, Incremental read: The fixture table has no snapshot before fixture.posted or no append after it.
[^3]: Iceberg Rust 0.11.0, Login: needs the `tls` workaround: iceberg-catalog-rest builds reqwest without a TLS backend, so an https catalog needs a direct reqwest 0.12 dependency with rustls-tls (apache/iceberg-rust#2888)
[^4]: Iceberg Rust 0.11.0, Incremental read: Iceberg Rust scans one snapshot only (TableScanBuilder::snapshot_id) and has no incremental append scan; reads between snapshot ids are open in apache/iceberg-rust#2152 (PR #2997)
[^5]: Iceberg Rust 0.11.0, Identifiers: needs the `schema-order` workaround: Iceberg Rust relabels the selected columns by position (RecordBatchTransformer::compare_schemas) when a Parquet field name differs from its Iceberg name, as Iceberg Java writes dash-name as dash_x2Dname, so a selection in another order than the file's returns values under other columns' names; the workaround selects the columns in the table's order. A fix is sketched in https://github.com/Cumulocity-IoT/iceberg-rust-fixes/pull/1.
[^6]: PyIceberg 0.12.0, Latest tables: PyIceberg raises on equality delete files and cannot read the table (apache/iceberg-python#1210).
[^7]: PyIceberg 0.12.0, Incremental read: The fixture table has no snapshot before fixture.posted or no append after it.
[^8]: Apache DataFusion 54.1.0, Login: needs the `tls` workaround: iceberg-catalog-rest builds reqwest without a TLS backend, so an https catalog needs a direct reqwest 0.12 dependency with rustls-tls (apache/iceberg-rust#2888)
[^9]: Apache DataFusion 54.1.0, Incremental read: iceberg-rust, which iceberg-datafusion is built on, scans one snapshot only (TableScanBuilder::snapshot_id) and has no incremental append scan; reads between snapshot ids are open in apache/iceberg-rust#2152 (PR #2997)
[^10]: Apache DataFusion 54.1.0, Views: iceberg-rust, which iceberg-datafusion is built on, has no Iceberg view support (apache/iceberg-rust#55)
[^11]: Apache Doris 4.1.5, Incremental read: The fixture table has no snapshot before fixture.posted or no append after it.
[^12]: Apache Doris 4.1.5, Timestamps: The MySQL protocol carries TIMESTAMPTZ(6) as text in the session time zone, so the instant is read as epoch milliseconds through MILLISECOND_TIMESTAMP.
[^13]: Apache Doris 4.1.5, Views: Doris 4.1.5 raises "Cannot get engine-name for view" because the view versions record no engine-name, which the Iceberg view spec makes optional; apache/doris#69829 (merged to master on 2026-11-02) loads such views.
[^14]: Apache Flink 2.3.0, Incremental read: The fixture table has no snapshot before the fixture was posted, or no append after it.
[^15]: Apache Flink 2.3.0, Timestamps: collect() raises a PickleException on TIMESTAMP_LTZ values (FLINK-38418), and to_pandas() returns 2026-11-02T10:11:12.345+05:30 for the instant 2026-11-02T10:11:12.345Z.
[^16]: Apache Flink 2.3.0, Views: Iceberg's Flink catalog refuses the view: Cannot read view view_measurement.c8y_Temperature: its default-namespace 'cdc_measurement' does not match the view's namespace 'view_measurement', and Flink resolves references against the view's own namespace.
[^17]: Apache Spark 4.1.4, Incremental read: The fixture table has no snapshot before fixture.posted or no append after it.
[^18]: ClickHouse 26.10.2.103, Incremental read: ClickHouse reads an Iceberg table at one snapshot (iceberg_snapshot_id, iceberg_timestamp_ms) and has no incremental read between snapshots (clickhouse.com/docs/engines/table-engines/integrations/iceberg).
[^19]: ClickHouse 26.10.2.103, Views: ClickHouse's Iceberg REST catalog loads only tables, never views (src/Databases/DataLake/RestCatalog.cpp), so the view is an unknown table (UNKNOWN_TABLE).
[^20]: Daft 0.7.26, CDC tables: NOT (source = S) counts 0 of the 69120 rows of other devices, which matches Eventual-Inc/Daft#7657, a comparison under NOT rewritten onto the source bucket partition.
[^21]: Daft 0.7.26, Latest tables: Daft plans Iceberg reads with PyIceberg, which raises on equality delete files; Daft applies position deletes only (Eventual-Inc/Daft#2458, equality deletes blocked by PyIceberg).
[^22]: Daft 0.7.26, Incremental read: Daft's read_iceberg reads one snapshot, branch or tag and has no snapshot-range read (Eventual-Inc/Daft#4139, open).
[^23]: Daft 0.7.26, Views: Daft's catalogs resolve only tables and namespaces, and its Iceberg catalog loads names with PyIceberg's load_table, so the view is not found (docs.daft.ai/en/stable/connectors/custom-catalogs): InvalidSQLException: Table not found: lake.view_measurement.c8y_Temperature
[^24]: DuckDB 2.0.0, Incremental read: The fixture table has no snapshot before or no append snapshot after the fixture was posted.
[^25]: DuckDB 2.0.0, Views: DuckDB runs only an Iceberg view's 'duckdb' SQL representation and these views carry DremioSQL, spark, trino and starrocks (Binder Error: Cannot query Iceberg view '"c8y_Temperature"': no SQL representation with dialect 'duckdb').
[^26]: Polars 2.1.0, Latest tables: The polars-iceberg planner rejects equality delete files and PyIceberg's reader, the fallback, raises 'PyIceberg does not yet support equality deletes' (apache/iceberg-python#1210).
[^27]: Polars 2.1.0, Incremental read: The fixture table has no snapshot before 2026-11-09T06:32:50.772573+00:00 or no append after it.
[^28]: Polars 2.1.0, Views: Polars reads Iceberg tables only: scan_iceberg and SQLContext.register_catalog load a name with load_table, which raises NoSuchTableError for the view (docs.pola.rs/api/python/stable/reference/api/polars.scan_iceberg.html).
[^29]: StarRocks 4.1.7, Incremental read: StarRocks SQL reads an Iceberg table at one snapshot only (VERSION AS OF, TIMESTAMP AS OF) and has no clause or table function for the rows appended between two snapshots (docs.starrocks.io/docs/data_source/catalog/iceberg/iceberg_timetravel).
[^30]: Trino 484, Login: needs the `s3-region` workaround: Without it the catalog does not load and the server does not start: SdkClientException: Unable to load region from any of the providers in the chain DefaultAwsRegionProviderChain.
[^31]: Trino 484, S3, CDC tables, Schema evolution, Decimal, Timestamps, Nesting, Large values, Identifiers: needs the `case-insensitive-names` workaround: Without it: TABLE_NOT_FOUND: line 1:15: Table 'lake.cdc_measurement.c8y_temperature' does not exist
[^32]: Trino 484, Incremental read: With the case-insensitive-names switch: The table has no snapshot before the fixture was posted, or no append after it.
[^33]: Trino 484, Views: With the case-insensitive-names switch: INVALID_VIEW: line 1:22: Failed analyzing stored view 'lake_case_insensitive_names.view_measurement.c8y_temperature': line 10:6: Schema must be specified when session schema is not set A fix is sketched in https://github.com/Cumulocity-IoT/trino-fixes/pull/1.
[^34]: Dremio Enterprise 26.1.12, Login, S3: A Dremio data source was provisioned with the OAuth client credential and credential vending.
[^35]: Dremio Enterprise 26.1.12, CDC tables, Latest tables, Schema evolution, Decimal, Timestamps, Nesting, Large values, Identifiers, Views: HTTPError: HTTP Error 400: Bad Request
[^36]: Dremio Enterprise 26.1.12, Incremental read: Dremio SQL reads an Iceberg table as of one snapshot only (AT SNAPSHOT, AT <timestamp>) and has no clause or table function for the rows appended between two snapshots (docs.dremio.com/current/reference/sql/commands/SELECT-statements).
[^37]: Snowflake 10.40.103, Login, S3: A Snowflake catalog integration was provisioned with the OAuth client credential and credential vending.
[^38]: Snowflake 10.40.103, Latest tables: A catalog-linked database does not read equality delete files ("Row-level deletes with equality delete files aren't supported", docs.snowflake.com/en/user-guide/tables-iceberg-catalog-linked-database); the query fails with ProgrammingError: 091968 (0A000): Equality deletes on Iceberg tables are not supported.
[^39]: Snowflake 10.40.103, Incremental read: Snowflake has no incremental read of an Iceberg table in a catalog-linked database; the CHANGES query fails with ProgrammingError: 091947 (0A000): CHANGES clause is not supported on external tables or Iceberg tables with an external catalog integration, saw 'compat_Fixture'.
[^40]: Snowflake 10.40.103, Views: A catalog-linked database syncs the remote namespaces as schemas and their Iceberg tables, not Iceberg views (docs.snowflake.com/en/user-guide/tables-iceberg-catalog-linked-database); the query fails with ProgrammingError: 002003 (42S02): SQL compilation error: Object 'T2027824580."view_measurement"."c8y_Temperature"' does not exist or not authorized. Your primary role COMPAT_READER or one of your secondary roles must have at least one privilege granted on TABLE T2027824580."view_measurement"."c8y_Temperature".
<!-- END GENERATED TABLE -->

✅ works · ⚠️ works with a workaround, see the footnote · ❌ fails · ➖ not supported by the client ·
❔ inconclusive · empty: not tested.

The latest version of each client is re-tested every week. Older versions keep their last result and its date,
and are not re-tested; an older version is listed only where one of its cells differs from the newer version's.

The table is for information only and is not a statement of support; see [Disclaimer](#disclaimer).

## Connecting a client

Each folder in [`clients/`](clients) holds an example for that client. The tests run the same connection code, so
an example that is listed as working has worked in the last run. Every client uses the same catalog settings:

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

This compatibility information is for informational purposes only. It records the results of automated read
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
