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

| Client | Version | Tested | Login | S3 | CDC tables | Latest tables | Schema evolution | Incremental read | Decimal | Timestamps | Nesting | Large values | Identifiers | Views |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| Iceberg Go | 0.7.0 | 2026-11-02 | ✅ | ✅ | ✅ | ✅ | ⚠️[^1] | ✅ | ⚠️[^1] | ⚠️[^1] | ✅ | ⚠️[^1] | ⚠️[^1] | ➖[^2] |
| Iceberg Java | 1.12.0 | 2026-11-02 | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ➖[^3] |
| PyIceberg | 0.12.0 | 2026-11-02 | ✅ | ✅ | ✅ | ➖[^4] | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ➖[^5] |

### Engines

| Client | Version | Tested | Login | S3 | CDC tables | Latest tables | Schema evolution | Incremental read | Decimal | Timestamps | Nesting | Large values | Identifiers | Views |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| Apache DataFusion | 0.10.1 | 2026-11-02 | ⚠️[^6] | ✅ | ✅ | ✅ | ✅ | ➖[^7] | ✅ | ✅ | ✅ | ✅ | ✅ | ➖[^8] |
| Apache Doris | 4.1.5 | 2026-11-02 | ✅ | ✅ | ✅ | ✅ | ✅ | ➖[^9] | ✅ | ✅[^10] | ✅ | ✅ | ✅ | ❌[^11] |
| Apache Flink | 2.3.0 | 2026-11-02 | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌[^12] | ✅ | ✅ | ✅ | ❌[^13] |
| Apache Spark | 4.1.4 | 2026-11-02 | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| ClickHouse | 26.10.2.103 | 2026-11-02 | ✅[^14] | ✅ | ✅ | ✅ | ✅ | ➖[^15] | ✅ | ✅ | ✅ | ✅ | ✅ | ➖[^16] |
| Daft | 0.7.26 | 2026-11-02 | ✅ | ✅ | ❌[^17] | ➖[^18] | ✅ | ➖[^19] | ✅ | ✅ | ✅ | ✅ | ✅ | ➖[^20] |
| DuckDB | 2.0.0 | 2026-11-02 | ✅ | ✅ | ✅ | ✅ | ✅ | ➖[^21] | ✅ | ✅ | ✅ | ✅ | ✅ | ❌[^22] |
| Polars | 2.1.0 | 2026-11-02 | ✅ | ✅ | ✅ | ➖[^23] | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ➖[^24] |
| StarRocks | 4.1.7 | 2026-11-02 | ✅ | ✅ | ✅ | ✅ | ✅ | ➖[^25] | ✅ | ✅ | ✅ | ✅ | ✅ | ✅[^26] |
| Trino | 484 | 2026-11-02 | ⚠️[^27] | ⚠️[^28] | ⚠️[^28] | ✅ | ⚠️[^28] | ⚠️[^28] | ⚠️[^28] | ⚠️[^28] | ⚠️[^28] | ⚠️[^28] | ⚠️[^28] | ❌[^29] |

### Products

| Client | Version | Tested | Login | S3 | CDC tables | Latest tables | Schema evolution | Incremental read | Decimal | Timestamps | Nesting | Large values | Identifiers | Views |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| Dremio Enterprise | 26.1.12 | 2026-11-02 | ✅[^30] | ✅[^31] | ✅ | ✅ | ✅ | ➖[^32] | ✅[^33] | ✅[^34] | ✅ | ✅ | ✅ | ✅ |

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

[^1]: Iceberg Go 0.7.0, Schema evolution, Decimal, Timestamps, Large values, Identifiers: needs the `filter-after-scan` workaround: a row filter combined with a projection that leaves out a list-of-structs column ahead of the filtered column is applied to the next column (pruneParquetSchema.Struct in table/internal), here giving the error "invalid"; the workaround plans the files with the filter, reads them without it and filters the rows after the scan.
[^2]: Iceberg Go 0.7.0, Views: Iceberg Go loads the view metadata (dialects DremioSQL, spark, trino, starrocks) but has no SQL engine to run it; the view spec leaves that to engines (iceberg.apache.org/view-spec).
[^3]: Iceberg Java 1.12.0, Views: Iceberg Java loads the view metadata (dialects DremioSQL, spark, trino, starrocks) but has no SQL engine to run it; the view spec leaves that to engines (iceberg.apache.org/view-spec).
[^4]: PyIceberg 0.12.0, Latest tables: PyIceberg raises on equality delete files and cannot read the table (apache/iceberg-python#1210).
[^5]: PyIceberg 0.12.0, Views: PyIceberg loads a view's metadata and SQL text but has no engine to run it (py.iceberg.apache.org/api/#load-a-view).
[^6]: Apache DataFusion 0.10.1, Login: needs the `tls` workaround: iceberg-catalog-rest builds reqwest without a TLS backend, so an https catalog needs a direct reqwest 0.12 dependency with rustls-tls (apache/iceberg-rust#2888)
[^7]: Apache DataFusion 0.10.1, Incremental read: iceberg-rust, which iceberg-datafusion is built on, scans one snapshot only (TableScanBuilder::snapshot_id) and has no incremental append scan; reads between snapshot ids are open in apache/iceberg-rust#2152 (PR #2997)
[^8]: Apache DataFusion 0.10.1, Views: iceberg-rust, which iceberg-datafusion is built on, has no Iceberg view support (apache/iceberg-rust#55)
[^9]: Apache Doris 4.1.5, Incremental read: Doris documents @incr incremental reads for Hudi and Paimon tables, not for Iceberg tables (doris.apache.org/docs/lakehouse/catalogs/iceberg-catalog); on an Iceberg table Doris accepts @incr and ignores it, returning all 8 rows of the table.
[^10]: Apache Doris 4.1.5, Timestamps: The MySQL protocol carries TIMESTAMPTZ(6) as text in the session time zone, so the instant is read as epoch milliseconds through MILLISECOND_TIMESTAMP.
[^11]: Apache Doris 4.1.5, Views: Doris 4.1.5 raises "Cannot get engine-name for view" because the view versions record no engine-name, which the Iceberg view spec makes optional; apache/doris#69829 (merged to master on 2026-11-02) loads such views.
[^12]: Apache Flink 2.3.0, Timestamps: collect() raises a PickleException on TIMESTAMP_LTZ values (FLINK-38418), and to_pandas() returns 2026-11-02T10:11:12.345+05:30 for the instant 2026-11-02T10:11:12.345Z.
[^13]: Apache Flink 2.3.0, Views: Iceberg's Flink catalog refuses the view: Cannot read view view_measurement.c8y_Temperature: its default-namespace 'cdc_measurement' does not match the view's namespace 'view_measurement', and Flink resolves references against the view's own namespace.
[^14]: ClickHouse 26.10.2.103, Login: ClickHouse lists the tables of each namespace, not the namespaces, so view_measurement, which holds only a view, is not listed.
[^15]: ClickHouse 26.10.2.103, Incremental read: ClickHouse reads an Iceberg table at one snapshot (iceberg_snapshot_id, iceberg_timestamp_ms) and has no incremental read between snapshots (clickhouse.com/docs/engines/table-engines/integrations/iceberg).
[^16]: ClickHouse 26.10.2.103, Views: ClickHouse's Iceberg REST catalog loads only tables, never views (src/Databases/DataLake/RestCatalog.cpp), so the view is an unknown table (UNKNOWN_TABLE).
[^17]: Daft 0.7.26, CDC tables: NOT (source = S) counts 0 of the 69120 rows of other devices, which matches Eventual-Inc/Daft#7657, a comparison under NOT rewritten onto the source bucket partition.
[^18]: Daft 0.7.26, Latest tables: Daft plans Iceberg reads with PyIceberg, which raises on equality delete files; Daft applies position deletes only (Eventual-Inc/Daft#2458, equality deletes blocked by PyIceberg).
[^19]: Daft 0.7.26, Incremental read: Daft's read_iceberg reads one snapshot, branch or tag and has no snapshot-range read (Eventual-Inc/Daft#4139, open).
[^20]: Daft 0.7.26, Views: Daft's catalogs resolve only tables and namespaces, and its Iceberg catalog loads names with PyIceberg's load_table, so the view is not found (docs.daft.ai/en/stable/connectors/custom-catalogs): InvalidSQLException: Table not found: lake.view_measurement.c8y_Temperature
[^21]: DuckDB 2.0.0, Incremental read: iceberg_scan reads one snapshot and has no start/end snapshot range (Binder Error: Invalid named parameter "start_snapshot_id" for function iceberg_scan); incremental reads are an open request (duckdb/duckdb-iceberg#1355).
[^22]: DuckDB 2.0.0, Views: DuckDB runs only an Iceberg view's 'duckdb' SQL representation and these views carry DremioSQL, spark, trino and starrocks (Binder Error: Cannot query Iceberg view '"c8y_Temperature"': no SQL representation with dialect 'duckdb').
[^23]: Polars 2.1.0, Latest tables: The polars-iceberg planner rejects equality delete files and PyIceberg's reader, the fallback, raises 'PyIceberg does not yet support equality deletes' (apache/iceberg-python#1210).
[^24]: Polars 2.1.0, Views: Polars reads Iceberg tables only: scan_iceberg and SQLContext.register_catalog load a name with load_table, which raises NoSuchTableError for the view (docs.pola.rs/api/python/stable/reference/api/polars.scan_iceberg.html).
[^25]: StarRocks 4.1.7, Incremental read: StarRocks SQL reads an Iceberg table at one snapshot only (VERSION AS OF, TIMESTAMP AS OF) and has no clause or table function for the rows appended between two snapshots (docs.starrocks.io/docs/data_source/catalog/iceberg/iceberg_timetravel).
[^26]: StarRocks 4.1.7, Views: StarRocks runs the view's starrocks SQL representation.
[^27]: Trino 484, Login: needs the `s3-region` workaround: Without it the catalog does not load and the server does not start: SdkClientException: Unable to load region from any of the providers in the chain DefaultAwsRegionProviderChain.
[^28]: Trino 484, S3, CDC tables, Schema evolution, Incremental read, Decimal, Timestamps, Nesting, Large values, Identifiers: needs the `case-insensitive-names` workaround: Without it: TABLE_NOT_FOUND: line 1:15: Table 'lake.cdc_measurement.c8y_temperature' does not exist
[^29]: Trino 484, Views: With the case-insensitive-names switch: INVALID_VIEW: line 1:22: Failed analyzing stored view 'lake_case_insensitive_names.view_measurement.c8y_temperature': line 10:6: Schema must be specified when session schema is not set
[^30]: Dremio Enterprise 26.1.12, Login: Tested through Cumulocity DataHub, whose provisioning gives the Dremio source an OAuth2 client credential for the catalog.
[^31]: Dremio Enterprise 26.1.12, S3: Tested through Cumulocity DataHub, whose provisioning sets the Dremio source to use vended storage credentials.
[^32]: Dremio Enterprise 26.1.12, Incremental read: Dremio SQL reads an Iceberg table as of one snapshot only (AT SNAPSHOT, AT <timestamp>) and has no clause or table function for the rows appended between two snapshots (docs.dremio.com/current/reference/sql/commands/SELECT-statements).
[^33]: Dremio Enterprise 26.1.12, Decimal: The REST API returns decimal(38,9) as exact JSON number literals, read here as decimals; a JSON parser that reads numbers as doubles rounds them.
[^34]: Dremio Enterprise 26.1.12, Timestamps: The REST API has no session time zone, so Asia/Kolkata could not be set; it returns the instant as UTC.
<!-- END GENERATED TABLE -->

✅ works · ⚠️ works with a workaround, see the footnote · ❌ fails · ➖ not supported by the client ·
❔ inconclusive · empty: not tested.

The latest version of each client is re-tested every week. Older versions keep their last result and its date,
and are not re-tested.

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
