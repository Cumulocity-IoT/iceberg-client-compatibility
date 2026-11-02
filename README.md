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

| Client | Version | Tested | Login | S3 | CDC tables | Latest tables | Identifiers | Views |
|---|---|---|---|---|---|---|---|---|
| Iceberg Java | 1.12.0 | 2026-11-02 | ✅ | ✅ | ✅ | ✅ | ❔[^1] | ➖[^2] |
| PyIceberg | 0.12.0 | 2026-11-02 | ✅ | ✅ | ✅ | ➖[^3] | ❔[^4] | ➖[^5] |

### Engines

| Client | Version | Built on | Tested | Login | S3 | CDC tables | Latest tables | Identifiers | Views |
|---|---|---|---|---|---|---|---|---|---|
| Apache DataFusion | 0.10.1 | iceberg-rust | 2026-11-02 | ⚠️[^6] | ✅ | ✅ | ✅ | ❔[^7] | ➖[^8] |
| Apache Spark | 4.1.4 | Iceberg Java | 2026-11-02 | ✅ | ✅ | ✅ | ✅ | ❔[^9] | ✅ |

### Products

| Client | Version | Built on | Tested | Login | S3 | CDC tables | Latest tables | Identifiers | Views |
|---|---|---|---|---|---|---|---|---|---|
| Cumulocity DataHub (Dremio) | 14.0.760 | Iceberg Java | 2026-11-02 | ➖[^10] | ➖[^11] | ✅ | ✅ | ❔[^12] | ✅ |

### What the columns mean

| Column | Meaning |
|---|---|
| Login | Logs in to the catalog with the client ID and secret you issue for your tenant (OAuth2 client credentials) and lists the namespaces. |
| S3 | Reads data files on Amazon S3 with the short-lived storage credentials the catalog hands out, with no storage keys or region of your own. |
| CDC tables | Reads the change data capture tables (`cdc_*`), which record every change. Filters by time, by device and with NOT return counts that add up, also across a month boundary, where the data is partitioned. |
| Latest tables | Reads the `latest_inventory` tables, which hold the current state of each device. They are updated with equality and position deletes; a client that cannot apply them cannot read these tables, or returns outdated rows. |
| Identifiers | Finds tables and columns under their exact names: camelCase, escaped special characters such as `compat_x002EEscaped`, dashes, and SQL reserved words. |
| Views | Runs the views in the `view_*` namespaces, which turn nested fields into plain columns, and gets the same rows as from their base tables. |

[^1]: Iceberg Java 1.12.0, Identifiers: The escaped table has no snapshot, so no rows could come back.
[^2]: Iceberg Java 1.12.0, Views: Iceberg Java loads the view metadata (dialects DremioSQL, spark, trino, starrocks) but has no SQL engine to run it; the view spec leaves that to engines (iceberg.apache.org/view-spec).
[^3]: PyIceberg 0.12.0, Latest tables: PyIceberg raises on equality delete files and cannot read the table (apache/iceberg-python#1210).
[^4]: PyIceberg 0.12.0, Identifiers: The table loads with the column under its exact name but has no snapshot, so there are no rows to read.
[^5]: PyIceberg 0.12.0, Views: PyIceberg loads a view's metadata and SQL text but has no engine to run it (py.iceberg.apache.org/api/#load-a-view).
[^6]: Apache DataFusion 0.10.1, Login: needs the `tls` workaround: iceberg-catalog-rest builds reqwest without a TLS backend, so an https catalog needs a direct reqwest 0.12 dependency with rustls-tls (apache/iceberg-rust#2888)
[^7]: Apache DataFusion 0.10.1, Identifiers: the escaped table has no snapshot in the catalog, so there are no rows to read
[^8]: Apache DataFusion 0.10.1, Views: iceberg-rust, which iceberg-datafusion is built on, has no Iceberg view support (apache/iceberg-rust#55)
[^9]: Apache Spark 4.1.4, Identifiers: The escaped table has no snapshot, so no rows could come back.
[^10]: Cumulocity DataHub (Dremio) 14.0.760, Login: DataHub connects to the catalog inside the platform, not through the public endpoint
[^11]: Cumulocity DataHub (Dremio) 14.0.760, S3: DataHub connects to the catalog inside the platform, not through the public endpoint
[^12]: Cumulocity DataHub (Dremio) 14.0.760, Identifiers: The escaped table has no snapshot yet, so it holds no rows to read.
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
