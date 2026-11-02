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

| Client | Version | Tested | Login | S3 | CDC tables | Latest tables | Schema evolution | Decimal | Timestamps | Nesting | Large values | Identifiers | Views |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| Iceberg Java | 1.12.0 | 2026-11-02 | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ➖[^1] |
| PyIceberg | 0.12.0 | 2026-11-02 | ✅ | ✅ | ✅ | ➖[^2] | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ➖[^3] |

### Engines

| Client | Version | Built on | Tested | Login | S3 | CDC tables | Latest tables | Schema evolution | Decimal | Timestamps | Nesting | Large values | Identifiers | Views |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| Apache DataFusion | 0.10.1 | iceberg-rust | 2026-11-02 | ⚠️[^4] | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ➖[^5] |
| Apache Spark | 4.1.4 | Iceberg Java | 2026-11-02 | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |

### Products

| Client | Version | Built on | Tested | Login | S3 | CDC tables | Latest tables | Schema evolution | Decimal | Timestamps | Nesting | Large values | Identifiers | Views |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| Cumulocity DataHub (Dremio) | 14.0.760 | Iceberg Java | 2026-11-02 | ✅[^6] | ✅[^7] | ✅ | ✅ | ✅ | ✅[^8] | ✅[^9] | ✅ | ✅ | ✅ | ✅ |

### What the columns mean

| Column | Meaning |
|---|---|
| Login | Logs in to the catalog with the client ID and secret you issue for your tenant (OAuth2 client credentials) and lists the namespaces. |
| S3 | Reads data files on Amazon S3 with the short-lived storage credentials the catalog hands out, with no storage keys or region of your own. |
| CDC tables | Reads the change data capture tables (`cdc_*`), which record every change. Filters by time, by device and with NOT return counts that add up, also across a month boundary, where the data is partitioned. |
| Latest tables | Reads the `latest_inventory` tables, which hold the current state of each device. They are updated with equality and position deletes; a client that cannot apply them cannot read these tables, or returns outdated rows. |
| Schema evolution | Reads columns that were added later: older rows return NULL, newer rows their value. |
| Decimal | Returns numbers, which the lake stores as `decimal(38,9)`, exactly, without rounding them through floating point. |
| Timestamps | Returns timestamps as the same instant, to the millisecond, whatever the time zone of the session. |
| Nesting | Reads structured values: fields of objects nested up to 16 levels deep, and lists of objects. |
| Large values | Reads strings of up to 32,768 bytes, binary values, and tables with many columns. |
| Identifiers | Finds tables and columns under their exact names: camelCase, escaped special characters such as `compat_x002EEscaped`, dashes, and SQL reserved words. |
| Views | Runs the views in the `view_*` namespaces, which turn nested fields into plain columns, and gets the same rows as from their base tables. |

[^1]: Iceberg Java 1.12.0, Views: Iceberg Java loads the view metadata (dialects DremioSQL, spark, trino, starrocks) but has no SQL engine to run it; the view spec leaves that to engines (iceberg.apache.org/view-spec).
[^2]: PyIceberg 0.12.0, Latest tables: PyIceberg raises on equality delete files and cannot read the table (apache/iceberg-python#1210).
[^3]: PyIceberg 0.12.0, Views: PyIceberg loads a view's metadata and SQL text but has no engine to run it (py.iceberg.apache.org/api/#load-a-view).
[^4]: Apache DataFusion 0.10.1, Login: needs the `tls` workaround: iceberg-catalog-rest builds reqwest without a TLS backend, so an https catalog needs a direct reqwest 0.12 dependency with rustls-tls (apache/iceberg-rust#2888)
[^5]: Apache DataFusion 0.10.1, Views: iceberg-rust, which iceberg-datafusion is built on, has no Iceberg view support (apache/iceberg-rust#55)
[^6]: Cumulocity DataHub (Dremio) 14.0.760, Login: DataHub logs in to the catalog with an OAuth2 client credential that provisioning issues for each tenant.
[^7]: Cumulocity DataHub (Dremio) 14.0.760, S3: Provisioning sets DataHub's source to use vended storage credentials.
[^8]: Cumulocity DataHub (Dremio) 14.0.760, Decimal: The REST API returns decimal(38,9) as exact JSON number literals, read here as decimals; a JSON parser that reads numbers as doubles rounds them.
[^9]: Cumulocity DataHub (Dremio) 14.0.760, Timestamps: The REST API has no session time zone, so Asia/Kolkata could not be set; it returns the instant as UTC.
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
