# Client contract

Every folder in `clients/` is one row group in the table and follows the same contract, so the harness can run
any client the same way.

## Files

| File | Purpose |
|---|---|
| `client.toml` | Name, section (`library`, `engine`, `product`), `built_on`, where releases are published |
| `example.*` | The snippet customers copy: connect with the catalog settings and read a few rows. Nothing else |
| `run.sh` | Entry point: `clients/<id>/run.sh <plan.json> <out.json>`, run from the repository root |
| checks (any name) | The checks below. They import or call the connection code of `example.*`, never a copy of it |

`run.sh` reads the settings from the environment (`set -a; . ./.env; set +a` locally, GitHub secrets in CI) and
installs what the client needs itself, pinned to an exact version: `uv run --script` with inline metadata for
Python, Maven for Java, Cargo for Rust.

## Rules

* **Read only.** A client never writes to Cumulocity, the catalog or the lake, by any route, DataHub included: SQL
  it sends is `SELECT` only. The catalog principal is read-only anyway. Test data reaches the lake only through the
  harness, which posts it to Cumulocity like a device. Clients keep their default telemetry: Iceberg Java's
  scan-metrics reports go to the catalog's metrics endpoint, which Polaris 1.8 only logs (reporter `default`),
  so nothing reaches the catalog's database. If the catalog ever switches to a persisting reporter, turn the
  reports off with `rest-metrics-reporting-enabled=false`.
* **No secrets in output.** Never print or write tokens, secrets or vended credentials, not even in error text you
  pass on; the harness scrubs `out.json` again, but logs it cannot.
* **Vended credentials only.** Configure no storage keys and no region, except as the named `s3-region`
  workaround. `run.sh` sources `harness/env.sh`, which loads the settings and keeps the machine's own AWS and
  Azure configuration away from the client.
* **Workarounds are named switches.** Run a check without switches first. Only if it fails, run it again with the
  switch that fixes it, and report `warn` with `"workaround": "<name>"`. A switch needed to connect at all, such
  as `tls`, is reported on `catalog-login` only; the other checks report how they behave once connected. Known
  names: `s3-region`, `token-refresh`, `case-insensitive-names`, `tls`. Everything in the README's catalog
  settings, the delegation header and the scope included, is standard configuration, never a workaround.
* **Pin what decides the result.** Pin the client and every library that reads the data (for example pyarrow)
  to exact versions, and name them in `build`.

## Checks (no fixtures needed)

The plan names the tables, the device (`source`) and a past time window `[from, to)` with a month `boundary`
inside. Every count is over rows with `from <= time < to`; the window is in the past, so counts agree across
clients.

| Column id | Check | `ok` when |
|---|---|---|
| `catalog-login` | OAuth2 client credentials with the explicit token endpoint and scope; list namespaces | the plan's namespaces are listed |
| `s3` | Read rows of the measurement table, with vended credentials only | at least one row comes back |
| `cdc-tables` | On the measurement table: `total`, `device` (`source = S`), `not_device` (`NOT (source = S)`), `before` (`time < boundary`), `after` (`time >= boundary`) | `device + not_device = total`, `before + after = total`, and `device`, `before`, `after` are all above 0 |
| `latest-tables` | On the latest table: `rows` = count of rows, `ids` = count of distinct `id` | `rows = ids` and both above 0 |
| `views` | On the view and on its base table (the measurement table): count rows with `source = S` in the window | both counts are equal and above 0 |

## Checks on the fixture data

The harness posts a fixture event for every run and waits until it is in the lake before any client runs. The
plan's `fixture` section names its tables and `run_id`, and `fixture.expect` holds every expected value. All
checks below read the run's row: the row of `fixture.table` with `runId = fixture.run_id`. These tables only
get appends, so no client needs delete support for them.

| Column id | Check | `ok` when |
|---|---|---|
| `identifiers` | The run's row of the escaped table (`plan.escaped`, column `runId`), and on the run's row of `fixture.table` the columns named in `expect.names` (camelCase, a dash, an escape, two reserved words) | the escaped table returns the row, and every column is found under its exact name with the expected value |
| `decimal` | The columns in `expect.decimals` | each equals its expected value **exactly**: compare as exact decimals (`BigDecimal`, Python `Decimal`), and convert a float exactly (`new BigDecimal(double)`, `Decimal(float)`), never through a short string form |
| `timestamps` | The column `expect.timestamp.column`, read with the session or process time zone set to `Asia/Kolkata` (UTC+05:30) | it is the instant `expect.timestamp.value`, to the millisecond |
| `nesting` | The leaf at `expect.nested.path` below `expect.nested.column`, through the client's own struct field access, and the list of structs `expect.list.column` | the leaf is `expect.nested.value`, and the list equals `expect.list.value` element by element (positions compared as exact decimals) |
| `large-values` | `expect.large.column` of the run's row, and the binary `data` of the row in `fixture.trash` with `id = fixture.event` whose `path` ends with `expect.binned.path_suffix` | the string has `chars` characters and `bytes` UTF-8 bytes, all of them `char`; the binary decodes as UTF-8 to `expect.binned.data` |
| `schema-evolution` | `expect.evolution.column` on the run's row and on the row with `runId = expect.evolution.baseline_run` | the run's row holds `expect.evolution.run`, the baseline row NULL |

For a mismatch, put the expected and the actual value in `note`, shortened to what shows the difference. The check
before the clients has already confirmed that the run's rows are in the lake, so a client that does not find them
reports `fail`, not `inconclusive`. Check the type a value comes back as, not only its text: a decimal or a
timestamp that arrives as a string or a float does not pass.

Status values:

* `ok`, and `warn` when it passes only with a workaround.
* `fail`, with one factual sentence in `note`.
* `na`: the client does not support the feature, documented upstream or stated by an explicit error of the client.
  The `note` cites the source.
* `inconclusive`: a precondition was checked and does not hold, for example a table with no snapshot. Zero rows
  alone is not enough: a table that has a snapshot but reads as empty is a `fail`.

## Output

`out.json`:

```json
{
  "client": "pyiceberg",
  "version": "0.12.0",
  "build": "pyiceberg 0.12.0, pyarrow 22.0.0",
  "checks": {
    "cdc-tables": {"status": "ok", "data": {"total": 172800, "device": 86400, "not_device": 86400, "before": 86400, "after": 86400}},
    "latest-tables": {"status": "na", "note": "PyIceberg raises on equality deletes (apache/iceberg-python#1210)"}
  }
}
```

`version` is the version of the row's client, `build` everything else that decides the result. `note` is one
factual sentence for the footnote. `data` carries the counts, so the harness can compare clients; it is not
shown in the table.
