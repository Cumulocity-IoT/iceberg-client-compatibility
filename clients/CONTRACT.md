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

* **Read only.** The catalog principal is read-only anyway. Never write to Cumulocity, except DataHub SQL queries.
* **No secrets in output.** Never print or write tokens, secrets or vended credentials, not even in error text you
  pass on; the harness scrubs `out.json` again, but logs it cannot.
* **Vended credentials only.** Configure no storage keys and no region, except as the named `s3-region`
  workaround.
* **Workarounds are named switches.** Run a check without switches first. Only if it fails, run it again with the
  switch that fixes it, and report `warn` with `"workaround": "<name>"`. Known names: `s3-region`,
  `token-refresh`, `delegation-header`, `case-insensitive-names`.

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
| `identifiers` | Read the escaped table and its camelCase column | rows come back and the column is found under its exact name |
| `views` | On the view and on its base table (the measurement table): count rows with `source = S` in the window | both counts are equal and above 0 |

Status values: `ok`, `warn` (passes only with a workaround), `fail`, `na` (the client does not support the
feature by design and documents that upstream; say where in `note`), `inconclusive` (a precondition did not
hold, for example no rows on one side of the boundary).

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
