"""Runs one compatibility run: python -m harness.run [client ...].

Posts the run's fixture data, waits in the check before the clients until it is in the lake, runs the clients
(without arguments, all in clients/ except the Spark services of the section managed-spark, which run in a workflow
of their own) against one plan, and stores each result scrubbed and dated in
results/<client>/<version>.json. A failed check before the clients records nothing.

A client that crashes, or whose result is worse in a column than its last recorded result, runs a second time after
all others. The second result is recorded when no column of it is worse than in the first; when one is, or when the
second run crashes, nothing is recorded and the client keeps its last result. The second runs are skipped, and those
clients record nothing, when the check after the first runs fails or more than half of the clients need one: the
lake or the network is then at fault, not single clients.

When the check after a client's runs fails, because a compaction removed the latest table's delete files or the
catalog did not answer, that client's latest-tables result is inconclusive. Any of these makes the run exit non-zero.
"""

import json
import os
import subprocess
import sys
import time
import tomllib
from datetime import datetime, timezone
from pathlib import Path

from harness.plan import plan
from harness.redact import redact
from harness.render import version_key
from harness.seed import fixture_plan, seed

ROOT = Path(__file__).resolve().parent.parent
# How good a status is: a column whose status gets worse is a regression.
RANK = {"ok": 0, "warn": 1, "inconclusive": 2, "na": 2, "fail": 3}
# The least time between a client's two runs, so that a short outage of the platform under test can end.
GAP_SECONDS = 300


def load_env():
    """The settings from .env, unless the environment already has them (as in CI)."""
    path = ROOT / ".env"
    if path.exists():
        for line in path.read_text().splitlines():
            if "=" in line and not line.lstrip().startswith("#"):
                key, value = line.split("=", 1)
                os.environ.setdefault(key.strip(), value.strip().strip('"'))


def default_clients(root=ROOT):
    """The clients of a run without arguments: every one with a run.sh, except the Spark services."""
    return [p.parent.name for p in sorted((root / "clients").glob("*/run.sh"))
            if tomllib.loads((p.parent / "client.toml").read_text()).get("section") != "managed-spark"]


def preflight(plan_path, *flags):
    # Through env.sh like every client, so the check reads storage with vended credentials only.
    command = f'. harness/env.sh && uv run --quiet --script harness/preflight.py "{plan_path}" {" ".join(flags)}'
    return subprocess.run(["bash", "-c", command], cwd=ROOT).returncode == 0


def previous(client, version, root=None):
    """The recorded result a new one replaces: the same version, else the client's highest version; None if none."""
    path = (root or ROOT) / "results" / client / f"{version}.json"
    if not path.exists():
        recorded = sorted(path.parent.glob("*.json"), key=lambda p: version_key(p.stem))
        path = recorded[-1] if recorded else None
    return json.loads(path.read_text()) if path else None


def regressions(result, before):
    """The columns in which this result is worse than the recorded one; a column new to the client is none."""
    if not before:
        return set()
    old = {column: check["status"] for column, check in before["checks"].items()}
    return {column for column, check in result["checks"].items()
            if column in old and RANK[check["status"]] > RANK[old[column]]}


def unconfirmed(first, second, before):
    """The regressions of the second run that the first run did not show at least as badly."""
    status = lambda result, column: result["checks"].get(column, {}).get("status", "ok") if result else "ok"
    return {column for column in regressions(second, before)
            if RANK[second["checks"][column]["status"]] > RANK[status(first, column)]}


def run_client(client, plan_path, out):
    """Runs one client and returns its scrubbed result, or None when it crashed."""
    print(f"== {client}", flush=True)
    try:
        failed = subprocess.run([f"clients/{client}/run.sh", str(plan_path), str(out)], cwd=ROOT).returncode
    except OSError as error:  # a run.sh that cannot start is that client's crash, not the run's
        print(f"{client}: {error}", flush=True)
        failed = True
    if failed or not out.exists():
        return None
    return json.loads(redact(out.read_text()))


def main(clients):
    load_env()
    now = datetime.now(timezone.utc).replace(microsecond=0)
    run_id = "run-" + now.strftime("%Y%m%dT%H%M%SZ")
    work = ROOT / "target" / run_id
    work.mkdir(parents=True)
    run_plan = plan(now, run_id)
    run_plan["fixture"] = fixture_plan(seed(run_id))
    plan_path = work / "plan.json"
    plan_path.write_text(json.dumps(run_plan, indent=2))

    print("== check before the clients", flush=True)
    if not preflight(plan_path):
        sys.exit("the check before the clients failed; nothing recorded")

    clients = clients or default_clients()
    results, ended = {}, {}
    for client in clients:
        result = run_client(client, plan_path, work / f"{client}.json")
        ended[client] = time.monotonic()
        if result is not None:
            results[client] = result

    retry = [c for c in clients if c not in results or regressions(results[c], previous(c, results[c]["version"]))]
    clean_first = preflight(plan_path, "--after")
    crashed, doubtful, second = [], [], set()
    if retry and (not clean_first or len(retry) > len(clients) / 2):
        print(f"== second runs skipped, nothing recorded for: {', '.join(retry)}", flush=True)
        doubtful = [f"{c} (second run skipped)" for c in retry]
        for client in retry:
            results.pop(client, None)
    else:
        for client in retry:
            time.sleep(max(0.0, GAP_SECONDS - (time.monotonic() - ended[client])))
            first = results.pop(client, None)
            again = run_client(client, plan_path, work / f"{client}.second.json")
            second.add(client)
            if again is None:
                crashed.append(client)
            elif columns := unconfirmed(first, again, previous(client, again["version"])):
                doubtful.append(f"{client} ({', '.join(sorted(columns))})")
            else:
                results[client] = again
    clean_second = preflight(plan_path, "--after") if second else clean_first

    for client, result in results.items():
        if not (clean_second if client in second else clean_first) and "latest-tables" in result["checks"]:
            result["checks"]["latest-tables"] = {
                "status": "inconclusive", "note": "A compaction removed the delete files during the run."}
        result["date"] = now.date().isoformat()
        dest = ROOT / "results" / client / f"{result['version']}.json"
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text(json.dumps(result, indent=2) + "\n")

    after_failed = not clean_first or not clean_second
    if crashed or doubtful or after_failed:
        sys.exit("; ".join(filter(None, [
            crashed and "crashed in the second run, nothing recorded: " + ", ".join(crashed),
            doubtful and "worse in a run without confirmation, nothing recorded: " + ", ".join(doubtful),
            after_failed and "the check after the clients failed: a compaction or the catalog"])))


if __name__ == "__main__":
    main(sys.argv[1:])
