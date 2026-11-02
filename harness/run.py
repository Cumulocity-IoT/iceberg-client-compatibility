"""Runs one compatibility run: python -m harness.run [client ...].

Posts the run's fixture data, waits in the check before the clients until it is in the lake, runs the clients
(all in clients/ without arguments) against one plan, and stores each result scrubbed and dated in
results/<client>/<version>.json. A failed check before the clients records nothing; a client that crashes
records nothing for itself; when a compaction removed the latest table's delete files during the run, the
latest-tables results of that run are inconclusive. Any of these makes the run exit non-zero.
"""

import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

from harness.plan import plan
from harness.redact import redact
from harness.seed import fixture_plan, seed

ROOT = Path(__file__).resolve().parent.parent


def load_env():
    """The settings from .env, unless the environment already has them (as in CI)."""
    path = ROOT / ".env"
    if path.exists():
        for line in path.read_text().splitlines():
            if "=" in line and not line.lstrip().startswith("#"):
                key, value = line.split("=", 1)
                os.environ.setdefault(key.strip(), value.strip().strip('"'))


def preflight(plan_path, *flags):
    # Through env.sh like every client, so the check reads storage with vended credentials only.
    command = f'. harness/env.sh && uv run --quiet --script harness/preflight.py "{plan_path}" {" ".join(flags)}'
    return subprocess.run(["bash", "-c", command], cwd=ROOT).returncode == 0


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

    results, crashed = {}, []
    for client in clients or sorted(p.parent.name for p in (ROOT / "clients").glob("*/run.sh")):
        out = work / f"{client}.json"
        print(f"== {client}", flush=True)
        try:
            failed = subprocess.run([f"clients/{client}/run.sh", str(plan_path), str(out)], cwd=ROOT).returncode
        except OSError as error:  # a run.sh that cannot start is that client's crash, not the run's
            print(f"{client}: {error}", flush=True)
            failed = True
        if failed or not out.exists():
            crashed.append(client)
            continue
        results[client] = json.loads(redact(out.read_text()))

    compacted = not preflight(plan_path, "--after")
    for client, result in results.items():
        if compacted and "latest-tables" in result["checks"]:
            result["checks"]["latest-tables"] = {
                "status": "inconclusive", "note": "A compaction removed the delete files during the run."}
        result["date"] = now.date().isoformat()
        dest = ROOT / "results" / client / f"{result['version']}.json"
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text(json.dumps(result, indent=2) + "\n")

    if crashed or compacted:
        sys.exit("; ".join(filter(None, [
            crashed and "crashed, nothing recorded: " + ", ".join(crashed),
            compacted and "a compaction removed the delete files during the run"])))


if __name__ == "__main__":
    main(sys.argv[1:])
