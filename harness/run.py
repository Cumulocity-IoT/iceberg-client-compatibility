"""Runs clients against one plan and stores their results: python -m harness.run [client ...].

Without arguments every client in clients/ runs. Each result is scrubbed, dated and written to
results/<client>/<version>.json; a client that crashes records nothing, and the run exits non-zero.
"""

import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

from harness.plan import plan
from harness.redact import redact

ROOT = Path(__file__).resolve().parent.parent


def main(clients):
    now = datetime.now(timezone.utc).replace(microsecond=0)
    work = ROOT / "target" / now.strftime("%Y%m%dT%H%M%SZ")
    work.mkdir(parents=True)
    plan_path = work / "plan.json"
    plan_path.write_text(json.dumps(plan(now, work.name), indent=2))

    crashed = []
    for client in clients or sorted(p.parent.name for p in (ROOT / "clients").glob("*/run.sh")):
        out = work / f"{client}.json"
        print(f"== {client}", flush=True)
        if subprocess.run([f"clients/{client}/run.sh", str(plan_path), str(out)], cwd=ROOT).returncode or not out.exists():
            crashed.append(client)
            continue
        result = json.loads(redact(out.read_text()))
        result["date"] = now.date().isoformat()
        dest = ROOT / "results" / client / f"{result['version']}.json"
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text(json.dumps(result, indent=2) + "\n")
    if crashed:
        sys.exit("crashed, nothing recorded: " + ", ".join(crashed))


if __name__ == "__main__":
    main(sys.argv[1:])
