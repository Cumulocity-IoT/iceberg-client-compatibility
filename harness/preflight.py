# /// script
# requires-python = ">=3.11"
# dependencies = ["pyiceberg[pyarrow]==0.12.0", "pyarrow==26.0.0"]
# ///
"""The check before the clients: preflight.py <plan.json> [--after].

Waits until the run's fixture data is in the lake and the latest table holds the equality delete for the
fixture device, next to position deletes. With --after it only checks that those delete files are still
there, because a compaction during the run would have removed them. Exits non-zero naming the cause.
"""

import json
import sys
import time
from datetime import datetime

sys.path[:0] = ["clients/pyiceberg", "."]
from example import connect  # noqa: E402
from pyiceberg.expressions import EqualTo  # noqa: E402

from harness.redact import redact  # noqa: E402

WAIT_SECONDS = 20 * 60


def fail(cause):
    sys.exit("preflight: " + redact(cause))


def deletes_in_place(catalog, plan):
    """Both delete kinds in the current latest snapshot, the equality delete covering the fixture device."""
    device = plan["fixture"]["device"]
    latest = catalog.load_table((plan["latest"]["namespace"], plan["latest"]["table"]))
    files = latest.inspect.delete_files().to_pylist()
    covering = [f for f in files if f["content"] == 2
                and (m := (f.get("readable_metrics") or {}).get("id"))
                and m["lower_bound"] <= device <= m["upper_bound"]]
    positions = [f for f in files if f["content"] == 1]
    return bool(covering) and bool(positions)


def fixture_visible(catalog, plan):
    f = plan["fixture"]
    rows = catalog.load_table((f["table"]["namespace"], f["table"]["table"])).scan(
        row_filter=EqualTo("runId", f["run_id"]), selected_fields=("runId",)).to_arrow()
    latest = catalog.load_table((plan["latest"]["namespace"], plan["latest"]["table"]))
    posted_ms = int(datetime.fromisoformat(f["posted"]).timestamp() * 1000)
    return rows.num_rows > 0 and any(s.timestamp_ms > posted_ms for s in latest.snapshots())


def main(plan_path, after=False):
    plan = json.loads(open(plan_path).read())
    try:
        catalog = connect()
        catalog.list_namespaces()
    except Exception as error:
        fail(f"catalog login failed: {error}")
    if after:
        if not deletes_in_place(catalog, plan):
            fail("the latest table's delete files for the fixture device are gone; a compaction ran during the run")
        return
    m = plan["measurement"]
    try:
        catalog.load_table((m["namespace"], m["table"])).scan(limit=1).to_arrow()
    except Exception as error:
        fail(f"no storage access with vended credentials: {error}")
    deadline = time.monotonic() + WAIT_SECONDS
    while not (fixture_visible(catalog, plan) and deletes_in_place(catalog, plan)):
        if time.monotonic() > deadline:
            fail(f"the fixture data of run {plan['fixture']['run_id']} is not in the lake after "
                 f"{WAIT_SECONDS // 60} minutes, or the latest table lacks its delete files")
        time.sleep(30)


if __name__ == "__main__":
    main(sys.argv[1], after="--after" in sys.argv)
