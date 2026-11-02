"""Writes the plan of a run: which tables, which device and which time window every client checks.

All clients of a run read the same plan, so their counts must agree. The window lies in the past and
straddles a month boundary, so the month(time) partition boundary is crossed and the append-only CDC
rows inside it no longer change.
"""

import json
import sys
from datetime import datetime, timedelta, timezone

SETTLE = timedelta(hours=1)  # ingestion latency is minutes; an hour keeps late commits out of the window


def month_start(t):
    return t.replace(day=1, hour=0, minute=0, second=0, microsecond=0)


def window(now):
    boundary = month_start(now)
    if now < boundary + timedelta(days=1) + SETTLE:
        boundary = month_start(boundary - timedelta(days=1))
    return boundary - timedelta(days=1), boundary, boundary + timedelta(days=1)


def plan(now, run_id):
    start, boundary, end = window(now)
    iso = lambda t: t.strftime("%Y-%m-%dT%H:%M:%SZ")
    return {
        "run_id": run_id,
        "started": iso(now),
        "window": {"from": iso(start), "boundary": iso(boundary), "to": iso(end)},
        # The temperature simulator of the test tenant writes this device's measurements.
        "measurement": {"namespace": "cdc_measurement", "table": "c8y_Temperature", "source": "811216"},
        "latest": {"namespace": "latest_inventory", "table": "inventory"},
        # A table name with escapes (from an OPC UA node id) and a camelCase column.
        "escaped": {"namespace": "cdc_inventory", "table": "opcuaVal_ns_x003D2_x003Bi_x003D10", "column": "lastUpdated"},
        "view": {"namespace": "view_measurement", "view": "c8y_Temperature"},
        # Only for the s3-region workaround: the region the catalog vends for this lake.
        "region": "eu-central-1",
    }


if __name__ == "__main__":
    now = datetime.now(timezone.utc).replace(microsecond=0)
    out = plan(now, sys.argv[2] if len(sys.argv) > 2 else now.strftime("%Y%m%dT%H%M%SZ"))
    with open(sys.argv[1], "w") as f:
        json.dump(out, f, indent=2)
