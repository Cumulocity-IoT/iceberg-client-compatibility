"""Posts the fixture data of a run to Cumulocity, the way a device would: python -m harness.seed <run_id>.

This is the only part of the tests that writes, and it writes only through the Cumulocity REST API: one fixture
device, found by its external id and created on first use, an event carrying the edge cases, and quick inventory
updates of the device that give latest_inventory an equality delete and a position delete within one batch.
Prints the JSON that describes what was posted.
"""

import base64
import json
import os
import sys
import urllib.error
import urllib.request
from datetime import datetime, timezone

EXTERNAL_ID = ("c8y_Serial", "iceberg-client-compatibility-fixture")
FRAGMENT = "compat_Fixture"
ESCAPED_FRAGMENT = "compat.Escaped"
# Cumulocity keeps event numbers as doubles, so a literal survives only with double precision. These do, and
# a client that reads decimal(38,9) through a double still changes them: 12345678901234567000 (posted as
# 1.2345678901234567E19, since Cumulocity parses an integer literal as a long) becomes
# 12345678901234567168, 0.123456789 becomes 0.12345678899999999...
DECIMALS = {"decimalValue": "1.2345678901234567E19", "fractionDecimal": "0.123456789", "tinyDecimal": "0.000000001"}
NESTING = 13  # objects below "nested": with the fragment, "nested" and the leaf, the path is 16 deep
TOO_DEEP = 15  # one path deeper than the limit, so Iceflow bins it into cdc_rejected.trash
UPDATES = 3  # quick inventory updates within one batch


def _session():
    base = os.environ["C8Y_BASEURL"].rstrip("/")
    user = f"{os.environ['C8Y_TENANT']}/{os.environ['C8Y_USER']}:{os.environ['C8Y_PASSWORD']}"
    auth = "Basic " + base64.b64encode(user.encode()).decode()

    def call(method, path, body=None):
        data = None if body is None else (body if isinstance(body, bytes) else json.dumps(body).encode())
        request = urllib.request.Request(base + path, data=data, method=method, headers={
            "Authorization": auth, "Accept": "application/json", "Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(request, timeout=60) as response:
                return json.loads(response.read() or b"null")
        except urllib.error.HTTPError as error:
            if error.code == 404:
                return None
            raise RuntimeError(f"{method} {path}: HTTP {error.code} {error.read().decode()[:300]}") from None

    return call


def fixture_device(call):
    kind, value = EXTERNAL_ID
    found = call("GET", f"/identity/externalIds/{kind}/{value}")
    if found:
        return found["managedObject"]["id"]
    device = call("POST", "/inventory/managedObjects",
                  {"name": "Iceberg client compatibility fixture", "type": "compat_Fixture", "c8y_IsDevice": {}})
    call("POST", f"/identity/globalIds/{device['id']}/externalIds", {"type": kind, "externalId": value})
    return device["id"]


def nested(depth):
    leaf = {"leaf": "deepest"}
    for level in range(depth, 0, -1):
        leaf = {f"level{level}": leaf}
    return leaf


def fixture_event(device, run_id, now, with_added_later=True):
    fragment = {
        "runId": run_id,
        **{name: f"__{name}__" for name in DECIMALS},
        # Iceflow parses timestamp strings with millisecond precision; finer ones stay strings.
        "stampMillis": "2026-11-02T10:11:12.345Z",
        "nested": nested(NESTING),
        "tooDeep": nested(TOO_DEEP),
        "listOfStructs": [{"position": 1, "label": "first"}, {"position": 2, "label": "second"}],
        "camelCaseName": "camel",
        "dash-name": "dash",
        "dot.name": "dot",
        "value": "reserved word value",
        "select": "reserved word select",
        "largeText": "ä" * 16384,  # 32,768 bytes of two-byte UTF-8, the largest string Iceflow keeps
    }
    if with_added_later:
        fragment["addedLater"] = run_id
    event = {
        "type": "compat_Fixture",
        "text": f"Iceberg client compatibility fixture {run_id}",
        "time": now.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        "source": {"id": device},
        FRAGMENT: fragment,
        ESCAPED_FRAGMENT: {"runId": run_id},
    }
    # json cannot write a Decimal, so the exact literals replace their placeholders in the text.
    text = json.dumps(event)
    for name, literal in DECIMALS.items():
        text = text.replace(f'"__{name}__"', literal)
    return text.encode()


def seed(run_id, baseline=False):
    call = _session()
    now = datetime.now(timezone.utc)
    device = fixture_device(call)
    event = call("POST", "/event/events", fixture_event(device, run_id, now, with_added_later=not baseline))
    if not baseline:
        for step in range(1, UPDATES + 1):
            call("PUT", f"/inventory/managedObjects/{device}", {"compatRunId": run_id, "compatStep": step})
    return {"run_id": run_id, "device": device, "event": event["id"], "posted": now.isoformat(), "baseline": baseline}


if __name__ == "__main__":
    print(json.dumps(seed(sys.argv[1], baseline="--baseline" in sys.argv), indent=2))
