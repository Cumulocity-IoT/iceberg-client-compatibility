"""Renders results/<client>/<version>.json into the table between the markers in README.md."""

import json
import re
import sys
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
BEGIN = "<!-- BEGIN GENERATED TABLE -->"
END = "<!-- END GENERATED TABLE -->"
SECTIONS = [("library", "Client libraries"), ("engine", "Engines"), ("product", "Products")]
ICONS = {"ok": "✅", "warn": "⚠️", "fail": "❌", "na": "➖", "inconclusive": "❔"}


def version_key(version):
    # Numbers compare as numbers and never meet strings; a text part such as "rc1" sorts below the
    # end-of-version sentinel (0, -1), so 4.1.7-rc1 < 4.1.7 < 4.1.7.1.
    parts = [(0, int(p)) if p.isdigit() else (-1, p) for p in re.split(r"[.+-]", version)]
    return parts + [(0, -1)]


def load(root=ROOT):
    columns = tomllib.loads((root / "columns.toml").read_text())["column"]
    clients = {p.parent.name: tomllib.loads(p.read_text()) for p in (root / "clients").glob("*/client.toml")}
    results = [json.loads(p.read_text()) for p in sorted((root / "results").glob("*/*.json"))]
    return columns, clients, results


def validate(columns, clients, results):
    known = {c["id"] for c in columns}
    for r in results:
        where = f"results/{r.get('client')}/{r.get('version')}.json"
        if r.get("client") not in clients:
            raise ValueError(f"{where}: no clients/{r.get('client')}/client.toml")
        for check, value in r["checks"].items():
            if check not in known:
                raise ValueError(f"{where}: unknown column {check!r}")
            if value.get("status") not in ICONS:
                raise ValueError(f"{where}: {check} has unknown status {value.get('status')!r}")


def footnote_key(check):
    """What a cell's footnote is about: its workaround, else its note; None for a cell without either."""
    if check.get("workaround"):
        return ("workaround", check["workaround"])
    return ("note", check["note"]) if check.get("note") else None


def render(columns, clients, results):
    validate(columns, clients, results)
    used = [c for c in columns if any(c["id"] in r["checks"] for r in results)]
    latest = {}
    for r in results:
        if r["client"] not in latest or version_key(r["version"]) > version_key(latest[r["client"]]):
            latest[r["client"]] = r["version"]

    notes, lines, refs = [], [], {}
    for section, heading in SECTIONS:
        rows = [r for r in results if clients[r["client"]]["section"] == section]
        if not rows:
            continue
        rows.sort(key=lambda r: version_key(r["version"]), reverse=True)
        rows.sort(key=lambda r: clients[r["client"]]["name"].lower())
        shown = [c for c in used if any(c["id"] in r["checks"] for r in rows)]
        cell_of = lambda r, c: tuple(r["checks"].get(c["id"], {}).get(k) for k in ("status", "workaround", "note"))
        # An older version whose cells all equal those of the newer version listed above it adds nothing.
        listed = []
        for r in rows:
            above = listed[-1] if listed and listed[-1]["client"] == r["client"] else None
            if above is None or any(cell_of(above, c) != cell_of(r, c) for c in shown):
                listed.append(r)
        # A client's cells that need the same workaround share one footnote across its versions, with the note of
        # the first of them; cells without a workaround share one when their notes are the same.
        groups = {}
        for r in listed:
            for c in shown:
                check = r["checks"].get(c["id"], {})
                if key := footnote_key(check):
                    group = groups.setdefault((r["client"], key), {"versions": [], "titles": set(), "note": check.get("note")})
                    group["versions"] += [r["version"]] if r["version"] not in group["versions"] else []
                    group["titles"].add(c["title"])
        header = ["Client", "Version", "Tested"] + [c["title"] for c in shown]
        lines += [f"### {heading}", "", "| " + " | ".join(header) + " |", "|" + "---|" * len(header)]
        for r in listed:
            client = clients[r["client"]]
            tested = r["date"] if r["version"] == latest[r["client"]] else f"{r['date']}, not re-tested"
            name = f"[{client['name']}]({client['url']})" if client.get("url") else client["name"]
            cells = [name, r["version"], tested]
            for c in shown:
                check = r["checks"].get(c["id"])
                if check is None:
                    cells.append("")
                    continue
                cell = ICONS[check["status"]]
                if key := footnote_key(check):
                    if (r["client"], key) not in refs:
                        group = groups[(r["client"], key)]
                        text = (f"needs the `{key[1]}` workaround" + (f": {group['note']}" if group["note"] else "")
                                if key[0] == "workaround" else group["note"])
                        titles = ", ".join(c["title"] for c in shown if c["title"] in group["titles"])
                        notes.append(f"{client['name']} {', '.join(group['versions'])}, {titles}: {text}")
                        refs[(r["client"], key)] = len(notes)
                    cell += f"[^{refs[(r['client'], key)]}]"
                cells.append(cell)
            lines.append("| " + " | ".join(cells) + " |")
        lines.append("")
    if used:
        lines += ["### What the columns mean", "", "| Column | Meaning |", "|---|---|"]
        lines += [f"| {c['title']} | {c['meaning']} |" for c in used]
        lines.append("")
    lines += [f"[^{i}]: {text}" for i, text in enumerate(notes, 1)]
    return "\n".join(lines).strip() + "\n"


def update_readme(table, readme):
    text = readme.read_text()
    start, end = text.index(BEGIN) + len(BEGIN), text.index(END)
    readme.write_text(text[:start] + "\n" + table + text[end:])


if __name__ == "__main__":
    update_readme(render(*load()), ROOT / "README.md")
    sys.exit(0)
