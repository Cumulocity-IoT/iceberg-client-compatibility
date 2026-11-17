"""Renders results/<client>/<version>.json into the table between the markers in README.md."""

import json
import re
import sys
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
BEGIN = "<!-- BEGIN GENERATED TABLE -->"
END = "<!-- END GENERATED TABLE -->"
# (section, heading, intro): a section with no results is left out, its intro with it.
SECTIONS = [
    ("library", "Client libraries", None),
    ("engine", "Engines", None),
    ("product", "Products", None),
    ("managed-spark", "Spark services that copy into your platform",
     "These services run a Spark job in your account on a data platform. The job reads the lakehouse through the "
     "catalog and writes a copy, such as hourly averages per device, into the platform's storage, where the "
     "platform's SQL engine and business intelligence tools query it. The copy is as current as the job's last run, "
     "and your cloud provider bills the job's compute, storage, and network traffic. The platform chooses the Spark "
     "version and, unless the setup guide adds JAR files, the Iceberg version, so a result here can differ from the "
     "Apache Spark row under Engines. Each service's folder holds its setup guide, `README.md`, next to the example."),
]
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
    footnotes = {f["id"]: f["text"] for f in tomllib.loads((root / "footnotes.toml").read_text())["footnote"]}
    return columns, clients, results, footnotes


def validate(columns, clients, results, footnotes=None):
    sections = {s for s, _, _ in SECTIONS}
    for client, config in clients.items():
        if config.get("section") not in sections:
            raise ValueError(f"clients/{client}/client.toml: unknown section {config.get('section')!r}")
        if not config.get("example"):
            raise ValueError(f"clients/{client}/client.toml: no example")
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
            if value.get("footnote") and value["footnote"] not in (footnotes or {}):
                raise ValueError(f"{where}: {check} names the footnote {value['footnote']!r}, which footnotes.toml lacks")


def footnote_key(client, check, column):
    """What a cell's footnote is about: a footnote of footnotes.toml, which every client that names it shares; else
    the client's workaround; else, for ➖ in a column with an `unsupported` text, the column's one footnote; else the
    client's note. None for a passing cell, whose setup belongs in the client's README."""
    if check.get("status", "ok") == "ok":
        return None
    if check.get("footnote"):
        return (None, "footnote", check["footnote"])
    if check.get("workaround"):
        return (client, "workaround", check["workaround"])
    if check["status"] == "na" and column.get("unsupported"):
        return (None, "unsupported", column["id"])
    return (client, "note", check["note"]) if check.get("note") else None


def listing(items):
    """a; a and b; a, b, and c."""
    return items[0] if len(items) == 1 else f"{items[0]} and {items[1]}" if len(items) == 2 else f"{', '.join(items[:-1])}, and {items[-1]}"


def render(columns, clients, results, footnotes=None):
    validate(columns, clients, results, footnotes)
    used = [c for c in columns if any(c["id"] in r["checks"] for r in results)]
    latest = {}
    for r in results:
        if r["client"] not in latest or version_key(r["version"]) > version_key(latest[r["client"]]):
            latest[r["client"]] = r["version"]

    sections = []
    for section, heading, intro in SECTIONS:
        rows = [r for r in results if clients[r["client"]]["section"] == section]
        if not rows:
            continue
        rows.sort(key=lambda r: version_key(r["version"]), reverse=True)
        rows.sort(key=lambda r: clients[r["client"]]["name"].lower())
        shown = [c for c in used if any(c["id"] in r["checks"] for r in rows)]
        cell_of = lambda r, c: tuple(r["checks"].get(c["id"], {}).get(k) for k in ("status", "workaround"))
        # An older version whose cells show the same as those of the newer version listed above it adds nothing.
        listed = []
        for r in rows:
            above = listed[-1] if listed and listed[-1]["client"] == r["client"] else None
            if above is None or any(cell_of(above, c) != cell_of(r, c) for c in shown):
                listed.append(r)
        sections.append((heading, intro, shown, listed))

    # Cells that name the same footnote of footnotes.toml share one, across clients and sections, and so do the ➖
    # cells of a column with an `unsupported` text. A client's cells that need the same workaround share one across
    # its versions, with the note of the first of them; its other cells share one when their notes are the same.
    groups = {}
    for _, _, shown, listed in sections:
        for r in listed:
            for c in shown:
                check = r["checks"].get(c["id"], {})
                if key := footnote_key(r["client"], check, c):
                    group = groups.setdefault(key, {"rows": [], "titles": set(), "note": check.get("note"), "sources": {}})
                    group["rows"] += [(r["client"], r["version"])] if (r["client"], r["version"]) not in group["rows"] else []
                    group["titles"].add(c["title"])
                    group["sources"].setdefault(r["client"], check.get("source"))

    def footnote(key):
        group = groups[key]
        versions = {}
        for client, version in group["rows"]:
            versions.setdefault(client, []).append(version)
        if key[1] == "footnote":
            return f"{listing([f'{clients[c]['name']} {listing(vs)}' for c, vs in versions.items()])}: {footnotes[key[2]]}"
        if key[1] == "unsupported":
            column = next(c for c in columns if c["id"] == key[2])
            source = lambda c: f" ({group['sources'][c]})" if group["sources"][c] else ""
            return (f"{column['title']}: {column['unsupported']}: "
                    + "; ".join(f"{clients[c]['name']} {listing(vs)}{source(c)}" for c, vs in versions.items()) + ".")
        name = clients[key[0]]["name"]
        text = (f"needs the `{key[2]}` workaround" + (f": {group['note']}" if group["note"] else "")
                if key[1] == "workaround" else group["note"])
        titles = ", ".join(c["title"] for c in columns if c["title"] in group["titles"])
        return f"{name} {', '.join(v for _, v in group['rows'])}, {titles}: {text}"

    notes, lines, refs = [], [], {}
    for heading, intro, shown, listed in sections:
        header = ["Client", "Example", "Version", "Tested"] + [c["title"] for c in shown]
        lines += [f"### {heading}", ""] + ([intro, ""] if intro else [])
        lines += ["| " + " | ".join(header) + " |", "|" + "---|" * len(header)]
        for r in listed:
            client = clients[r["client"]]
            current = r["version"] == latest[r["client"]]
            tested = r["date"] if current else f"{r['date']}, not re-tested"
            name = f"[{client['name']}]({client['url']})" if client.get("url") else client["name"]
            # The example pins the latest version, so only that version's row links to it.
            example = f'[🔗](clients/{r["client"]}/{client["example"]} "Example for {client["name"]}")' if current else ""
            cells = [name, example, r["version"], tested]
            for c in shown:
                check = r["checks"].get(c["id"])
                if check is None:
                    cells.append("")
                    continue
                cell = ICONS[check["status"]]
                if key := footnote_key(r["client"], check, c):
                    if key not in refs:
                        notes.append(footnote(key))
                        refs[key] = len(notes)
                    cell += f"[^{refs[key]}]"
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
