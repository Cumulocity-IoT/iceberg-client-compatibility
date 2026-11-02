"""Settles a run before it is published: python -m harness.summary.

Keeps each pin that harness.releases raised only when the run recorded a result for the new version, and puts the
others back, so main never pins a release no run got through. Stages the results and the kept pins, and writes the
cells that changed against the last recorded result of each client to the job summary ($GITHUB_STEP_SUMMARY) and
to stdout.
"""

import json
import os
import subprocess
from pathlib import Path

from harness.render import version_key

ROOT = Path(__file__).resolve().parent.parent


def git(*args):
    return subprocess.run(["git", *args], cwd=ROOT, check=True, capture_output=True, text=True).stdout


def changes(before, after):
    """(column, before, after) for every column whose status differs; a column one side lacks is None there."""
    status = lambda checks, c: checks.get(c, {}).get("status")
    return [(c, status(before, c), status(after, c)) for c in sorted({*before, *after}) if status(before, c) != status(after, c)]


def previous(path):
    """The recorded result this one replaces: the same file at HEAD, else the client's highest version at HEAD."""
    tracked = git("ls-tree", "--name-only", "HEAD", f"{path.parent.relative_to(ROOT)}/").split()
    rel = str(path.relative_to(ROOT))
    if rel in tracked:
        return json.loads(git("show", f"HEAD:{rel}"))
    if tracked:
        return json.loads(git("show", f"HEAD:{max(tracked, key=lambda p: version_key(Path(p).stem))}"))
    return None


def main():
    lines = ["## Compatibility run", ""]
    written = {line[3:] for line in git("status", "--porcelain", "--untracked-files=all", "--", "results").splitlines()}

    bumped_path = ROOT / "target" / "bumped.json"
    bumped = json.loads(bumped_path.read_text()) if bumped_path.exists() else {}
    for client, b in bumped.items():
        if f"results/{client}/{b['to']}.json" in written:
            git("add", f"clients/{client}")
            lines.append(f"* {client}: pins raised, {b['pins']}")
        else:
            git("checkout", "--", f"clients/{client}")
            lines.append(f"* {client}: **{b['pins']} recorded no result, pins kept**")

    rows = []
    for rel in sorted(written):
        path = ROOT / rel
        new, old = json.loads(path.read_text()), previous(path)
        against = f"{old['version']}" if old else "none"
        for column, before, after in changes(old["checks"] if old else {}, new["checks"]):
            rows.append(f"| {new['client']} | {new['version']} | {against} | {column} | {before or ''} | {after or ''} |")
    git("add", "results")
    lines += ["", "| Client | Version | Compared with | Column | Before | After |", "|---|---|---|---|---|---|", *rows] \
        if rows else ["", "No cell changed."]

    text = "\n".join(lines) + "\n"
    print(text)
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as summary:
            summary.write(text)


if __name__ == "__main__":
    main()
