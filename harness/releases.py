"""Raises client pins to the latest stable release: python -m harness.releases [--bump] [client ...].

Reads `release`, `pin` and `requires` from each clients/<id>/client.toml (see clients/CONTRACT.md). Without --bump
it lists every client with its pinned version and the one it would move to; with --bump it also rewrites the pins of the clients that are
behind and records them in target/bumped.json, which harness.summary reads after the run.
"""

import functools
import json
import os
import re
import sys
import tomllib
import urllib.error
import urllib.request
from pathlib import Path

from harness.render import version_key

ROOT = Path(__file__).resolve().parent.parent
SKIP = {"target", "__pycache__", ".venv", "client.toml"}
STABLE = r"v?([0-9]+(?:\.[0-9]+)*)"  # a release tag or version without a pre-release or build suffix


@functools.cache  # target() asks for the same listings once per candidate release
def fetch(url):
    headers = {"User-Agent": "Cumulocity-IoT/iceberg-client-compatibility", "Accept": "application/json"}
    if url.startswith("https://api.github.com/") and os.environ.get("GITHUB_TOKEN"):
        headers["Authorization"] = "Bearer " + os.environ["GITHUB_TOKEN"]
    with urllib.request.urlopen(urllib.request.Request(url, headers=headers), timeout=60) as response:
        return response.read()


def published(release):
    """Every version the registry lists, newest last not guaranteed; tags for GitHub."""
    if "pypi" in release:
        files = json.loads(fetch(f"https://pypi.org/pypi/{release['pypi']}/json"))["releases"]
        return [v for v, f in files.items() if f and not all(x.get("yanked") for x in f)]
    if "maven" in release:
        group, artifact = release["maven"].split(":")
        xml = fetch(f"https://repo1.maven.org/maven2/{group.replace('.', '/')}/{artifact}/maven-metadata.xml").decode()
        return re.findall(r"<version>([^<]+)</version>", xml)
    if "crates" in release:
        data = json.loads(fetch(f"https://crates.io/api/v1/crates/{release['crates']}/versions"))
        return [v["num"] for v in data["versions"] if not v["yanked"]]
    if "github" in release:
        data = json.loads(fetch(f"https://api.github.com/repos/{release['github']}/releases?per_page=100"))
        return [r["tag_name"] for r in data if not r["draft"] and not r["prerelease"]]
    if "dockerhub" in release:
        url = f"https://hub.docker.com/v2/repositories/{release['dockerhub']}/tags?page_size=100&ordering=last_updated"
        return [t["name"] for t in json.loads(fetch(url))["results"]]  # the 100 most recently pushed tags
    raise ValueError(f"unknown release source {release}")


def stable(versions, tag=STABLE):
    """The versions that fully match `tag`, as its one group, newest first."""
    return sorted({m.group(1) for m in map(re.compile(tag).fullmatch, versions) if m}, key=version_key, reverse=True)


def latest(versions, tag=STABLE):
    return next(iter(stable(versions, tag)), None)


def depends_on(requirement, version):
    """Whether a Cargo requirement such as "^54" or "^53.1.0" takes this version's major."""
    # shortcut: compares the major of a caret requirement only; upgrade when a crate declares a range.
    found = re.match(r"[\^=~]?\s*([0-9]+)", requirement or "")
    return bool(found) and found.group(1) == version.split(".")[0]


def runtime(requires, version):
    """The latest release of what `requires` names for this version, None if there is none: for Maven the one built
    for its major.minor line, for crates the one whose `dependency` takes its major."""
    if "dependency" in requires:
        for candidate in stable(published(requires)):
            deps = json.loads(fetch(f"https://crates.io/api/v1/crates/{requires['crates']}/{candidate}/dependencies"))
            if any(d["crate_id"] == requires["dependency"] and depends_on(d["req"], version) for d in deps["dependencies"]):
                return candidate
        return None
    minor = ".".join(version.split(".")[:2])
    try:
        return latest(published({k: v.format(minor=minor) for k, v in requires.items() if k != "pin"}))
    except urllib.error.HTTPError as error:
        if error.code == 404:
            return None
        raise


def target(config, current):
    """The version to pin and, with `requires`, the runtime built for it: the newest line that has one."""
    release, requires = config["release"], config.get("requires")
    newest = stable(published(release), release.get("tag", STABLE))
    if not requires:
        return (newest[0] if newest else current), None
    for version in newest:
        if version_key(version) < version_key(current):
            break
        if found := runtime(requires, version):
            return version, found
    return current, runtime(requires, current)


def files(client):
    return [p for p in (ROOT / "clients" / client).rglob("*") if p.is_file() and not SKIP & {p.name, *p.parts}]


def pinned(client, match):
    found = {m.group(1) for p in files(client) for m in re.finditer(match, p.read_text(errors="ignore"))}
    if len(found) != 1:
        raise ValueError(f"{client}: the pin {match!r} finds {sorted(found) or 'nothing'}, not one version")
    return found.pop()


def bump(text, match, version):
    return re.sub(match, lambda m: m.group(0)[:m.start(1) - m.start()] + version + m.group(0)[m.end(1) - m.start():], text)


def main(args):
    write = "--bump" in args
    wanted = [a for a in args if not a.startswith("--")]
    bumped = {}
    for path in sorted((ROOT / "clients").glob("*/client.toml")):
        client, config = path.parent.name, tomllib.loads(path.read_text())
        if wanted and client not in wanted or "pin" not in config:
            continue
        pins = [(config["pin"]["match"], pinned(client, config["pin"]["match"]))]
        version, extra = target(config, pins[0][1])
        wants = [version]
        if "requires" in config:
            pins.append((config["requires"]["pin"], pinned(client, config["requires"]["pin"])))
            wants.append(extra or pins[1][1])
        behind = [(match, want) for (match, have), want in zip(pins, wants) if version_key(want) > version_key(have)]
        shown = ", ".join(f"{have} -> {want}" if version_key(want) > version_key(have) else have
                          for (_, have), want in zip(pins, wants))
        print(f"{client}: {shown}")
        if write and behind:
            for p in files(client):
                text = original = p.read_text(errors="ignore")
                for match, want in behind:
                    text = bump(text, match, want)
                if text != original:
                    p.write_text(text)
            bumped[client] = {"from": pins[0][1], "to": version, "pins": shown}
    if write:
        (ROOT / "target").mkdir(exist_ok=True)
        (ROOT / "target" / "bumped.json").write_text(json.dumps(bumped, indent=2) + "\n")


if __name__ == "__main__":
    main(sys.argv[1:])
