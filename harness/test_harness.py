import contextlib
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from harness.redact import redact
from harness.releases import bump, depends_on, latest
from harness.render import ROOT, load, render, validate, version_key
from harness.run import default_clients, previous, regressions, unconfirmed
from harness.summary import changes

COLUMNS = [
    {"id": "catalog-login", "title": "Login", "meaning": "Logs in."},
    {"id": "latest-tables", "title": "Latest tables", "meaning": "Reads the latest tables."},
    {"id": "views", "title": "Views", "meaning": "Runs the views."},
]
CLIENTS = {
    "pyiceberg": {"name": "PyIceberg", "section": "library", "example": "example.py"},
    "datafusion": {"name": "Apache DataFusion", "section": "engine", "example": "src/example.rs"},
}
PY = '[🔗](clients/pyiceberg/example.py "Example for PyIceberg")'
DF = '[🔗](clients/datafusion/src/example.rs "Example for Apache DataFusion")'


def result(client, version, date, **checks):
    return {"client": client, "version": version, "date": date, "checks": checks}


class RenderTest(unittest.TestCase):
    def test_versions_compare_numerically(self):
        self.assertGreater(version_key("0.12.0"), version_key("0.9.1"))
        self.assertGreater(version_key("1.10.0"), version_key("1.9.2"))
        self.assertGreater(version_key("4.1.7"), version_key("4.1.7-rc1"))
        self.assertGreater(version_key("4.1.7.1"), version_key("4.1.7"))

    def test_sections_latest_marker_and_footnotes(self):
        table = render(COLUMNS, CLIENTS, [
            result("pyiceberg", "0.11.1", "2026-11-02", **{"catalog-login": {"status": "ok"}}),
            result("pyiceberg", "0.12.0", "2026-11-09", **{
                "catalog-login": {"status": "ok"},
                "latest-tables": {"status": "fail", "note": "raises on equality deletes"},
            }),
            result("datafusion", "53.1.0", "2026-11-09", **{
                "catalog-login": {"status": "warn", "workaround": "token-refresh"},
            }),
        ])
        lines = table.splitlines()
        self.assertLess(lines.index("### Client libraries"), lines.index("### Engines"))
        self.assertNotIn("Views", table)  # no result carries the column, so neither the table nor the meanings
        self.assertIn("| Latest tables | Reads the latest tables. |", lines)
        self.assertIn(f"| PyIceberg | {PY} | 0.12.0 | 2026-11-09 | ✅ | ❌[^1] |", lines)
        self.assertIn("| PyIceberg |  | 0.11.1 | 2026-11-02, not re-tested | ✅ |  |", lines)  # the example is for the latest
        self.assertIn(f"| Apache DataFusion | {DF} | 53.1.0 | 2026-11-09 | ⚠️[^2] |", lines)  # its section carries only Login
        self.assertIn("[^1]: PyIceberg 0.12.0, Latest tables: raises on equality deletes", lines)
        self.assertIn("[^2]: Apache DataFusion 53.1.0, Login: needs the `token-refresh` workaround", lines)

    def test_each_section_shows_the_columns_its_results_carry(self):
        table = render(COLUMNS, CLIENTS, [
            result("pyiceberg", "0.12.0", "2026-11-09", **{"catalog-login": {"status": "ok"}}),
            result("datafusion", "53.1.0", "2026-11-09", **{"catalog-login": {"status": "ok"}, "views": {"status": "ok"}}),
        ])
        lines = table.splitlines()
        self.assertIn("| Client | Example | Version | Tested | Login |", lines[:lines.index("### Engines")])
        self.assertIn("| Client | Example | Version | Tested | Login | Views |", lines[lines.index("### Engines"):])
        self.assertEqual(1, lines.count("| Views | Runs the views. |"))

    def test_cells_with_the_same_workaround_share_a_footnote(self):
        checks = {"catalog-login": {"status": "warn", "workaround": "tls", "note": "first"},
                  "latest-tables": {"status": "warn", "workaround": "tls", "note": "second"},
                  "views": {"status": "warn", "workaround": "s3-region"}}
        table = render(COLUMNS, CLIENTS, [{"client": "pyiceberg", "version": "1.0", "date": "2026-11-02", "checks": checks}])
        self.assertIn("| ⚠️[^1] | ⚠️[^1] | ⚠️[^2] |", table)
        self.assertIn("[^1]: PyIceberg 1.0, Login, Latest tables: needs the `tls` workaround: first", table)
        self.assertIn("[^2]: PyIceberg 1.0, Views: needs the `s3-region` workaround", table)

    def test_cells_with_the_same_note_share_a_footnote(self):
        checks = {"catalog-login": {"status": "ok", "note": "one setup"}, "latest-tables": {"status": "ok", "note": "one setup"},
                  "views": {"status": "na", "note": "other"}}
        table = render(COLUMNS, CLIENTS, [{"client": "pyiceberg", "version": "1.0", "date": "2026-11-02", "checks": checks}])
        self.assertIn("| ✅[^1] | ✅[^1] | ➖[^2] |", table)
        self.assertIn("[^1]: PyIceberg 1.0, Login, Latest tables: one setup", table)

    def test_a_client_with_a_url_is_linked_by_name(self):
        clients = {"datafusion": {"name": "Apache DataFusion", "section": "engine", "url": "https://datafusion.apache.org",
                                   "example": "src/example.rs"}}
        table = render(COLUMNS, clients, [result("datafusion", "54.1.0", "2026-11-09", **{"catalog-login": {"status": "ok"}})])
        self.assertIn(f"| [Apache DataFusion](https://datafusion.apache.org) | {DF} | 54.1.0 | 2026-11-09 | ✅ |", table)
        self.assertNotIn("[^", table)

    def test_an_older_version_with_the_same_cells_is_left_out(self):
        tls = {"status": "warn", "workaround": "tls", "note": "no TLS backend"}
        table = render(COLUMNS, CLIENTS, [
            result("datafusion", "53.1.0", "2026-11-02", **{"catalog-login": tls, "latest-tables": {"status": "ok"}}),
            result("datafusion", "54.1.0", "2026-11-09", **{"catalog-login": tls, "latest-tables": {"status": "ok"}}),
        ])
        self.assertIn(f"| Apache DataFusion | {DF} | 54.1.0 | 2026-11-09 | ⚠️[^1] | ✅ |", table)
        self.assertNotIn("53.1.0", table)

    def test_versions_share_a_footnote_for_the_same_symptom(self):
        tls = {"status": "warn", "workaround": "tls", "note": "no TLS backend"}
        table = render(COLUMNS, CLIENTS, [
            result("datafusion", "53.1.0", "2026-11-02", **{"catalog-login": tls, "latest-tables": {"status": "fail", "note": "old rows"}}),
            result("datafusion", "54.1.0", "2026-11-09", **{"catalog-login": tls, "latest-tables": {"status": "ok"}}),
        ])
        self.assertIn(f"| Apache DataFusion | {DF} | 54.1.0 | 2026-11-09 | ⚠️[^1] | ✅ |", table)
        self.assertIn("| Apache DataFusion |  | 53.1.0 | 2026-11-02, not re-tested | ⚠️[^1] | ❌[^2] |", table)
        self.assertIn("[^1]: Apache DataFusion 54.1.0, 53.1.0, Login: needs the `tls` workaround: no TLS backend", table)
        self.assertIn("[^2]: Apache DataFusion 53.1.0, Latest tables: old rows", table)

    def test_every_client_names_an_example_that_exists(self):
        _, clients, _ = load()
        for name, client in clients.items():
            self.assertTrue((ROOT / "clients" / name / client["example"]).is_file(), f"clients/{name}: no {client['example']}")

    def test_unknown_status_is_rejected(self):
        with self.assertRaises(ValueError):
            render(COLUMNS, CLIENTS, [result("pyiceberg", "0.12.0", "2026-11-09", views={"status": "maybe"})])

    def test_unknown_section_is_rejected_even_without_results(self):
        clients = {**CLIENTS, "typo": {"name": "Typo", "section": "products"}}
        with self.assertRaisesRegex(ValueError, "clients/typo/client.toml: unknown section 'products'"):
            render(COLUMNS, clients, [result("pyiceberg", "0.12.0", "2026-11-09", **{"catalog-login": {"status": "ok"}})])

    def test_spark_services_come_last_with_their_intro(self):
        clients = {**CLIENTS, "dremio": {"name": "Dremio", "section": "product", "example": "example.py"},
                   "google": {"name": "Google", "section": "managed-spark", "example": "example.py"}}
        table = render(COLUMNS, clients, [
            result("google", "2.3", "2026-11-09", **{"catalog-login": {"status": "ok"}}),
            result("dremio", "26.1", "2026-11-09", **{"catalog-login": {"status": "ok"}}),
        ])
        lines = table.splitlines()
        heading = lines.index("### Spark services that copy into your platform")
        self.assertLess(lines.index("### Products"), heading)
        self.assertTrue(lines[heading + 2].startswith("These services run a Spark job"))
        self.assertEqual("| Client | Example | Version | Tested | Login |", lines[heading + 4])
        self.assertIn('| Google | [🔗](clients/google/example.py "Example for Google") | 2.3 | 2026-11-09 | ✅ |', lines)

    def test_the_repository_clients_and_results_are_valid(self):
        validate(*load())  # fails the self-test, before fixtures are posted, instead of the publish step

    def test_a_run_without_arguments_leaves_out_the_spark_services(self):
        with tempfile.TemporaryDirectory() as tmp:
            for client, section in [("duckdb", "engine"), ("google", "managed-spark"), ("dremio", "product")]:
                folder = Path(tmp, "clients", client)
                folder.mkdir(parents=True)
                (folder / "client.toml").write_text(f'name = "{client}"\nsection = "{section}"\n')
                (folder / "run.sh").write_text("")
            Path(tmp, "clients", "spare").mkdir()  # no run.sh, no client
            self.assertEqual(["dremio", "duckdb"], default_clients(Path(tmp)))


class PlanTest(unittest.TestCase):
    def test_window_straddles_a_settled_month_boundary(self):
        from datetime import datetime, timezone

        from harness.plan import window

        def at(text):
            return datetime.fromisoformat(text).replace(tzinfo=timezone.utc)

        self.assertEqual(window(at("2026-11-02T14:30:00")), (at("2026-10-31"), at("2026-11-01"), at("2026-11-02")))
        # Less than a day and an hour after the month started: the previous boundary is used.
        self.assertEqual(window(at("2026-11-02T00:30:00")), (at("2026-09-30"), at("2026-10-01"), at("2026-10-02")))
        self.assertEqual(window(at("2027-01-01T05:00:00")), (at("2026-11-30"), at("2026-12-01"), at("2026-12-02")))


class RedactTest(unittest.TestCase):
    def test_secrets_are_scrubbed(self):
        cases = {
            "GET https://acct.dfs.core.windows.net/c/f.parquet?sv=2024&sig=abc%2Bdef&se=1": "sig=REDACTED&se=1",
            "Authorization: Bearer eyJhbGciOiJSUzI1NiJ9.payload.sig": "Bearer REDACTED",
            '{"s3.session-token": "IQoJb3JpZ2luX2VjE"}': '"s3.session-token": "REDACTED"',
            "credential=client-id:very-secret": "credential=REDACTED",
            "X-Amz-Security-Token=FwoGZXIvYXdz&X-Amz-Date=2026": "X-Amz-Security-Token=REDACTED&X-Amz-Date",
            "The AWS Access Key Id ASIAQ3EGVBCDEFGHIJKL you provided": "Key Id REDACTED you",
            '"s3.access-key-id": "ASIAQ3EGVBCDEFGHIJKL"': '"s3.access-key-id": "REDACTED"',
            '{"userName": "compat", "password": "s3cret-pw"}': '"password": "REDACTED"',
            "Authorization: _dremio6u2rgc4q8mrcsl3kd2kqvp1sjf": "_dremioREDACTED",
        }
        for raw, expected in cases.items():
            self.assertIn(expected, redact(raw), raw)
            self.assertNotIn("secret", redact(raw).replace("client_secret", ""), raw)

    def test_harmless_text_is_kept(self):
        text = '{"token_type": "bearer", "issued_token_type": "urn:ietf:params:oauth:token-type:access_token"}'
        self.assertEqual(text, redact(text))


class ReleasesTest(unittest.TestCase):
    def test_latest_skips_pre_releases_and_reads_tags(self):
        self.assertEqual("1.10.0", latest(["1.9.2", "1.10.0", "1.11.0rc1", "1.11.0-SNAPSHOT"]))
        self.assertEqual("0.4.0", latest(["v0.3.1", "v0.4.0", "v0.5.0-rc1"]))
        tags = ["v25.10.1.3832-stable", "v25.8.11.66-lts", "v25.11.1.1-testing"]
        self.assertEqual("25.10.1.3832", latest(tags, r"v([0-9.]+)-stable"))
        self.assertIsNone(latest(["2.0.0b1"]))

    def test_bump_replaces_only_the_group_everywhere(self):
        text = 'iceberg = "=0.10.1"\niceberg-datafusion = { version = "=0.10.1", features = ["x"] }\ndatafusion = "=53.1.0"\n'
        match = r'(?m)^iceberg[a-z-]* = (?:\{ version = )?"=([0-9][^"]*)"'
        self.assertEqual(text.replace("0.10.1", "0.11.0"), bump(text, match, "0.11.0"))

    def test_a_crate_requirement_takes_its_major(self):
        self.assertTrue(depends_on("^54", "54.1.0"))
        self.assertTrue(depends_on("^53.1.0", "53.2.0"))
        self.assertFalse(depends_on("^53.1.0", "54.1.0"))
        self.assertFalse(depends_on(None, "54.1.0"))


class SummaryTest(unittest.TestCase):
    def test_changes_lists_only_differing_cells(self):
        before = {"s3": {"status": "ok"}, "views": {"status": "na"}, "decimal": {"status": "ok"}}
        after = {"s3": {"status": "ok"}, "views": {"status": "fail"}, "incremental-read": {"status": "ok"}}
        self.assertEqual([("decimal", "ok", None), ("incremental-read", None, "ok"), ("views", "na", "fail")],
                         changes(before, after))


def checks(**statuses):
    return {"version": "1.0", "checks": {c.replace("_", "-"): {"status": s} for c, s in statuses.items()}}


class SecondRunTest(unittest.TestCase):
    BEFORE = checks(cdc_tables="ok", views="warn", decimal="fail", nesting="na", identifiers="inconclusive")

    def test_every_worse_status_is_a_regression(self):
        now = checks(cdc_tables="warn", views="fail", decimal="fail", nesting="fail", identifiers="na",
                     timestamps="fail")
        # ok to warn, warn to fail, na to fail; fail stays, inconclusive to na is no worse, timestamps is new
        self.assertEqual({"cdc-tables", "views", "nesting"}, regressions(now, self.BEFORE))
        self.assertEqual(set(), regressions(now, None))  # a first result has nothing to regress from

    def test_a_regression_that_does_not_repeat_is_not_kept(self):
        second = checks(cdc_tables="ok", views="warn")
        self.assertEqual(set(), unconfirmed(checks(cdc_tables="fail", views="fail"), second, self.BEFORE))

    def test_a_repeated_regression_is_confirmed(self):
        second = checks(cdc_tables="fail", views="warn")
        self.assertEqual(set(), unconfirmed(checks(cdc_tables="fail", views="fail"), second, self.BEFORE))

    def test_a_regression_worse_than_in_the_first_run_is_unconfirmed(self):
        self.assertEqual({"views"}, unconfirmed(checks(cdc_tables="fail", views="warn"),
                                                checks(cdc_tables="ok", views="fail"), self.BEFORE))
        self.assertEqual({"cdc-tables"}, unconfirmed(checks(cdc_tables="warn"), checks(cdc_tables="fail"), self.BEFORE))

    def test_after_a_crash_only_a_second_run_without_regressions_counts(self):
        self.assertEqual(set(), unconfirmed(None, checks(cdc_tables="ok"), self.BEFORE))
        self.assertEqual({"cdc-tables"}, unconfirmed(None, checks(cdc_tables="fail"), self.BEFORE))

    def run_main(self, first, second, checks_after):
        """Runs harness.run.main with stubbed clients: first and second map a client to its result or None."""
        import harness.run as run
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for client in first:
                (root / "results" / client).mkdir(parents=True)
                (root / "results" / client / "1.0.json").write_text(json.dumps(checks(cdc_tables="ok", latest_tables="ok")))
            outcome = lambda c, out: (second if out.name.endswith(".second.json") else first)[c]
            client = mock.Mock(side_effect=lambda c, p, out: outcome(c, out))
            with mock.patch.multiple(run, ROOT=root, load_env=mock.DEFAULT, seed=mock.DEFAULT,
                                     fixture_plan=mock.Mock(return_value={}), plan=mock.Mock(return_value={}),
                                     preflight=mock.Mock(side_effect=[True, *checks_after]), run_client=client), \
                    mock.patch.object(run.time, "sleep"), contextlib.redirect_stdout(io.StringIO()):
                try:
                    run.main(list(first))
                    message = None
                except SystemExit as exit_:
                    message = str(exit_)
            recorded = {p.parent.name: json.loads(p.read_text())["checks"] for p in root.glob("results/*/1.0.json")}
            return recorded, message, client.call_args_list

    def test_main_runs_regressed_and_crashed_clients_again(self):
        recorded, message, calls = self.run_main(
            first={"flaky": checks(cdc_tables="fail", latest_tables="ok"), "steady": checks(cdc_tables="ok", latest_tables="ok"),
                   "down": None, "worse": checks(cdc_tables="warn", latest_tables="ok"),
                   "calm": checks(cdc_tables="ok", latest_tables="ok"), "quiet": checks(cdc_tables="ok", latest_tables="ok")},
            second={"flaky": checks(cdc_tables="ok", latest_tables="ok"), "down": None,
                    "worse": checks(cdc_tables="fail", latest_tables="ok")},
            checks_after=[True, False])
        self.assertEqual(["flaky", "steady", "down", "worse", "calm", "quiet", "flaky", "down", "worse"],
                         [c.args[0] for c in calls])  # three of six need a second run: not more than half
        self.assertEqual("ok", recorded["flaky"]["cdc-tables"]["status"])  # the second run's result
        self.assertEqual("inconclusive", recorded["flaky"]["latest-tables"]["status"])  # its after-check failed
        self.assertEqual("ok", recorded["steady"]["latest-tables"]["status"])  # its after-check passed
        self.assertEqual("ok", recorded["worse"]["cdc-tables"]["status"])  # unconfirmed: the old file stays
        self.assertIn("crashed in the second run, nothing recorded: down", message)
        self.assertIn("worse in a run without confirmation, nothing recorded: worse (cdc-tables)", message)

    def test_main_skips_second_runs_when_most_clients_regress(self):
        recorded, message, calls = self.run_main(
            first={"a": checks(cdc_tables="fail", latest_tables="ok"), "b": checks(cdc_tables="fail", latest_tables="ok"),
                   "c": checks(cdc_tables="ok", latest_tables="ok")},
            second={}, checks_after=[True])
        self.assertEqual(["a", "b", "c"], [c.args[0] for c in calls])
        self.assertEqual("ok", recorded["a"]["cdc-tables"]["status"])  # kept, not the failed first run
        self.assertIn("a (second run skipped), b (second run skipped)", message)

    def test_previous_is_the_same_version_else_the_highest(self):
        with tempfile.TemporaryDirectory() as tmp:
            folder = Path(tmp, "results", "duckdb")
            folder.mkdir(parents=True)
            for version in ("1.9.2", "1.10.0"):
                (folder / f"{version}.json").write_text(json.dumps({"version": version, "checks": {}}))
            self.assertEqual("1.9.2", previous("duckdb", "1.9.2", Path(tmp))["version"])
            self.assertEqual("1.10.0", previous("duckdb", "2.0.0", Path(tmp))["version"])
            self.assertIsNone(previous("polars", "2.1.0", Path(tmp)))


if __name__ == "__main__":
    unittest.main()
