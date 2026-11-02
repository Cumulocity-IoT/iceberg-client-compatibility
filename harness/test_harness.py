import unittest

from harness.redact import redact
from harness.releases import bump, latest
from harness.render import render, version_key
from harness.summary import changes

COLUMNS = [
    {"id": "catalog-login", "title": "Login", "meaning": "Logs in."},
    {"id": "latest-tables", "title": "Latest tables", "meaning": "Reads the latest tables."},
    {"id": "views", "title": "Views", "meaning": "Runs the views."},
]
CLIENTS = {
    "pyiceberg": {"name": "PyIceberg", "section": "library"},
    "datafusion": {"name": "Apache DataFusion", "section": "engine"},
}


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
        self.assertIn("| PyIceberg | 0.12.0 | 2026-11-09 | ✅ | ❌[^1] |", lines)
        self.assertIn("| PyIceberg | 0.11.1 | 2026-11-02, not re-tested | ✅ |  |", lines)
        self.assertIn("| Apache DataFusion | 53.1.0 | 2026-11-09 | ⚠️[^2] |  |", lines)
        self.assertIn("[^1]: PyIceberg 0.12.0, Latest tables: raises on equality deletes", lines)
        self.assertIn("[^2]: Apache DataFusion 53.1.0, Login: needs the `token-refresh` workaround", lines)

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

    def test_unknown_status_is_rejected(self):
        with self.assertRaises(ValueError):
            render(COLUMNS, CLIENTS, [result("pyiceberg", "0.12.0", "2026-11-09", views={"status": "maybe"})])


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


class SummaryTest(unittest.TestCase):
    def test_changes_lists_only_differing_cells(self):
        before = {"s3": {"status": "ok"}, "views": {"status": "na"}, "decimal": {"status": "ok"}}
        after = {"s3": {"status": "ok"}, "views": {"status": "fail"}, "incremental-read": {"status": "ok"}}
        self.assertEqual([("decimal", "ok", None), ("incremental-read", None, "ok"), ("views", "na", "fail")],
                         changes(before, after))


if __name__ == "__main__":
    unittest.main()
