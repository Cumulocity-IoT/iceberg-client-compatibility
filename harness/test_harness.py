import unittest

from harness.redact import redact
from harness.render import render, version_key

COLUMNS = [
    {"id": "catalog-login", "title": "Login"},
    {"id": "latest-tables", "title": "Latest tables"},
    {"id": "views", "title": "Views"},
]
CLIENTS = {
    "pyiceberg": {"name": "PyIceberg", "section": "library"},
    "iceberg-rust": {"name": "iceberg-rust", "section": "library"},
    "datafusion": {"name": "Apache DataFusion", "section": "engine", "built_on": "iceberg-rust"},
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
        self.assertNotIn("Views", table)  # no result carries the column
        self.assertIn("| PyIceberg | 0.12.0 | 2026-11-09 | ✅ | ❌[^1] |", lines)
        self.assertIn("| PyIceberg | 0.11.1 | 2026-11-02, not re-tested | ✅ |  |", lines)
        self.assertIn("| Apache DataFusion | 53.1.0 | iceberg-rust | 2026-11-09 | ⚠️[^2] |  |", lines)
        self.assertIn("[^1]: PyIceberg 0.12.0, Latest tables: raises on equality deletes", lines)
        self.assertIn("[^2]: Apache DataFusion 53.1.0, Login: needs the `token-refresh` workaround", lines)

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
        }
        for raw, expected in cases.items():
            self.assertIn(expected, redact(raw), raw)
            self.assertNotIn("secret", redact(raw).replace("client_secret", ""), raw)

    def test_harmless_text_is_kept(self):
        text = '{"token_type": "bearer", "issued_token_type": "urn:ietf:params:oauth:token-type:access_token"}'
        self.assertEqual(text, redact(text))


if __name__ == "__main__":
    unittest.main()
