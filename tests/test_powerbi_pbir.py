"""PBIR (one file per page / visual) layout support.

The fixtures are the classic ``report.json`` fixtures rewritten into PBIR
files by ``_to_pbir`` — an independent model of the format — so every test
can assert "PBIR parses exactly like the same report stored the old way".
"""

import base64
import json

import orjson
import pytest

from dbt_meta.command_impl import powerbi as cmd
from dbt_meta.powerbi.index import build_index
from dbt_meta.powerbi.pbir_parser import (
    parse_pbir_legacy,
    parse_report_filters,
    pbir_parts_to_legacy,
)
from dbt_meta.utils import powerbi as pb
from tests import powerbi_fixtures as fx


def _filters(raw):
    """Classic filters (JSON string) → PBIR ``filterConfig`` (``field`` key)."""
    items = json.loads(raw) if isinstance(raw, str) else (raw or [])
    out = []
    for f in items:
        f = dict(f)
        f["field"] = f.pop("expression")
        out.append(f)
    return {"filters": out}


def _to_pbir(report):
    """Rewrite a classic report.json dict into decoded PBIR parts."""
    parts = {"definition/report.json": {"filterConfig": _filters(report.get("filters"))}}
    order = []
    for i, section in enumerate(report["sections"]):
        # Folder names sort opposite to the page order: pageOrder must win.
        folder = f"p{9 - i}"
        order.append(folder)
        base = f"definition/pages/{folder}"
        parts[f"{base}/page.json"] = {
            "name": section["name"],
            "displayName": section["displayName"],
            "filterConfig": _filters(section.get("filters")),
        }
        for j, vc in enumerate(section["visualContainers"]):
            single = json.loads(vc["config"])["singleVisual"]
            visual = {
                "visualType": single["visualType"],
                "query": {
                    "queryState": {
                        role: {
                            "projections": [{"field": {}, "queryRef": i["queryRef"]} for i in items]
                        }
                        for role, items in single["projections"].items()
                    }
                },
            }
            if "vcObjects" in single:
                visual["visualContainerObjects"] = single["vcObjects"]
            parts[f"{base}/visuals/v{j:02d}/visual.json"] = {
                "name": f"v{j}",
                "position": {"x": 0, "y": 0},
                "visual": visual,
                "filterConfig": _filters(vc.get("filters")),
            }
    parts["definition/pages/pages.json"] = {"pageOrder": order}
    return parts


def _encode(parts, extra=()):
    encoded = [
        {"path": path, "payload": base64.b64encode(orjson.dumps(data)).decode()}
        for path, data in parts.items()
    ]
    return {"definition": {"parts": encoded + list(extra)}}


@pytest.mark.parametrize("report", [fx.PBIR_LEGACY_REPORT, fx.PBIR_LEGACY_SEMANTICS])
def test_pbir_parses_like_the_classic_report(report):
    folded = pbir_parts_to_legacy(_to_pbir(report))
    assert parse_pbir_legacy(folded, {"total_revenue"}) == parse_pbir_legacy(
        report, {"total_revenue"}
    )
    assert parse_report_filters(folded) == parse_report_filters(report)


def test_semantics_survive_the_fold():
    pages = parse_pbir_legacy(pbir_parts_to_legacy(_to_pbir(fx.PBIR_LEGACY_SEMANTICS)))
    bar = pages[0].visuals[0]
    assert bar.title == "Revenue by stage"
    assert bar.filters and bar.filters[0].op == "cmp"
    assert pages[0].filters[0].op == "relative_date"


class TestFoldEdgeCases:
    def test_pages_missing_from_page_order_follow_sorted(self):
        parts = {
            "definition/pages/pages.json": {"pageOrder": ["b", "ghost"]},
            "definition/pages/c/page.json": {"displayName": "C"},
            "definition/pages/a/page.json": {"displayName": "A"},
            "definition/pages/b/page.json": {"displayName": "B"},
        }
        names = [s["displayName"] for s in pbir_parts_to_legacy(parts)["sections"]]
        assert names == ["B", "A", "C"]

    def test_no_page_order_and_name_falls_back_to_folder(self):
        parts = {"definition/pages/x/page.json": {}}
        section = pbir_parts_to_legacy(parts)["sections"][0]
        assert section["name"] == "x"
        assert section["displayName"] == ""
        assert section["visualContainers"] == []

    def test_visual_groups_and_malformed_visuals_are_skipped(self):
        base = "definition/pages/p"
        parts = {
            f"{base}/page.json": {"name": "p"},
            f"{base}/visuals/g/visual.json": {"name": "g", "visualGroup": {"displayName": "G"}},
            f"{base}/visuals/bad/visual.json": ["not", "a", "dict"],
            f"{base}/visuals/nq/visual.json": {"visual": {"visualType": "card", "query": "x"}},
            f"{base}/visuals/np/visual.json": {
                "visual": {
                    "visualType": "card",
                    "query": {"queryState": {"Values": {"projections": "x"}, "Y": "x"}},
                }
            },
            f"{base}/visuals/v/mobile.json": {"visual": {"visualType": "card"}},
            f"{base}/other.json": {},
        }
        section = pbir_parts_to_legacy(parts)["sections"][0]
        assert len(section["visualContainers"]) == 2
        pages = parse_pbir_legacy(pbir_parts_to_legacy(parts))
        assert [v.type for v in pages[0].visuals] == ["card", "card"]
        assert all(v.fields == {} for v in pages[0].visuals)

    def test_malformed_filter_config_is_ignored(self):
        parts = {
            "definition/report.json": {"filterConfig": {"filters": "x"}},
            "definition/pages/p/page.json": {"filterConfig": ["x"]},
        }
        folded = pbir_parts_to_legacy(parts)
        assert folded["filters"] == []
        assert folded["sections"][0]["filters"] == []

    def test_non_dict_filter_entries_and_missing_report_part(self):
        parts = {"definition/pages/p/page.json": {"filterConfig": {"filters": [1, None]}}}
        folded = pbir_parts_to_legacy(parts)
        assert folded["filters"] == []
        assert folded["sections"][0]["filters"] == []

    def test_classic_expression_key_is_kept(self):
        f = {"expression": {"a": 1}, "field": {"b": 2}}
        parts = {"definition/pages/p/page.json": {"filterConfig": {"filters": [f]}}}
        kept = pbir_parts_to_legacy(parts)["sections"][0]["filters"][0]
        assert kept["expression"] == {"a": 1}

    def test_empty(self):
        assert pbir_parts_to_legacy({}) == {"sections": [], "filters": []}


class TestFetchLayout:
    def test_pbir_format_is_requested_and_decoded(self, monkeypatch):
        urls = []
        body = orjson.dumps(
            _encode(
                _to_pbir(fx.PBIR_LEGACY_REPORT),
                extra=[
                    {"path": "StaticResources/logo.png", "payload": "iVBORw0K"},
                    {"path": "definition/broken.json", "payload": "!!!"},
                ],
            )
        ).decode()

        def fake(token, url, method="GET", body_=None, timeout=90, **k):
            urls.append(url)
            return 200, {}, body

        monkeypatch.setattr(pb, "_fabric_call", fake)
        report, reason = pb.fetch_report_layout("t", "ws", "r", fmt="PBIR")
        assert reason == ""
        assert urls[0].endswith("/getDefinition?format=PBIR")
        assert [s["displayName"] for s in report["sections"]] == ["PPC Reg Cohorts", "Detail"]

    def test_pbir_report_json_alone_is_not_a_classic_layout(self, monkeypatch):
        body = orjson.dumps(_encode({"definition/report.json": {}})).decode()
        monkeypatch.setattr(pb, "_fabric_call", lambda *a, **k: (200, {}, body))
        report, reason = pb.fetch_report_layout("t", "ws", "r", fmt="PBIR")
        assert report is None
        assert "no report.json or PBIR pages" in reason
        assert "definition/report.json" in reason

    def test_reason_lists_at_most_five_parts(self, monkeypatch):
        extra = [{"path": f"f{i}.png", "payload": "x"} for i in range(7)]
        body = orjson.dumps({"definition": {"parts": extra}}).decode()
        monkeypatch.setattr(pb, "_fabric_call", lambda *a, **k: (200, {}, body))
        _, reason = pb.fetch_report_layout("t", "ws", "r")
        assert reason.endswith("f4.png, ...)")

    def test_empty_parts_reason(self, monkeypatch):
        body = orjson.dumps({"definition": {"parts": []}}).decode()
        monkeypatch.setattr(pb, "_fabric_call", lambda *a, **k: (200, {}, body))
        assert pb.fetch_report_layout("t", "ws", "r")[1].endswith("(parts: none)")

    @pytest.mark.parametrize(
        ("code", "body", "reason"),
        [
            (
                400,
                '{"errorCode": "InvalidDefinitionFormat", "message": "Format not supported"}',
                "HTTP 400 (InvalidDefinitionFormat: Format not supported)",
            ),
            (401, '{"error": {"code": "TokenExpired"}}', "HTTP 401 (TokenExpired)"),
            (403, "denied", "HTTP 403"),
            (404, "{}", "HTTP 404"),
            (502, "[1, 2]", "HTTP 502"),
            (500, '{"error": "x"}', "HTTP 500"),
            (0, "", "request failed (curl missing, timeout or network error)"),
            (200, "{bad", "getDefinition returned malformed JSON"),
            (200, "{}", "getDefinition returned no definition"),
            (202, "", "getDefinition 202 without a Location header"),
        ],
    )
    def test_direct_failure_reasons(self, monkeypatch, code, body, reason):
        monkeypatch.setattr(pb, "_fabric_call", lambda *a, **k: (code, {}, body))
        assert pb.fetch_report_layout("t", "ws", "r", poll_interval=0) == (None, reason)

    @pytest.mark.parametrize(
        ("status_body", "result_body", "reason"),
        [
            (
                '{"status": "Failed", "error": {"errorCode": "PbirNotSupported"}}',
                "",
                "operation failed (PbirNotSupported)",
            ),
            ('{"status": "Failed"}', "", "operation failed"),
            ("{bad", "", "operation status returned malformed JSON"),
            ('{"status": "Running"}', "", "operation still running after 2 polls"),
            ('{"status": "Succeeded"}', "{bad", "operation result returned malformed JSON"),
        ],
    )
    def test_lro_failure_reasons(self, monkeypatch, status_body, result_body, reason):
        def fake(token, url, method="GET", body=None, timeout=90):
            if method == "POST":
                return 202, {"location": "https://poll", "retry-after": "0"}, ""
            if url.endswith("/result"):
                return 200, {}, result_body
            return 200, {}, status_body

        monkeypatch.setattr(pb, "_fabric_call", fake)
        got = pb.fetch_report_layout("t", "ws", "r", poll_interval=0, max_polls=2)
        assert got == (None, reason)

    def test_get_report_definition_passes_format(self, monkeypatch):
        seen = {}

        def fake(token, ws, rid, fmt, *a):
            seen["fmt"] = fmt
            return {"sections": []}, ""

        monkeypatch.setattr(pb, "fetch_report_layout", fake)
        assert pb.get_report_definition("t", "ws", "r", fmt="PBIR") == {"sections": []}
        assert seen["fmt"] == "PBIR"


def _scan(*reports):
    return {
        "workspaces": [
            {
                "id": "ws1",
                "name": "BI Trading",
                "datasets": [{"id": "ds1", "name": "DS", "tables": []}],
                "reports": [dict(r, datasetId="ds1") for r in reports],
            }
        ]
    }


class TestEnrichFormatsAndCopies:
    def test_pbir_report_is_fetched_as_pbir(self, monkeypatch):
        scan = _scan(
            {"id": "a", "name": "Legacy"},
            {"id": "b", "name": "Payment Systems", "format": "PBIR"},
        )
        index = build_index(scan, {"nodes": {}})
        fmts = {}

        def fake(token, ws, rid, fmt="PBIR-Legacy", timeout=90):
            fmts[rid] = fmt
            return fx.PBIR_LEGACY_REPORT, ""

        monkeypatch.setattr(cmd, "fetch_report_layout", fake)
        assert cmd.enrich_with_layouts(index, scan, "tok") == (2, 2)
        assert fmts == {"a": "PBIR-Legacy", "b": "PBIR"}

    def test_app_copy_takes_the_original_layout_without_a_fetch(self, monkeypatch, capsys):
        scan = _scan(
            {"id": "copy", "name": "[App] Retention", "originalReportObjectId": "orig"},
            {"id": "orig", "name": "Retention", "format": "PBIR"},
        )
        index = build_index(scan, {"nodes": {}})
        fetched = []

        def fake(token, ws, rid, fmt="PBIR-Legacy", timeout=90):
            fetched.append(rid)
            return fx.PBIR_LEGACY_SEMANTICS, ""

        monkeypatch.setattr(cmd, "fetch_report_layout", fake)
        assert cmd.enrich_with_layouts(index, scan, "tok") == (2, 2)
        assert fetched == ["orig"]
        copy, orig = index.reports
        assert copy.pages == orig.pages and copy.pages
        assert copy.filters == orig.filters and copy.filters
        assert capsys.readouterr().err == ""

    def test_skips_are_logged_with_reasons(self, monkeypatch, capsys):
        scan = _scan(
            {"id": "bad", "name": "Broken", "format": "PBIR"},
            {"id": "empty", "name": "Empty"},
            {"id": "c1", "name": "[App] Broken", "originalReportObjectId": "bad"},
            {"id": "c2", "name": "[App] Gone", "originalReportObjectId": "elsewhere"},
            {"id": "", "name": "No id"},
        )
        index = build_index(scan, {"nodes": {}})

        def fake(token, ws, rid, fmt="PBIR-Legacy", timeout=90):
            if rid == "bad":
                return None, "HTTP 400 (InvalidDefinitionFormat)"
            return {"sections": []}, ""

        monkeypatch.setattr(cmd, "fetch_report_layout", fake)
        assert cmd.enrich_with_layouts(index, scan, "tok") == (0, 5)
        err = capsys.readouterr().err.splitlines()
        assert err == [
            "layout skipped: BI Trading / Broken: PBIR: HTTP 400 (InvalidDefinitionFormat)",
            "layout skipped: BI Trading / Empty: PBIR-Legacy: definition has no pages",
            "layout skipped: BI Trading / No id: no workspace or report id in the scan",
            "layout skipped: BI Trading / [App] Broken: app copy: original bad has no layout",
            "layout skipped: BI Trading / [App] Gone: app copy: original elsewhere is not in the scan",
        ]
        assert all(r.pages == [] for r in index.reports)
