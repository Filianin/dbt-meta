"""Tests for Power BI data-layer compliance (meta powerbi compliance).

A dashboard reference is compliant only when it resolves to a dbt model in the
``core`` or ``report`` layer with a non-legacy domain. Sources, external tables,
other-layer models and ``domain = legacy`` models are violations; reports with
unparsed source are reported as unverified.
"""

import json

import orjson

import dbt_meta.command_impl.powerbi as cmd
from dbt_meta.powerbi.artifact import save_index
from dbt_meta.powerbi.compliance import (
    ComplianceReport,
    check_compliance,
    classify,
    report_to_dict,
)
from dbt_meta.powerbi.index import (
    PowerBiIndex,
    ReportEntry,
    SqlAnalysisEntry,
    TableRef,
)
from dbt_meta.powerbi.mapper import DbtTableIndex

DB = "admirals-bi-dwh"


def _model(name, schema, layer, alias=None, domain=None):
    config = {"alias": alias or name}
    if domain is not None:
        config["meta"] = {"domain": domain}
    return {
        "resource_type": "model",
        "name": name,
        "database": DB,
        "schema": schema,
        "config": config,
        "fqn": ["proj", layer, name],
    }


MANIFEST = {
    "nodes": {
        "model.proj.core_ok": _model("core_ok", "core_client", "core"),
        "model.proj.report_ok": _model("report_ok", "datamart", "report"),
        "model.proj.stg_bad": _model("stg_bad", "staging", "staging"),
        "model.proj.int_bad": _model("int_bad", "intermediate", "intermediate"),
        "model.proj.core_legacy": _model("core_legacy", "core_x", "core", domain="legacy"),
    },
    "sources": {
        "source.proj.raw.events": {
            "resource_type": "source",
            "name": "events",
            "database": DB,
            "schema": "raw_amas",
            "identifier": "events_raw",
        },
    },
}

IDX = DbtTableIndex(MANIFEST)


# ── classify ──────────────────────────────────────────────────────────────────


class TestClassify:
    def test_core_model_is_compliant(self):
        assert classify(IDX.lookup(f"{DB}.core_client.core_ok")) is None

    def test_report_model_is_compliant(self):
        assert classify(IDX.lookup(f"{DB}.datamart.report_ok")) is None

    def test_staging_model_is_violation(self):
        assert classify(IDX.lookup(f"{DB}.staging.stg_bad")) == "layer:staging"

    def test_intermediate_model_is_violation(self):
        assert classify(IDX.lookup(f"{DB}.intermediate.int_bad")) == "layer:intermediate"

    def test_source_is_violation(self):
        assert classify(IDX.lookup(f"{DB}.raw_amas.events_raw")) == "layer:sources"

    def test_external_is_violation(self):
        assert classify(IDX.lookup("other.foo.bar")) == "layer:external"

    def test_legacy_core_model_is_violation(self):
        assert classify(IDX.lookup(f"{DB}.core_x.core_legacy")) == "domain:legacy"

    def test_legacy_list_domain_is_violation(self):
        """Prod stores domain as a list — a ``["legacy"]`` core model is a violation."""
        idx = DbtTableIndex(
            {"nodes": {"model.p.m": _model("m", "core_l", "core", domain=["legacy"])}}
        )
        assert classify(idx.lookup(f"{DB}.core_l.m")) == "domain:legacy"

    def test_multivalue_domain_with_legacy_is_violation(self):
        idx = DbtTableIndex(
            {"nodes": {"model.p.m": _model("m", "core_l", "core", domain=["crm", "legacy"])}}
        )
        assert classify(idx.lookup(f"{DB}.core_l.m")) == "domain:legacy"

    def test_nonlegacy_list_domain_is_compliant(self):
        idx = DbtTableIndex(
            {"nodes": {"model.p.m": _model("m", "core_l", "core", domain=["crm", "sales"])}}
        )
        assert classify(idx.lookup(f"{DB}.core_l.m")) is None


# ── check_compliance ───────────────────────────────────────────────────────────


def _report(name, ws, tables, sql_analysis=None):
    return ReportEntry(
        workspace=ws,
        report=name,
        dataset="ds",
        tables=[TableRef(bq=t, status="", dbt_model=None) for t in tables],
        sql_analysis=sql_analysis or [],
    )


class TestCheckCompliance:
    def test_all_compliant(self):
        index = PowerBiIndex(
            reports=[
                _report("Clean", "BI", [f"{DB}.core_client.core_ok", f"{DB}.datamart.report_ok"]),
            ]
        )
        rep = check_compliance(index, IDX)
        assert rep.compliant is True
        assert rep.violations == []

    def test_flags_each_violation_type(self):
        index = PowerBiIndex(
            reports=[
                _report(
                    "Mixed",
                    "BI",
                    [
                        f"{DB}.core_client.core_ok",  # ok
                        f"{DB}.staging.stg_bad",  # layer:staging
                        f"{DB}.raw_amas.events_raw",  # layer:sources
                        f"{DB}.core_x.core_legacy",  # domain:legacy
                        "other.foo.bar",  # layer:external
                    ],
                ),
            ]
        )
        rep = check_compliance(index, IDX)
        assert rep.compliant is False
        reasons = {v.table: v.reason for v in rep.violations}
        assert reasons == {
            "staging.stg_bad": "layer:staging",
            "raw_amas.events_raw": "layer:sources",
            "core_x.core_legacy": "domain:legacy",
            "foo.bar": "layer:external",
        }

    def test_owner_attached_from_map(self):
        index = PowerBiIndex(reports=[_report("R", "BI", [f"{DB}.staging.stg_bad"])])
        rep = check_compliance(index, IDX, owners={("BI", "R"): "Ivan Petrov"})
        assert rep.violations[0].owner == "Ivan Petrov"

    def test_dedups_identical_schema_table(self):
        index = PowerBiIndex(
            reports=[
                _report("R", "BI", [f"{DB}.staging.stg_bad", f"{DB}.staging.stg_bad"]),
            ]
        )
        rep = check_compliance(index, IDX)
        assert len(rep.violations) == 1

    def test_unparsed_sql_marks_unverified(self):
        index = PowerBiIndex(
            reports=[
                _report(
                    "R",
                    "BI",
                    [f"{DB}.core_client.core_ok"],
                    sql_analysis=[
                        SqlAnalysisEntry(
                            query="q",
                            tables=(),
                            filters=(),
                            joins=(),
                            group_by=(),
                            parse_status="partial",
                        ),
                    ],
                ),
            ]
        )
        rep = check_compliance(index, IDX)
        # A parse gap never asserts compliance: the report is flagged unverified.
        assert rep.unverified and rep.unverified[0].reason == "parse_error"

    def test_compliant_flag_ignores_unverified(self):
        """compliant reflects confirmed violations only; unverified is separate."""
        index = PowerBiIndex(
            reports=[
                _report(
                    "R",
                    "BI",
                    [f"{DB}.core_client.core_ok"],
                    sql_analysis=[
                        SqlAnalysisEntry(
                            query="q",
                            tables=(),
                            filters=(),
                            joins=(),
                            group_by=(),
                            parse_status="partial",
                        ),
                    ],
                ),
            ]
        )
        rep = check_compliance(index, IDX)
        assert rep.compliant is True
        assert rep.unverified


# ── report_to_dict ──────────────────────────────────────────────────────────


def test_report_to_dict_shape():
    rep = ComplianceReport(compliant=False, violations=[], unverified=[])
    d = report_to_dict(rep)
    assert set(d) == {"compliant", "violations", "unverified"}


# ── compliance_cmd (end-to-end file I/O) ──────────────────────────────────────


def test_compliance_cmd_end_to_end(tmp_path):
    index = PowerBiIndex(
        reports=[
            _report(
                "Sales Overview",
                "BI",
                [
                    f"{DB}.core_client.core_ok",
                    f"{DB}.staging.stg_bad",
                ],
            ),
        ]
    )
    index_path = tmp_path / "powerbi_index.json"
    save_index(index, str(index_path))

    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_bytes(orjson.dumps(MANIFEST))

    raw = {
        "workspaces": [
            {
                "name": "BI",
                "reports": [
                    {
                        "name": "Sales Overview",
                        "users": [
                            {"displayName": "Ivan Petrov", "reportUserAccessRight": "Owner"},
                        ],
                    }
                ],
            }
        ]
    }
    raw_path = tmp_path / "powerbi_raw.json"
    raw_path.write_bytes(orjson.dumps(raw))

    out_path = tmp_path / "powerbi_compliance.json"
    summary = cmd.compliance_cmd(str(index_path), str(raw_path), str(manifest_path), str(out_path))

    assert summary["compliant"] is False
    assert summary["violations"] == 1

    payload = json.loads(out_path.read_text())
    v = payload["violations"][0]
    assert v["dashboard"] == "Sales Overview"
    assert v["table"] == "staging.stg_bad"
    assert v["owner"] == "Ivan Petrov"
    assert v["reason"] == "layer:staging"
