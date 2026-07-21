"""Data-layer compliance for Power BI dashboards.

Verifies that every BigQuery table a Power BI report reads resolves to a curated
dbt model in the ``core`` or ``report`` layer (and is not a ``domain = legacy``
model). Anything else is a violation:

* a model in ``intermediate`` / ``staging`` / ``deprecated`` (or any layer other
  than ``core`` / ``report``);
* a declared dbt ``source`` (raw layer);
* an ``external`` table not in the dbt project at all;
* an approved-layer model tagged ``domain = legacy``.

Reports whose source could not be parsed (native SQL the analyzer choked on)
are reported separately as *unverified* — we never assert compliance we could
not verify.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .index import PowerBiIndex, ReportEntry
from .mapper import DbtTableIndex, TableMapping

APPROVED_LAYERS = frozenset({"core", "report"})
LEGACY_DOMAIN = "legacy"


@dataclass(frozen=True)
class Violation:
    """One dashboard → out-of-policy table reference."""

    dashboard: str
    workspace: str
    table: str  # physical schema.table
    owner: str | None
    reason: str  # layer:<name> | domain:legacy


@dataclass(frozen=True)
class Unverified:
    """A dashboard whose data sources could not be fully resolved."""

    dashboard: str
    workspace: str
    reason: str  # parse_error


@dataclass
class ComplianceReport:
    compliant: bool
    violations: list[Violation] = field(default_factory=list)
    unverified: list[Unverified] = field(default_factory=list)


def _schema_table(bq: str) -> str:
    """``project.schema.table`` -> ``schema.table`` (physical, project dropped)."""
    parts = bq.split(".")
    return ".".join(parts[-2:]) if len(parts) >= 2 else bq


def classify(mapping: TableMapping) -> str | None:
    """Return a violation ``reason``, or ``None`` when the table is compliant.

    Approved = ``model`` in an approved layer with a non-legacy domain. A legacy
    approved-layer model is ``domain:legacy``; everything else is
    ``layer:<layer or status>`` (e.g. ``layer:staging``, ``layer:sources``,
    ``layer:external``).
    """
    if mapping.status == "model" and mapping.layer in APPROVED_LAYERS:
        if LEGACY_DOMAIN in mapping.domain:
            return "domain:legacy"
        return None
    layer = mapping.layer or mapping.status
    return f"layer:{layer}"


def check_report(
    report: ReportEntry, dbt_index: DbtTableIndex, owner: str | None
) -> tuple[list[Violation], bool]:
    """Violations for one report, plus whether it had unparsed source."""
    violations: list[Violation] = []
    seen: set[str] = set()
    for tref in report.tables:
        reason = classify(dbt_index.lookup(tref.bq))
        if reason is None:
            continue
        st = _schema_table(tref.bq)
        if st in seen:  # dedup identical (dashboard, schema.table)
            continue
        seen.add(st)
        violations.append(
            Violation(
                dashboard=report.report,
                workspace=report.workspace,
                table=st,
                owner=owner,
                reason=reason,
            )
        )
    unparsed = any(s.parse_status != "ok" for s in report.sql_analysis) or any(
        t.parse_status != "ok" for t in report.tables
    )
    return violations, unparsed


def check_compliance(
    index: PowerBiIndex,
    dbt_index: DbtTableIndex,
    owners: dict[tuple[str, str], str] | None = None,
) -> ComplianceReport:
    """Classify every report in ``index`` against the dbt manifest.

    ``owners`` maps ``(workspace, report)`` to the owner's display name (from the
    raw scan), attached to each violation for @mention resolution downstream.
    """
    owners = owners or {}
    violations: list[Violation] = []
    unverified: list[Unverified] = []
    for report in index.reports:
        owner = owners.get((report.workspace, report.report))
        report_violations, unparsed = check_report(report, dbt_index, owner)
        violations.extend(report_violations)
        if unparsed:
            unverified.append(
                Unverified(
                    dashboard=report.report,
                    workspace=report.workspace,
                    reason="parse_error",
                )
            )
    return ComplianceReport(
        compliant=not violations,
        violations=violations,
        unverified=unverified,
    )


def report_to_dict(report: ComplianceReport) -> dict[str, Any]:
    return {
        "compliant": report.compliant,
        "violations": [
            {
                "dashboard": v.dashboard,
                "workspace": v.workspace,
                "table": v.table,
                "owner": v.owner,
                "reason": v.reason,
            }
            for v in report.violations
        ],
        "unverified": [
            {"dashboard": u.dashboard, "workspace": u.workspace, "reason": u.reason}
            for u in report.unverified
        ],
    }
