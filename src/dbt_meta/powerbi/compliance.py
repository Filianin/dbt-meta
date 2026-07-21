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
# Physical-schema names that map to the curated core/report layers. Used as a
# fallback for tables the manifest does not resolve (built by another project or
# outside dbt), so e.g. ``core_utils`` / ``report_tableau`` are not flagged.
# Matched on a token boundary — the exact name or a ``<name>_`` prefix — so
# ``core``/``core_utils``/``report_tableau`` pass but ``reporting_stale`` and
# ``coreanything`` (which are NOT the core/report layer) do not.
APPROVED_SCHEMA_ROOTS = ("core", "report")
LEGACY_DOMAIN = "legacy"
# Power BI republishes an App copy of a report as "[App] <name>": it loses owner
# metadata and always mirrors a subset of the base report's tables, so it is a
# pure duplicate we skip to avoid double-counting.
APP_REPORT_PREFIX = "[App] "


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
    owner: str | None = None


@dataclass
class ComplianceReport:
    compliant: bool
    violations: list[Violation] = field(default_factory=list)
    unverified: list[Unverified] = field(default_factory=list)


def _schema_table(bq: str) -> str:
    """``project.schema.table`` -> ``schema.table`` (physical, project dropped)."""
    parts = bq.split(".")
    return ".".join(parts[-2:]) if len(parts) >= 2 else bq


def _schema(bq: str) -> str:
    """``project.schema.table`` -> ``schema`` (physical schema segment)."""
    parts = bq.split(".")
    return parts[-2] if len(parts) >= 2 else ""


def _schema_root_approved(schema: str) -> bool:
    """True when ``schema`` is an approved root, matched on a token boundary.

    Approves the exact root (``core``) or a ``<root>_`` prefix (``core_utils``),
    but never a bare-prefix collision like ``reporting_stale``/``coreanything``.
    """
    return any(
        schema == root or schema.startswith(f"{root}_")
        for root in APPROVED_SCHEMA_ROOTS
    )


def classify(mapping: TableMapping) -> str | None:
    """Return a violation ``reason``, or ``None`` when the table is compliant.

    Hybrid classification:

    * a manifest-resolved ``model`` is judged by its dbt layer — approved only in
      ``core`` / ``report`` (a legacy-domain approved model is ``domain:legacy``),
      otherwise ``layer:<layer>`` (e.g. ``layer:staging``);
    * a declared ``source`` is always ``layer:sources``;
    * a table the manifest does not resolve falls back to the physical schema
      convention — ``core`` / ``core_*`` / ``report`` / ``report_*`` schemas are
      approved (token boundary, not bare prefix), everything else is
      ``layer:external``.
    """
    if mapping.status == "model":
        if mapping.layer in APPROVED_LAYERS:
            if LEGACY_DOMAIN in mapping.domain:
                return "domain:legacy"
            return None
        return f"layer:{mapping.layer}"
    if mapping.status == "source":
        return "layer:sources"
    if _schema_root_approved(_schema(mapping.bq)):
        return None
    return "layer:external"


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
    exclude_workspaces: set[str] | None = None,
) -> ComplianceReport:
    """Classify every report in ``index`` against the dbt manifest.

    ``owners`` maps ``(workspace, report)`` to the owner's display name (from the
    raw scan), attached to each violation for @mention resolution downstream.

    ``exclude_workspaces`` names workspaces dropped from the check entirely (e.g.
    dev sandboxes) — their reports produce neither violations nor unverified.
    """
    owners = owners or {}
    excluded = exclude_workspaces or set()
    violations: list[Violation] = []
    unverified: list[Unverified] = []
    for report in index.reports:
        if report.workspace in excluded:
            continue
        if report.report.startswith(APP_REPORT_PREFIX):
            continue  # App-published duplicate of the base report
        owner = owners.get((report.workspace, report.report))
        report_violations, unparsed = check_report(report, dbt_index, owner)
        violations.extend(report_violations)
        if unparsed:
            unverified.append(
                Unverified(
                    dashboard=report.report,
                    workspace=report.workspace,
                    reason="parse_error",
                    owner=owner,
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
            {
                "dashboard": u.dashboard,
                "workspace": u.workspace,
                "reason": u.reason,
                "owner": u.owner,
            }
            for u in report.unverified
        ],
    }
