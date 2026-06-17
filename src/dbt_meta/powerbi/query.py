"""Query the Power BI index — the agent-facing surface.

``find`` answers "dashboard name / metric from a screenshot → which reports and
tables"; ``show`` gives the full breakdown of one report (tables + SQL analysis).
Matching is substring + case-insensitive across report names, table ids and metric
names — good enough to go from a fuzzy screenshot caption to concrete BigQuery
tables.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from .index import PowerBiIndex, ReportEntry


@dataclass
class FindResult:
    reports: list[ReportEntry] = field(default_factory=list)
    metrics: dict[str, list[str]] = field(default_factory=dict)


def _report_matches(report: ReportEntry, needle: str) -> bool:
    if needle in report.report.lower():
        return True
    if needle in report.dataset.lower():
        return True
    return any(needle in t.bq.lower() for t in report.tables)


def find(index: PowerBiIndex, query: str) -> FindResult:
    """Find reports and metrics matching ``query`` (substring, case-insensitive)."""
    needle = query.lower()
    reports = [r for r in index.reports if _report_matches(r, needle)]
    metrics = {
        name: tables
        for name, tables in index.metric_index.items()
        if needle in name.lower()
    }
    return FindResult(reports=reports, metrics=metrics)


def show(index: PowerBiIndex, report_name: str) -> ReportEntry | None:
    """Return one report's full breakdown by exact then substring name match."""
    needle = report_name.lower()
    for report in index.reports:
        if report.report.lower() == needle:
            return report
    for report in index.reports:
        if needle in report.report.lower():
            return report
    return None
