"""Power BI Admin Scanner API client — full multi-workspace metadata dump.

Async scan flow: ``getInfo`` (start) → poll ``scanStatus`` → ``scanResult``. Up to
100 workspaces fit in one ``getInfo`` call, so the team's 5 workspaces go in a
single batch. Full enrichment flags are requested so the result carries
M-expressions, dataset schema, lineage, datasource details and artifact users.

Auth and the HTTP boundary are reused from :mod:`dbt_meta.utils.powerbi`
(``curl`` under the hood). The token is passed in by the caller and never written
to disk.
"""

from __future__ import annotations

import time
from typing import Any

from ..utils.powerbi import _call_powerbi_api, get_powerbi_token

# Full enrichment — everything the index builder and reverse index need.
SCAN_FLAGS = (
    "lineage=true"
    "&datasourceDetails=true"
    "&datasetSchema=true"
    "&datasetExpressions=true"
    "&getArtifactUsers=true"
)

__all__ = ["SCAN_FLAGS", "get_powerbi_token", "scan_workspaces"]

# User emails (``emailAddress`` and the ``identifier`` UPN, which is the email in
# practice) are PII. ``displayName`` and the opaque Azure AD ``graphId`` are kept
# so users stay trackable without storing emails.
_PII_USER_FIELDS = ("emailAddress", "identifier")

# Scalar UPN/email fields attached to artifacts (reports, dashboards, datasets,
# dataflows). These are full user emails — dropped entirely from the scanResult
# so no PII reaches disk. Downstream readers (e.g. ``powerbi owners``) surface
# them as ``null``.
_PII_SCALAR_FIELDS = ("modifiedBy", "createdBy", "configuredBy")


def _scrub_pii(node: Any) -> None:
    """Recursively strip user emails from a scanResult, in place.

    Two PII shapes are removed:

    * ``users`` arrays (workspace / dashboard / dataset / report level) — drop
      the email fields from each user object, keep names.
    * Scalar ``modifiedBy`` / ``createdBy`` / ``configuredBy`` on any artifact —
      these are full UPN/email strings; remove the key entirely.
    """
    if isinstance(node, dict):
        users = node.get("users")
        if isinstance(users, list):
            for user in users:
                if isinstance(user, dict):
                    for field in _PII_USER_FIELDS:
                        user.pop(field, None)
        for field in _PII_SCALAR_FIELDS:
            node.pop(field, None)
        for value in node.values():
            _scrub_pii(value)
    elif isinstance(node, list):
        for item in node:
            _scrub_pii(item)


def scan_workspaces(
    token: str,
    workspace_ids: list[str],
    poll_interval: int = 2,
    max_polls: int = 120,
    timeout: int = 60,
) -> dict[str, Any] | None:
    """Scan ``workspace_ids`` and return the full scanResult, or ``None`` on error."""
    if not workspace_ids:
        return None

    started = _call_powerbi_api(
        token,
        f"/admin/workspaces/getInfo?{SCAN_FLAGS}",
        method="POST",
        data={"workspaces": list(workspace_ids)},
        timeout=timeout,
    )
    if not started or "id" not in started:
        return None
    scan_id = started["id"]

    status_endpoint = f"/admin/workspaces/scanStatus/{scan_id}"
    for _ in range(max_polls):
        status = _call_powerbi_api(token, status_endpoint, timeout=timeout)
        if not status:
            return None
        state = status.get("status")
        if state == "Succeeded":
            break
        if state in ("Failed", "Cancelled"):
            return None
        if poll_interval:
            time.sleep(poll_interval)
    else:
        return None

    result = _call_powerbi_api(
        token, f"/admin/workspaces/scanResult/{scan_id}", timeout=timeout
    )
    if result and "workspaces" in result:
        _scrub_pii(result)
        return result
    return None
