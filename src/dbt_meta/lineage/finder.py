"""Locate lineage.json artifact.

Lineage is a prod-only concept (column-level lineage of the deployed state).
Priority:
    1. explicit path argument
    2. DBT_PROD_LINEAGE_PATH env var (if set)
    3. ~/dbt-state/lineage.json (default prod location)
"""

from __future__ import annotations

import os
from pathlib import Path


def find_lineage_artifact(
    explicit_path: str | None = None,
) -> str:
    """Find lineage.json with prod-first priority.

    Args:
        explicit_path: Explicit path (highest priority).

    Returns:
        Absolute path to lineage.json.

    Raises:
        FileNotFoundError: if no lineage artifact found.
    """
    if explicit_path:
        path = Path(explicit_path).expanduser()
        if path.exists():
            return str(path.absolute())
        raise FileNotFoundError(f"Lineage artifact not found at: {explicit_path}")

    prod_env = os.getenv("DBT_PROD_LINEAGE_PATH")
    if prod_env:
        prod_path = Path(prod_env).expanduser()
        if prod_path.exists():
            return str(prod_path.absolute())
        raise FileNotFoundError(
            f"Production lineage artifact not found at: {prod_env}\n"
            f"DBT_PROD_LINEAGE_PATH is set but file doesn't exist."
        )

    default_prod = Path.home() / "dbt-state" / "lineage.json"
    if default_prod.exists():
        return str(default_prod.absolute())

    raise FileNotFoundError(
        "No lineage.json found. Tried:\n"
        "  1. DBT_PROD_LINEAGE_PATH (not set)\n"
        "  2. ~/dbt-state/lineage.json (not found)\n"
        "\n"
        "BUILD ARTIFACT:\n"
        "  meta lineage build           # build prod artifact\n"
    )
