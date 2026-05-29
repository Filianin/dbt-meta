"""Locate lineage.json artifact (mirrors ManifestFinder priority order).

Priority (without --dev):
    1. explicit path argument
    2. DBT_PROD_LINEAGE_PATH env var (if set)
    3. ./target/lineage.json (simple mode)
    4. ~/dbt-state/lineage.json (default prod location)

Priority (with --dev):
    1. explicit path argument
    2. DBT_DEV_LINEAGE_PATH env var (default: ./target/lineage.json)
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Optional


def find_lineage_artifact(
    explicit_path: Optional[str] = None,
    use_dev: bool = False,
) -> str:
    """Find lineage.json with prod-first priority.

    Args:
        explicit_path: Explicit path (highest priority).
        use_dev: If True, search dev lineage path.

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

    if use_dev:
        dev_env = os.getenv("DBT_DEV_LINEAGE_PATH", "./target/lineage.json")
        dev_path = Path(dev_env).expanduser()
        if dev_path.exists():
            return str(dev_path.absolute())
        raise FileNotFoundError(
            f"Dev lineage artifact not found at: {dev_env}\n"
            f"Hint: Run `meta lineage build --dev` first.\n"
            f"      Or set DBT_DEV_LINEAGE_PATH to a custom location."
        )

    prod_env = os.getenv("DBT_PROD_LINEAGE_PATH")
    if prod_env:
        prod_path = Path(prod_env).expanduser()
        if prod_path.exists():
            return str(prod_path.absolute())
        raise FileNotFoundError(
            f"Production lineage artifact not found at: {prod_env}\n"
            f"DBT_PROD_LINEAGE_PATH is set but file doesn't exist."
        )

    simple = Path.cwd() / "target" / "lineage.json"
    if simple.exists():
        return str(simple.absolute())

    default_prod = Path.home() / "dbt-state" / "lineage.json"
    if default_prod.exists():
        return str(default_prod.absolute())

    raise FileNotFoundError(
        "No lineage.json found. Tried:\n"
        "  1. DBT_PROD_LINEAGE_PATH (not set)\n"
        "  2. ./target/lineage.json (not found)\n"
        "  3. ~/dbt-state/lineage.json (not found)\n"
        "\n"
        "BUILD ARTIFACT:\n"
        "  meta lineage build           # build prod artifact\n"
        "  meta lineage build --dev     # build dev artifact\n"
    )
