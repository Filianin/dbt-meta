"""
ManifestFinder - Locate dbt manifest.json

Priority order:
1. Explicit manifest path (via --manifest flag or function parameter)
2. DBT_PROD_MANIFEST_PATH (default: ~/dbt-state/manifest.json)
3. DBT_DEV_MANIFEST_PATH (default: ./target/manifest.json) - only when use_dev=True
"""

import os
from pathlib import Path
from typing import Optional


class ManifestFinder:
    """Find dbt manifest.json with production-first priority"""

    @staticmethod
    def find(explicit_path: Optional[str] = None, use_dev: bool = False) -> str:
        """
        Find manifest.json using simplified priority search

        Args:
            explicit_path: Explicit manifest path (from --manifest flag)
            use_dev: If True, use dev manifest (DBT_DEV_MANIFEST_PATH)

        Returns:
            Absolute path to manifest.json

        Raises:
            FileNotFoundError: If no manifest found in any location
        """
        # Priority 1: Explicit path from --manifest flag
        if explicit_path:
            path = Path(explicit_path).expanduser()
            if path.exists():
                return str(path.absolute())
            raise FileNotFoundError(f"Manifest not found at explicit path: {explicit_path}")

        # Priority 2: Dev manifest (if use_dev=True)
        if use_dev:
            dev_manifest_path = os.getenv("DBT_DEV_MANIFEST_PATH", "./target/manifest.json")
            dev_path = Path(dev_manifest_path).expanduser()
            if dev_path.exists():
                return str(dev_path.absolute())
            raise FileNotFoundError(
                f"Dev manifest not found at: {dev_manifest_path}\n"
                f"Set DBT_DEV_MANIFEST_PATH or ensure ./target/manifest.json exists"
            )

        # Priority 3: Production manifest (default)
        prod_manifest_path = os.getenv("DBT_PROD_MANIFEST_PATH", str(Path.home() / "dbt-state" / "manifest.json"))
        prod_path = Path(prod_manifest_path).expanduser()
        if prod_path.exists():
            return str(prod_path.absolute())

        # No manifest found - raise error with helpful message
        raise FileNotFoundError(
            "No production manifest found. Searched:\n"
            f"  DBT_PROD_MANIFEST_PATH (default: ~/dbt-state/manifest.json)\n"
            "\n"
            "SETUP REQUIRED:\n"
            "\n"
            "1. Set manifest path in ~/.zshrc or ~/.bashrc:\n"
            "   export DBT_PROD_MANIFEST_PATH=~/dbt-state/manifest.json\n"
            "\n"
            "2. Place production manifest at the configured location:\n"
            "   mkdir -p ~/dbt-state\n"
            "   cp /path/to/prod/manifest.json ~/dbt-state/\n"
            "\n"
            "3. IMPORTANT: Set up automatic manifest updates (e.g., hourly via cron/CI)\n"
            "   to keep metadata in sync with production state.\n"
            "\n"
            "   Example cron job:\n"
            "   0 * * * * cp /prod/manifest/path/manifest.json ~/dbt-state/manifest.json\n"
            "\n"
            "WHY: dbt-meta requires production manifest to extract metadata.\n"
            "     Without regular updates, metadata becomes stale and queries may fail.\n"
        )
