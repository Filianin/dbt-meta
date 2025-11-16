"""
ManifestFinder - Locate dbt manifest.json

Priority order (without --dev):
1. --manifest PATH (explicit override)
2. DBT_PROD_MANIFEST_PATH (if set - production mode)
3. ./target/manifest.json (simple mode fallback)
4. ~/dbt-state/manifest.json (backward compatibility)

Priority order (with --dev):
1. --manifest PATH (explicit override, ignores --dev)
2. DBT_DEV_MANIFEST_PATH (default: ./target/manifest.json)
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

        Supports two modes:
        - Simple mode: Works out-of-box with ./target/manifest.json (after dbt compile)
        - Production mode: Uses DBT_PROD_MANIFEST_PATH for defer workflow

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
                f"Hint: Run 'defer run --select model_name' first to build dev table\n"
                f"      Or set DBT_DEV_MANIFEST_PATH to custom location"
            )

        # Priority 3: Production manifest (if DBT_PROD_MANIFEST_PATH is set)
        prod_manifest_env = os.getenv("DBT_PROD_MANIFEST_PATH")
        if prod_manifest_env:
            prod_path = Path(prod_manifest_env).expanduser()
            if prod_path.exists():
                return str(prod_path.absolute())
            # Environment variable is set but file doesn't exist - raise error
            raise FileNotFoundError(
                f"Production manifest not found at: {prod_manifest_env}\n"
                f"DBT_PROD_MANIFEST_PATH is set but file doesn't exist.\n"
                f"Ensure manifest file exists at the configured location."
            )

        # Priority 4: Simple mode fallback (./target/manifest.json)
        # This allows dbt-meta to work out-of-box after 'dbt compile'
        simple_mode_path = Path.cwd() / "target" / "manifest.json"
        if simple_mode_path.exists():
            return str(simple_mode_path.absolute())

        # Priority 5: Default production path (backward compatibility)
        default_prod_path = Path.home() / "dbt-state" / "manifest.json"
        if default_prod_path.exists():
            return str(default_prod_path.absolute())

        # No manifest found - raise error with helpful message
        raise FileNotFoundError(
            "No manifest.json found. Tried:\n"
            "  1. DBT_PROD_MANIFEST_PATH (not set)\n"
            "  2. ./target/manifest.json (not found)\n"
            "  3. ~/dbt-state/manifest.json (not found)\n"
            "\n"
            "SIMPLE SETUP (single project):\n"
            "  Run: dbt compile\n"
            "  This creates ./target/manifest.json automatically\n"
            "\n"
            "PRODUCTION SETUP (defer workflow):\n"
            "  1. Set manifest path in ~/.zshrc:\n"
            "     export DBT_PROD_MANIFEST_PATH=~/dbt-state/manifest.json\n"
            "\n"
            "  2. Place production manifest:\n"
            "     mkdir -p ~/dbt-state\n"
            "     cp /path/to/prod/manifest.json ~/dbt-state/\n"
            "\n"
            "  3. Set up auto-update (hourly cron):\n"
            "     0 * * * * cp /prod/path/manifest.json ~/dbt-state/\n"
        )
