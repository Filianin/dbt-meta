"""
ManifestFinder - Locate dbt manifest.json with 8-level priority search

Priority order (highest to lowest):
1. DBT_MANIFEST_PATH environment variable (explicit override)
2. ./{DBT_PROD_STATE_PATH}/manifest.json (production - PREFERRED, default: .dbt-state)
3. ./target/manifest.json (current directory)
4. $DBT_PROJECT_PATH/{DBT_PROD_STATE_PATH}/manifest.json (production)
5. $DBT_PROJECT_PATH/target/manifest.json
6. Search upward for {DBT_PROD_STATE_PATH}/manifest.json (production)
7. Search upward for target/manifest.json
8. target/manifest.json (fallback)

Production manifest directory is configurable via DBT_PROD_STATE_PATH (default: .dbt-state).
"""

import os
from pathlib import Path
from typing import Optional


class ManifestFinder:
    """Find dbt manifest.json with production-first priority"""

    @staticmethod
    def find() -> str:
        """
        Find manifest.json using 8-level priority search

        Returns:
            Absolute path to manifest.json

        Raises:
            FileNotFoundError: If no manifest found in any location
        """
        # Get production state path (default: .dbt-state)
        prod_state_path = os.getenv("DBT_PROD_STATE_PATH", ".dbt-state")
        project_path = os.getenv("DBT_PROJECT_PATH")

        # Priority 1: DBT_MANIFEST_PATH environment variable
        if env_path := os.getenv("DBT_MANIFEST_PATH"):
            if Path(env_path).exists():
                return str(Path(env_path).absolute())

        # Priority 2: ./{prod_state_path}/manifest.json (PRODUCTION)
        cwd_prod = Path.cwd() / prod_state_path / "manifest.json"
        if cwd_prod.exists():
            return str(cwd_prod.absolute())

        # Priority 3: ./target/manifest.json (current directory)
        cwd_dev = Path.cwd() / "target" / "manifest.json"
        if cwd_dev.exists():
            return str(cwd_dev.absolute())

        # Priority 4: $DBT_PROJECT_PATH/{prod_state_path}/manifest.json (PRODUCTION)
        if project_path:
            project_prod = Path(project_path) / prod_state_path / "manifest.json"
            if project_prod.exists():
                return str(project_prod.absolute())

            # Priority 5: $DBT_PROJECT_PATH/target/manifest.json
            project_dev = Path(project_path) / "target" / "manifest.json"
            if project_dev.exists():
                return str(project_dev.absolute())

        # Priority 6-7: Search upward for manifest
        if upward_path := ManifestFinder._search_upward(prod_state_path):
            return str(upward_path.absolute())

        # Priority 8: Fallback to target/manifest.json (will likely fail)
        fallback = Path("target/manifest.json")
        if fallback.exists():
            return str(fallback.absolute())

        # No manifest found - raise error with helpful message
        raise FileNotFoundError(
            "No manifest.json found. Searched:\n"
            "  1. DBT_MANIFEST_PATH environment variable\n"
            f"  2. ./{prod_state_path}/manifest.json (production)\n"
            "  3. ./target/manifest.json (dev)\n"
            f"  4. {project_path}/{prod_state_path}/manifest.json (if DBT_PROJECT_PATH set)\n"
            f"  5. {project_path}/target/manifest.json (if DBT_PROJECT_PATH set)\n"
            f"  6. Parent directories for {prod_state_path}/manifest.json\n"
            "  7. Parent directories for target/manifest.json\n"
            "  8. ./target/manifest.json (fallback)\n"
            "\n"
            "Make sure you have run 'dbt compile' or 'dbt parse' to generate manifest.json\n"
            f"Production manifest directory: {prod_state_path} (configure via DBT_PROD_STATE_PATH)"
        )

    @staticmethod
    def _search_upward(prod_state_path: str = ".dbt-state") -> Optional[Path]:
        """
        Search upward from current directory for manifest.json

        Searches for {prod_state_path}/manifest.json first (production priority),
        then target/manifest.json.

        Args:
            prod_state_path: Production state directory name (default: .dbt-state)

        Returns:
            Path to manifest if found, None otherwise
        """
        current = Path.cwd()

        # Search up to root (max 10 levels to avoid infinite loop)
        for _ in range(10):
            # Priority 6: Look for production manifest
            prod_manifest = current / prod_state_path / "manifest.json"
            if prod_manifest.exists():
                return prod_manifest

            # Priority 7: Look for dev manifest
            dev_manifest = current / "target" / "manifest.json"
            if dev_manifest.exists():
                return dev_manifest

            # Move up one directory
            parent = current.parent
            if parent == current:  # Reached root
                break
            current = parent

        return None
