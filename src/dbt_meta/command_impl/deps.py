"""Deps command - Extract dependencies by type."""

import sys
from typing import Optional

from dbt_meta.command_impl.base import BaseCommand
from dbt_meta.fallback import FallbackLevel


class DepsCommand(BaseCommand):
    """Extract dependencies by type.

    Returns:
        Dictionary with:
        - refs: List of model dependencies
        - sources: List of source dependencies
        - macros: List of macro dependencies

        Returns None if model not found.

    Behavior with use_dev=True:
        - Searches dev manifest (target/) FIRST
        - Returns dev-specific dependencies
        - NO BigQuery fallback (lineage is manifest-only)

    Behavior with use_dev=False (default):
        - Searches production manifest (~/dbt-state/) first
        - Falls back to dev manifest if DBT_FALLBACK_TARGET=true
        - NO BigQuery fallback (dependencies are dbt-specific)
    """

    SUPPORTS_BIGQUERY = False  # Dependencies are manifest-only
    SUPPORTS_DEV = True

    def execute(self) -> Optional[dict[str, list[str]]]:
        """Execute deps command.

        Returns:
            Dependencies dictionary (always returns dict with refs/sources/macros)
            Even for nonexistent models, returns {'refs': [], 'sources': []}
        """
        from dbt_meta.utils import get_cached_parser as _get_cached_parser

        # Use parser to get dependencies (returns {'refs': [], 'sources': []} for missing models)
        parser = _get_cached_parser(self.manifest_path)
        result = parser.get_dependencies(self.model_name)

        if result is None:
            # Print helpful error message for truly missing models
            print(f"❌ Dependencies not available for '{self.model_name}': model not in manifest",
                  file=sys.stderr)
            print("   Dependencies are dbt-specific (refs, sources, macros) and cannot be inferred from BigQuery.",
                  file=sys.stderr)
            print(f"   Hint: Run 'defer run --select {self.model_name}' to add model to manifest",
                  file=sys.stderr)

        return result

    def process_model(self, model: dict, level: Optional[FallbackLevel] = None) -> Optional[dict[str, list[str]]]:
        """Process model data and return dependencies.

        Args:
            model: Model data from manifest
            level: Fallback level (not used for deps command)

        Returns:
            Dependencies dictionary
        """
        # Not used - execute() directly calls parser.get_dependencies()
        pass
