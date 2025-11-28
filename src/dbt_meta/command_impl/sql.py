"""SQL command - Extract SQL code."""

import sys
from typing import Optional

from dbt_meta.command_impl.base import BaseCommand
from dbt_meta.fallback import FallbackLevel


class SqlCommand(BaseCommand):
    """Extract SQL code.

    Returns:
        SQL code as string
        Returns None if model not found.

    Behavior with use_dev=True:
        - Searches dev manifest (target/) FIRST
        - Returns dev-specific SQL
        - NO BigQuery fallback (SQL is dbt-specific)

    Behavior with use_dev=False (default):
        - Searches production manifest (~/dbt-state/) first
        - Falls back to dev manifest if DBT_FALLBACK_TARGET=true
        - NO BigQuery fallback (SQL is dbt-specific)
    """

    SUPPORTS_BIGQUERY = False  # SQL is manifest-only
    SUPPORTS_DEV = True

    def __init__(self, *args, raw: bool = False, **kwargs):
        """Initialize SQL command.

        Args:
            raw: If True, return raw SQL with Jinja. If False, return compiled SQL.
        """
        super().__init__(*args, **kwargs)
        self.raw = raw

    def execute(self) -> Optional[str]:
        """Execute sql command.

        Returns:
            SQL code string, or None if model not found
        """
        model = self.get_model_with_fallback()
        if not model:
            # Print helpful error message
            print(f"❌ SQL code not available for '{self.model_name}': model not in manifest",
                  file=sys.stderr)
            print(f"   Hint: If new model, run 'defer run --select {self.model_name}' first",
                  file=sys.stderr)
            print(f"   Or use 'meta path {self.model_name}' to locate source file",
                  file=sys.stderr)
            return None

        return self.process_model(model)

    def process_model(self, model: dict, level: Optional[FallbackLevel] = None) -> Optional[str]:
        """Process model data and return SQL code.

        Args:
            model: Model data from manifest
            level: Fallback level (not used for sql command)

        Returns:
            SQL code string
        """
        # Return raw or compiled SQL
        # Note: Git warnings are automatically shown by BaseCommand.get_model_with_fallback()
        # if model is modified/new/deleted, so no additional warnings needed here
        if self.raw:
            return model.get('raw_code', '')
        else:
            return model.get('compiled_code', '')
