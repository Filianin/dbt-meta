"""Validate command - Validate SQL syntax using BigQuery dry run."""

import sys
from typing import Optional

from dbt_meta.command_impl.base import BaseCommand
from dbt_meta.fallback import FallbackLevel
from dbt_meta.utils.bigquery import run_dry_run_query
from dbt_meta.utils.compiled_sql import get_compiled_sql
from dbt_meta.utils.dev import find_dev_manifest as _find_dev_manifest


class ValidateCommand(BaseCommand):
    """Validate model SQL syntax using BigQuery dry run.

    Uses `bq query --dry_run` to validate SQL without executing it.

    Returns:
        Dictionary with validation result:
        - model: Model name
        - valid: True if SQL is valid
        - error: Error message if invalid (None if valid)

    Behavior:
        - Fetches compiled SQL (manifest → target/compiled → dbt compile)
        - Validates against BigQuery (checks syntax, table/column existence)
        - Does NOT execute the query
    """

    SUPPORTS_BIGQUERY = False  # Needs compiled SQL from manifest
    SUPPORTS_DEV = True

    def execute(self) -> Optional[dict]:
        """Execute validate command.

        Returns:
            Validation result dict, or None if model not found
        """
        model = self.get_model_with_fallback()
        if not model:
            print(f"❌ Cannot validate '{self.model_name}': model not in manifest",
                  file=sys.stderr)
            return None

        return self.process_model(model)

    def process_model(self, model: dict, level: Optional[FallbackLevel] = None) -> dict:
        """Validate model SQL.

        Args:
            model: Model data from manifest
            level: Fallback level (not used)

        Returns:
            Validation result dict
        """
        # When validating dev SQL, infer project root from the dev manifest
        # so that disk lookup (target/compiled/) and auto-compile run in the
        # correct project directory, not relative to the prod manifest path.
        manifest_for_sql = self.manifest_path
        if self.use_dev:
            manifest_for_sql = _find_dev_manifest(self.manifest_path) or self.manifest_path

        sql, error = get_compiled_sql(
            model=model,
            model_name=self.model_name,
            manifest_path=manifest_for_sql,
            use_dev=self.use_dev,
        )
        if sql is None:
            return {
                'model': self.model_name,
                'valid': False,
                'error': error or 'No compiled SQL available',
            }

        result = run_dry_run_query(sql)

        return {
            'model': self.model_name,
            'valid': result['valid'],
            'error': result.get('error')
        }
