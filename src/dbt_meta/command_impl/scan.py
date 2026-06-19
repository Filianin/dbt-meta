"""Scan command - Estimate query scan size using BigQuery dry run."""

import sys
from typing import Any, Optional

from dbt_meta.command_impl.base import BaseCommand
from dbt_meta.fallback import FallbackLevel
from dbt_meta.utils.bigquery import format_bytes, run_dry_run_query
from dbt_meta.utils.compiled_sql import get_compiled_sql
from dbt_meta.utils.dev import find_dev_manifest as _find_dev_manifest


class ScanCommand(BaseCommand):
    """Estimate query scan size using BigQuery dry run.

    Uses `bq query --dry_run` to estimate bytes scanned without executing.

    Returns:
        Dictionary with scan estimate:
        - model: Model name
        - bytes: Estimated bytes to scan
        - formatted: Human-readable size (e.g., "1.5 GB")
        - error: Error message if validation failed

    Behavior:
        - Fetches compiled SQL (manifest → target/compiled → dbt compile)
        - Runs dry_run to get estimated scan size
        - Does NOT execute the query
    """

    SUPPORTS_BIGQUERY = False  # Needs compiled SQL from manifest
    SUPPORTS_DEV = True

    def execute(self) -> Optional[dict[str, Any]]:
        """Execute scan command.

        Returns:
            Scan estimate dict, or None if model not found
        """
        model = self.get_model_with_fallback()
        if not model:
            print(f"❌ Cannot estimate scan for '{self.model_name}': model not in manifest",
                  file=sys.stderr)
            return None

        return self.process_model(model)

    def process_model(self, model: dict[str, Any], level: Optional[FallbackLevel] = None) -> dict[str, Any]:
        """Estimate query scan size.

        Args:
            model: Model data from manifest
            level: Fallback level (not used)

        Returns:
            Scan estimate dict
        """
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
                'bytes': None,
                'formatted': None,
                'error': error or 'No compiled SQL available',
            }

        result = run_dry_run_query(sql)

        if result['valid']:
            bytes_processed = result.get('bytes_processed')
            return {
                'model': self.model_name,
                'bytes': bytes_processed,
                'formatted': format_bytes(bytes_processed) if bytes_processed else None,
                'error': None
            }
        else:
            return {
                'model': self.model_name,
                'bytes': None,
                'formatted': None,
                'error': result.get('error')
            }
