"""Cost command - Estimate query scan size using BigQuery dry run."""

import sys
from typing import Optional

from dbt_meta.command_impl.base import BaseCommand
from dbt_meta.fallback import FallbackLevel
from dbt_meta.utils.bigquery import format_bytes, run_dry_run_query


class CostCommand(BaseCommand):
    """Estimate query scan size using BigQuery dry run.

    Uses `bq query --dry_run` to estimate bytes scanned without executing.

    Returns:
        Dictionary with cost estimate:
        - model: Model name
        - bytes: Estimated bytes to scan
        - formatted: Human-readable size (e.g., "1.5 GB")
        - error: Error message if validation failed

    Behavior:
        - Fetches compiled SQL from manifest
        - Runs dry_run to get estimated scan size
        - Does NOT execute the query
    """

    SUPPORTS_BIGQUERY = False  # Needs compiled SQL from manifest
    SUPPORTS_DEV = True

    def execute(self) -> Optional[dict]:
        """Execute cost command.

        Returns:
            Cost estimate dict, or None if model not found
        """
        model = self.get_model_with_fallback()
        if not model:
            print(f"❌ Cannot estimate cost for '{self.model_name}': model not in manifest",
                  file=sys.stderr)
            return None

        return self.process_model(model)

    def process_model(self, model: dict, level: Optional[FallbackLevel] = None) -> dict:
        """Estimate query cost.

        Args:
            model: Model data from manifest
            level: Fallback level (not used)

        Returns:
            Cost estimate dict
        """
        sql = model.get('compiled_code', '')
        if not sql:
            return {
                'model': self.model_name,
                'bytes': None,
                'formatted': None,
                'error': 'No compiled SQL found in manifest'
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
