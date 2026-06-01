"""Columns command - ALWAYS use BigQuery for accurate column data.

CRITICAL: This command NEVER uses model.get('columns', {}) from manifest!
Manifest columns are unreliable (64.2% missing, 35.8% stale).

Always fetches fresh, accurate data from BigQuery.
"""

from typing import Optional

from dbt_meta.command_impl.base import BaseCommand
from dbt_meta.command_impl.column_source import ColumnSourceFactory
from dbt_meta.fallback import FallbackLevel
from dbt_meta.utils.state_detector import ModelStateDetector


class ColumnsCommand(BaseCommand):
    """Extract column list with types - ALWAYS from BigQuery.

    Key Principle: ACCURACY over SPEED
    - Manifest columns are unreliable (64.2% have NO columns, 35.8% may be stale)
    - Always query BigQuery for fresh, accurate column data
    - Performance: ~2.5s per query (acceptable trade-off for accuracy)

    Returns:
        List of dictionaries with:
        - name: Column name
        - data_type: Column data type

        Returns None if model not found in any source.
    """

    SUPPORTS_BIGQUERY = True
    SUPPORTS_DEV = True

    def execute(self) -> Optional[list[dict[str, str]]]:
        """Execute columns command.

        1. Detect model state (git + manifests)
        2. Pick column source (catalog or BigQuery) based on state
        3. Fetch and return columns
        """
        detected = ModelStateDetector(self.config, self.model_name, self.use_dev).detect()
        self.emit_warnings(detected.warnings)
        source = ColumnSourceFactory.for_state(
            detected.state,
            config=self.config,
            use_dev=self.use_dev,
            json_output=self.json_output,
        )
        return source.fetch(
            model=detected.model,
            model_name=self.model_name,
            state=detected.state,
            prod_model=detected.prod_model,
        )

    def process_model(self, model: dict, level: Optional[FallbackLevel] = None) -> Optional[list[dict[str, str]]]:
        """Not used — all logic is in execute()."""
        return None
