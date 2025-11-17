"""Columns command - Extract column list with types."""

from typing import Optional, List, Dict

from dbt_meta.command_impl.base import BaseCommand
from dbt_meta.fallback import FallbackLevel
from dbt_meta.utils.dev import (
    calculate_dev_schema as _calculate_dev_schema,
)
from dbt_meta.utils.bigquery import (
    fetch_columns_from_bigquery_direct as _fetch_columns_from_bigquery_direct,
    fetch_columns_from_bigquery as _fetch_columns_from_bigquery,
)


class ColumnsCommand(BaseCommand):
    """Extract column list with types.

    Returns:
        List of dictionaries with:
        - name: Column name
        - data_type: Column data type

        Returns None if model not found.
        Preserves column order from manifest.

        Falls back to BigQuery if columns not in manifest.

    Behavior with use_dev=True:
        - Searches dev manifest (target/) FIRST
        - Returns columns from dev manifest if available
        - Falls back to BigQuery (dev schema) if not in manifest
        - Falls back to BigQuery if columns missing from manifest

    Behavior with use_dev=False (default):
        - Searches production manifest (.dbt-state/) first
        - Falls back to dev manifest if DBT_FALLBACK_TARGET=true
        - Falls back to BigQuery if DBT_FALLBACK_BIGQUERY=true
        - Falls back to BigQuery if model found but columns missing
    """

    SUPPORTS_BIGQUERY = True
    SUPPORTS_DEV = True

    def execute(self) -> Optional[List[Dict[str, str]]]:
        """Execute columns command.

        Returns:
            List of column dictionaries, or None if model not found
        """
        model = self.get_model_with_fallback()
        if not model:
            return None

        return self.process_model(model)

    def process_model(self, model: dict, level: Optional[FallbackLevel] = None) -> Optional[List[Dict[str, str]]]:
        """Process model data and return column list.

        Args:
            model: Model data from manifest or BigQuery
            level: Fallback level (not used for columns command)

        Returns:
            List of column dictionaries, or None if not available
        """
        # Extract columns from model
        model_columns = model.get('columns', {})

        # If no columns in manifest, fallback to BigQuery
        if not model_columns:
            return _fetch_columns_from_bigquery(self.manifest_path, self.model_name)

        # Convert to list format, preserving order
        # Note: dev mode does NOT lowercase, prod mode does (backward compatibility)
        result = []
        for col_name, col_data in model_columns.items():
            data_type = col_data.get('data_type', 'unknown' if self.use_dev else 'string')
            if not self.use_dev:
                data_type = data_type.lower()
            result.append({
                'name': col_name,
                'data_type': data_type
            })

        return result

    def _get_model_bigquery_dev(self) -> Optional[Dict]:
        """Get model from BigQuery in dev mode.

        For dev mode, uses full model name as table name (no splitting by __).
        Note: Returns columns directly, not model data.

        Returns:
            Model-like data with columns, or None
        """
        dev_schema = _calculate_dev_schema()
        columns = _fetch_columns_from_bigquery_direct(dev_schema, self.model_name)

        if not columns:
            return None

        # Return model-like dict with columns that process_model can handle
        # Convert columns list to dict format expected by process_model
        columns_dict = {col['name']: {'data_type': col['data_type']} for col in columns}

        return {
            'name': self.model_name,
            'columns': columns_dict
        }
