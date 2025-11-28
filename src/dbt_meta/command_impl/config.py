"""Config command - Extract full dbt config."""

import sys
from typing import Any, Optional

from dbt_meta.command_impl.base import BaseCommand
from dbt_meta.fallback import FallbackLevel
from dbt_meta.utils.bigquery import (
    fetch_table_metadata_from_bigquery as _fetch_table_metadata_from_bigquery,
)
from dbt_meta.utils.dev import calculate_dev_schema as _calculate_dev_schema


class ConfigCommand(BaseCommand):
    """Extract full dbt config.

    Returns:
        Full config dictionary with all 29+ fields:
        materialized, partition_by, cluster_by, unique_key,
        incremental_strategy, on_schema_change, grants, etc.

        Returns None if model not found.

    Behavior with use_dev=True:
        - Searches dev manifest (target/) FIRST
        - Returns dev-specific config
        - Falls back to BigQuery if not in dev manifest
          (partial config: materialized, partition_by, cluster_by)

    Behavior with use_dev=False (default):
        - Searches production manifest (~/dbt-state/) first
        - Falls back to dev manifest if DBT_FALLBACK_TARGET=true
        - Falls back to BigQuery if DBT_FALLBACK_BIGQUERY=true
          (partial config: materialized, partition_by, cluster_by)
    """

    SUPPORTS_BIGQUERY = True  # Partial config from BigQuery
    SUPPORTS_DEV = True

    def execute(self) -> Optional[dict[str, Any]]:
        """Execute config command.

        Returns:
            Config dictionary, or None if model not found
        """
        model = self.get_model_with_fallback()
        if not model:
            return None

        return self.process_model(model)

    def process_model(self, model: dict, level: Optional[FallbackLevel] = None) -> Optional[dict[str, Any]]:
        """Process model data and return config.

        Args:
            model: Model data from manifest
            level: Fallback level (not used for config command)

        Returns:
            Config dictionary
        """
        return model.get('config', {})

    def _get_model_bigquery_dev(self) -> Optional[dict]:
        """Get model from BigQuery in dev mode.

        For dev mode, uses full model name as table name (no splitting by __).
        Returns partial config from BigQuery metadata.

        Returns:
            Model-like data from BigQuery with config, or None
        """
        dev_schema = _calculate_dev_schema()
        bq_metadata = _fetch_table_metadata_from_bigquery(dev_schema, self.model_name)

        if not bq_metadata:
            return None

        # Print warnings about partial config
        print(f"⚠️  Model not in manifest, using BigQuery config: {dev_schema}.{self.model_name}",
              file=sys.stderr)
        print("⚠️  Partial config available (dbt-specific settings unavailable)",
              file=sys.stderr)

        # Map BigQuery → dbt config
        table_type = bq_metadata.get('type', 'TABLE')
        config_result = {
            'materialized': 'table' if table_type == 'TABLE' else 'view',
            'partition_by': None,
            'cluster_by': None,
            # dbt-specific (not available from BigQuery)
            'unique_key': None,
            'incremental_strategy': None,
            'on_schema_change': None,
            'grants': {},
            'tags': [],
            'meta': {},
            'enabled': True,
            'alias': None,
            'schema': None,
            'database': None,
            'pre_hook': [],
            'post_hook': [],
            'quoting': {},
            'column_types': {},
            'persist_docs': {},
            'full_refresh': None,
        }

        # Extract partition info
        if 'timePartitioning' in bq_metadata:
            config_result['partition_by'] = bq_metadata['timePartitioning'].get('field')

        # Extract clustering info
        if 'clustering' in bq_metadata:
            config_result['cluster_by'] = bq_metadata['clustering'].get('fields', [])

        # Return model-like dict that process_model can handle
        return {
            'name': self.model_name,
            'config': config_result
        }
