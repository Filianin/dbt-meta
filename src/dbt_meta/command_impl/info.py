"""Info command - Extract basic model information."""

from typing import Any, Optional

from dbt_meta.command_impl.base import BaseCommand
from dbt_meta.fallback import FallbackLevel
from dbt_meta.utils.bigquery import (
    fetch_table_metadata_from_bigquery as _fetch_table_metadata_from_bigquery,
)
from dbt_meta.utils.dev import (
    build_dev_table_name as _build_dev_table_name,
)
from dbt_meta.utils.dev import (
    calculate_dev_schema as _calculate_dev_schema,
)


class InfoCommand(BaseCommand):
    """Extract basic model information.

    Returns:
        Dictionary with:
        - name: Model name
        - database: BigQuery project (empty for dev)
        - schema: BigQuery dataset (dev schema for use_dev=True)
        - table: Table name (filename for dev, alias for prod)
        - full_name: database.schema.table (or schema.table for dev)
        - materialized: Materialization type
        - file: Relative file path
        - tags: List of tags
        - unique_id: Full unique identifier

    Behavior with use_dev=True:
        - Searches dev manifest (target/) FIRST
        - Returns dev schema name (e.g., personal_USERNAME)
        - Uses model filename, NOT alias
        - Falls back to BigQuery if not in dev manifest

    Behavior with use_dev=False (default):
        - Searches production manifest (~/dbt-state/) first
        - Falls back to dev manifest if DBT_FALLBACK_TARGET=true
        - Falls back to BigQuery if DBT_FALLBACK_BIGQUERY=true
    """

    SUPPORTS_BIGQUERY = True
    SUPPORTS_DEV = True

    def execute(self) -> Optional[dict[str, Any]]:
        """Execute info command.

        Returns:
            Model info dictionary, or None if model not found
        """
        model = self.get_model_with_fallback()
        if not model:
            return None

        return self.process_model(model)

    def process_model(self, model: dict, level: Optional[FallbackLevel] = None) -> dict[str, Any]:
        """Process model data and return formatted info.

        Args:
            model: Model data from manifest or BigQuery
            level: Fallback level (not used for info command)

        Returns:
            Formatted model info dictionary
        """
        config = model.get('config', {})

        # Dev mode: use dev schema and dev table name
        if self.use_dev:
            dev_schema = _calculate_dev_schema()
            table_name = _build_dev_table_name(model, self.model_name)

            return {
                'name': self.model_name,
                'database': '',  # Dev doesn't use database
                'schema': dev_schema,
                'table': table_name,
                'full_name': f"{dev_schema}.{table_name}",
                'materialized': config.get('materialized', 'table'),
                'file': model.get('original_file_path', ''),
                'tags': model.get('tags', []),
                'unique_id': model.get('unique_id', '')
            }

        # Production mode: use model data directly
        database = model.get('database', '')
        schema_name = model.get('schema', '')
        table_name = config.get('alias', model.get('name', ''))

        return {
            'name': self.model_name,
            'database': database,
            'schema': schema_name,
            'table': table_name,
            'full_name': f"{database}.{schema_name}.{table_name}",
            'materialized': config.get('materialized', 'table'),
            'file': model.get('original_file_path', ''),
            'tags': model.get('tags', []),
            'unique_id': model.get('unique_id', '')
        }

    def _get_model_bigquery_dev(self) -> Optional[dict[str, Any]]:
        """Get model from BigQuery in dev mode.

        For dev mode, uses full model name as table name (no splitting by __).

        Returns:
            Model-like data from BigQuery, or None
        """
        dev_schema = _calculate_dev_schema()
        bq_metadata = _fetch_table_metadata_from_bigquery(dev_schema, self.model_name)

        if not bq_metadata:
            return None

        bq_metadata.get('tableReference', {})
        table_type = bq_metadata.get('type', 'TABLE')

        # Return model-like dict that process_model can handle
        return {
            'name': self.model_name,
            'database': '',
            'schema': dev_schema,
            'config': {
                'alias': self.model_name,
                'materialized': 'table' if table_type == 'TABLE' else 'view'
            },
            'original_file_path': '',
            'tags': [],
            'unique_id': ''
        }
