"""Context command - Single bundle of queryable-shape metadata for one model.

Orchestrates existing commands (info/config/columns/docs/catalog-stats) into one
JSON payload so an agent can write a precise BigQuery query in a single offline
call instead of probing the table with exploratory SELECT */DISTINCT/COUNT queries.

Boundary: queryable-shape of the OUTPUT table only. Model logic (sql), upstream
(parents), lineage are NOT included - those are separate commands.
"""

import os
from typing import Any, Optional

from dbt_meta.catalog.parser import CatalogParser
from dbt_meta.command_impl.base import BaseCommand
from dbt_meta.command_impl.columns import ColumnsCommand
from dbt_meta.command_impl.docs import DocsCommand
from dbt_meta.fallback import FallbackLevel
from dbt_meta.utils.dev import (
    build_dev_table_name as _build_dev_table_name,
)
from dbt_meta.utils.dev import (
    calculate_dev_schema as _calculate_dev_schema,
)


class ContextCommand(BaseCommand):
    """Build a single metadata bundle for one model.

    Returns a dict with:
        - name, full_name, materialized, description, tags
        - partition_by, cluster_by, unique_key
        - row_count, bytes (from catalog, or None)
        - columns: [{name, data_type, description}]

    Column list spine comes from BigQuery/catalog (what you can actually SELECT);
    descriptions are left-joined from the manifest (schema.yml). Missing
    descriptions are expected (~64% of manifest columns have none) and rendered
    as empty strings.

    Returns None if the model is not found in any source.

    Behavior with use_dev=True:
        - Resolves dev schema (personal_USERNAME) for full_name
        - Reuses dev manifest / dev catalog
    """

    SUPPORTS_BIGQUERY = True
    SUPPORTS_DEV = True

    def execute(self) -> Optional[dict[str, Any]]:
        """Build the metadata bundle for the model.

        Returns:
            Bundle dictionary, or None if model not found in any source.
        """
        model = self.get_model_with_fallback()
        if not model:
            return None

        return self.process_model(model)

    def process_model(self, model: dict[str, Any], level: Optional[FallbackLevel] = None) -> dict[str, Any]:
        """Assemble the bundle from the resolved model plus column/catalog sources.

        Args:
            model: Model data from manifest or BigQuery
            level: Fallback level (unused; column source resolves its own state)

        Returns:
            Bundle dictionary
        """
        config = model.get('config', {})

        # FQN + table name: dev uses personal_USERNAME schema + SQL filename;
        # prod uses node database/schema + config.alias.
        if self.use_dev:
            schema_name = _calculate_dev_schema()
            database = ''
            table_name = _build_dev_table_name(model, self.model_name)
            full_name = f"{schema_name}.{table_name}"
        else:
            database = model.get('database', '')
            schema_name = model.get('schema', '')
            table_name = config.get('alias', model.get('name', ''))
            full_name = f"{database}.{schema_name}.{table_name}"

        # Column spine from BigQuery/catalog, descriptions left-joined from manifest.
        columns = self._build_columns()

        # Table stats from catalog (free; None when catalog absent).
        row_count, byte_size = self._table_stats()

        return {
            'name': self.model_name,
            'full_name': full_name,
            'materialized': config.get('materialized', 'table'),
            'description': model.get('description', ''),
            'tags': model.get('tags', []),
            'partition_by': config.get('partition_by'),
            'cluster_by': config.get('cluster_by'),
            'unique_key': config.get('unique_key'),
            'row_count': row_count,
            'bytes': byte_size,
            'columns': columns,
        }

    def _build_columns(self) -> list[dict[str, str]]:
        """Column spine from BQ/catalog with manifest descriptions left-joined."""
        spine = ColumnsCommand(
            self.config, self.manifest_path, self.model_name, self.use_dev, self.json_output
        ).execute() or []

        docs = DocsCommand(
            self.manifest_path, self.model_name, self.use_dev, self.json_output
        ).execute() or []
        descriptions = {col['name']: col.get('description', '') for col in docs}

        return [
            {
                'name': col['name'],
                'data_type': col.get('data_type', ''),
                'description': descriptions.get(col['name'], ''),
            }
            for col in spine
        ]

    def _table_stats(self) -> tuple[Optional[int], Optional[int]]:
        """Read row_count/bytes from catalog; (None, None) when unavailable."""
        catalog_path = (
            self.config.dev_catalog_path if self.use_dev else self.config.prod_catalog_path
        )
        if not catalog_path or not os.path.exists(catalog_path):
            return None, None

        try:
            stats = CatalogParser(catalog_path).get_table_stats(self.model_name)
        except (OSError, ValueError, KeyError):
            return None, None

        if not stats:
            return None, None
        return stats.get('row_count'), stats.get('bytes')
