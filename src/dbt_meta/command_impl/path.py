"""Path command - Get relative file path."""

import os
from typing import Optional

from dbt_meta.command_impl.base import BaseCommand
from dbt_meta.errors import ManifestNotFoundError, ManifestParseError
from dbt_meta.fallback import FallbackLevel


class PathCommand(BaseCommand):
    """Get relative file path.

    Returns:
        Relative file path (e.g., "models/core/client/model.sql")
        Returns None if model not found.

    Behavior with use_dev=True:
        - Searches dev manifest (target/) FIRST
        - Returns dev-specific file path
        - NO BigQuery fallback (file path is dbt-specific)
        - Supports BigQuery format search (schema.table) in dev manifest

    Behavior with use_dev=False (default):
        - Searches production manifest (~/dbt-state/) first
        - Falls back to dev manifest if DBT_FALLBACK_TARGET=true
        - NO BigQuery fallback (file path is dbt-specific)
    """

    SUPPORTS_BIGQUERY = False  # Path is manifest-only
    SUPPORTS_DEV = True

    def execute(self) -> Optional[str]:
        """Execute path command.

        Returns:
            File path string, or None if model not found
        """
        # Try normal model lookup
        model = self.get_model_with_fallback()

        # If not found and model_name contains dots, try BigQuery format search
        if not model and '.' in self.model_name:
            if self.use_dev:
                model = self._search_by_bigquery_format_dev()
            else:
                model = self._search_by_bigquery_format_prod()

        if not model:
            return None

        return self.process_model(model)

    def process_model(self, model: dict, level: Optional[FallbackLevel] = None) -> Optional[str]:
        """Process model data and return file path.

        Args:
            model: Model data from manifest
            level: Fallback level (not used for path command)

        Returns:
            File path string
        """
        return model.get('original_file_path', '')

    def _search_by_bigquery_format_dev(self) -> Optional[dict]:
        """Search for model using BigQuery format (schema.table) in dev manifest.

        This is used when model_name contains dots and direct lookup fails.
        Only used in dev mode.

        Returns:
            Model data if found, None otherwise
        """
        from dbt_meta.utils import get_cached_parser as _get_cached_parser
        from dbt_meta.utils.dev import find_dev_manifest

        if not self.use_dev:
            return None

        # Find dev manifest
        dev_manifest = find_dev_manifest(self.manifest_path)
        if not dev_manifest:
            return None

        try:
            parser_dev = _get_cached_parser(dev_manifest)
        except (ManifestNotFoundError, ManifestParseError):
            # Dev manifest not available or invalid - cannot search
            return None

        # Parse BigQuery format: schema.table
        parts = self.model_name.split('.')
        if len(parts) < 2:
            return None

        bq_schema = parts[-2]
        bq_table = parts[-1]

        # Get dev table pattern for matching
        dev_pattern = os.environ.get('DBT_DEV_TABLE_PATTERN', 'name')

        nodes = parser_dev.manifest.get('nodes', {})
        for _node_id, node_data in nodes.items():
            if node_data.get('resource_type') != 'model':
                continue

            # Check schema match (dev or config schema)
            node_dev_schema = node_data.get('schema', '')
            node_config_schema = node_data.get('config', {}).get('schema', '')

            if node_dev_schema != bq_schema and node_config_schema != bq_schema:
                continue

            # Build expected dev table name based on pattern
            node_name = node_data.get('name', '')
            node_alias = node_data.get('config', {}).get('alias', '')

            if dev_pattern == 'name':
                expected_table = node_name
            elif dev_pattern == 'alias':
                expected_table = node_alias if node_alias else node_name
            else:
                # For custom patterns, try name
                expected_table = node_name

            if expected_table == bq_table:
                return node_data

        return None

    def _search_by_bigquery_format_prod(self) -> Optional[dict]:
        """Search for model using BigQuery format (schema.table) in production manifest.

        This is used when model_name contains dots and direct lookup fails.
        Only used in production mode.

        Returns:
            Model data if found, None otherwise
        """
        from dbt_meta.utils import get_cached_parser as _get_cached_parser

        try:
            parser = _get_cached_parser(self.manifest_path)
        except (ManifestNotFoundError, ManifestParseError):
            # Manifest not available or invalid - cannot search
            return None

        # Parse BigQuery format: schema.table or project.schema.table
        parts = self.model_name.split('.')
        if len(parts) < 2:
            return None

        bq_schema = parts[-2]
        bq_table = parts[-1]

        # Search all models for matching schema + alias/name
        nodes = parser.manifest.get('nodes', {})
        for _node_id, node_data in nodes.items():
            if node_data.get('resource_type') != 'model':
                continue

            # Check schema match
            node_schema = node_data.get('schema', '')
            if node_schema != bq_schema:
                continue

            # Check table match: alias OR name
            # Note: in test data, 'alias' is at top level, but in real manifest it's in 'config'
            node_alias = node_data.get('alias', '') or node_data.get('config', {}).get('alias', '')
            node_name = node_data.get('name', '')

            if node_alias == bq_table or node_name == bq_table:
                return node_data

        return None
