"""Schema command - Extract schema/table location information."""

import subprocess
import sys
from typing import Optional

from dbt_meta.command_impl.base import BaseCommand
from dbt_meta.errors import ModelNotFoundError
from dbt_meta.fallback import FallbackLevel
from dbt_meta.utils.bigquery import (
    run_bq_command as _run_bq_command,
)
from dbt_meta.utils.dev import (
    build_dev_schema_result as _build_dev_schema_result,
)
from dbt_meta.utils.dev import (
    calculate_dev_schema as _calculate_dev_schema,
)


class SchemaCommand(BaseCommand):
    """Extract schema/table location information.

    Returns:
        Dictionary with:
        - database: BigQuery project (prod) or empty (dev)
        - schema: BigQuery dataset (prod schema or dev schema like personal_USERNAME)
        - table: Table name (prod: alias or name, dev: filename)
        - full_name: database.schema.table (prod) or schema.table (dev)

    Behavior with use_dev=True:
        - Searches dev manifest (target/) FIRST
        - Returns dev schema name (e.g., personal_alice)
        - Uses model filename, NOT alias
        - Falls back to BigQuery if not in dev manifest

    Behavior with use_dev=False (default):
        - Searches production manifest (~/dbt-state/) first
        - Falls back to dev manifest if DBT_FALLBACK_TARGET=true
          (returns DEV schema location when found in dev manifest)
        - Falls back to BigQuery if DBT_FALLBACK_BIGQUERY=true

    Environment variables:
        DBT_PROD_TABLE_NAME: Table name resolution strategy (prod only)
            - "alias_or_name" (default): Use alias if present, else name
            - "name": Always use model name
            - "alias": Always use alias (fallback to name)

        DBT_PROD_SCHEMA_SOURCE: Schema/database resolution strategy (prod only)
            - "config_or_model" (default): Use config if present, else model
            - "model": Always use model.schema and model.database
            - "config": Always use config.schema and config.database (fallback to model)
    """

    SUPPORTS_BIGQUERY = True
    SUPPORTS_DEV = True

    def __init__(self, *args, **kwargs):
        """Initialize schema command."""
        super().__init__(*args, **kwargs)
        self._fallback_level = None  # Track which level found the model

    def execute(self) -> Optional[dict[str, str]]:
        """Execute schema command.

        Returns:
            Schema location dictionary, or None if model not found
        """
        model = self.get_model_with_fallback()
        if not model:
            return None

        return self.process_model(model, self._fallback_level)

    def _get_model_prod_mode(self) -> Optional[dict]:
        """Override to track fallback level for special dev manifest handling.

        When model found in dev manifest (LEVEL 2), schema command returns
        dev schema location, not production schema.
        """
        from dbt_meta.utils import get_cached_parser as _get_cached_parser

        parser = _get_cached_parser(self.manifest_path)

        # Build allowed fallback levels
        allowed_levels = [FallbackLevel.PROD_MANIFEST, FallbackLevel.DEV_MANIFEST]
        if self.SUPPORTS_BIGQUERY:
            allowed_levels.append(FallbackLevel.BIGQUERY)

        try:
            result = self._fallback_strategy.get_model(
                model_name=self.model_name,
                prod_parser=parser,
                allowed_levels=allowed_levels
            )

            # Track fallback level
            self._fallback_level = result.level

            # Collect warnings from fallback
            if result.warnings:
                from dbt_meta.utils import print_warnings as _print_warnings
                fallback_warnings = [
                    {
                        "type": f"{result.level.value}_fallback" if result.level else "fallback",
                        "severity": "warning",
                        "message": warning,
                        "source": result.level.value if result.level else "unknown"
                    }
                    for warning in result.warnings
                ]
                _print_warnings(fallback_warnings, self.json_output)

            return result.data

        except ModelNotFoundError:
            # Model not found in any fallback level - expected error
            return None

    def process_model(self, model: dict, level: Optional[FallbackLevel] = None) -> dict[str, str]:
        """Process model data and return schema location.

        Args:
            model: Model data from manifest or BigQuery
            level: Fallback level where model was found

        Returns:
            Schema location dictionary
        """
        # Special case: if found in dev manifest fallback (LEVEL 2), return dev schema
        # This happens when model not in prod manifest but found in dev manifest
        if level == FallbackLevel.DEV_MANIFEST and not self.use_dev:
            return _build_dev_schema_result(model, self.model_name)

        # Dev mode: use dev schema
        if self.use_dev:
            return _build_dev_schema_result(model, self.model_name)

        # Production mode: use production schema
        config = model.get('config', {})

        # Schema/database resolution based on config
        model_database = model.get('database', '')
        model_schema = model.get('schema', '')
        config_database = config.get('database', '')
        config_schema = config.get('schema', '')

        if self.config.prod_schema_source == 'model':
            database = model_database
            schema_name = model_schema
        elif self.config.prod_schema_source == 'config':
            database = config_database or model_database
            schema_name = config_schema or model_schema
        else:  # 'config_or_model' (default)
            database = config_database or model_database
            schema_name = config_schema or model_schema

        # Table name resolution based on config
        alias = config.get('alias', '')
        name = model.get('name', '')

        if self.config.prod_table_name_strategy == 'name':
            table_name = name or alias
        elif self.config.prod_table_name_strategy == 'alias':
            table_name = alias or name
        else:  # 'alias_or_name' (default)
            table_name = alias or name

        return {
            'database': database,
            'schema': schema_name,
            'table': table_name,
            'full_name': f"{database}.{schema_name}.{table_name}"
        }

    def _get_model_bigquery_dev(self) -> Optional[dict]:
        """Get model from BigQuery in dev mode.

        For dev mode, uses full model name as table name (no splitting by __).

        Returns:
            Model-like data from BigQuery, or None
        """
        dev_schema = _calculate_dev_schema()
        full_table = f"{dev_schema}.{self.model_name}"

        try:
            _run_bq_command(['show', '--format=json', full_table], timeout=10)
            print(f"⚠️  Model not in manifest, using BigQuery table: {full_table}",
                  file=sys.stderr)

            # Return model-like dict that process_model can handle
            return {
                'name': self.model_name,
                'database': '',
                'schema': dev_schema,
                'config': {
                    'alias': self.model_name
                }
            }
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired, FileNotFoundError):
            return None
