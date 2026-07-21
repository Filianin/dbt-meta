"""Schema command - Extract schema/table location information."""

import subprocess
import sys
from typing import Any, Optional

from dbt_meta.command_impl.base import BaseCommand
from dbt_meta.command_impl.resolver import resolve_location
from dbt_meta.fallback import FallbackLevel
from dbt_meta.utils.bigquery import (
    run_bq_command as _run_bq_command,
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

    def execute(self) -> Optional[dict[str, str]]:
        """Execute schema command.

        Returns:
            Schema location dictionary, or None if model not found
        """
        model = self.get_model_with_fallback()
        if not model:
            return None

        # _fallback_level is populated by BaseCommand.get_model_with_fallback.
        # When the model was found in the dev manifest (LEVEL 2), process_model
        # returns the dev schema location, not production.
        return self.process_model(model, self._fallback_level)

    def process_model(self, model: dict[str, Any], level: Optional[FallbackLevel] = None) -> dict[str, str]:
        """Process model data and return schema location.

        Delegates to the shared ``resolve_location`` seam, which honours
        invariant #4 (dev-manifest hit → dev schema) and the prod
        schema/table resolution strategies.

        Args:
            model: Model data from manifest or BigQuery
            level: Fallback level where model was found

        Returns:
            Schema location dictionary
        """
        return resolve_location(
            model, self.model_name, level, self.use_dev, self.config
        ).as_dict()

    def _get_model_bigquery_dev(self) -> Optional[dict[str, Any]]:
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
