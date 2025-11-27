"""Columns command - ALWAYS use BigQuery for accurate column data.

CRITICAL: This command NEVER uses model.get('columns', {}) from manifest!
Manifest columns are unreliable (64.2% missing, 35.8% stale).

Always fetches fresh, accurate data from BigQuery.
"""

import contextlib
import os
import sys
from typing import Optional

from dbt_meta.command_impl.base import BaseCommand
from dbt_meta.fallback import FallbackLevel
from dbt_meta.utils import get_cached_parser as _get_cached_parser
from dbt_meta.utils.bigquery import (
    fetch_columns_from_bigquery_direct as _fetch_columns_from_bigquery_direct,
)
from dbt_meta.utils.dev import calculate_dev_schema as _calculate_dev_schema
from dbt_meta.utils.git import get_model_git_status
from dbt_meta.utils.model_state import ModelState, detect_model_state


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

    Behavior:
        1. Get model from manifest (for schema/table location ONLY, NOT columns)
        2. Detect git status (new/modified/stable)
        3. Detect model state (14 possible states)
        4. ALWAYS fetch columns from BigQuery (never from manifest)
        5. Generate informative messages based on state
    """

    SUPPORTS_BIGQUERY = True
    SUPPORTS_DEV = True

    def execute(self) -> Optional[list[dict[str, str]]]:
        """Execute columns command - ALWAYS use BigQuery.

        CRITICAL: NEVER use model.get('columns', {}) from manifest!

        Process:
        1. Get model metadata (for schema/table name only)
        2. Detect git status
        3. Determine model state
        4. Fetch from BigQuery (prod or dev based on state)
        5. Return columns with state-aware messages

        Returns:
            List of column dictionaries, or None if not found
        """
        # Try to get model from manifests (for table location)
        model = self.get_model_with_fallback()

        # Get file path from model for git status detection
        # CRITICAL: Extract file_path BEFORE git status to avoid CWD-dependent search
        file_path = None
        if model:
            file_path = model.get('original_file_path') or model.get('path')

        # Detect git status (pass file_path to avoid CWD-dependent search)
        git_status = get_model_git_status(self.model_name, file_path=file_path)

        # Get parsers to check manifest presence
        # CRITICAL: Use REAL production manifest path, not self.manifest_path
        # self.manifest_path might be ./target/manifest.json (dev manifest) when DBT_PROD_MANIFEST_PATH not set
        prod_parser = None
        prod_manifest_path = self.config.prod_manifest_path
        if prod_manifest_path and os.path.exists(prod_manifest_path):
            with contextlib.suppress(FileNotFoundError, OSError):
                prod_parser = self._get_cached_parser(prod_manifest_path)

        dev_parser = None
        if self.config.fallback_dev_enabled:
            dev_manifest_path = self.config.dev_manifest_path
            if dev_manifest_path and os.path.exists(dev_manifest_path):
                with contextlib.suppress(FileNotFoundError, OSError):
                    dev_parser = self._get_cached_parser(dev_manifest_path)

        # Determine model state
        in_prod_manifest = prod_parser.get_model(self.model_name) is not None if prod_parser else False
        in_dev_manifest = dev_parser.get_model(self.model_name) is not None if dev_parser else False

        # Get production model for schema resolution (used when model comes from dev manifest fallback)
        # CRITICAL: For MODIFIED states without --dev, we need production schema/table, not dev
        prod_model = prod_parser.get_model(self.model_name) if prod_parser else None

        state = detect_model_state(
            self.model_name,
            in_prod_manifest=in_prod_manifest,
            in_dev_manifest=in_dev_manifest,
            git_status=git_status,
            model=model,
            file_path=file_path
        )

        # Strategy based on model state:
        # - MODIFIED/NEW → ALWAYS BigQuery (skip catalog, need fresh data)
        # - STABLE → Try catalog first (fast), then BigQuery (accurate)

        if state in [
            ModelState.MODIFIED_UNCOMMITTED,
            ModelState.MODIFIED_IN_DEV,
            ModelState.NEW_UNCOMMITTED,
            ModelState.NEW_COMMITTED,
            ModelState.NEW_IN_DEV,
        ]:
            # Changed models → SKIP catalog, go directly to BigQuery
            if model:
                return self._fetch_from_bigquery_with_model(model, state, prod_model)
            else:
                return self._fetch_from_bigquery_without_model(state, prod_model)

        # Stable models → Try catalog first
        if model:
            columns = self._try_fetch_from_catalog(model, state)
            if columns:
                return columns  # Success from catalog

            # Catalog failed → fallback to BigQuery
            return self._fetch_from_bigquery_with_model(model, state, prod_model)
        else:
            return self._fetch_from_bigquery_without_model(state, prod_model)

    def _fetch_from_bigquery_with_model(
        self,
        model: dict,
        state: ModelState,
        prod_model: Optional[dict] = None
    ) -> Optional[list[dict[str, str]]]:
        """Fetch columns from BigQuery using model metadata.

        Args:
            model: Model data from manifest (for schema/table location)
            state: Detected model state
            prod_model: Production model data (for schema resolution when model from dev manifest)

        Returns:
            List of columns, or None if fetch fails
        """
        # Determine table name based on mode
        if self.use_dev:
            # Dev mode: use dev schema and FULL model_name as table name
            database = model.get('database', '')
            schema = model.get('schema', '')
            table = self.model_name
        else:
            # Production mode: ALWAYS use production model's schema/table
            # This handles cases where model was found in dev manifest with dev schema
            # CRITICAL FIX: For MODIFIED states, prod_model has correct production schema
            source_model = prod_model if prod_model else model
            database = source_model.get('database', '')
            schema = source_model.get('schema', '')
            table = source_model.get('alias') or source_model.get('name', '')

        # Print informative message based on state
        self._print_state_message(state, searching=True)

        # Fetch from BigQuery
        columns = _fetch_columns_from_bigquery_direct(schema, table, database)

        if columns:
            # Success - print result message
            self._print_result_message(state, len(columns), f"{schema}.{table}", is_dev_table=self.use_dev)
            return columns

        # Failed to fetch
        self._print_not_found_message(state, f"{schema}.{table}")
        return None

    def _fetch_from_bigquery_without_model(
        self,
        state: ModelState,
        prod_model: Optional[dict] = None
    ) -> Optional[list[dict[str, str]]]:
        """Fetch columns from BigQuery without model in manifest.

        This handles cases where model exists only in git but not in manifests.

        Args:
            state: Detected model state
            prod_model: Production model data (for schema resolution when available)

        Returns:
            List of columns, or None if fetch fails
        """
        # Determine schema and table based on state and mode
        if state == ModelState.MODIFIED_UNCOMMITTED and not self.use_dev:
            # MODIFIED model without --dev: Query PRODUCTION table
            # CRITICAL FIX: Use production schema, not dev schema
            if prod_model:
                # Use production model's schema/table
                schema = prod_model.get('schema', '')
                table = prod_model.get('alias') or prod_model.get('name', '')
                database = prod_model.get('database', '')
            else:
                # Fallback: infer from model name (best effort)
                from dbt_meta.utils.bigquery import infer_table_parts
                schema, table = infer_table_parts(self.model_name)
                database = None

            self._print_state_message(state, searching=True)

            columns = _fetch_columns_from_bigquery_direct(schema, table, database)

            if columns:
                # MODIFIED without --dev: querying production table
                self._print_result_message(state, len(columns), f"{schema}.{table}", is_dev_table=False)
                return columns

        elif state in [ModelState.NEW_UNCOMMITTED, ModelState.NEW_COMMITTED]:
            # NEW models: Try dev schema (they only exist in dev after build)
            dev_schema = _calculate_dev_schema()
            table = self.model_name

            self._print_state_message(state, searching=True)

            columns = _fetch_columns_from_bigquery_direct(dev_schema, table)

            if columns:
                # NEW models: always from dev table
                self._print_result_message(state, len(columns), f"{dev_schema}.{table}", is_dev_table=True)

                print("\n💡 To build and query:", file=sys.stderr)
                print(f"   defer run --select {self.model_name}", file=sys.stderr)

                return columns

        # Not found anywhere
        self._print_not_found_message(state, None)

        # Suggest build command for NEW models
        if state in [ModelState.NEW_UNCOMMITTED, ModelState.NEW_COMMITTED]:
            print("\n💡 To build and query:", file=sys.stderr)
            print(f"   defer run --select {self.model_name}", file=sys.stderr)

        return None

    def _try_fetch_from_catalog(
        self,
        model: dict,
        state: ModelState
    ) -> Optional[list[dict[str, str]]]:
        """Try to fetch columns from catalog.json with fallback to BigQuery.

        Args:
            model: Model data from manifest
            state: Detected model state

        Returns:
            List of columns if found in catalog, None for BigQuery fallback

        Fallback scenarios (returns None = use BigQuery):
            1. Catalog fallback disabled (DBT_FALLBACK_CATALOG=false)
            2. Catalog path not configured
            3. Catalog file doesn't exist
            4. Catalog too old (>24h)
            5. Model not in catalog
            6. Catalog parse error
        """
        # Check if catalog fallback is enabled
        if not self.config.fallback_catalog_enabled:
            if os.environ.get('DBT_META_DEBUG'):
                print("\n💡 Catalog disabled (DBT_FALLBACK_CATALOG=false), using BigQuery", file=sys.stderr)
            return None

        # Determine catalog path (prod or dev)
        catalog_path = self.config.prod_catalog_path if not self.use_dev else self.config.dev_catalog_path

        # Fallback: catalog path not configured
        if not catalog_path:
            if os.environ.get('DBT_META_DEBUG'):
                mode = "dev" if self.use_dev else "prod"
                print(f"\n💡 Catalog path not configured (DBT_{mode.upper()}_CATALOG_PATH), using BigQuery", file=sys.stderr)
            return None

        # Fallback: catalog file doesn't exist
        if not os.path.exists(catalog_path):
            if os.environ.get('DBT_META_DEBUG'):
                print(f"\n💡 Catalog not found ({catalog_path}), using BigQuery", file=sys.stderr)
            return None

        try:
            from dbt_meta.catalog.parser import CatalogParser

            parser = CatalogParser(catalog_path)

            # Check if catalog FILE is too stale (not updated by CI/CD)
            file_age_hours = parser.get_file_age_hours()
            if file_age_hours and file_age_hours > 24:
                # File not updated for >24h - CI/CD might be broken
                print(f"\n⚠️  Catalog file not updated for {file_age_hours:.1f}h (>24h), using BigQuery", file=sys.stderr)
                return None

            # Info message if internal generated_at is very old (>7 days)
            internal_age_hours = parser.get_age_hours()
            if internal_age_hours and internal_age_hours > 168:  # 7 days
                days = int(internal_age_hours // 24)
                hours = int(internal_age_hours % 24)
                print(f"\nℹ️  Catalog was generated {days}d {hours}h ago", file=sys.stderr)

            # Fetch columns from catalog
            columns = parser.get_columns(self.model_name)

            if columns:
                # Success - print message
                self._print_catalog_message(state, len(columns), internal_age_hours)
                return columns

            # Fallback: model not in catalog
            if os.environ.get('DBT_META_DEBUG'):
                print("\n💡 Model not in catalog, using BigQuery", file=sys.stderr)
            return None

        except Exception as e:
            # Fallback: catalog parse failed
            if os.environ.get('DBT_META_DEBUG'):
                print(f"\n⚠️  Catalog read failed ({e}), using BigQuery", file=sys.stderr)
            return None

    def _print_catalog_message(self, state: ModelState, column_count: int, age_hours: Optional[float]):
        """Print message when using catalog.json.

        Args:
            state: Model state
            column_count: Number of columns retrieved
            age_hours: Catalog age in hours
        """
        import sys

        # State message
        state_messages = {
            ModelState.PROD_STABLE: f"✅ Model '{self.model_name}' found in production",
            ModelState.DELETED_LOCALLY: f"⚠️  Model '{self.model_name}' is DELETED locally",
        }
        message = state_messages.get(state, f"Model '{self.model_name}' state: {state.value}")
        print(f"\n{message}", file=sys.stderr)

        # Catalog info
        age_str = f" (cached {age_hours:.1f}h ago)" if age_hours else ""
        print(f"\n✅ Retrieved {column_count} columns from catalog.json{age_str}", file=sys.stderr)
        print("\nData source: catalog.json (fast)", file=sys.stderr)

    def _print_state_message(self, state: ModelState, searching: bool = False):
        """Print state-aware message to stderr.

        Args:
            state: Model state
            searching: If True, print "searching" message
        """
        # Map states to user-friendly messages
        state_messages = {
            ModelState.NEW_UNCOMMITTED: f"⚠️  Model '{self.model_name}' is NEW (uncommitted changes)",
            ModelState.NEW_COMMITTED: f"⚠️  Model '{self.model_name}' is NEW (committed but not in production)",
            ModelState.NEW_IN_DEV: f"⚠️  Model '{self.model_name}' is NEW (in dev manifest)",
            ModelState.MODIFIED_UNCOMMITTED: f"⚠️  Model '{self.model_name}' has UNCOMMITTED changes",
            ModelState.MODIFIED_IN_DEV: f"⚠️  Model '{self.model_name}' has UNCOMMITTED changes (compiled in dev)",
            ModelState.PROD_STABLE: f"✅ Model '{self.model_name}' found in production",
            ModelState.DELETED_LOCALLY: f"⚠️  Model '{self.model_name}' is DELETED locally",
            ModelState.DELETED_DEPLOYED: f"❌ Model '{self.model_name}' is DELETED from production",
            ModelState.NOT_FOUND: f"❌ Model '{self.model_name}' NOT FOUND",
        }

        message = state_messages.get(state, f"Model '{self.model_name}' state: {state.value}")
        print(f"\n{message}", file=sys.stderr)

    def _print_result_message(self, state: ModelState, column_count: int, table: str, is_dev_table: bool = False):
        """Print success message with column count.

        Args:
            state: Model state
            column_count: Number of columns retrieved
            table: Full table name
            is_dev_table: Whether data is from dev table (default: False = prod table)
        """
        print(f"\n✅ Retrieved {column_count} columns from BigQuery", file=sys.stderr)

        # Show prod/dev table info
        table_type = "dev table" if is_dev_table else "prod table"
        print(f"\nData source: BigQuery ({table_type}: {table})", file=sys.stderr)

        # Add state-specific info only for dev tables
        if is_dev_table:
            if state == ModelState.MODIFIED_UNCOMMITTED:
                print("\n⚠️  Using dev version (reflects your uncommitted changes)", file=sys.stderr)
            elif state == ModelState.MODIFIED_IN_DEV:
                print("\n⚠️  Using dev version (compiled in dev)", file=sys.stderr)

    def _print_not_found_message(self, state: ModelState, attempted_table: Optional[str]):
        """Print error message when columns not found.

        Args:
            state: Model state
            attempted_table: Table name that was attempted (if any)
        """
        print("\n❌ Model not found in BigQuery", file=sys.stderr)

        if attempted_table:
            print(f"   Tried: {attempted_table}", file=sys.stderr)

        # State-specific suggestions
        if state in [ModelState.NEW_UNCOMMITTED, ModelState.NEW_COMMITTED]:
            print("\n💡 Model appears to be NEW but not built in dev", file=sys.stderr)
        elif state == ModelState.NOT_FOUND:
            print("\n💡 To find similar models:", file=sys.stderr)
            print("   meta list | grep keyword", file=sys.stderr)
            print("   meta search \"keyword\"", file=sys.stderr)

    def _get_cached_parser(self, manifest_path: str):
        """Get cached manifest parser.

        Args:
            manifest_path: Path to manifest.json

        Returns:
            ManifestParser instance
        """
        return _get_cached_parser(manifest_path)

    def process_model(self, model: dict, level: Optional[FallbackLevel] = None) -> Optional[list[dict[str, str]]]:
        """Process model data - DEPRECATED, use execute() instead.

        This method is kept for backward compatibility with BaseCommand interface,
        but the actual logic is now in execute().

        Args:
            model: Model data (unused)
            level: Fallback level (unused)

        Returns:
            None (actual logic in execute())
        """
        # This method is no longer used - all logic moved to execute()
        # Kept for interface compatibility
        return None
