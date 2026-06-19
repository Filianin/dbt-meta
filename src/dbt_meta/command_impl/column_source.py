"""Column data source strategy classes.

Each source knows how to fetch columns from one data store.
ColumnSourceFactory routes to the right source based on model state.
"""

import os
import sys
from abc import ABC, abstractmethod
from typing import Any, Optional

from dbt_meta.catalog.parser import CatalogParser
from dbt_meta.config import Config
from dbt_meta.utils.bigquery import (
    fetch_columns_from_bigquery_direct as _fetch_columns_from_bigquery_direct,
)
from dbt_meta.utils.bigquery import (
    infer_table_parts,
)
from dbt_meta.utils.dev import calculate_dev_schema as _calculate_dev_schema
from dbt_meta.utils.model_state import ModelState

# States where catalog is unreliable — go directly to BigQuery
_SKIP_CATALOG_STATES = frozenset({
    ModelState.MODIFIED_UNCOMMITTED,
    ModelState.MODIFIED_COMMITTED,
    ModelState.MODIFIED_IN_DEV,
    ModelState.NEW_UNCOMMITTED,
    ModelState.NEW_COMMITTED,
    ModelState.NEW_IN_DEV,
})


class ColumnSource(ABC):
    """Abstract column data source."""

    @abstractmethod
    def fetch(
        self,
        model: Optional[dict[str, Any]],
        model_name: str,
        state: ModelState,
        prod_model: Optional[dict[str, Any]] = None,
    ) -> Optional[list[dict[str, str]]]:
        """Fetch column list for the given model.

        Args:
            model: Model dict from manifest (may be None if not in any manifest).
            model_name: dbt model name.
            state: Detected model lifecycle state.
            prod_model: Production manifest model dict. Used when `model` came
                from dev manifest so prod schema can be used for table location.

        Returns:
            List of {"name": ..., "data_type": ...} dicts, or None on failure.
        """


class BigQueryColumnSource(ColumnSource):
    """Fetches columns directly from BigQuery INFORMATION_SCHEMA."""

    def __init__(self, use_dev: bool = False, json_output: bool = False) -> None:
        self._use_dev = use_dev
        self._json_output = json_output

    def fetch(
        self,
        model: Optional[dict[str, Any]],
        model_name: str,
        state: ModelState,
        prod_model: Optional[dict[str, Any]] = None,
    ) -> Optional[list[dict[str, str]]]:
        if model is not None:
            return self._fetch_with_model(model, model_name, state, prod_model)
        return self._fetch_without_model(model_name, state, prod_model)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _fetch_with_model(
        self,
        model: dict[str, Any],
        model_name: str,
        state: ModelState,
        prod_model: Optional[dict[str, Any]],
    ) -> Optional[list[dict[str, str]]]:
        if self._use_dev:
            database = model.get('database', '')
            schema = model.get('schema', '')
            table = model_name
        else:
            source = prod_model if prod_model else model
            database = source.get('database', '')
            schema = source.get('schema', '')
            table = source.get('alias') or source.get('name', '')

        self._print_state_message(state, model_name)
        columns = _fetch_columns_from_bigquery_direct(schema, table, database)

        if columns:
            self._print_result_message(state, len(columns), f"{schema}.{table}", is_dev=self._use_dev)
            return columns

        self._print_not_found_message(state, model_name, f"{schema}.{table}")
        return None

    def _fetch_without_model(
        self,
        model_name: str,
        state: ModelState,
        prod_model: Optional[dict[str, Any]],
    ) -> Optional[list[dict[str, str]]]:
        if state == ModelState.MODIFIED_UNCOMMITTED and not self._use_dev:
            if prod_model:
                schema = prod_model.get('schema', '')
                table = prod_model.get('alias') or prod_model.get('name', '')
                database = prod_model.get('database', '')
            else:
                schema, table = infer_table_parts(model_name)
                database = None

            self._print_state_message(state, model_name)
            columns = _fetch_columns_from_bigquery_direct(schema, table, database)

            if columns:
                self._print_result_message(state, len(columns), f"{schema}.{table}", is_dev=False)
                return columns

        elif state in (ModelState.NEW_UNCOMMITTED, ModelState.NEW_COMMITTED):
            dev_schema = _calculate_dev_schema()
            self._print_state_message(state, model_name)
            columns = _fetch_columns_from_bigquery_direct(dev_schema, model_name)

            if columns:
                self._print_result_message(state, len(columns), f"{dev_schema}.{model_name}", is_dev=True)
                print("\n\U0001f4a1 To build and query:", file=sys.stderr)
                print(f"   defer run --select {model_name}", file=sys.stderr)
                return columns

        self._print_not_found_message(state, model_name, None)

        if state in (ModelState.NEW_UNCOMMITTED, ModelState.NEW_COMMITTED):
            print("\n\U0001f4a1 To build and query:", file=sys.stderr)
            print(f"   defer run --select {model_name}", file=sys.stderr)

        return None

    # ------------------------------------------------------------------
    # Output helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _print_state_message(state: ModelState, model_name: str) -> None:
        messages = {
            ModelState.NEW_UNCOMMITTED: f"⚠️  Model '{model_name}' is NEW (uncommitted changes)",
            ModelState.NEW_COMMITTED: f"⚠️  Model '{model_name}' is NEW (committed but not in production)",
            ModelState.NEW_IN_DEV: f"⚠️  Model '{model_name}' is NEW (in dev manifest)",
            ModelState.MODIFIED_UNCOMMITTED: f"⚠️  Model '{model_name}' has UNCOMMITTED changes",
            ModelState.MODIFIED_COMMITTED: f"⚠️  Model '{model_name}' has COMMITTED changes (not deployed)",
            ModelState.MODIFIED_IN_DEV: f"⚠️  Model '{model_name}' has UNCOMMITTED changes (compiled in dev)",
            ModelState.PROD_STABLE: f"✅ Model '{model_name}' found in production",
            ModelState.DELETED_LOCALLY: f"⚠️  Model '{model_name}' is DELETED locally",
            ModelState.DELETED_DEPLOYED: f"❌ Model '{model_name}' is DELETED from production",
            ModelState.NOT_FOUND: f"❌ Model '{model_name}' NOT FOUND",
        }
        print(f"\n{messages.get(state, f'Model {model_name!r} state: {state.value}')}", file=sys.stderr)

    @staticmethod
    def _print_result_message(state: ModelState, count: int, table: str, is_dev: bool) -> None:
        print(f"\n✅ Retrieved {count} columns from BigQuery", file=sys.stderr)
        table_type = "dev table" if is_dev else "prod table"
        print(f"\nData source: BigQuery ({table_type}: {table})", file=sys.stderr)
        if is_dev:
            if state == ModelState.MODIFIED_UNCOMMITTED:
                print("\n⚠️  Using dev version (reflects your uncommitted changes)", file=sys.stderr)
            elif state == ModelState.MODIFIED_IN_DEV:
                print("\n⚠️  Using dev version (compiled in dev)", file=sys.stderr)

    def _print_not_found_message(self, state: ModelState, model_name: str, attempted: Optional[str]) -> None:
        if self._json_output:
            return
        print("\n❌ Model not found in BigQuery", file=sys.stderr)
        if attempted:
            print(f"   Tried: {attempted}", file=sys.stderr)
        if state in (ModelState.NEW_UNCOMMITTED, ModelState.NEW_COMMITTED):
            print("\n\U0001f4a1 Model appears to be NEW but not built in dev", file=sys.stderr)
        elif state == ModelState.NOT_FOUND:
            print("\n\U0001f4a1 To find similar models:", file=sys.stderr)
            print("   meta list | grep keyword", file=sys.stderr)
            print('   meta search "keyword"', file=sys.stderr)


class CatalogColumnSource(ColumnSource):
    """Fetches columns from catalog.json, falling back to BigQuery on any failure.

    Fallback triggers (any returns None → BigQuery):
    - DBT_FALLBACK_CATALOG=false
    - Catalog path not configured
    - Catalog file missing
    - File mtime > 24 h (CI/CD likely broken)
    - Model not in catalog
    - Parse error
    """

    def __init__(self, config: Config, use_dev: bool = False, json_output: bool = False) -> None:
        self._config = config
        self._use_dev = use_dev
        self._json_output = json_output

    def fetch(
        self,
        model: Optional[dict[str, Any]],
        model_name: str,
        state: ModelState,
        prod_model: Optional[dict[str, Any]] = None,
    ) -> Optional[list[dict[str, str]]]:
        if model is not None:
            columns = self._try_catalog(model, model_name, state)
            if columns is not None:
                return columns
        # Catalog unavailable or model absent → BigQuery fallback
        bq = BigQueryColumnSource(use_dev=self._use_dev, json_output=self._json_output)
        return bq.fetch(model, model_name, state, prod_model)

    def _try_catalog(
        self,
        model: dict[str, Any],
        model_name: str,
        state: ModelState,
    ) -> Optional[list[dict[str, str]]]:
        debug = bool(os.environ.get('DBT_META_DEBUG'))

        if not self._config.fallback_catalog_enabled:
            if debug:
                print("\n\U0001f4a1 Catalog disabled (DBT_FALLBACK_CATALOG=false), using BigQuery", file=sys.stderr)
            return None

        catalog_path = self._config.dev_catalog_path if self._use_dev else self._config.prod_catalog_path
        if not catalog_path:
            if debug:
                mode = "dev" if self._use_dev else "prod"
                print(f"\n\U0001f4a1 Catalog path not configured (DBT_{mode.upper()}_CATALOG_PATH), using BigQuery", file=sys.stderr)
            return None

        if not os.path.exists(catalog_path):
            if debug:
                print(f"\n\U0001f4a1 Catalog not found ({catalog_path}), using BigQuery", file=sys.stderr)
            return None

        try:
            parser = CatalogParser(catalog_path)

            file_age = parser.get_file_age_hours()
            if file_age and file_age > 24:
                print(f"\n⚠️  Catalog file not updated for {file_age:.1f}h (>24h), using BigQuery", file=sys.stderr)
                return None

            internal_age = parser.get_age_hours()
            if internal_age and internal_age > 168:  # 7 days
                days, hours = divmod(int(internal_age), 24)
                print(f"\nℹ️  Catalog was generated {days}d {hours}h ago", file=sys.stderr)

            columns = parser.get_columns(model_name)
            if columns:
                self._print_catalog_message(state, model_name, len(columns), internal_age)
                return columns

            if debug:
                print("\n\U0001f4a1 Model not in catalog, using BigQuery", file=sys.stderr)
            return None

        except Exception as exc:
            if debug:
                print(f"\n⚠️  Catalog read failed ({exc}), using BigQuery", file=sys.stderr)
            return None

    @staticmethod
    def _print_catalog_message(
        state: ModelState,
        model_name: str,
        count: int,
        age_hours: Optional[float],
    ) -> None:
        messages = {
            ModelState.PROD_STABLE: f"✅ Model '{model_name}' found in production",
            ModelState.DELETED_LOCALLY: f"⚠️  Model '{model_name}' is DELETED locally",
        }
        msg = messages.get(state, f"Model '{model_name}' state: {state.value}")
        print(f"\n{msg}", file=sys.stderr)
        age_str = f" (cached {age_hours:.1f}h ago)" if age_hours else ""
        print(f"\n✅ Retrieved {count} columns from catalog.json{age_str}", file=sys.stderr)
        print("\nData source: catalog.json (fast)", file=sys.stderr)


class ColumnSourceFactory:
    """Routes to the appropriate ColumnSource based on model state."""

    @staticmethod
    def for_state(
        state: ModelState,
        config: Config,
        use_dev: bool = False,
        json_output: bool = False,
    ) -> ColumnSource:
        """Return the correct source for a given model state.

        Changed/new models bypass the catalog (data would be stale by definition).
        Stable models try catalog first for speed, then fall back to BigQuery.
        """
        if state in _SKIP_CATALOG_STATES:
            return BigQueryColumnSource(use_dev=use_dev, json_output=json_output)
        return CatalogColumnSource(config=config, use_dev=use_dev, json_output=json_output)
