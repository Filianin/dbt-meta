"""Single source of truth for a model's physical location.

Both prod and dev commands must answer one question the same way: *given a
model dict and the fallback level it was found at, where does its data live?*
Before this module the answer was re-implemented at 5+ sites (schema.py,
context.py, dev.build_dev_schema_result, base._get_model_dev_mode,
state_detector) — and only schema.py honoured Hard Invariant #4 (found in dev →
use dev schema, never re-search prod). Generic prod-mode dev-fallback silently
kept the prod schema baked into the dev manifest.

``resolve_location`` centralises invariant #4: a model is treated as *dev* when
the command runs with ``--dev`` OR when it was found via the prod→dev fallback
(``level == DEV_MANIFEST``). Dev location = ``personal_<user>`` schema + SQL
filename (2-part FQN); prod location = config/model schema + alias/name (3-part
FQN), honouring ``DBT_PROD_SCHEMA_SOURCE`` / ``DBT_PROD_TABLE_NAME``.
"""

from dataclasses import dataclass
from typing import Any, Optional

from dbt_meta.config import Config
from dbt_meta.fallback import FallbackLevel
from dbt_meta.utils.dev import build_dev_table_name, calculate_dev_schema


@dataclass
class ResolvedLocation:
    """Physical location of a model's output table.

    Attributes:
        database: BigQuery project (prod) or ``""`` (dev — no project prefix).
        schema: BigQuery dataset (prod schema or ``personal_<user>``).
        table: Table name (prod: alias/name, dev: SQL filename).
        is_dev: Whether dev semantics were applied (invariant #4).
    """

    database: str
    schema: str
    table: str
    is_dev: bool

    @property
    def full_name(self) -> str:
        """FQN: 2-part (``schema.table``) for dev, 3-part for prod.

        Prod stays 3-part even when ``database`` is empty (e.g. BigQuery
        fallback) to preserve the historical ``.schema.table`` shape.
        """
        if self.is_dev:
            return f"{self.schema}.{self.table}"
        return f"{self.database}.{self.schema}.{self.table}"

    def as_dict(self) -> dict[str, str]:
        """Public schema-location dict (keys: database, schema, table, full_name)."""
        return {
            "database": self.database,
            "schema": self.schema,
            "table": self.table,
            "full_name": self.full_name,
        }


def resolve_location(
    model: dict[str, Any],
    model_name: str,
    level: Optional[FallbackLevel],
    use_dev: bool,
    config: Config,
) -> ResolvedLocation:
    """Resolve a model's physical location, honouring Hard Invariant #4.

    Args:
        model: Model data from a manifest or BigQuery.
        model_name: Original dbt model name (used for dev filename table names).
        level: Fallback level where the model was found (``None`` if unknown).
        use_dev: Whether the command was invoked with ``--dev``.
        config: Active configuration (prod schema/table resolution strategies).

    Returns:
        ResolvedLocation with dev or prod semantics applied.
    """
    # Invariant #4: dev when explicitly requested OR found via prod→dev fallback.
    is_dev = use_dev or level == FallbackLevel.DEV_MANIFEST
    if is_dev:
        return ResolvedLocation(
            database="",
            schema=calculate_dev_schema(),
            table=build_dev_table_name(model, model_name),
            is_dev=True,
        )

    node_config = model.get("config", {})
    model_database = model.get("database", "")
    model_schema = model.get("schema", "")
    config_database = node_config.get("database", "")
    config_schema = node_config.get("schema", "")

    if config.prod_schema_source == "model":
        database = model_database
        schema = model_schema
    else:  # 'config' and 'config_or_model' both prefer config, fall back to model
        database = config_database or model_database
        schema = config_schema or model_schema

    alias = node_config.get("alias", "")
    name = model.get("name", "")
    if config.prod_table_name_strategy == "name":
        table = name or alias
    else:  # 'alias' and 'alias_or_name' both prefer alias, fall back to name
        table = alias or name

    return ResolvedLocation(database=database, schema=schema, table=table, is_dev=False)
