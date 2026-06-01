"""Validate command - Validate SQL syntax using BigQuery dry run."""

import re
import sys
from typing import Optional

from dbt_meta.command_impl.base import BaseCommand
from dbt_meta.errors import ManifestNotFoundError, ManifestParseError
from dbt_meta.fallback import FallbackLevel
from dbt_meta.utils.bigquery import run_dry_run_query
from dbt_meta.utils.compiled_sql import get_compiled_sql

# Maps BigQuery partition column data types to the type used in a
# DECLARE statement. dbt injects ``_dbt_max_partition`` at the top of
# incremental + insert_overwrite SQL at runtime; a static dry run has
# no such injection, so we prepend an equivalent DECLARE ourselves.
_PARTITION_TYPE_TO_DECLARE = {
    'date': 'DATE',
    'timestamp': 'TIMESTAMP',
    'datetime': 'DATETIME',
    'int64': 'INT64',
    'integer': 'INT64',
}


class ValidateCommand(BaseCommand):
    """Validate model SQL syntax using BigQuery dry run.

    Uses `bq query --dry_run` to validate SQL without executing it.

    Returns:
        Dictionary with validation result:
        - model: Model name
        - valid: True if SQL is valid
        - error: Error message if invalid (None if valid)

    Behavior:
        - Fetches compiled SQL (manifest → target/compiled → dbt compile)
        - Validates against BigQuery (checks syntax, table/column existence)
        - Does NOT execute the query
    """

    SUPPORTS_BIGQUERY = False  # Needs compiled SQL from manifest
    SUPPORTS_DEV = True

    def execute(self) -> Optional[dict]:
        """Execute validate command.

        Returns:
            Validation result dict, or None if neither model nor test found.
            For tests, returns a structured result with kind='test'.
        """
        model = self.get_model_with_fallback()
        if not model:
            test_node = self._lookup_test_node()
            if test_node is not None:
                if not self.json_output:
                    print(
                        f"⚠️  '{self.model_name}' is a dbt test, not a model — "
                        f"validate targets models only.",
                        file=sys.stderr,
                    )
                return {
                    'model': self.model_name,
                    'valid': False,
                    'kind': 'test',
                    'error': (
                        f"'{self.model_name}' is a dbt test "
                        f"({test_node.get('resource_type', 'test')}), "
                        f"not a model. Use `dbt test --select {self.model_name}` to run it."
                    ),
                }
            print(f"❌ Cannot validate '{self.model_name}': model not in manifest",
                  file=sys.stderr)
            return None

        return self.process_model(model)

    def _lookup_test_node(self) -> Optional[dict]:
        """Find a manifest node that matches ``self.model_name`` and is a test.

        Tests have unique_id ``test.<package>.<test_name>...`` and
        resource_type 'test'. We match by trailing name segment.
        """
        from dbt_meta.utils import get_cached_parser as _get_cached_parser
        try:
            parser = _get_cached_parser(self.manifest_path)
        except (FileNotFoundError, OSError, ManifestNotFoundError, ManifestParseError):
            return None
        nodes = parser.manifest.get('nodes', {})
        for uid, node in nodes.items():
            if node.get('resource_type') != 'test':
                continue
            if uid.split('.')[-1] == self.model_name or node.get('name') == self.model_name:
                return node
        return None

    def process_model(self, model: dict, level: Optional[FallbackLevel] = None) -> dict:
        """Validate model SQL.

        Args:
            model: Model data from manifest
            level: Fallback level (not used)

        Returns:
            Validation result dict
        """
        sql, error = get_compiled_sql(
            model=model,
            model_name=self.model_name,
            manifest_path=self.manifest_path,
            use_dev=self.use_dev,
        )
        if sql is None:
            return {
                'model': self.model_name,
                'valid': False,
                'error': error or 'No compiled SQL available',
            }

        warnings = []
        raw_sql = model.get('raw_code') or model.get('raw_sql') or ''
        if raw_sql:
            unguarded = _find_unguarded_max_partition(raw_sql)
            if unguarded:
                warnings.append({
                    'code': 'unguarded_dbt_max_partition',
                    'severity': 'error',
                    'message': (
                        f"_dbt_max_partition referenced outside "
                        f"{{% if is_incremental() %}} guard at line(s) "
                        f"{', '.join(str(n) for n in unguarded)}"
                    ),
                    'hint': (
                        "Wrap usage in {% if is_incremental() %} ... {% endif %} — "
                        "_dbt_max_partition is injected by dbt only on incremental runs."
                    ),
                })

        sql = _prepend_dbt_runtime_decls(sql, model)
        result = run_dry_run_query(sql)

        out = {
            'model': self.model_name,
            'valid': result['valid'],
            'error': result.get('error')
        }
        if warnings:
            out['warnings'] = warnings
        return out


_JINJA_COMMENT = re.compile(r'\{#.*?#\}', re.DOTALL)
_JINJA_IF = re.compile(r'\{%-?\s*if\b(.*?)-?%\}', re.DOTALL)
_JINJA_ENDIF = re.compile(r'\{%-?\s*endif\s*-?%\}')


def _find_unguarded_max_partition(raw_sql: str) -> list[int]:
    """Return line numbers where ``_dbt_max_partition`` appears outside any
    ``{% if is_incremental() %}`` Jinja block.

    Approach: strip ``{# … #}`` comments, then scan tokens (``{% if … %}``,
    ``{% endif %}``, ``_dbt_max_partition``) in source order, maintaining a
    stack flag of whether the current `if` is incremental-guarded. Any
    occurrence found while the guard stack is empty (or holds only non-
    incremental `if`s) is reported.
    """
    cleaned = _JINJA_COMMENT.sub(lambda m: ' ' * len(m.group(0)), raw_sql)

    events: list[tuple[int, str, bool]] = []
    for m in _JINJA_IF.finditer(cleaned):
        body = m.group(1)
        is_inc = 'is_incremental(' in body
        events.append((m.start(), 'if', is_inc))
    for m in _JINJA_ENDIF.finditer(cleaned):
        events.append((m.start(), 'endif', False))
    for m in re.finditer(r'_dbt_max_partition', cleaned):
        events.append((m.start(), 'ref', False))
    events.sort()

    guard_stack: list[bool] = []
    unguarded_offsets: list[int] = []
    for pos, kind, payload in events:
        if kind == 'if':
            guard_stack.append(payload)
        elif kind == 'endif':
            if guard_stack:
                guard_stack.pop()
        else:  # ref
            if not any(guard_stack):
                unguarded_offsets.append(pos)

    if not unguarded_offsets:
        return []
    return sorted({raw_sql.count('\n', 0, off) + 1 for off in unguarded_offsets})


def _prepend_dbt_runtime_decls(sql: str, model: dict) -> str:
    """Prepend DECLARE statements that dbt injects at runtime.

    Currently handles ``_dbt_max_partition`` for incremental models with
    the ``insert_overwrite`` strategy. Without this, dry-running the
    compiled SQL fails with ``Unrecognized name: _dbt_max_partition``
    even though the model itself is fine.
    """
    cfg = model.get('config') or {}
    if cfg.get('materialized') != 'incremental':
        return sql
    if cfg.get('incremental_strategy') != 'insert_overwrite':
        return sql

    pby = cfg.get('partition_by') or {}
    data_type = str(pby.get('data_type') or 'date').lower()
    declare_type = _PARTITION_TYPE_TO_DECLARE.get(data_type, 'DATE')

    return f'DECLARE _dbt_max_partition {declare_type} DEFAULT NULL;\n{sql}'
