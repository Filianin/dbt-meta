"""Compiled SQL retrieval with 2-level fallback.

Used by ``meta validate`` and ``meta scan`` to get compiled SQL for
BigQuery dry run.

Fallback strategy:

1. ``model['compiled_code']`` from manifest (fast; populated by
   ``dbt compile`` / ``dbt run``).
2. ``target/compiled/{package}/{original_file_path}`` on disk (works
   when an earlier ``dbt compile`` ran but the dev manifest itself was
   regenerated via ``dbt parse``).

If neither yields SQL, callers are pointed at the full-project compile
pre-flight (``_ensure_manifest_compiled`` in ``cli.py``), which is
already invoked automatically by ``validate``/``scan`` before this
function runs unless ``--no-compile`` was passed. Per project
convention, ``dbt compile`` is always run for the WHOLE project, never
for a single model — partial compiles leave gaps that re-trigger on
the next command.

Returns ``(sql, error_message)`` where exactly one is non-None.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any


def get_compiled_sql(
    model: dict[str, Any],
    model_name: str,
    manifest_path: str,
    use_dev: bool = False,
    auto_compile: bool = True,  # kept for API stability; ignored
) -> tuple[str | None, str | None]:
    """Get compiled SQL for a model via 2-level lookup.

    Args:
        model: Model data from manifest (provides ``original_file_path``,
            ``package_name``, ``compiled_code``).
        model_name: Model name (used in error messages).
        manifest_path: Path to ``manifest.json`` (used to infer the
            project root for the on-disk lookup).
        use_dev: True when ``--dev`` was passed; affects the error
            message hint.
        auto_compile: Accepted for backward compatibility only. The
            single-model compile fallback has been removed; full-project
            compile is performed up-front by the CLI pre-flight.

    Returns:
        ``(sql, error)``:
            - On success: ``(sql, None)``.
            - On failure: ``(None, human_readable_error)``.
    """
    # Level 1: manifest
    sql = (model.get('compiled_code') or '').strip()
    if sql:
        return sql, None

    project_root = _infer_project_root(manifest_path)
    package_name = _extract_package_name(model)
    original_file_path = model.get('original_file_path') or model.get('path') or ''

    # Level 2: target/compiled/ on disk
    if project_root and package_name and original_file_path:
        sql = _read_compiled_file(project_root, package_name, original_file_path)
        if sql:
            return sql, None

    # Lookup exhausted. The full-project compile pre-flight is supposed
    # to populate compiled_code before we get here; if it didn't, the
    # user likely passed --no-compile or has a corner case.
    if use_dev:
        return None, _msg_dev_no_compile(model_name)
    return None, _msg_prod_no_compile(model_name)


def _infer_project_root(manifest_path: str) -> str | None:
    """Find project root by looking for ``dbt_project.yml`` above the manifest path."""
    if not manifest_path:
        return None
    try:
        resolved = Path(manifest_path).expanduser().resolve()
    except OSError:
        return None

    for parent in [resolved.parent, *resolved.parents]:
        if (parent / 'dbt_project.yml').exists():
            return str(parent)
    return None


def _extract_package_name(model: dict[str, Any]) -> str:
    """Extract package name from model metadata.

    Priority: ``model['package_name']`` → ``unique_id`` prefix
    ``model.<pkg>.<name>``.
    """
    pkg = model.get('package_name')
    if pkg:
        return str(pkg)

    unique_id = str(model.get('unique_id', ''))
    parts = unique_id.split('.')
    if len(parts) >= 3 and parts[0] == 'model':
        return parts[1]
    return ''


def _read_compiled_file(project_root: str, package_name: str, original_file_path: str) -> str | None:
    """Read compiled SQL from ``target/compiled/{package}/{path}`` if present and non-empty."""
    compiled_path = Path(project_root) / 'target' / 'compiled' / package_name / original_file_path
    if not compiled_path.is_file():
        return None
    try:
        content = compiled_path.read_text()
    except OSError:
        return None
    return content if content.strip() else None


def _msg_dev_no_compile(model_name: str) -> str:
    return (
        f"No compiled SQL for '{model_name}' in dev manifest or target/compiled/.\n"
        f"The CLI normally runs `dbt compile` for the whole project before "
        f"validate/scan — did you pass --no-compile? Remove it, or run "
        f"`dbt compile` manually in the project."
    )


def _msg_prod_no_compile(model_name: str) -> str:
    return (
        f"No compiled SQL for '{model_name}' in production manifest.\n"
        f"For local changes, use: meta validate --dev {model_name} "
        f"(or set DBT_PROD_MANIFEST_PATH to a fully-compiled manifest)."
    )
