"""Compiled SQL retrieval with 3-level fallback.

Used by `meta validate` and `meta scan` to get compiled SQL for BigQuery dry run.

Fallback strategy:
1. model['compiled_code'] from manifest (fast, works for `dbt compile`/`dbt run` output)
2. target/compiled/{package}/{original_file_path} on disk (works when compile ran
   but dev manifest only had `dbt parse`)
3. Run `dbt compile --select <model> --target dev` (use_dev=True only), then re-check disk

Returns (sql, error_message) where exactly one is non-None.
"""

from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

DBT_COMPILE_TIMEOUT = 180  # seconds


def get_compiled_sql(
    model: dict[str, Any],
    model_name: str,
    manifest_path: str,
    use_dev: bool = False,
    auto_compile: bool = True,
) -> tuple[str | None, str | None]:
    """Get compiled SQL for a model with 3-level fallback.

    Args:
        model: Model data from manifest (provides original_file_path, package_name, compiled_code)
        model_name: Model name (for dbt compile --select and error messages)
        manifest_path: Path to manifest.json (for inferring project root)
        use_dev: If True, `dbt compile --target dev` can be invoked automatically
        auto_compile: If False, skip the dbt compile fallback (for tests, CI, etc.)

    Returns:
        (sql, error_message):
            - On success: (sql_string, None)
            - On failure: (None, human_readable_error)
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

    # Level 3: auto-compile (only in --dev mode)
    if use_dev and auto_compile:
        if not project_root:
            return None, _msg_no_project_root(model_name)

        print(
            f"ℹ️  No compiled SQL for '{model_name}'. Running `dbt compile --select {model_name} --target dev`...",
            file=sys.stderr,
        )
        ok, err = _run_dbt_compile(model_name, project_root)
        if not ok:
            return None, (
                f"dbt compile failed:\n{err}\n"
                f"Try running manually: dbt compile --select {model_name} --target dev"
            )

        if package_name and original_file_path:
            sql = _read_compiled_file(project_root, package_name, original_file_path)
            if sql:
                return sql, None

        return None, (
            f"dbt compile succeeded but compiled SQL not found at expected path.\n"
            f"Expected: target/compiled/{package_name}/{original_file_path}"
        )

    # Fallback exhausted
    if use_dev:
        return None, _msg_dev_no_compile(model_name)
    return None, _msg_prod_no_compile(model_name)


def _infer_project_root(manifest_path: str) -> str | None:
    """Find project root by looking for dbt_project.yml above the manifest path."""
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

    Priority: model['package_name'] → unique_id prefix `model.<pkg>.<name>`.
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
    """Read compiled SQL from target/compiled/{package}/{path} if it exists and is non-empty."""
    compiled_path = Path(project_root) / 'target' / 'compiled' / package_name / original_file_path
    if not compiled_path.is_file():
        return None
    try:
        content = compiled_path.read_text()
    except OSError:
        return None
    return content if content.strip() else None


def _run_dbt_compile(
    model_name: str,
    project_root: str,
    timeout: int = DBT_COMPILE_TIMEOUT,
) -> tuple[bool, str | None]:
    """Invoke `dbt compile --select <model> --target dev` in the project root.

    Returns:
        (success, error_text)
    """
    dbt_cmd = shutil.which('dbt')
    if not dbt_cmd:
        return False, "dbt CLI not found in PATH"

    try:
        result = subprocess.run(
            [dbt_cmd, 'compile', '--select', model_name, '--target', 'dev'],
            cwd=project_root,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired:
        return False, f"dbt compile timed out after {timeout}s"
    except OSError as exc:
        return False, f"Failed to launch dbt: {exc}"

    if result.returncode != 0:
        output = (result.stdout or '') + (result.stderr or '')
        return False, output.strip() or f"dbt compile exited with code {result.returncode}"

    return True, None


def _msg_dev_no_compile(model_name: str) -> str:
    return (
        f"No compiled SQL for '{model_name}' in dev manifest or target/compiled/.\n"
        f"Try: defer run --select {model_name}\n"
        f"Or:  dbt compile --select {model_name} --target dev"
    )


def _msg_prod_no_compile(model_name: str) -> str:
    return (
        f"No compiled SQL for '{model_name}' in production manifest.\n"
        f"For local changes, use: meta validate --dev {model_name}"
    )


def _msg_no_project_root(model_name: str) -> str:
    return (
        f"Could not auto-compile '{model_name}': project root not found "
        f"(no dbt_project.yml above the manifest path).\n"
        f"Run from inside the dbt project directory, or: "
        f"dbt compile --select {model_name} --target dev"
    )
