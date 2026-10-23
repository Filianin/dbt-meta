"""Compiled SQL retrieval + the shared dbt-compile toolkit.

This module is the single home for four concerns that used to be re-implemented
across ``usage/advisor_refresh.py`` and ``cli.py``:

* :func:`infer_project_root` — walk up to the dbt project root (manifest path,
  then cwd fallback).
* :func:`find_dbt_executable` — venv-aware discovery of the ``dbt`` CLI.
* :func:`read_disk_compiled` — read ``target/compiled/.../<file>.sql`` for a node.
* :func:`run_dbt_compile` — the one ``dbt compile`` subprocess runner, with
  single / bulk / whole-project modes selected by keyword args.
* :func:`run_dbt_parse` — ``dbt parse`` (graph refresh only, used by ``meta run-set``).

:func:`get_compiled_sql` (used by ``meta validate`` and ``meta scan``) is the
3-level fallback built on top of these:

1. model['compiled_code'] from manifest (fast, works for `dbt compile`/`dbt run` output)
2. target/compiled/{package}/{original_file_path} on disk (works when compile ran
   but dev manifest only had `dbt parse`)
3. Run `dbt compile --select <model> --target dev` (use_dev=True only), then re-check disk

Returns (sql, error_message) where exactly one is non-None.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

DBT_COMPILE_TIMEOUT = 180  # seconds — single/bulk compile budget


# ── shared dbt-compile toolkit ──────────────────────────────────────────────


def infer_project_root(manifest_path: str | None) -> str | None:
    """Find a dbt project root by walking from the manifest path, then cwd.

    Two-step strategy:
      1. Walk up from the manifest path itself (handles dev manifest at
         ``<project>/target/manifest.json``).
      2. Walk up from ``Path.cwd()`` (handles prod manifest at
         ``~/dbt-state/manifest.json`` when the user is invoking ``meta``
         from inside ``reports/`` or ``reports-spare/``). To avoid
         accidentally picking up unrelated ``dbt_project.yml`` files higher
         up the home directory, step 2 additionally requires a ``target/``
         subdirectory next to ``dbt_project.yml`` — a strong signal the user
         actually built the project (``dbt parse``/``compile``/``run`` all
         create it).

    Without step 2, prod-manifest runs from a real dbt project couldn't
    trigger auto-compile because ``~/dbt-state/`` is not a dbt project.
    """
    if manifest_path:
        try:
            resolved: Path | None = Path(manifest_path).expanduser().resolve()
        except OSError:
            resolved = None
        if resolved is not None:
            for parent in [resolved.parent, *resolved.parents]:
                if (parent / "dbt_project.yml").exists():
                    return str(parent)

    try:
        cwd = Path.cwd().resolve()
    except OSError:
        return None
    for parent in [cwd, *cwd.parents]:
        if (parent / "dbt_project.yml").exists() and (parent / "target").is_dir():
            return str(parent)
    return None


def find_dbt_executable(project_root: str | None) -> str | None:
    """Locate the dbt CLI to use, preferring the project's own venv.

    Order:
      1. ``<project_root>/.venv/bin/dbt`` (most common convention)
      2. ``<project_root>/venv/bin/dbt``
      3. ``$VIRTUAL_ENV/bin/dbt`` if the user has a venv activated
      4. ``shutil.which("dbt")`` — system PATH

    The project-local lookup matters because the project pins its dbt
    (dbt-core 1.x or dbt 2.0) inside its own venv, while a different ``dbt``
    may sit on PATH. Without this the wrong CLI would be invoked and either
    fail or produce a differently-parsed manifest.
    """
    if project_root:
        for candidate in (".venv/bin/dbt", "venv/bin/dbt"):
            local = Path(project_root) / candidate
            if local.is_file():
                return str(local)
    venv_env = os.environ.get("VIRTUAL_ENV")
    if venv_env:
        venv_dbt = Path(venv_env) / "bin" / "dbt"
        if venv_dbt.is_file():
            return str(venv_dbt)
    return shutil.which("dbt")


def _node_package(node: dict[str, Any]) -> str:
    """Package name for a node: ``package_name`` → unique_id ``model.<pkg>.<name>``."""
    pkg = node.get("package_name")
    if pkg:
        return str(pkg)
    parts = str(node.get("unique_id", "")).split(".")
    if len(parts) >= 3 and parts[0] == "model":
        return parts[1]
    return ""


def read_disk_compiled(project_root: str, node: dict[str, Any]) -> str:
    """Read compiled SQL from ``target/compiled/{package}/{original_file_path}``.

    Returns empty string when the file is missing, empty, or unreadable.
    """
    package = _node_package(node)
    path = node.get("original_file_path") or node.get("path") or ""
    if not (package and path):
        return ""
    compiled_path = Path(project_root) / "target" / "compiled" / package / path
    if not compiled_path.is_file():
        return ""
    try:
        content = compiled_path.read_text()
    except OSError:
        return ""
    return content if content.strip() else ""


def _run_dbt(
    project_root: str,
    subcommand: str,
    extra: list[str],
    timeout: int,
) -> tuple[bool, str | None]:
    """Run ``dbt <subcommand> <extra...>`` in ``project_root``. Returns (success, error_text).

    ``--project-dir`` is always passed: dbt 2.0 loads the project's ``.env``
    on its own, and a ``DBT_PROJECT_DIR`` there (e.g. a Docker path) would
    otherwise redirect the run, failing on its ``logs/`` directory.
    """
    dbt_cmd = find_dbt_executable(project_root)
    if not dbt_cmd:
        return False, "dbt CLI not found in project venv or PATH"

    try:
        result = subprocess.run(
            [dbt_cmd, subcommand, "--project-dir", project_root, *extra],
            cwd=project_root,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired:
        return False, f"dbt {subcommand} timed out after {timeout}s"
    except OSError as exc:
        return False, f"failed to launch dbt: {exc}"

    if result.returncode != 0:
        output = ((result.stdout or "") + (result.stderr or "")).strip()
        return False, output or f"dbt {subcommand} exited with code {result.returncode}"
    return True, None


def run_dbt_compile(
    project_root: str,
    *,
    select: list[str] | None = None,
    target: str | None = None,
    profiles_dir: str | None = None,
    timeout: int = DBT_COMPILE_TIMEOUT,
) -> tuple[bool, str | None]:
    """Invoke ``dbt compile`` in ``project_root``. Returns (success, error_text).

    Modes are expressed through the keyword args:
      * single: ``select=[model], target="dev"``
      * bulk:   ``select=[m1, m2, ...]``
      * whole:  ``select=None`` (compile the entire project)

    ``profiles_dir`` adds ``--profiles-dir`` (projects that ship their own
    ``profiles.yml`` at root). Without it dbt falls back to ``~/.dbt``.
    """
    extra: list[str] = []
    if select:
        extra += ["--select", *select]
    if target:
        extra += ["--target", target]
    if profiles_dir:
        extra += ["--profiles-dir", profiles_dir]
    return _run_dbt(project_root, "compile", extra, timeout)


def run_dbt_parse(
    project_root: str,
    *,
    target: str | None = "dev",
    timeout: int = DBT_COMPILE_TIMEOUT,
) -> tuple[bool, str | None]:
    """Invoke ``dbt parse`` in ``project_root`` — refreshes the manifest graph
    (nodes, ``depends_on``, config) without rendering SQL. Returns (success, error_text)."""
    extra = ["--target", target] if target else []
    return _run_dbt(project_root, "parse", extra, timeout)


# ── validate / scan: 3-level compiled-SQL fallback ──────────────────────────


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

    project_root = infer_project_root(manifest_path)

    # Level 2: target/compiled/ on disk
    if project_root:
        disk = read_disk_compiled(project_root, model)
        if disk:
            return disk, None

    # Level 3: auto-compile (only in --dev mode)
    if use_dev and auto_compile:
        if not project_root:
            return None, _msg_no_project_root(model_name)

        print(
            f"ℹ️  No compiled SQL for '{model_name}'. Running `dbt compile --select {model_name} --target dev`...",
            file=sys.stderr,
        )
        ok, err = run_dbt_compile(project_root, select=[model_name], target="dev")
        if not ok:
            return None, (
                f"dbt compile failed:\n{err}\n"
                f"Try running manually: dbt compile --select {model_name} --target dev"
            )

        disk = read_disk_compiled(project_root, model)
        if disk:
            return disk, None

        package_name = _node_package(model)
        original_file_path = model.get('original_file_path') or model.get('path') or ''
        return None, (
            f"dbt compile succeeded but compiled SQL not found at expected path.\n"
            f"Expected: target/compiled/{package_name}/{original_file_path}"
        )

    # Fallback exhausted
    if use_dev:
        return None, _msg_dev_no_compile(model_name)
    return None, _msg_prod_no_compile(model_name)


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
