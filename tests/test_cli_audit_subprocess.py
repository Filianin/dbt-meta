"""End-to-end CLI tests for 0.3.1 audit additions.

Covers the typer wiring for `meta find`, `meta columns --all` and
`meta children --source` — flags, exit codes, stdout JSON contract.
The corresponding `dbt_meta.commands.*` callables are unit-tested
separately; these tests guard the CLI surface against silent flag drift.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys

import pytest

from dbt_meta.manifest.parser import ManifestParser


def _run(args: list[str], env_extra: dict | None = None) -> tuple[int, str, str]:
    env = os.environ.copy()
    if env_extra:
        env.update(env_extra)
    proc = subprocess.run(
        [sys.executable, "-m", "dbt_meta.cli", *args],
        capture_output=True,
        text=True,
        env=env,
    )
    return proc.returncode, proc.stdout, proc.stderr


def _pick_model(manifest_path: str) -> dict:
    parser = ManifestParser(str(manifest_path))
    for uid, node in parser.manifest.get('nodes', {}).items():
        if not uid.startswith('model.') or not node.get('schema'):
            continue
        return {
            'uid': uid,
            'name': uid.split('.')[-1],
            'schema': node['schema'],
            'table': node.get('config', {}).get('alias') or node.get('name'),
        }
    pytest.skip("No models with schema in manifest")


def _pick_documented_column(manifest_path: str) -> tuple[str, str]:
    parser = ManifestParser(str(manifest_path))
    for uid, node in parser.manifest.get('nodes', {}).items():
        if not uid.startswith('model.'):
            continue
        cols = node.get('columns') or {}
        if cols:
            return uid, next(iter(cols))
    pytest.skip("No models with documented columns in manifest")


def _pick_consumed_source(manifest_path: str) -> dict:
    parser = ManifestParser(str(manifest_path))
    child_map = parser.manifest.get('child_map', {})
    for uid, src in parser.manifest.get('sources', {}).items():
        if not uid.startswith('source.') or not child_map.get(uid):
            continue
        return {
            'uid': uid,
            'schema': src.get('schema', ''),
            'identifier': src.get('identifier') or src.get('name', ''),
        }
    pytest.skip("No consumed sources in manifest")


def test_cli_find_emits_json_array(prod_manifest):
    target = _pick_model(str(prod_manifest))
    code, stdout, _ = _run(
        ["find", target['table'], "-j"],
        env_extra={"DBT_PROD_MANIFEST_PATH": str(prod_manifest)},
    )
    assert code == 0, f"non-zero exit; stdout={stdout!r}"
    payload = json.loads(stdout)
    assert isinstance(payload, list) and payload
    assert any(r['unique_id'] == target['uid'] for r in payload)


def test_cli_columns_all_flag_routes_to_search(prod_manifest):
    """`meta columns --all <col>` must hit columns_search, not the single-model branch."""
    uid, col = _pick_documented_column(str(prod_manifest))
    code, stdout, _ = _run(
        ["columns", "--all", col, "-j"],
        env_extra={"DBT_PROD_MANIFEST_PATH": str(prod_manifest)},
    )
    assert code == 0, f"non-zero exit; stdout={stdout!r}"
    payload = json.loads(stdout)
    assert isinstance(payload, list) and payload
    assert any(r['unique_id'] == uid and r['column'] == col for r in payload)


def test_cli_children_source_flag_routes_to_source_lookup(prod_manifest):
    """`meta children --source schema.table` must resolve via sources, not nodes."""
    src = _pick_consumed_source(str(prod_manifest))
    ref = f"{src['schema']}.{src['identifier']}"
    code, stdout, _ = _run(
        ["children", "--source", ref, "-j"],
        env_extra={"DBT_PROD_MANIFEST_PATH": str(prod_manifest)},
    )
    assert code == 0, f"non-zero exit; stdout={stdout!r}"
    payload = json.loads(stdout)
    assert isinstance(payload, list) and payload


def test_cli_find_rejects_four_part_fqn(prod_manifest):
    code, stdout, stderr = _run(
        ["find", "a.b.c.d", "-j"],
        env_extra={"DBT_PROD_MANIFEST_PATH": str(prod_manifest)},
    )
    assert code != 0
    # Error must NOT pollute stdout when -j is on.
    if stdout.strip():
        payload = json.loads(stdout)
        assert isinstance(payload, dict) and 'error' in payload
    else:
        assert stderr  # at least one channel must report the error
