"""End-to-end: ValidateCommand.process_model emits `warnings` for unguarded
`_dbt_max_partition`. Complements the helper-level tests in
test_validate_static_checks.py — guards the helper→output wiring.
"""

from __future__ import annotations

from unittest.mock import patch

from dbt_meta.command_impl.validate import ValidateCommand
from dbt_meta.config import Config


def _make_cmd(prod_manifest, model_name: str) -> ValidateCommand:
    cfg = Config()
    cfg.prod_manifest_path = str(prod_manifest)
    return ValidateCommand(
        config=cfg,
        manifest_path=str(prod_manifest),
        model_name=model_name,
        use_dev=False,
        json_output=True,
    )


def _stub_model(name: str = 'stub_model') -> dict:
    return {
        'name': name,
        'resource_type': 'model',
        'config': {'materialized': 'view'},
    }


def test_process_model_attaches_warnings_for_unguarded_max_partition(prod_manifest, test_model):
    """Inject unguarded `_dbt_max_partition` into raw_code; expect a warning in result."""
    cmd = _make_cmd(prod_manifest, test_model)
    model = _stub_model(test_model)
    model['raw_code'] = (
        "SELECT * FROM {{ ref('foo') }}\n"
        "WHERE event_date >= _dbt_max_partition\n"
    )

    with patch('dbt_meta.command_impl.validate.get_compiled_sql',
               return_value=('SELECT 1', None)), \
         patch('dbt_meta.command_impl.validate.run_dry_run_query',
               return_value={'valid': True, 'error': None}):
        result = cmd.process_model(model)

    assert 'warnings' in result, f"warnings missing: {result}"
    codes = [w['code'] for w in result['warnings']]
    assert 'unguarded_dbt_max_partition' in codes
    w = next(w for w in result['warnings'] if w['code'] == 'unguarded_dbt_max_partition')
    assert w['severity'] == 'error'
    assert 'line(s) 2' in w['message']
    assert 'is_incremental' in w['hint']


def test_process_model_no_warnings_when_guarded(prod_manifest, test_model):
    """Properly guarded usage must NOT produce the warning."""
    cmd = _make_cmd(prod_manifest, test_model)
    model = _stub_model(test_model)
    model['raw_code'] = (
        "SELECT * FROM {{ ref('foo') }}\n"
        "{% if is_incremental() %}\n"
        "WHERE event_date >= _dbt_max_partition\n"
        "{% endif %}\n"
    )

    with patch('dbt_meta.command_impl.validate.get_compiled_sql',
               return_value=('SELECT 1', None)), \
         patch('dbt_meta.command_impl.validate.run_dry_run_query',
               return_value={'valid': True, 'error': None}):
        result = cmd.process_model(model)

    assert 'warnings' not in result, f"unexpected warnings: {result}"


def test_process_model_no_warnings_when_raw_code_absent(prod_manifest, test_model):
    """No raw_code → no static-check warnings (and no crash)."""
    cmd = _make_cmd(prod_manifest, test_model)
    model = _stub_model(test_model)

    with patch('dbt_meta.command_impl.validate.get_compiled_sql',
               return_value=('SELECT 1', None)), \
         patch('dbt_meta.command_impl.validate.run_dry_run_query',
               return_value={'valid': True, 'error': None}):
        result = cmd.process_model(model)

    assert 'warnings' not in result
