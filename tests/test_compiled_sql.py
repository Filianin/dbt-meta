"""Tests for utils/compiled_sql.py — 3-level compiled SQL fallback.

Levels:
1. model['compiled_code'] from manifest
2. target/compiled/{package}/{original_file_path} on disk
3. Auto-run `dbt compile --select <model> --target dev` (use_dev only)
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from dbt_meta.utils.compiled_sql import (
    _extract_package_name,
    _infer_project_root,
    _read_compiled_file,
    get_compiled_sql,
)


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def dbt_project(tmp_path):
    """Create a minimal dbt project layout.

    Returns dict with paths:
        root, manifest, compiled_dir, model_file (relative), package_name
    """
    root = tmp_path / 'my_project'
    (root).mkdir()
    (root / 'dbt_project.yml').write_text('name: my_pkg\nversion: "1.0"\n')

    target = root / 'target'
    target.mkdir()
    manifest = target / 'manifest.json'
    manifest.write_text('{}')  # content doesn't matter for these tests

    compiled_dir = target / 'compiled' / 'my_pkg' / 'models'
    compiled_dir.mkdir(parents=True)

    return {
        'root': root,
        'manifest': manifest,
        'compiled_dir': compiled_dir,
        'package_name': 'my_pkg',
        'model_rel_path': 'models/events.sql',
    }


def make_model(
    compiled_code: str = '',
    package_name: str = 'my_pkg',
    original_file_path: str = 'models/events.sql',
    unique_id: str = 'model.my_pkg.events',
) -> dict:
    return {
        'compiled_code': compiled_code,
        'package_name': package_name,
        'original_file_path': original_file_path,
        'unique_id': unique_id,
    }


# =============================================================================
# Level 1: manifest compiled_code
# =============================================================================


class TestManifestLevel:
    def test_returns_compiled_code_when_present(self):
        model = make_model(compiled_code='SELECT 1')
        sql, error = get_compiled_sql(
            model=model,
            model_name='events',
            manifest_path='/fake/manifest.json',
            use_dev=False,
        )
        assert sql == 'SELECT 1'
        assert error is None

    def test_empty_compiled_code_treated_as_missing(self, tmp_path):
        model = make_model(compiled_code='   \n\t  ')
        sql, error = get_compiled_sql(
            model=model,
            model_name='events',
            manifest_path=str(tmp_path / 'm.json'),
            use_dev=False,
            auto_compile=False,
        )
        assert sql is None
        assert 'No compiled SQL' in error


# =============================================================================
# Level 2: target/compiled disk fallback
# =============================================================================


class TestDiskFallback:
    def test_reads_from_target_compiled(self, dbt_project):
        # Create compiled file
        compiled = dbt_project['compiled_dir'] / 'events.sql'
        compiled.write_text('SELECT * FROM foo')

        model = make_model(compiled_code='')
        sql, error = get_compiled_sql(
            model=model,
            model_name='events',
            manifest_path=str(dbt_project['manifest']),
            use_dev=False,
        )
        assert sql == 'SELECT * FROM foo'
        assert error is None

    def test_empty_compiled_file_is_ignored(self, dbt_project):
        compiled = dbt_project['compiled_dir'] / 'events.sql'
        compiled.write_text('   ')  # only whitespace

        model = make_model(compiled_code='')
        sql, error = get_compiled_sql(
            model=model,
            model_name='events',
            manifest_path=str(dbt_project['manifest']),
            use_dev=False,
            auto_compile=False,
        )
        assert sql is None
        assert error is not None

    def test_missing_compiled_file_returns_error(self, dbt_project):
        # No compiled file written
        model = make_model(compiled_code='')
        sql, error = get_compiled_sql(
            model=model,
            model_name='events',
            manifest_path=str(dbt_project['manifest']),
            use_dev=False,
            auto_compile=False,
        )
        assert sql is None
        assert 'No compiled SQL' in error

    def test_unique_id_used_when_package_name_missing(self, dbt_project):
        compiled = dbt_project['compiled_dir'] / 'events.sql'
        compiled.write_text('SELECT * FROM bar')

        # Remove package_name but provide unique_id
        model = make_model(compiled_code='')
        model.pop('package_name')
        model['unique_id'] = 'model.my_pkg.events'

        sql, error = get_compiled_sql(
            model=model,
            model_name='events',
            manifest_path=str(dbt_project['manifest']),
            use_dev=False,
        )
        assert sql == 'SELECT * FROM bar'


# =============================================================================
# Level 3: auto-compile via dbt compile
# =============================================================================


class TestAutoCompile:
    def test_auto_compile_runs_when_use_dev_true(self, dbt_project, monkeypatch):
        compiled = dbt_project['compiled_dir'] / 'events.sql'

        # Simulate successful dbt compile by writing the file when subprocess runs
        def fake_subprocess_run(*args, **kwargs):
            compiled.write_text('COMPILED FROM DBT')

            class R:
                returncode = 0
                stdout = 'OK'
                stderr = ''
            return R()

        monkeypatch.setattr('dbt_meta.utils.compiled_sql.shutil.which', lambda _: '/usr/bin/dbt')
        monkeypatch.setattr('dbt_meta.utils.compiled_sql.subprocess.run', fake_subprocess_run)

        model = make_model(compiled_code='')
        sql, error = get_compiled_sql(
            model=model,
            model_name='events',
            manifest_path=str(dbt_project['manifest']),
            use_dev=True,
        )
        assert sql == 'COMPILED FROM DBT'
        assert error is None

    def test_auto_compile_not_triggered_when_use_dev_false(self, dbt_project, monkeypatch):
        called = {'yes': False}

        def should_not_run(*args, **kwargs):
            called['yes'] = True
            class R:
                returncode = 0
                stdout = ''
                stderr = ''
            return R()

        monkeypatch.setattr('dbt_meta.utils.compiled_sql.shutil.which', lambda _: '/usr/bin/dbt')
        monkeypatch.setattr('dbt_meta.utils.compiled_sql.subprocess.run', should_not_run)

        model = make_model(compiled_code='')
        sql, error = get_compiled_sql(
            model=model,
            model_name='events',
            manifest_path=str(dbt_project['manifest']),
            use_dev=False,
        )
        assert sql is None
        assert called['yes'] is False

    def test_auto_compile_disabled_by_flag(self, dbt_project, monkeypatch):
        called = {'yes': False}

        def should_not_run(*args, **kwargs):
            called['yes'] = True
            class R:
                returncode = 0
                stdout = ''
                stderr = ''
            return R()

        monkeypatch.setattr('dbt_meta.utils.compiled_sql.shutil.which', lambda _: '/usr/bin/dbt')
        monkeypatch.setattr('dbt_meta.utils.compiled_sql.subprocess.run', should_not_run)

        model = make_model(compiled_code='')
        sql, error = get_compiled_sql(
            model=model,
            model_name='events',
            manifest_path=str(dbt_project['manifest']),
            use_dev=True,
            auto_compile=False,
        )
        assert sql is None
        assert called['yes'] is False

    def test_auto_compile_fails_when_dbt_not_in_path(self, dbt_project, monkeypatch):
        monkeypatch.setattr('dbt_meta.utils.compiled_sql.shutil.which', lambda _: None)

        model = make_model(compiled_code='')
        sql, error = get_compiled_sql(
            model=model,
            model_name='events',
            manifest_path=str(dbt_project['manifest']),
            use_dev=True,
        )
        assert sql is None
        assert 'dbt CLI not found' in error

    def test_auto_compile_returns_error_when_exit_nonzero(self, dbt_project, monkeypatch):
        def failing_run(*args, **kwargs):
            class R:
                returncode = 1
                stdout = ''
                stderr = 'syntax error on line 5'
            return R()

        monkeypatch.setattr('dbt_meta.utils.compiled_sql.shutil.which', lambda _: '/usr/bin/dbt')
        monkeypatch.setattr('dbt_meta.utils.compiled_sql.subprocess.run', failing_run)

        model = make_model(compiled_code='')
        sql, error = get_compiled_sql(
            model=model,
            model_name='events',
            manifest_path=str(dbt_project['manifest']),
            use_dev=True,
        )
        assert sql is None
        assert 'dbt compile failed' in error
        assert 'syntax error on line 5' in error

    def test_auto_compile_handles_timeout(self, dbt_project, monkeypatch):
        import subprocess as sub

        def timeout_run(*args, **kwargs):
            raise sub.TimeoutExpired(cmd='dbt', timeout=1)

        monkeypatch.setattr('dbt_meta.utils.compiled_sql.shutil.which', lambda _: '/usr/bin/dbt')
        monkeypatch.setattr('dbt_meta.utils.compiled_sql.subprocess.run', timeout_run)

        model = make_model(compiled_code='')
        sql, error = get_compiled_sql(
            model=model,
            model_name='events',
            manifest_path=str(dbt_project['manifest']),
            use_dev=True,
        )
        assert sql is None
        assert 'timed out' in error

    def test_auto_compile_succeeds_but_file_still_missing(self, dbt_project, monkeypatch):
        """dbt compile exits 0 but didn't produce the expected file."""
        def fake_run(*args, **kwargs):
            class R:
                returncode = 0
                stdout = ''
                stderr = ''
            return R()

        monkeypatch.setattr('dbt_meta.utils.compiled_sql.shutil.which', lambda _: '/usr/bin/dbt')
        monkeypatch.setattr('dbt_meta.utils.compiled_sql.subprocess.run', fake_run)

        model = make_model(compiled_code='')
        sql, error = get_compiled_sql(
            model=model,
            model_name='events',
            manifest_path=str(dbt_project['manifest']),
            use_dev=True,
        )
        assert sql is None
        assert 'not found at expected path' in error

    def test_auto_compile_fails_when_no_project_root(self, tmp_path, monkeypatch):
        """When manifest is outside a dbt project, auto-compile aborts with clear error."""
        # tmp_path has no dbt_project.yml anywhere up the tree
        manifest = tmp_path / 'manifest.json'
        manifest.write_text('{}')

        called = {'yes': False}

        def should_not_run(*args, **kwargs):
            called['yes'] = True
            class R:
                returncode = 0
                stdout = ''
                stderr = ''
            return R()

        monkeypatch.setattr('dbt_meta.utils.compiled_sql.shutil.which', lambda _: '/usr/bin/dbt')
        monkeypatch.setattr('dbt_meta.utils.compiled_sql.subprocess.run', should_not_run)

        model = make_model(compiled_code='')
        sql, error = get_compiled_sql(
            model=model,
            model_name='events',
            manifest_path=str(manifest),
            use_dev=True,
        )
        assert sql is None
        assert called['yes'] is False
        assert 'project root not found' in error


# =============================================================================
# Helper functions
# =============================================================================


class TestExtractPackageName:
    def test_uses_package_name_field(self):
        model = {'package_name': 'explicit_pkg'}
        assert _extract_package_name(model) == 'explicit_pkg'

    def test_falls_back_to_unique_id(self):
        model = {'unique_id': 'model.from_unique.events'}
        assert _extract_package_name(model) == 'from_unique'

    def test_returns_empty_for_malformed_unique_id(self):
        model = {'unique_id': 'source.x.y'}  # not a model
        assert _extract_package_name(model) == ''

    def test_returns_empty_for_missing_data(self):
        assert _extract_package_name({}) == ''


class TestInferProjectRoot:
    def test_finds_project_root_with_dbt_project_yml(self, dbt_project):
        root = _infer_project_root(str(dbt_project['manifest']))
        assert root is not None
        assert Path(root) == dbt_project['root']

    def test_returns_none_when_no_project_file(self, tmp_path):
        manifest = tmp_path / 'nowhere.json'
        manifest.write_text('{}')
        assert _infer_project_root(str(manifest)) is None

    def test_returns_none_for_empty_path(self):
        assert _infer_project_root('') is None


class TestRareErrorPaths:
    """Cover OSError branches that happen in rare filesystem failure modes."""

    def test_infer_project_root_handles_os_error(self, monkeypatch):
        def raise_oserror(self):
            raise OSError('nope')

        monkeypatch.setattr(Path, 'resolve', raise_oserror, raising=True)
        assert _infer_project_root('/some/path') is None

    def test_read_compiled_file_handles_os_error(self, dbt_project, monkeypatch):
        compiled = dbt_project['compiled_dir'] / 'events.sql'
        compiled.write_text('SELECT 1')

        def raise_oserror(self, *args, **kwargs):
            raise OSError('read failed')

        monkeypatch.setattr(Path, 'read_text', raise_oserror, raising=True)
        result = _read_compiled_file(
            project_root=str(dbt_project['root']),
            package_name='my_pkg',
            original_file_path='models/events.sql',
        )
        assert result is None

    def test_auto_compile_handles_os_error_launching_dbt(self, dbt_project, monkeypatch):
        def raise_oserror(*args, **kwargs):
            raise OSError('cannot launch')

        monkeypatch.setattr('dbt_meta.utils.compiled_sql.shutil.which', lambda _: '/usr/bin/dbt')
        monkeypatch.setattr('dbt_meta.utils.compiled_sql.subprocess.run', raise_oserror)

        model = make_model(compiled_code='')
        sql, error = get_compiled_sql(
            model=model,
            model_name='events',
            manifest_path=str(dbt_project['manifest']),
            use_dev=True,
        )
        assert sql is None
        assert 'Failed to launch dbt' in error


class TestReadCompiledFile:
    def test_reads_existing_file(self, dbt_project):
        compiled = dbt_project['compiled_dir'] / 'x.sql'
        compiled.write_text('SELECT 2')

        result = _read_compiled_file(
            project_root=str(dbt_project['root']),
            package_name='my_pkg',
            original_file_path='models/x.sql',
        )
        assert result == 'SELECT 2'

    def test_returns_none_when_missing(self, dbt_project):
        result = _read_compiled_file(
            project_root=str(dbt_project['root']),
            package_name='my_pkg',
            original_file_path='models/nope.sql',
        )
        assert result is None

    def test_returns_none_for_empty_file(self, dbt_project):
        compiled = dbt_project['compiled_dir'] / 'blank.sql'
        compiled.write_text('')

        result = _read_compiled_file(
            project_root=str(dbt_project['root']),
            package_name='my_pkg',
            original_file_path='models/blank.sql',
        )
        assert result is None


# =============================================================================
# Integration with ValidateCommand / ScanCommand
# =============================================================================


class TestValidateWithDiskFallback:
    """End-to-end: compiled SQL missing from manifest, present on disk → validate succeeds."""

    def test_validate_uses_disk_fallback(self, dbt_project, monkeypatch):
        import json as _json

        from dbt_meta.commands import validate

        # Manifest has model but no compiled_code
        manifest = dbt_project['manifest']
        manifest.write_text(_json.dumps({
            'nodes': {
                'model.my_pkg.events': {
                    'name': 'events',
                    'package_name': 'my_pkg',
                    'unique_id': 'model.my_pkg.events',
                    'original_file_path': 'models/events.sql',
                    'schema': 's',
                    'database': 'd',
                    'config': {},
                    # no compiled_code
                },
            },
        }))

        # Write compiled SQL to disk
        compiled = dbt_project['compiled_dir'] / 'events.sql'
        compiled.write_text('SELECT 1')

        # Stub run_dry_run_query to avoid calling bq
        monkeypatch.setattr(
            'dbt_meta.command_impl.validate.run_dry_run_query',
            lambda sql: {'valid': True, 'error': None, 'bytes_processed': 0},
        )

        monkeypatch.setenv('DBT_PROD_MANIFEST_PATH', str(manifest))
        with patch('dbt_meta.config.Config.find_config_file', return_value=None):
            result = validate(
                str(manifest),
                'events',
                use_dev=False,
                json_output=False,
            )

        assert result is not None
        assert result['valid'] is True
        assert result['error'] is None
