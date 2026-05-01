"""Tests covering gap branches in command_impl/powerbi.py.

Targets:
- line 105: token acquisition returns None
- line 113: workspace scan returns None
- lines 155-161: measures extraction with dataset_obj lookup
- lines 166-172: columns extraction with dataset_obj lookup
- line 189: table_result['measures'] assignment
- line 193: table_result['columns'] assignment
- line 199: tables_not_in_manifest counter
- lines 313-335: _parse_refresh_schedule branches (enabled variants)
"""

from __future__ import annotations

import pytest

from dbt_meta.command_impl.powerbi import PowerBiCommand
from dbt_meta.config import Config
from dbt_meta.errors import DbtMetaError


@pytest.fixture
def powerbi_config():
    """Config with Power BI fully enabled and credentials set."""
    return Config(
        powerbi_enabled=True,
        powerbi_tenant_id="test-tenant",
        powerbi_client_id="test-client",
        powerbi_client_secret="test-secret",
        powerbi_workspaces=["test-workspace"],
    )


@pytest.fixture
def mock_pbi_api(monkeypatch):
    """Monkeypatch all Power BI API entry points and manifest parser.

    Usage: tests call this fixture then call `mock_pbi_api(token=..., scan=..., tables=..., reports=..., models=...)`.
    """
    defaults = {
        'token': 'mock-token',
        'scan': {'name': 'Test Workspace', 'datasets': []},
        'tables': {},
        'reports': {},
        'models': {},
    }

    def _apply(**overrides):
        values = {**defaults, **overrides}
        monkeypatch.setattr(
            'dbt_meta.command_impl.powerbi.get_powerbi_token',
            lambda **kw: values['token'],
        )
        monkeypatch.setattr(
            'dbt_meta.command_impl.powerbi.fetch_workspace_scan',
            lambda *a, **kw: values['scan'],
        )
        monkeypatch.setattr(
            'dbt_meta.command_impl.powerbi.extract_tables_from_expressions',
            lambda *a, **kw: values['tables'],
        )
        monkeypatch.setattr(
            'dbt_meta.command_impl.powerbi.build_dataset_to_reports_map',
            lambda *a, **kw: values['reports'],
        )

        class MockParser:
            def __init__(self, models):
                self._models = models

            def get_all_models(self):
                return self._models

        monkeypatch.setattr(
            'dbt_meta.command_impl.powerbi.get_cached_parser',
            lambda _p: MockParser(values['models']),
        )

    return _apply


class TestPowerBiExecuteErrors:
    """Errors raised from execute() when API calls fail."""

    def test_raises_when_token_is_none(self, powerbi_config, mock_pbi_api):
        mock_pbi_api(token=None)
        command = PowerBiCommand(powerbi_config, '/fake/manifest.json')

        with pytest.raises(DbtMetaError) as exc:
            command.execute()

        assert 'access token' in exc.value.message.lower()

    def test_raises_when_scan_result_is_none(self, powerbi_config, mock_pbi_api):
        mock_pbi_api(scan=None)
        command = PowerBiCommand(powerbi_config, '/fake/manifest.json')

        with pytest.raises(DbtMetaError) as exc:
            command.execute()

        assert 'scan workspace' in exc.value.message.lower()


class TestPowerBiMetadataExtraction:
    """Cover measures/columns extraction paths and counter increments."""

    def _scan_with_dataset(self):
        """Scan result with one dataset exposing measures + columns."""
        return {
            'name': 'Test Workspace',
            'datasets': [
                {
                    'name': 'Ds A',
                    'tables': [
                        {
                            'name': 'client_events',
                            'measures': [
                                {'name': 'ClientCount', 'expression': 'COUNT(client_id)'}
                            ],
                            'columns': [
                                {'name': 'client_id', 'dataType': 'int64'}
                            ],
                        }
                    ],
                }
            ],
        }

    def test_measures_are_attached_to_table(self, powerbi_config, mock_pbi_api):
        mock_pbi_api(
            scan=self._scan_with_dataset(),
            tables={
                'Ds A': {
                    'id': 'ds-a',
                    'tables': ['core.client_events'],
                    'content_provider_type': 'Import',
                }
            },
            reports={'ds-a': ['Rep 1']},
        )
        command = PowerBiCommand(
            powerbi_config,
            '/fake/manifest.json',
            show_measures=True,
        )
        result = command.execute()

        table = result['datasets'][0]['tables'][0]
        assert 'measures' in table
        assert table['measures'][0]['name'] == 'ClientCount'

    def test_columns_are_attached_to_table(self, powerbi_config, mock_pbi_api):
        mock_pbi_api(
            scan=self._scan_with_dataset(),
            tables={
                'Ds A': {
                    'id': 'ds-a',
                    'tables': ['core.client_events'],
                    'content_provider_type': 'Import',
                }
            },
            reports={'ds-a': ['Rep 1']},
        )
        command = PowerBiCommand(
            powerbi_config,
            '/fake/manifest.json',
            show_columns=True,
        )
        result = command.execute()

        table = result['datasets'][0]['tables'][0]
        assert 'columns' in table
        assert table['columns'][0]['name'] == 'client_id'

    def test_show_full_returns_both_measures_and_columns(self, powerbi_config, mock_pbi_api):
        mock_pbi_api(
            scan=self._scan_with_dataset(),
            tables={
                'Ds A': {
                    'id': 'ds-a',
                    'tables': ['core.client_events'],
                    'content_provider_type': 'Import',
                }
            },
            reports={'ds-a': ['Rep 1']},
        )
        command = PowerBiCommand(
            powerbi_config,
            '/fake/manifest.json',
            show_full=True,
        )
        result = command.execute()

        table = result['datasets'][0]['tables'][0]
        assert 'measures' in table
        assert 'columns' in table

    def test_measures_skipped_when_dataset_name_missing(self, powerbi_config, mock_pbi_api):
        """dataset_obj is None when no matching dataset name — should not crash."""
        mock_pbi_api(
            scan={'name': 'WS', 'datasets': []},  # no dataset with matching name
            tables={
                'Missing': {
                    'id': 'ds-x',
                    'tables': ['core.x'],
                    'content_provider_type': 'Import',
                }
            },
            reports={'ds-x': ['Rep']},
        )
        command = PowerBiCommand(
            powerbi_config,
            '/fake/manifest.json',
            show_measures=True,
            show_columns=True,
        )
        result = command.execute()

        table = result['datasets'][0]['tables'][0]
        # No measures/columns attached because dataset_obj was None
        assert 'measures' not in table
        assert 'columns' not in table

    def test_counts_tables_in_manifest(self, powerbi_config, mock_pbi_api):
        """A table mapped to a dbt model increments tables_in_manifest."""
        mock_pbi_api(
            scan={'name': 'WS', 'datasets': []},
            tables={
                'Ds A': {
                    'id': 'ds-a',
                    'tables': ['core.client_events'],
                    'content_provider_type': 'Import',
                }
            },
            reports={'ds-a': ['Rep 1']},
            models={
                'model.pkg.core__client_events': {
                    'schema': 'core',
                    'alias': 'client_events',
                    'name': 'client_events',
                }
            },
        )
        command = PowerBiCommand(powerbi_config, '/fake/manifest.json')
        result = command.execute()

        assert result['summary']['tables_in_manifest'] == 1
        assert result['summary']['tables_not_in_manifest'] == 0
        table = result['datasets'][0]['tables'][0]
        assert table['in_manifest'] is True
        assert table['dbt_model'] == 'core__client_events'

    def test_counts_tables_not_in_manifest(self, powerbi_config, mock_pbi_api):
        """A table with no dbt model mapping increments tables_not_in_manifest."""
        mock_pbi_api(
            scan={'name': 'WS', 'datasets': []},
            tables={
                'Ds A': {
                    'id': 'ds-a',
                    'tables': ['unknown_schema.unknown_table'],
                    'content_provider_type': 'Import',
                }
            },
            reports={'ds-a': []},
            models={},  # empty reverse lookup → no manifest match
        )
        command = PowerBiCommand(powerbi_config, '/fake/manifest.json')
        result = command.execute()

        assert result['summary']['tables_in_manifest'] == 0
        assert result['summary']['tables_not_in_manifest'] == 1
        table = result['datasets'][0]['tables'][0]
        assert table['in_manifest'] is False
        assert table['dbt_model'] is None


class TestParseRefreshSchedule:
    """Cover _parse_refresh_schedule branches (lines 313-335)."""

    def _make(self):
        cfg = Config(
            powerbi_enabled=True,
            powerbi_tenant_id='t',
            powerbi_client_id='c',
            powerbi_client_secret='s',
            powerbi_workspaces=['w'],
        )
        return PowerBiCommand(cfg, '/fake/manifest.json')

    def test_returns_none_when_schedule_is_none(self):
        assert self._make()._parse_refresh_schedule(None) is None

    def test_disabled_schedule(self):
        result = self._make()._parse_refresh_schedule({'enabled': False})
        assert result == {'enabled': False, 'frequency': 'disabled'}

    def test_daily_with_single_time(self):
        schedule = {
            'enabled': True,
            'days': ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'],
            'times': ['08:00'],
        }
        result = self._make()._parse_refresh_schedule(schedule)
        assert result['enabled'] is True
        assert result['frequency'] == 'daily'
        assert result['times'] == ['08:00']

    def test_daily_with_multiple_times(self):
        schedule = {
            'enabled': True,
            'days': ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'],
            'times': ['08:00', '14:00', '20:00'],
        }
        result = self._make()._parse_refresh_schedule(schedule)
        assert result['frequency'] == '3x daily'

    def test_daily_implicit_when_no_times_and_no_days(self):
        # days=[] → days_per_week=7; times=[] → refreshes_per_day=0 → 'daily'
        schedule = {'enabled': True, 'days': [], 'times': []}
        result = self._make()._parse_refresh_schedule(schedule)
        assert result['frequency'] == 'daily'

    def test_partial_week(self):
        schedule = {
            'enabled': True,
            'days': ['Mon', 'Wed', 'Fri'],
            'times': ['09:00', '17:00'],
        }
        result = self._make()._parse_refresh_schedule(schedule)
        assert result['frequency'] == '2x on 3 days/week'

    def test_timezone_included_in_output(self):
        schedule = {
            'enabled': True,
            'days': [],
            'times': ['10:00'],
            'localTimeZoneId': 'Europe/Tallinn',
        }
        result = self._make()._parse_refresh_schedule(schedule)
        assert result['timezone'] == 'Europe/Tallinn'

    def test_default_timezone_utc(self):
        schedule = {'enabled': True, 'days': [], 'times': []}
        result = self._make()._parse_refresh_schedule(schedule)
        assert result['timezone'] == 'UTC'
