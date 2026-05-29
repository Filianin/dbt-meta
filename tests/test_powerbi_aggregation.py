"""Tests for Power BI table aggregation feature."""

import pytest

from dbt_meta.command_impl.powerbi import PowerBiCommand
from dbt_meta.config import Config


class TestAggregateTablesUsage:
    """Test _aggregate_tables_usage method."""

    @pytest.fixture
    def command(self):
        """Create PowerBiCommand instance for testing."""
        config = Config(
            powerbi_enabled=True,
            powerbi_tenant_id="test-tenant",
            powerbi_client_id="test-client",
            powerbi_client_secret="test-secret",
            powerbi_workspaces=["test-workspace"],
        )
        return PowerBiCommand(
            config=config,
            manifest_path="test-manifest.json",
            by_table=True,
        )

    def test_single_table_single_dataset(self, command):
        """Test aggregating single table in single dataset."""
        datasets_result = [
            {
                'name': 'Dataset A',
                'reports': ['Report 1', 'Report 2'],
                'tables': [
                    {
                        'bigquery_table': 'core.events',
                        'dbt_model': 'core__events',
                        'in_manifest': True
                    }
                ]
            }
        ]

        result = command._aggregate_tables_usage(datasets_result, {})

        assert len(result) == 1
        assert result[0]['bigquery_table'] == 'core.events'
        assert result[0]['dbt_model'] == 'core__events'
        assert result[0]['in_manifest'] is True
        assert result[0]['report_count'] == 2
        assert result[0]['dataset_count'] == 1
        assert result[0]['datasets'] == ['Dataset A']
        assert result[0]['reports'] == ['Report 1', 'Report 2']

    def test_table_across_multiple_datasets(self, command):
        """Test aggregating table used across multiple datasets."""
        datasets_result = [
            {
                'name': 'Dataset A',
                'reports': ['Report 1', 'Report 2'],
                'tables': [
                    {
                        'bigquery_table': 'core.events',
                        'dbt_model': 'core__events',
                        'in_manifest': True
                    }
                ]
            },
            {
                'name': 'Dataset B',
                'reports': ['Report 3'],
                'tables': [
                    {
                        'bigquery_table': 'core.events',
                        'dbt_model': 'core__events',
                        'in_manifest': True
                    }
                ]
            }
        ]

        result = command._aggregate_tables_usage(datasets_result, {})

        assert len(result) == 1
        assert result[0]['bigquery_table'] == 'core.events'
        assert result[0]['report_count'] == 3
        assert result[0]['dataset_count'] == 2
        assert set(result[0]['datasets']) == {'Dataset A', 'Dataset B'}
        assert set(result[0]['reports']) == {'Report 1', 'Report 2', 'Report 3'}

    def test_multiple_reports_in_same_dataset(self, command):
        """Test that reports are counted correctly within same dataset."""
        datasets_result = [
            {
                'name': 'Dataset A',
                'reports': ['Report 1', 'Report 2', 'Report 3'],
                'tables': [
                    {
                        'bigquery_table': 'core.events',
                        'dbt_model': 'core__events',
                        'in_manifest': True
                    }
                ]
            }
        ]

        result = command._aggregate_tables_usage(datasets_result, {})

        assert result[0]['report_count'] == 3
        assert result[0]['dataset_count'] == 1

    def test_deduplication_of_reports(self, command):
        """Test that duplicate reports are deduplicated."""
        datasets_result = [
            {
                'name': 'Dataset A',
                'reports': ['Report 1', 'Report 2'],
                'tables': [
                    {
                        'bigquery_table': 'core.events',
                        'dbt_model': 'core__events',
                        'in_manifest': True
                    }
                ]
            },
            {
                'name': 'Dataset B',
                'reports': ['Report 1', 'Report 3'],  # Report 1 appears again
                'tables': [
                    {
                        'bigquery_table': 'core.events',
                        'dbt_model': 'core__events',
                        'in_manifest': True
                    }
                ]
            }
        ]

        result = command._aggregate_tables_usage(datasets_result, {})

        # Report 1 should only be counted once
        assert result[0]['report_count'] == 3
        assert set(result[0]['reports']) == {'Report 1', 'Report 2', 'Report 3'}

    def test_sorting_by_report_count_desc(self, command):
        """Test that results are sorted by report count descending."""
        datasets_result = [
            {
                'name': 'Dataset A',
                'reports': ['Report 1'],
                'tables': [
                    {
                        'bigquery_table': 'core.events',
                        'dbt_model': 'core__events',
                        'in_manifest': True
                    }
                ]
            },
            {
                'name': 'Dataset B',
                'reports': ['Report 2', 'Report 3', 'Report 4'],
                'tables': [
                    {
                        'bigquery_table': 'staging.data',
                        'dbt_model': 'staging__data',
                        'in_manifest': True
                    }
                ]
            }
        ]

        result = command._aggregate_tables_usage(datasets_result, {})

        assert len(result) == 2
        # staging.data should be first (3 reports > 1 report)
        assert result[0]['bigquery_table'] == 'staging.data'
        assert result[0]['report_count'] == 3
        assert result[1]['bigquery_table'] == 'core.events'
        assert result[1]['report_count'] == 1

    def test_dbt_model_mapping_preserved(self, command):
        """Test that dbt model mapping is preserved in aggregation."""
        datasets_result = [
            {
                'name': 'Dataset A',
                'reports': ['Report 1'],
                'tables': [
                    {
                        'bigquery_table': 'core.events',
                        'dbt_model': 'core__events',
                        'in_manifest': True
                    },
                    {
                        'bigquery_table': 'staging.unknown',
                        'dbt_model': None,
                        'in_manifest': False
                    }
                ]
            }
        ]

        result = command._aggregate_tables_usage(datasets_result, {})

        assert len(result) == 2
        # Check first table
        events_table = next(t for t in result if t['bigquery_table'] == 'core.events')
        assert events_table['dbt_model'] == 'core__events'
        assert events_table['in_manifest'] is True

        # Check second table
        unknown_table = next(t for t in result if t['bigquery_table'] == 'staging.unknown')
        assert unknown_table['dbt_model'] is None
        assert unknown_table['in_manifest'] is False

    def test_empty_datasets_result(self, command):
        """Test aggregation with empty datasets list."""
        datasets_result = []
        result = command._aggregate_tables_usage(datasets_result, {})
        assert result == []

    def test_datasets_with_no_tables(self, command):
        """Test aggregation when datasets have no tables."""
        datasets_result = [
            {
                'name': 'Dataset A',
                'reports': ['Report 1'],
                'tables': []
            }
        ]
        result = command._aggregate_tables_usage(datasets_result, {})
        assert result == []

    def test_secondary_sorting_by_table_name(self, command):
        """Test that tables with same report count are sorted by name."""
        datasets_result = [
            {
                'name': 'Dataset A',
                'reports': ['Report 1'],
                'tables': [
                    {
                        'bigquery_table': 'schema.table_z',
                        'dbt_model': 'schema__table_z',
                        'in_manifest': True
                    },
                    {
                        'bigquery_table': 'schema.table_a',
                        'dbt_model': 'schema__table_a',
                        'in_manifest': True
                    }
                ]
            }
        ]

        result = command._aggregate_tables_usage(datasets_result, {})

        assert len(result) == 2
        # Both have 1 report, should be sorted alphabetically
        assert result[0]['bigquery_table'] == 'schema.table_a'
        assert result[1]['bigquery_table'] == 'schema.table_z'


class TestPowerBiCommandByTableMode:
    """Test PowerBiCommand.execute() with by_table flag."""

    @pytest.fixture
    def mock_powerbi_env(self, monkeypatch):
        """Mock Power BI environment variables."""
        monkeypatch.setenv('POWERBI_ENABLED', 'true')
        monkeypatch.setenv('POWERBI_TENANT_ID', 'test-tenant')
        monkeypatch.setenv('POWERBI_CLIENT_ID', 'test-client')
        monkeypatch.setenv('POWERBI_CLIENT_SECRET', 'test-secret')
        monkeypatch.setenv('POWERBI_WORKSPACES', 'test-workspace')

    def test_by_table_flag_changes_output_structure(self, mock_powerbi_env, monkeypatch):
        """Test that by_table flag changes the output structure."""
        # Mock the Power BI API calls
        def mock_get_token(*args, **kwargs):
            return "mock-token"

        def mock_fetch_workspace(*args, **kwargs):
            return {
                'name': 'Test Workspace',
                'datasets': []
            }

        def mock_extract_tables(*args, **kwargs):
            return {}

        def mock_build_dataset_map(*args, **kwargs):
            return {}

        monkeypatch.setattr(
            'dbt_meta.command_impl.powerbi.get_powerbi_token',
            mock_get_token
        )
        monkeypatch.setattr(
            'dbt_meta.command_impl.powerbi.fetch_workspace_scan',
            mock_fetch_workspace
        )
        monkeypatch.setattr(
            'dbt_meta.command_impl.powerbi.extract_tables_from_expressions',
            mock_extract_tables
        )
        monkeypatch.setattr(
            'dbt_meta.command_impl.powerbi.build_dataset_to_reports_map',
            mock_build_dataset_map
        )

        # Mock manifest parser
        class MockParser:
            def get_all_models(self):
                return {}

        monkeypatch.setattr(
            'dbt_meta.command_impl.powerbi.get_cached_parser',
            lambda x: MockParser()
        )

        config = Config.from_env()

        # Test default mode
        command_default = PowerBiCommand(
            config=config,
            manifest_path="test-manifest.json",
            by_table=False,
        )
        result_default = command_default.execute()
        assert 'datasets' in result_default
        assert 'view' not in result_default

        # Test by_table mode
        command_by_table = PowerBiCommand(
            config=config,
            manifest_path="test-manifest.json",
            by_table=True,
        )
        result_by_table = command_by_table.execute()
        assert 'view' in result_by_table
        assert result_by_table['view'] == 'by_table'
        assert 'tables' in result_by_table
        assert 'datasets' not in result_by_table

    def test_summary_counts_match_between_views(self, mock_powerbi_env, monkeypatch):
        """Test that summary counts are consistent between views."""
        # Mock the Power BI API calls with real data
        def mock_get_token(*args, **kwargs):
            return "mock-token"

        def mock_fetch_workspace(*args, **kwargs):
            return {
                'name': 'Test Workspace',
                'datasets': []
            }

        def mock_extract_tables(*args, **kwargs):
            return {
                'Dataset A': {
                    'id': 'dataset-a',
                    'tables': ['core.events', 'staging.data'],
                    'content_provider_type': 'Import',
                }
            }

        def mock_build_dataset_map(*args, **kwargs):
            return {
                'dataset-a': ['Report 1', 'Report 2']
            }

        monkeypatch.setattr(
            'dbt_meta.command_impl.powerbi.get_powerbi_token',
            mock_get_token
        )
        monkeypatch.setattr(
            'dbt_meta.command_impl.powerbi.fetch_workspace_scan',
            mock_fetch_workspace
        )
        monkeypatch.setattr(
            'dbt_meta.command_impl.powerbi.extract_tables_from_expressions',
            mock_extract_tables
        )
        monkeypatch.setattr(
            'dbt_meta.command_impl.powerbi.build_dataset_to_reports_map',
            mock_build_dataset_map
        )

        # Mock manifest parser
        class MockParser:
            def get_all_models(self):
                return {}

        monkeypatch.setattr(
            'dbt_meta.command_impl.powerbi.get_cached_parser',
            lambda x: MockParser()
        )

        config = Config.from_env()

        # Test default mode
        command_default = PowerBiCommand(
            config=config,
            manifest_path="test-manifest.json",
            by_table=False,
        )
        result_default = command_default.execute()

        # Test by_table mode
        command_by_table = PowerBiCommand(
            config=config,
            manifest_path="test-manifest.json",
            by_table=True,
        )
        result_by_table = command_by_table.execute()

        # Summary counts should match
        assert result_default['summary']['total_tables'] == result_by_table['summary']['total_tables']
        assert result_default['summary']['total_reports'] == result_by_table['summary']['total_reports']
        assert result_default['summary']['total_datasets'] == result_by_table['summary']['total_datasets']
