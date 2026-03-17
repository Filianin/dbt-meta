"""Tests for Power BI integration module."""

import json
import subprocess
from unittest.mock import MagicMock, patch

import pytest

from dbt_meta.config import Config
from dbt_meta.errors import DbtMetaError
from dbt_meta.utils.powerbi import (
    _call_powerbi_api,
    _parse_m_expression,
    build_dataset_to_reports_map,
    extract_columns_from_dataset,
    extract_measures_from_dataset,
    extract_tables_from_expressions,
    fetch_dataset_refreshes,
    fetch_workspace_scan,
    get_powerbi_token,
    get_workspace_info,
    list_workspaces,
    parse_dax_references,
)


class TestParseMExpression:
    """Test M-expression parsing for BigQuery table extraction."""

    def test_parse_schema_table_pattern(self):
        """Test parsing standard [Name="schema",Kind="Schema"]...[Name="table",Kind="Table"]."""
        expression = '''
        let
            Source = GoogleBigQuery.Database(null),
            #"admirals-bi-dwh" = Source{[Name="admirals-bi-dwh"]}[Data],
            core_client_Schema = #"admirals-bi-dwh"{[Name="core_client",Kind="Schema"]}[Data],
            client_info_Table = core_client_Schema{[Name="client_info",Kind="Table"]}[Data]
        in
            client_info_Table
        '''

        tables = _parse_m_expression(expression)

        assert 'core_client.client_info' in tables

    def test_parse_view_pattern(self):
        """Test parsing View kind instead of Table."""
        expression = '''
        #"admirals-bi-dwh"{[Name="staging_amas",Kind="Schema"]}[Data],
        partners_View = staging_amas_Schema{[Name="partners",Kind="View"]}[Data]
        '''

        tables = _parse_m_expression(expression)

        assert 'staging_amas.partners' in tables

    def test_parse_native_query_from_clause(self):
        """Test parsing FROM clause in native SQL queries."""
        expression = '''
        Value.NativeQuery(
            Source,
            "SELECT * FROM `core_client.client_profiles` WHERE date > '2024-01-01'"
        )
        '''

        tables = _parse_m_expression(expression)

        assert 'core_client.client_profiles' in tables

    def test_parse_multiple_tables(self):
        """Test parsing multiple tables from same expression."""
        expression = '''
        schema1 = #"proj"{[Name="schema1",Kind="Schema"]}[Data],
        table1 = schema1{[Name="table1",Kind="Table"]}[Data],
        schema2 = #"proj"{[Name="schema2",Kind="Schema"]}[Data],
        table2 = schema2{[Name="table2",Kind="View"]}[Data]
        '''

        tables = _parse_m_expression(expression)

        assert 'schema1.table1' in tables
        assert 'schema2.table2' in tables
        assert len(tables) == 2

    def test_parse_empty_expression(self):
        """Test parsing empty expression returns empty list."""
        tables = _parse_m_expression('')
        assert tables == []

    def test_parse_no_bigquery_references(self):
        """Test parsing expression without BigQuery references."""
        expression = '''
        let
            Source = Excel.Workbook(File.Contents("data.xlsx")),
            Sheet1 = Source{[Name="Sheet1"]}[Data]
        in
            Sheet1
        '''

        tables = _parse_m_expression(expression)

        assert tables == []


class TestExtractTablesFromExpressions:
    """Test extraction of tables from workspace scan result."""

    def test_extract_from_single_dataset(self):
        """Test extracting tables from single dataset."""
        scan_result = {
            'datasets': [
                {
                    'id': 'dataset-1',
                    'name': 'Traffic Dataset',
                    'contentProviderType': 'PbixInDirectQueryMode',
                    'tables': [
                        {
                            'source': [
                                {
                                    'expression': '''
                                    #"proj"{[Name="core_client",Kind="Schema"]}[Data],
                                    {[Name="events",Kind="Table"]}[Data]
                                    '''
                                }
                            ]
                        }
                    ]
                }
            ]
        }

        result = extract_tables_from_expressions(scan_result)

        assert 'Traffic Dataset' in result
        dataset_info = result['Traffic Dataset']
        assert dataset_info['id'] == 'dataset-1'
        assert 'core_client.events' in dataset_info['tables']
        assert dataset_info['content_provider_type'] == 'PbixInDirectQueryMode'

    def test_extract_from_multiple_datasets(self):
        """Test extracting tables from multiple datasets."""
        scan_result = {
            'datasets': [
                {
                    'id': 'ds-1',
                    'name': 'Dataset 1',
                    'contentProviderType': 'PbixInImportMode',
                    'tables': [
                        {'source': [{'expression': '#"proj"{[Name="s1",Kind="Schema"]}[Data]{[Name="t1",Kind="Table"]}'}]}
                    ]
                },
                {
                    'id': 'ds-2',
                    'name': 'Dataset 2',
                    'contentProviderType': 'PbixInDirectQueryMode',
                    'tables': [
                        {'source': [{'expression': '#"proj"{[Name="s2",Kind="Schema"]}[Data]{[Name="t2",Kind="View"]}'}]}
                    ]
                }
            ]
        }

        result = extract_tables_from_expressions(scan_result)

        assert len(result) == 2
        assert 's1.t1' in result['Dataset 1']['tables']
        assert 's2.t2' in result['Dataset 2']['tables']
        assert result['Dataset 1']['content_provider_type'] == 'PbixInImportMode'
        assert result['Dataset 2']['content_provider_type'] == 'PbixInDirectQueryMode'

    def test_deduplicate_tables_within_dataset(self):
        """Test that duplicate tables are deduplicated."""
        scan_result = {
            'datasets': [
                {
                    'id': 'ds-1',
                    'name': 'Dataset 1',
                    'tables': [
                        {'source': [{'expression': '#"proj"{[Name="schema",Kind="Schema"]}[Data]{[Name="table",Kind="Table"]}'}]},
                        {'source': [{'expression': '#"proj"{[Name="schema",Kind="Schema"]}[Data]{[Name="table",Kind="Table"]}'}]}
                    ]
                }
            ]
        }

        result = extract_tables_from_expressions(scan_result)

        tables = result['Dataset 1']['tables']
        assert tables.count('schema.table') == 1

    def test_skip_datasets_without_bigquery(self):
        """Test that datasets without BigQuery tables are skipped."""
        scan_result = {
            'datasets': [
                {
                    'id': 'ds-1',
                    'name': 'Excel Dataset',
                    'tables': [
                        {'source': [{'expression': 'Excel.Workbook(File.Contents("data.xlsx"))'}]}
                    ]
                }
            ]
        }

        result = extract_tables_from_expressions(scan_result)

        assert len(result) == 0


class TestBuildDatasetToReportsMap:
    """Test building dataset to reports mapping."""

    def test_single_report_per_dataset(self):
        """Test mapping single report to dataset."""
        scan_result = {
            'reports': [
                {'name': 'Report 1', 'datasetId': 'ds-1'},
            ]
        }

        result = build_dataset_to_reports_map(scan_result)

        assert result == {'ds-1': ['Report 1']}

    def test_multiple_reports_per_dataset(self):
        """Test mapping multiple reports to same dataset."""
        scan_result = {
            'reports': [
                {'name': 'Report 1', 'datasetId': 'ds-1'},
                {'name': 'Report 2', 'datasetId': 'ds-1'},
                {'name': 'Report 3', 'datasetId': 'ds-2'},
            ]
        }

        result = build_dataset_to_reports_map(scan_result)

        assert 'ds-1' in result
        assert len(result['ds-1']) == 2
        assert 'Report 1' in result['ds-1']
        assert 'Report 2' in result['ds-1']
        assert result['ds-2'] == ['Report 3']

    def test_skip_reports_without_dataset_id(self):
        """Test that reports without datasetId are skipped."""
        scan_result = {
            'reports': [
                {'name': 'Report 1', 'datasetId': 'ds-1'},
                {'name': 'Report 2'},  # No datasetId
            ]
        }

        result = build_dataset_to_reports_map(scan_result)

        assert len(result) == 1
        assert 'ds-1' in result

    def test_empty_reports(self):
        """Test handling empty reports list."""
        scan_result = {'reports': []}

        result = build_dataset_to_reports_map(scan_result)

        assert result == {}


class TestPowerBiConfig:
    """Test Power BI configuration in Config class."""

    def test_default_powerbi_disabled(self):
        """Test that Power BI is disabled by default."""
        config = Config()

        assert config.powerbi_enabled is False
        assert config.powerbi_tenant_id is None
        assert config.powerbi_client_id is None
        assert config.powerbi_client_secret is None
        assert config.powerbi_workspaces == []

    def test_powerbi_config_from_env(self, monkeypatch):
        """Test loading Power BI config from environment variables."""
        monkeypatch.setenv('POWERBI_ENABLED', 'true')
        monkeypatch.setenv('POWERBI_TENANT_ID', 'test-tenant')
        monkeypatch.setenv('POWERBI_CLIENT_ID', 'test-client')
        monkeypatch.setenv('POWERBI_CLIENT_SECRET', 'test-secret')
        monkeypatch.setenv('POWERBI_WORKSPACES', 'ws1,ws2,ws3')

        config = Config.from_env()

        assert config.powerbi_enabled is True
        assert config.powerbi_tenant_id == 'test-tenant'
        assert config.powerbi_client_id == 'test-client'
        assert config.powerbi_client_secret == 'test-secret'
        assert config.powerbi_workspaces == ['ws1', 'ws2', 'ws3']

    def test_powerbi_workspaces_single_value(self, monkeypatch):
        """Test loading single workspace from environment."""
        monkeypatch.setenv('POWERBI_WORKSPACES', 'single-workspace')

        config = Config.from_env()

        assert config.powerbi_workspaces == ['single-workspace']

    def test_powerbi_workspaces_strips_whitespace(self, monkeypatch):
        """Test that workspace IDs are stripped of whitespace."""
        monkeypatch.setenv('POWERBI_WORKSPACES', ' ws1 , ws2 , ws3 ')

        config = Config.from_env()

        assert config.powerbi_workspaces == ['ws1', 'ws2', 'ws3']


class TestPowerBiCommand:
    """Test PowerBiCommand class."""

    def test_raises_when_disabled(self, monkeypatch):
        """Test that command raises when Power BI is disabled."""
        from dbt_meta.command_impl.powerbi import PowerBiCommand

        monkeypatch.setenv('POWERBI_ENABLED', 'false')
        config = Config.from_env()

        command = PowerBiCommand(config, '/fake/manifest.json')

        with pytest.raises(DbtMetaError) as exc_info:
            command.execute()

        assert 'disabled' in str(exc_info.value.message).lower()

    def test_raises_when_credentials_missing(self, monkeypatch):
        """Test that command raises when credentials are missing."""
        from dbt_meta.command_impl.powerbi import PowerBiCommand

        monkeypatch.setenv('POWERBI_ENABLED', 'true')
        monkeypatch.delenv('POWERBI_TENANT_ID', raising=False)
        monkeypatch.delenv('POWERBI_CLIENT_ID', raising=False)
        monkeypatch.delenv('POWERBI_CLIENT_SECRET', raising=False)

        config = Config.from_env()
        command = PowerBiCommand(config, '/fake/manifest.json')

        with pytest.raises(DbtMetaError) as exc_info:
            command.execute()

        assert 'credentials' in str(exc_info.value.message).lower()

    def test_raises_when_no_workspace_id(self, monkeypatch):
        """Test that command raises when no workspace ID available."""
        from dbt_meta.command_impl.powerbi import PowerBiCommand

        monkeypatch.setenv('POWERBI_ENABLED', 'true')
        monkeypatch.setenv('POWERBI_TENANT_ID', 'test-tenant')
        monkeypatch.setenv('POWERBI_CLIENT_ID', 'test-client')
        monkeypatch.setenv('POWERBI_CLIENT_SECRET', 'test-secret')
        monkeypatch.delenv('POWERBI_WORKSPACES', raising=False)

        config = Config.from_env()
        command = PowerBiCommand(config, '/fake/manifest.json', workspace_id=None)

        with pytest.raises(DbtMetaError) as exc_info:
            command._resolve_workspace_id()

        assert 'workspace' in str(exc_info.value.message).lower()

    def test_uses_first_workspace_from_config(self, monkeypatch):
        """Test that first workspace from config is used when no ID provided."""
        from dbt_meta.command_impl.powerbi import PowerBiCommand

        monkeypatch.setenv('POWERBI_ENABLED', 'true')
        monkeypatch.setenv('POWERBI_TENANT_ID', 'test-tenant')
        monkeypatch.setenv('POWERBI_CLIENT_ID', 'test-client')
        monkeypatch.setenv('POWERBI_CLIENT_SECRET', 'test-secret')
        monkeypatch.setenv('POWERBI_WORKSPACES', 'ws-first,ws-second')

        config = Config.from_env()
        command = PowerBiCommand(config, '/fake/manifest.json', workspace_id=None)

        workspace_id = command._resolve_workspace_id()

        assert workspace_id == 'ws-first'

    def test_uses_provided_workspace_id(self, monkeypatch):
        """Test that provided workspace ID takes priority."""
        from dbt_meta.command_impl.powerbi import PowerBiCommand

        monkeypatch.setenv('POWERBI_ENABLED', 'true')
        monkeypatch.setenv('POWERBI_TENANT_ID', 'test-tenant')
        monkeypatch.setenv('POWERBI_CLIENT_ID', 'test-client')
        monkeypatch.setenv('POWERBI_CLIENT_SECRET', 'test-secret')
        monkeypatch.setenv('POWERBI_WORKSPACES', 'ws-config')

        config = Config.from_env()
        command = PowerBiCommand(config, '/fake/manifest.json', workspace_id='ws-explicit')

        workspace_id = command._resolve_workspace_id()

        assert workspace_id == 'ws-explicit'


class TestPowerBiReverseModelLookup:
    """Test reverse model lookup functionality."""

    def test_builds_lookup_from_manifest(self, monkeypatch):
        """Test that reverse lookup is built correctly from manifest."""
        from dbt_meta.command_impl.powerbi import PowerBiCommand

        monkeypatch.setenv('POWERBI_ENABLED', 'true')
        config = Config.from_env()

        # Create mock parser
        mock_parser = MagicMock()
        mock_parser.get_all_models.return_value = {
            'model.project.core_client__events': {
                'schema': 'core_client',
                'alias': 'events',
                'name': 'core_client__events',
            },
            'model.project.staging_amas__partners': {
                'schema': 'staging_amas',
                'name': 'staging_amas__partners',
                # No alias - should use name
            },
        }

        command = PowerBiCommand(config, '/fake/manifest.json')
        result = command._build_reverse_model_lookup(mock_parser)

        assert result['core_client.events'] == 'core_client__events'
        assert result['staging_amas.staging_amas__partners'] == 'staging_amas__partners'


class TestParseDaxReferences:
    """Test DAX expression parsing for table and column references."""

    def test_parse_simple_sum(self):
        """Test parsing simple SUM('table'[column])."""
        dax = "SUM('installs'[count])"
        refs = parse_dax_references(dax)

        assert 'installs' in refs['tables']
        assert 'count' in refs['columns']
        assert 'SUM' in refs['functions']

    def test_parse_calculate_with_filter(self):
        """Test parsing CALCULATE with filter."""
        dax = "CALCULATE(SUM('sales'[amount]), 'dates'[year] = 2024)"
        refs = parse_dax_references(dax)

        assert 'sales' in refs['tables']
        assert 'dates' in refs['tables']
        assert 'amount' in refs['columns']
        assert 'year' in refs['columns']
        assert 'CALCULATE' in refs['functions']
        assert 'SUM' in refs['functions']

    def test_parse_multiple_tables(self):
        """Test parsing multiple table references."""
        dax = "SUMX('table1', 'table1'[value] * RELATED('table2'[rate]))"
        refs = parse_dax_references(dax)

        assert 'table1' in refs['tables']
        assert 'table2' in refs['tables']
        assert 'value' in refs['columns']
        assert 'rate' in refs['columns']
        assert 'SUMX' in refs['functions']
        assert 'RELATED' in refs['functions']

    def test_parse_empty_expression(self):
        """Test parsing empty expression."""
        refs = parse_dax_references('')

        assert refs['tables'] == []
        assert refs['columns'] == []
        assert refs['functions'] == []

    def test_parse_double_quotes(self):
        """Test parsing with double quotes instead of single quotes."""
        dax = 'DISTINCTCOUNT("users"[user_id])'
        refs = parse_dax_references(dax)

        assert 'users' in refs['tables']
        assert 'user_id' in refs['columns']
        assert 'DISTINCTCOUNT' in refs['functions']


class TestExtractMeasuresFromDataset:
    """Test extraction of measures from dataset."""

    def test_extract_single_measure(self):
        """Test extracting single measure from table."""
        dataset = {
            'tables': [
                {
                    'name': 'installs',
                    'measures': [
                        {
                            'name': 'Total Installs',
                            'expression': "SUM('installs'[count])",
                            'formatString': '#,0',
                            'description': 'Total number of installs'
                        }
                    ]
                }
            ]
        }

        result = extract_measures_from_dataset(dataset)

        assert 'installs' in result
        assert len(result['installs']) == 1
        assert result['installs'][0]['name'] == 'Total Installs'
        assert result['installs'][0]['expression'] == "SUM('installs'[count])"
        assert result['installs'][0]['format_string'] == '#,0'
        assert result['installs'][0]['description'] == 'Total number of installs'
        assert 'count' in result['installs'][0]['references_columns']
        assert 'installs' in result['installs'][0]['references_tables']

    def test_extract_multiple_measures(self):
        """Test extracting multiple measures from table."""
        dataset = {
            'tables': [
                {
                    'name': 'sales',
                    'measures': [
                        {'name': 'Total Sales', 'expression': "SUM('sales'[amount])"},
                        {'name': 'Avg Sale', 'expression': "AVERAGE('sales'[amount])"},
                    ]
                }
            ]
        }

        result = extract_measures_from_dataset(dataset)

        assert len(result['sales']) == 2
        assert result['sales'][0]['name'] == 'Total Sales'
        assert result['sales'][1]['name'] == 'Avg Sale'

    def test_extract_from_multiple_tables(self):
        """Test extracting measures from multiple tables."""
        dataset = {
            'tables': [
                {'name': 'table1', 'measures': [{'name': 'M1', 'expression': "SUM('table1'[col1])"}]},
                {'name': 'table2', 'measures': [{'name': 'M2', 'expression': "COUNT('table2'[col2])"}]},
            ]
        }

        result = extract_measures_from_dataset(dataset)

        assert 'table1' in result
        assert 'table2' in result
        assert result['table1'][0]['name'] == 'M1'
        assert result['table2'][0]['name'] == 'M2'

    def test_skip_tables_without_measures(self):
        """Test that tables without measures are skipped."""
        dataset = {
            'tables': [
                {'name': 'table1', 'measures': [{'name': 'M1', 'expression': "SUM('table1'[col])"}]},
                {'name': 'table2', 'measures': []},  # No measures
                {'name': 'table3'},  # Missing measures key
            ]
        }

        result = extract_measures_from_dataset(dataset)

        assert 'table1' in result
        assert 'table2' not in result
        assert 'table3' not in result

    def test_handle_hidden_measures(self):
        """Test handling of hidden measures."""
        dataset = {
            'tables': [
                {
                    'name': 'table1',
                    'measures': [
                        {'name': 'M1', 'expression': "SUM('t'[c])", 'isHidden': True}
                    ]
                }
            ]
        }

        result = extract_measures_from_dataset(dataset)

        assert result['table1'][0]['is_hidden'] is True


class TestExtractColumnsFromDataset:
    """Test extraction of columns from dataset."""

    def test_extract_single_column(self):
        """Test extracting single column from table."""
        dataset = {
            'tables': [
                {
                    'name': 'events',
                    'columns': [
                        {
                            'name': 'event_date',
                            'dataType': 'DateTime',
                            'isHidden': False,
                            'formatString': 'yyyy-MM-dd',
                            'summarizeBy': 'none'
                        }
                    ]
                }
            ]
        }

        result = extract_columns_from_dataset(dataset)

        assert 'events' in result
        assert len(result['events']) == 1
        assert result['events'][0]['name'] == 'event_date'
        assert result['events'][0]['data_type'] == 'DateTime'
        assert result['events'][0]['is_hidden'] is False
        assert result['events'][0]['format_string'] == 'yyyy-MM-dd'
        assert result['events'][0]['summarize_by'] == 'none'

    def test_extract_multiple_columns(self):
        """Test extracting multiple columns from table."""
        dataset = {
            'tables': [
                {
                    'name': 'users',
                    'columns': [
                        {'name': 'user_id', 'dataType': 'Int64'},
                        {'name': 'email', 'dataType': 'String'},
                        {'name': 'signup_date', 'dataType': 'DateTime'},
                    ]
                }
            ]
        }

        result = extract_columns_from_dataset(dataset)

        assert len(result['users']) == 3
        assert result['users'][0]['name'] == 'user_id'
        assert result['users'][1]['name'] == 'email'
        assert result['users'][2]['name'] == 'signup_date'

    def test_extract_from_multiple_tables(self):
        """Test extracting columns from multiple tables."""
        dataset = {
            'tables': [
                {'name': 'table1', 'columns': [{'name': 'col1', 'dataType': 'Int64'}]},
                {'name': 'table2', 'columns': [{'name': 'col2', 'dataType': 'String'}]},
            ]
        }

        result = extract_columns_from_dataset(dataset)

        assert 'table1' in result
        assert 'table2' in result
        assert result['table1'][0]['name'] == 'col1'
        assert result['table2'][0]['name'] == 'col2'

    def test_skip_tables_without_columns(self):
        """Test that tables without columns are skipped."""
        dataset = {
            'tables': [
                {'name': 'table1', 'columns': [{'name': 'col1', 'dataType': 'Int64'}]},
                {'name': 'table2', 'columns': []},  # No columns
                {'name': 'table3'},  # Missing columns key
            ]
        }

        result = extract_columns_from_dataset(dataset)

        assert 'table1' in result
        assert 'table2' not in result
        assert 'table3' not in result

    def test_handle_hidden_columns(self):
        """Test handling of hidden columns."""
        dataset = {
            'tables': [
                {
                    'name': 'table1',
                    'columns': [
                        {'name': 'col1', 'dataType': 'String', 'isHidden': True}
                    ]
                }
            ]
        }

        result = extract_columns_from_dataset(dataset)

        assert result['table1'][0]['is_hidden'] is True

    def test_handle_optional_fields(self):
        """Test handling of optional fields (sortByColumn, summarizeBy)."""
        dataset = {
            'tables': [
                {
                    'name': 'table1',
                    'columns': [
                        {
                            'name': 'col1',
                            'dataType': 'String',
                            'sortByColumn': 'col2',
                            'summarizeBy': 'count'
                        }
                    ]
                }
            ]
        }

        result = extract_columns_from_dataset(dataset)

        assert result['table1'][0]['sort_by_column'] == 'col2'
        assert result['table1'][0]['summarize_by'] == 'count'


class TestGetPowerBiToken:
    """Test OAuth token acquisition."""

    def test_returns_token_on_success(self):
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = json.dumps({'access_token': 'test-token-123'})

        with patch('shutil.which', return_value='/usr/bin/curl'), \
             patch('subprocess.run', return_value=mock_result):
            token = get_powerbi_token('tenant', 'client', 'secret')

        assert token == 'test-token-123'

    def test_returns_none_when_curl_not_found(self):
        with patch('shutil.which', return_value=None):
            token = get_powerbi_token('tenant', 'client', 'secret')

        assert token is None

    def test_returns_none_on_nonzero_returncode(self):
        mock_result = MagicMock()
        mock_result.returncode = 1

        with patch('shutil.which', return_value='/usr/bin/curl'), \
             patch('subprocess.run', return_value=mock_result):
            token = get_powerbi_token('tenant', 'client', 'secret')

        assert token is None

    def test_returns_none_on_timeout(self):
        with patch('shutil.which', return_value='/usr/bin/curl'), \
             patch('subprocess.run', side_effect=subprocess.TimeoutExpired('curl', 30)):
            token = get_powerbi_token('tenant', 'client', 'secret')

        assert token is None

    def test_returns_none_on_invalid_json(self):
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = 'not-json'

        with patch('shutil.which', return_value='/usr/bin/curl'), \
             patch('subprocess.run', return_value=mock_result):
            token = get_powerbi_token('tenant', 'client', 'secret')

        assert token is None

    def test_returns_none_when_no_access_token_in_response(self):
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = json.dumps({'error': 'invalid_client'})

        with patch('shutil.which', return_value='/usr/bin/curl'), \
             patch('subprocess.run', return_value=mock_result):
            token = get_powerbi_token('tenant', 'client', 'secret')

        assert token is None


class TestCallPowerBiApi:
    """Test Power BI API calls."""

    def test_returns_response_on_success(self):
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = json.dumps({'value': [{'id': 'ws-1'}]})

        with patch('shutil.which', return_value='/usr/bin/curl'), \
             patch('subprocess.run', return_value=mock_result):
            result = _call_powerbi_api('token', '/admin/groups')

        assert result == {'value': [{'id': 'ws-1'}]}

    def test_returns_none_when_curl_not_found(self):
        with patch('shutil.which', return_value=None):
            result = _call_powerbi_api('token', '/endpoint')

        assert result is None

    def test_returns_none_on_nonzero_returncode(self):
        mock_result = MagicMock()
        mock_result.returncode = 1

        with patch('shutil.which', return_value='/usr/bin/curl'), \
             patch('subprocess.run', return_value=mock_result):
            result = _call_powerbi_api('token', '/endpoint')

        assert result is None

    def test_returns_empty_dict_on_empty_stdout(self):
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = '   '

        with patch('shutil.which', return_value='/usr/bin/curl'), \
             patch('subprocess.run', return_value=mock_result):
            result = _call_powerbi_api('token', '/endpoint')

        assert result == {}

    def test_post_with_data(self):
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = json.dumps({'id': 'scan-123'})

        with patch('shutil.which', return_value='/usr/bin/curl'), \
             patch('subprocess.run', return_value=mock_result) as mock_run:
            result = _call_powerbi_api('token', '/endpoint', method='POST', data={'key': 'val'})

        assert result == {'id': 'scan-123'}
        call_args = mock_run.call_args[0][0]
        assert '-d' in call_args
        assert json.dumps({'key': 'val'}) in call_args

    def test_returns_none_on_timeout(self):
        with patch('shutil.which', return_value='/usr/bin/curl'), \
             patch('subprocess.run', side_effect=subprocess.TimeoutExpired('curl', 30)):
            result = _call_powerbi_api('token', '/endpoint')

        assert result is None

    def test_returns_none_on_invalid_json(self):
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = 'invalid-json'

        with patch('shutil.which', return_value='/usr/bin/curl'), \
             patch('subprocess.run', return_value=mock_result):
            result = _call_powerbi_api('token', '/endpoint')

        assert result is None


class TestFetchWorkspaceScan:
    """Test workspace scan polling."""

    def _make_run_result(self, data: dict) -> MagicMock:
        m = MagicMock()
        m.returncode = 0
        m.stdout = json.dumps(data)
        return m

    def test_returns_workspace_on_success(self):
        workspace_data = {'id': 'ws-1', 'datasets': []}
        scan_result = {'workspaces': [workspace_data]}

        call_count = 0

        def side_effect(cmd, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return self._make_run_result({'id': 'scan-123'})
            elif call_count == 2:
                return self._make_run_result({'status': 'Succeeded'})
            else:
                return self._make_run_result(scan_result)

        with patch('shutil.which', return_value='/usr/bin/curl'), \
             patch('subprocess.run', side_effect=side_effect), \
             patch('time.sleep'):
            result = fetch_workspace_scan('token', 'ws-id')

        assert result == workspace_data

    def test_returns_none_when_scan_start_fails(self):
        with patch('shutil.which', return_value='/usr/bin/curl'), \
             patch('subprocess.run', return_value=MagicMock(returncode=1)):
            result = fetch_workspace_scan('token', 'ws-id')

        assert result is None

    def test_returns_none_when_no_scan_id(self):
        mock_result = self._make_run_result({'status': 'pending'})  # No 'id'

        with patch('shutil.which', return_value='/usr/bin/curl'), \
             patch('subprocess.run', return_value=mock_result):
            result = fetch_workspace_scan('token', 'ws-id')

        assert result is None

    def test_returns_none_on_failed_status(self):
        call_count = 0

        def side_effect(cmd, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return self._make_run_result({'id': 'scan-123'})
            return self._make_run_result({'status': 'Failed'})

        with patch('shutil.which', return_value='/usr/bin/curl'), \
             patch('subprocess.run', side_effect=side_effect), \
             patch('time.sleep'):
            result = fetch_workspace_scan('token', 'ws-id')

        assert result is None

    def test_returns_none_on_cancelled_status(self):
        call_count = 0

        def side_effect(cmd, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return self._make_run_result({'id': 'scan-123'})
            return self._make_run_result({'status': 'Cancelled'})

        with patch('shutil.which', return_value='/usr/bin/curl'), \
             patch('subprocess.run', side_effect=side_effect), \
             patch('time.sleep'):
            result = fetch_workspace_scan('token', 'ws-id')

        assert result is None

    def test_returns_none_on_poll_timeout(self):
        call_count = 0

        def side_effect(cmd, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return self._make_run_result({'id': 'scan-123'})
            return self._make_run_result({'status': 'Running'})

        with patch('shutil.which', return_value='/usr/bin/curl'), \
             patch('subprocess.run', side_effect=side_effect), \
             patch('time.sleep'):
            result = fetch_workspace_scan('token', 'ws-id', max_polls=3)

        assert result is None

    def test_returns_none_when_scan_result_has_empty_workspaces(self):
        call_count = 0

        def side_effect(cmd, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return self._make_run_result({'id': 'scan-123'})
            elif call_count == 2:
                return self._make_run_result({'status': 'Succeeded'})
            else:
                return self._make_run_result({'workspaces': []})

        with patch('shutil.which', return_value='/usr/bin/curl'), \
             patch('subprocess.run', side_effect=side_effect), \
             patch('time.sleep'):
            result = fetch_workspace_scan('token', 'ws-id')

        assert result is None

    def test_returns_none_when_poll_api_fails(self):
        call_count = 0

        def side_effect(cmd, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return self._make_run_result({'id': 'scan-123'})
            return MagicMock(returncode=1)

        with patch('shutil.which', return_value='/usr/bin/curl'), \
             patch('subprocess.run', side_effect=side_effect), \
             patch('time.sleep'):
            result = fetch_workspace_scan('token', 'ws-id')

        assert result is None


class TestFetchDatasetRefreshes:
    """Test dataset refresh history."""

    def test_returns_refresh_list(self):
        refreshes = [{'requestId': 'r1', 'status': 'Completed'}]
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = json.dumps({'value': refreshes})

        with patch('shutil.which', return_value='/usr/bin/curl'), \
             patch('subprocess.run', return_value=mock_result):
            result = fetch_dataset_refreshes('token', 'ws-id', 'ds-id')

        assert result == refreshes

    def test_returns_empty_list_on_api_failure(self):
        with patch('shutil.which', return_value='/usr/bin/curl'), \
             patch('subprocess.run', return_value=MagicMock(returncode=1)):
            result = fetch_dataset_refreshes('token', 'ws-id', 'ds-id')

        assert result == []

    def test_returns_empty_list_when_no_value_key(self):
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = json.dumps({'error': 'not found'})

        with patch('shutil.which', return_value='/usr/bin/curl'), \
             patch('subprocess.run', return_value=mock_result):
            result = fetch_dataset_refreshes('token', 'ws-id', 'ds-id')

        assert result == []


class TestGetWorkspaceInfo:
    """Test workspace info retrieval."""

    def test_returns_workspace_info(self):
        ws_info = {'id': 'ws-1', 'name': 'BI Marketing'}
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = json.dumps(ws_info)

        with patch('shutil.which', return_value='/usr/bin/curl'), \
             patch('subprocess.run', return_value=mock_result):
            result = get_workspace_info('token', 'ws-1')

        assert result == ws_info

    def test_returns_none_on_failure(self):
        with patch('shutil.which', return_value='/usr/bin/curl'), \
             patch('subprocess.run', return_value=MagicMock(returncode=1)):
            result = get_workspace_info('token', 'ws-1')

        assert result is None


class TestListWorkspaces:
    """Test workspace listing."""

    def test_returns_workspace_list(self):
        workspaces = [{'id': 'ws-1', 'name': 'WS 1'}, {'id': 'ws-2', 'name': 'WS 2'}]
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = json.dumps({'value': workspaces})

        with patch('shutil.which', return_value='/usr/bin/curl'), \
             patch('subprocess.run', return_value=mock_result):
            result = list_workspaces('token')

        assert result == workspaces

    def test_returns_empty_list_on_failure(self):
        with patch('shutil.which', return_value='/usr/bin/curl'), \
             patch('subprocess.run', return_value=MagicMock(returncode=1)):
            result = list_workspaces('token')

        assert result == []

    def test_returns_empty_list_when_no_value_key(self):
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = json.dumps({})

        with patch('shutil.which', return_value='/usr/bin/curl'), \
             patch('subprocess.run', return_value=mock_result):
            result = list_workspaces('token')

        assert result == []


class TestExtractTablesRefreshSchedule:
    """Test refresh schedule extraction in extract_tables_from_expressions."""

    def test_uses_refresh_schedule_when_present(self):
        schedule = {'days': ['Monday'], 'times': ['08:00']}
        scan_result = {
            'datasets': [{
                'id': 'ds-1',
                'name': 'Dataset 1',
                'refreshSchedule': schedule,
                'tables': [{'source': [{'expression': '#"p"{[Name="s",Kind="Schema"]}[Data]{[Name="t",Kind="Table"]}'}]}]
            }]
        }

        result = extract_tables_from_expressions(scan_result)

        assert result['Dataset 1']['refresh_schedule'] == schedule

    def test_uses_direct_query_schedule_as_fallback(self):
        dq_schedule = {'frequency': 'OneHour'}
        scan_result = {
            'datasets': [{
                'id': 'ds-1',
                'name': 'Dataset 1',
                'directQueryRefreshSchedule': dq_schedule,
                'tables': [{'source': [{'expression': '#"p"{[Name="s",Kind="Schema"]}[Data]{[Name="t",Kind="Table"]}'}]}]
            }]
        }

        result = extract_tables_from_expressions(scan_result)

        assert result['Dataset 1']['refresh_schedule'] == dq_schedule

    def test_skips_sources_with_empty_expression(self):
        scan_result = {
            'datasets': [{
                'id': 'ds-1',
                'name': 'Dataset 1',
                'tables': [{'source': [
                    {'expression': ''},   # empty — should be skipped
                    {'expression': '#"p"{[Name="s",Kind="Schema"]}[Data]{[Name="t",Kind="Table"]}'},
                ]}]
            }]
        }

        result = extract_tables_from_expressions(scan_result)

        assert 's.t' in result['Dataset 1']['tables']

    def test_refresh_schedule_none_when_both_absent(self):
        scan_result = {
            'datasets': [{
                'id': 'ds-1',
                'name': 'Dataset 1',
                'tables': [{'source': [{'expression': '#"p"{[Name="s",Kind="Schema"]}[Data]{[Name="t",Kind="Table"]}'}]}]
            }]
        }

        result = extract_tables_from_expressions(scan_result)

        assert result['Dataset 1']['refresh_schedule'] is None
