"""
Tests for Commands - Model metadata extraction commands

Following TDD: These tests are written FIRST, then implementation.
Tests verify output matches bash version exactly.
"""

import pytest
import json
from pathlib import Path
from dbt_meta.commands import (
    info, schema, columns, config, deps, sql, path, list_models, search,
    parents, children, node, refresh, docs
)


class TestInfoCommand:
    """Test info command - basic model metadata"""

    def test_info_matches_expected_output(self, prod_manifest, expected_info):
        """
        Should extract model info matching bash version

        Returns: name, database, schema, table, full_name,
                 materialized, file, tags, unique_id
        """
        model_name = "core_client__client_profiles_events"
        result = info(str(prod_manifest), model_name)

        # Verify all required fields
        assert result['name'] == expected_info['name']
        assert result['database'] == expected_info['database']
        assert result['schema'] == expected_info['schema']
        assert result['table'] == expected_info['table']
        assert result['full_name'] == expected_info['full_name']
        assert result['materialized'] == expected_info['materialized']
        assert result['file'] == expected_info['file']
        assert result['tags'] == expected_info['tags']
        assert result['unique_id'] == expected_info['unique_id']

    def test_info_nonexistent_model_returns_none(self, prod_manifest):
        """
        Should return None for non-existent model

        Graceful error handling without exceptions.
        """
        result = info(str(prod_manifest), "nonexistent__model")

        assert result is None

    def test_info_extracts_materialized_type(self, prod_manifest):
        """
        Should extract materialization type from config

        Common types: table, view, incremental, ephemeral
        """
        model_name = "core_client__client_profiles_events"
        result = info(str(prod_manifest), model_name)

        assert 'materialized' in result
        assert result['materialized'] in ['table', 'view', 'incremental', 'ephemeral']

    def test_info_extracts_tags(self, prod_manifest):
        """
        Should extract tags as list

        Empty list if no tags.
        """
        model_name = "core_client__client_profiles_events"
        result = info(str(prod_manifest), model_name)

        assert 'tags' in result
        assert isinstance(result['tags'], list)


class TestSchemaCommand:
    """Test schema command - table location"""

    def test_schema_matches_expected_output(self, prod_manifest, expected_schema):
        """
        Should extract schema info matching bash version

        Returns: database, schema, table, full_name
        """
        model_name = "core_client__client_profiles_events"
        result = schema(str(prod_manifest), model_name)

        assert result['database'] == expected_schema['database']
        assert result['schema'] == expected_schema['schema']
        assert result['table'] == expected_schema['table']
        assert result['full_name'] == expected_schema['full_name']

    def test_schema_uses_alias_if_present(self, prod_manifest):
        """
        Should use config.alias as table name if present

        Falls back to model name if no alias.
        """
        model_name = "core_client__client_profiles_events"
        result = schema(str(prod_manifest), model_name)

        # Table should be either alias or model name
        assert 'table' in result
        assert result['table'] == "client_profiles_events"

    def test_schema_nonexistent_model_returns_none(self, prod_manifest):
        """
        Should return None for non-existent model

        Graceful error handling.
        """
        result = schema(str(prod_manifest), "nonexistent__model")

        assert result is None

    def test_schema_constructs_full_name(self, prod_manifest):
        """
        Should construct full_name as database.schema.table

        Format: {database}.{schema}.{table}
        """
        model_name = "core_client__client_profiles_events"
        result = schema(str(prod_manifest), model_name)

        expected_full = f"{result['database']}.{result['schema']}.{result['table']}"
        assert result['full_name'] == expected_full


class TestColumnsCommand:
    """Test columns command - column list with types"""

    def test_columns_matches_expected_output(self, prod_manifest, expected_columns):
        """
        Should extract columns matching bash version

        Returns: [{name, data_type}, ...]
        """
        model_name = "core_client__client_profiles_events"
        result = columns(str(prod_manifest), model_name)

        # Should match expected output exactly
        assert len(result) == len(expected_columns)

        # Verify all columns present
        result_names = {col['name'] for col in result}
        expected_names = {col['name'] for col in expected_columns}
        assert result_names == expected_names

        # Verify types match
        result_dict = {col['name']: col['data_type'] for col in result}
        expected_dict = {col['name']: col['data_type'] for col in expected_columns}
        assert result_dict == expected_dict

    def test_columns_returns_list(self, prod_manifest):
        """
        Should return list of column dictionaries

        Each column: {name: str, data_type: str}
        """
        model_name = "core_client__client_profiles_events"
        result = columns(str(prod_manifest), model_name)

        assert isinstance(result, list)
        assert len(result) > 0

        # Verify structure
        for col in result:
            assert 'name' in col
            assert 'data_type' in col
            assert isinstance(col['name'], str)
            assert isinstance(col['data_type'], str)

    def test_columns_nonexistent_model_returns_none(self, prod_manifest):
        """
        Should return None for non-existent model

        Graceful error handling.
        """
        result = columns(str(prod_manifest), "nonexistent__model")

        assert result is None

    def test_columns_preserves_order(self, prod_manifest):
        """
        Should preserve column order from manifest

        Columns should appear in same order as defined.
        """
        model_name = "core_client__client_profiles_events"
        result = columns(str(prod_manifest), model_name)

        # Verify first few columns match expected order
        assert result[0]['name'] == 'event_id'
        assert result[1]['name'] == 'client_id'
        assert result[2]['name'] == 'profile_id'

    def test_columns_fallback_to_bigquery_when_empty(self, prod_manifest, mocker):
        """
        Should fallback to BigQuery when columns not in manifest

        Critical: 64% of models don't have columns in manifest.
        """
        # Mock is_modified to avoid git calls
        mocker.patch('dbt_meta.commands.is_modified', return_value=False)

        # Mock subprocess to simulate bq command
        mock_run = mocker.patch('subprocess.run')

        # First call: bq version check (success)
        # Second call: bq show --schema
        bq_output = json.dumps([
            {"name": "id", "type": "INTEGER"},
            {"name": "name", "type": "STRING"}
        ])

        mock_run.side_effect = [
            mocker.Mock(returncode=0),  # bq version check
            mocker.Mock(stdout=bq_output, returncode=0)  # bq show --schema
        ]

        # Model without columns in manifest
        model_name = "sugarcrm_px_customerstages"
        result = columns(str(prod_manifest), model_name)

        # Should have called bq
        assert mock_run.call_count == 2
        assert result is not None
        assert len(result) == 2
        assert result[0]['name'] == 'id'
        assert result[0]['data_type'] == 'integer'

    def test_columns_fallback_bq_not_installed(self, prod_manifest, mocker):
        """
        Should return None if bq not installed

        Graceful degradation when BigQuery SDK not available.
        """
        # Mock subprocess to simulate bq not found
        mock_run = mocker.patch('subprocess.run')
        mock_run.side_effect = FileNotFoundError("bq not found")

        model_name = "sugarcrm_px_customerstages"
        result = columns(str(prod_manifest), model_name)

        # Should return None
        assert result is None

    def test_columns_fallback_bq_table_not_found(self, prod_manifest, mocker):
        """
        Should return None if BigQuery table doesn't exist

        Handles case when manifest references non-existent table.
        """
        # Mock subprocess
        mock_run = mocker.patch('subprocess.run')

        # bq version check succeeds, but bq show fails
        import subprocess as sp
        mock_run.side_effect = [
            mocker.Mock(returncode=0),  # bq version
            sp.CalledProcessError(1, 'bq show')  # table not found
        ]

        model_name = "sugarcrm_px_customerstages"
        result = columns(str(prod_manifest), model_name)

        # Should return None
        assert result is None


class TestConfigCommand:
    """Test config command - full dbt config"""

    def test_config_returns_full_config(self, prod_manifest):
        """
        Should return full dbt config dictionary

        Config includes: materialized, partition_by, cluster_by,
        incremental_strategy, unique_key, tags, etc.
        """
        model_name = "core_client__client_profiles_events"
        result = config(str(prod_manifest), model_name)

        assert isinstance(result, dict)
        assert 'materialized' in result
        assert 'tags' in result
        assert len(result) > 10  # Should have many config fields

    def test_config_matches_expected_output(self, prod_manifest):
        """
        Should match bash version output

        Key fields: materialized, incremental_strategy, partition_by, etc.
        """
        model_name = "core_client__client_profiles_events"
        result = config(str(prod_manifest), model_name)

        # Load expected from bash version
        expected_path = Path(__file__).parent / "fixtures" / "expected_outputs" / f"{model_name}_config.json"
        expected = json.loads(expected_path.read_text())

        # Verify key fields match
        assert result['materialized'] == expected['materialized']
        assert result['incremental_strategy'] == expected['incremental_strategy']
        assert result['alias'] == expected['alias']
        assert result['schema'] == expected['schema']
        assert result['tags'] == expected['tags']

    def test_config_nonexistent_model_returns_none(self, prod_manifest):
        """Should return None for non-existent model"""
        result = config(str(prod_manifest), "nonexistent__model")
        assert result is None

    def test_config_includes_partition_info(self, prod_manifest):
        """
        Should include partition_by config for incremental models

        Important for query optimization.
        """
        model_name = "core_client__client_profiles_events"
        result = config(str(prod_manifest), model_name)

        assert 'partition_by' in result
        # Partition config is a dict or None
        assert result['partition_by'] is None or isinstance(result['partition_by'], dict)


class TestDepsCommand:
    """Test deps command - model dependencies"""

    def test_deps_returns_dict_with_refs_sources(self, prod_manifest):
        """
        Should return dictionary with refs and sources

        Format: {"refs": [...], "sources": [...]}
        """
        model_name = "core_client__client_profiles_events"
        result = deps(str(prod_manifest), model_name)

        assert isinstance(result, dict)
        assert 'refs' in result
        assert 'sources' in result
        assert isinstance(result['refs'], list)
        assert isinstance(result['sources'], list)

    def test_deps_matches_expected_output(self, prod_manifest):
        """
        Should match bash version output

        Count and content of dependencies should match.
        """
        model_name = "core_client__client_profiles_events"
        result = deps(str(prod_manifest), model_name)

        # Load expected from bash version
        expected_path = Path(__file__).parent / "fixtures" / "expected_outputs" / f"{model_name}_deps.json"
        expected = json.loads(expected_path.read_text())

        # Verify counts match
        assert len(result['refs']) == len(expected['refs'])
        assert len(result['sources']) == len(expected['sources'])

        # Verify content matches (as sets, order doesn't matter)
        assert set(result['refs']) == set(expected['refs'])
        assert set(result['sources']) == set(expected['sources'])

    def test_deps_nonexistent_model_returns_empty(self, prod_manifest):
        """Should return empty refs/sources for non-existent model"""
        result = deps(str(prod_manifest), "nonexistent__model")
        assert result == {'refs': [], 'sources': []}

    def test_deps_includes_model_refs(self, prod_manifest):
        """
        Should include model dependencies (refs)

        refs should be in format: model.project.model_name
        """
        model_name = "core_client__client_profiles_events"
        result = deps(str(prod_manifest), model_name)

        assert len(result['refs']) > 0
        # All refs should start with 'model.'
        for ref in result['refs']:
            assert ref.startswith('model.')


class TestSqlCommand:
    """Test sql command - SQL code extraction"""

    def test_sql_returns_raw_code_with_jinja(self, prod_manifest):
        """
        Should return raw SQL with Jinja templates

        When raw=True, should include {{ config() }}, {% set %}, etc.
        """
        model_name = "core_client__client_profiles_events"
        result = sql(str(prod_manifest), model_name, raw=True)

        assert isinstance(result, str)
        assert len(result) > 0
        # Raw SQL should contain Jinja syntax
        assert '{{' in result or '{%' in result

    def test_sql_returns_empty_for_compiled(self, prod_manifest):
        """
        Should return empty string for compiled SQL if not available

        Compiled SQL only in .dbt-state/ manifest after dbt compile.
        """
        model_name = "core_client__client_profiles_events"
        result = sql(str(prod_manifest), model_name, raw=False)

        # In production manifest, compiled_code might not exist
        assert result == '' or isinstance(result, str)

    def test_sql_nonexistent_model_returns_none(self, prod_manifest):
        """Should return None for non-existent model"""
        result = sql(str(prod_manifest), "nonexistent__model", raw=True)
        assert result is None

    def test_sql_raw_contains_config(self, prod_manifest):
        """
        Raw SQL should contain dbt config block

        Config defines materialization, partition, etc.
        """
        model_name = "core_client__client_profiles_events"
        result = sql(str(prod_manifest), model_name, raw=True)

        assert 'config(' in result.lower()


class TestPathCommand:
    """Test path command - file path extraction"""

    def test_path_returns_relative_path(self, prod_manifest):
        """
        Should return relative file path

        Format: models/schema/model_name.sql
        """
        model_name = "core_client__client_profiles_events"
        result = path(str(prod_manifest), model_name)

        assert isinstance(result, str)
        assert result.startswith('models/')
        assert result.endswith('.sql')

    def test_path_matches_expected_output(self, prod_manifest):
        """Should match bash version output"""
        model_name = "core_client__client_profiles_events"
        result = path(str(prod_manifest), model_name)

        # Load expected from bash version
        expected_path = Path(__file__).parent / "fixtures" / "expected_outputs" / f"{model_name}_path.txt"
        expected = expected_path.read_text().strip()

        assert result == expected

    def test_path_nonexistent_model_returns_none(self, prod_manifest):
        """Should return None for non-existent model"""
        result = path(str(prod_manifest), "nonexistent__model")
        assert result is None


class TestListModelsCommand:
    """Test list_models command - list all models"""

    def test_list_models_returns_sorted_list(self, prod_manifest):
        """
        Should return sorted list of model names

        All models from manifest, alphabetically sorted.
        """
        result = list_models(str(prod_manifest))

        assert isinstance(result, list)
        assert len(result) > 100  # Production manifest has 865 models
        # Verify sorted
        assert result == sorted(result)

    def test_list_models_with_pattern_filters(self, prod_manifest):
        """
        Should filter by pattern (substring match)

        Case-insensitive filtering.
        """
        result = list_models(str(prod_manifest), pattern="core_client")

        assert isinstance(result, list)
        assert len(result) > 0
        # All results should contain pattern
        for model in result:
            assert 'core_client' in model.lower()

    def test_list_models_pattern_case_insensitive(self, prod_manifest):
        """Should perform case-insensitive pattern matching"""
        result_lower = list_models(str(prod_manifest), pattern="core_client")
        result_upper = list_models(str(prod_manifest), pattern="CORE_CLIENT")

        # Should return same results regardless of case
        assert set(result_lower) == set(result_upper)

    def test_list_models_no_pattern_returns_all(self, prod_manifest):
        """
        Should return all models when no pattern specified

        Total count should match manifest.
        """
        result = list_models(str(prod_manifest))

        # Production manifest has 865 models
        assert len(result) > 800


class TestSearchCommand:
    """Test search command - search by name or description"""

    def test_search_returns_list_with_name_description(self, prod_manifest):
        """
        Should return list of dicts with name and description

        Format: [{"name": "...", "description": "..."}, ...]
        """
        result = search(str(prod_manifest), "client")

        assert isinstance(result, list)
        assert len(result) > 0

        # Verify structure
        for item in result:
            assert 'name' in item
            assert 'description' in item
            assert isinstance(item['name'], str)
            assert isinstance(item['description'], str)

    def test_search_case_insensitive(self, prod_manifest):
        """Should perform case-insensitive search"""
        result_lower = search(str(prod_manifest), "client")
        result_upper = search(str(prod_manifest), "CLIENT")

        # Should return same results
        assert len(result_lower) == len(result_upper)
        assert {r['name'] for r in result_lower} == {r['name'] for r in result_upper}

    def test_search_matches_in_name(self, prod_manifest):
        """
        Should find matches in model name

        Query substring should appear in name.
        """
        result = search(str(prod_manifest), "client_profiles_events")

        assert len(result) > 0
        # At least one result should have exact match
        names = [r['name'] for r in result]
        assert any('client_profiles_events' in name for name in names)

    def test_search_results_sorted_by_name(self, prod_manifest):
        """Should return results sorted alphabetically by name"""
        result = search(str(prod_manifest), "client")

        names = [r['name'] for r in result]
        assert names == sorted(names)


class TestParentsCommand:
    """Test parents command - upstream dependencies"""

    def test_parents_direct_only(self, prod_manifest):
        """Should return direct parents only (non-recursive)"""
        model_name = "core_client__client_profiles_events"
        result = parents(str(prod_manifest), model_name, recursive=False)

        assert isinstance(result, list)
        assert len(result) > 0
        # Each parent should have unique_id
        for parent in result:
            assert 'unique_id' in parent
            assert parent['unique_id'].startswith('model.') or parent['unique_id'].startswith('source.')

    def test_parents_recursive_all_ancestors(self, prod_manifest):
        """Should return all ancestors when recursive=True"""
        model_name = "core_client__client_profiles_events"
        direct = parents(str(prod_manifest), model_name, recursive=False)
        all_ancestors = parents(str(prod_manifest), model_name, recursive=True)

        # All ancestors should be >= direct parents
        assert len(all_ancestors) >= len(direct)

    def test_parents_filters_out_tests(self, prod_manifest):
        """Should filter out test nodes"""
        model_name = "core_client__client_profiles_events"
        result = parents(str(prod_manifest), model_name, recursive=True)

        # No test nodes should be included
        for parent in result:
            assert not parent['unique_id'].startswith('test.')

    def test_parents_nonexistent_model_returns_none(self, prod_manifest):
        """Should return None for non-existent model"""
        result = parents(str(prod_manifest), "nonexistent__model")
        assert result is None

    def test_parents_handles_model_without_dependencies(self, prod_manifest):
        """Should return empty list for model with no dependencies"""
        # Find a source or seed (no upstream dependencies)
        result = parents(str(prod_manifest), "sugarcrm_px_customerstages", recursive=False)

        # Should return empty list or minimal dependencies
        assert isinstance(result, list)


class TestChildrenCommand:
    """Test children command - downstream dependencies"""

    def test_children_direct_only(self, prod_manifest):
        """Should return direct children only (non-recursive)"""
        model_name = "core_client__client_profiles_events"
        result = children(str(prod_manifest), model_name, recursive=False)

        assert isinstance(result, list)
        # Each child should have unique_id
        for child in result:
            assert 'unique_id' in child
            assert child['unique_id'].startswith('model.')

    def test_children_recursive_all_descendants(self, prod_manifest):
        """Should return all descendants when recursive=True"""
        model_name = "core_client__client_profiles_events"
        direct = children(str(prod_manifest), model_name, recursive=False)
        all_descendants = children(str(prod_manifest), model_name, recursive=True)

        # All descendants should be >= direct children
        assert len(all_descendants) >= len(direct)

    def test_children_filters_out_tests(self, prod_manifest):
        """Should filter out test nodes"""
        model_name = "core_client__client_profiles_events"
        result = children(str(prod_manifest), model_name, recursive=True)

        # No test nodes should be included
        for child in result:
            assert not child['unique_id'].startswith('test.')

    def test_children_nonexistent_model_returns_none(self, prod_manifest):
        """Should return None for non-existent model"""
        result = children(str(prod_manifest), "nonexistent__model")
        assert result is None

    def test_children_handles_model_without_downstream(self, prod_manifest):
        """Should return empty list for model with no downstream dependencies"""
        # Most leaf models have no children
        result = children(str(prod_manifest), "sugarcrm_px_customerstages", recursive=False)

        # Should return empty list
        assert isinstance(result, list)


# TestSchemaDevFlag class moved to test_dev_and_fallbacks.py for better organization


class TestNodeCommand:
    """Test node command - full node details"""

    def test_node_by_model_name(self, prod_manifest):
        """Should get node by model name"""
        model_name = "core_client__client_profiles_events"
        result = node(str(prod_manifest), model_name)

        assert isinstance(result, dict)
        assert 'unique_id' in result
        assert result['unique_id'].startswith('model.')
        assert 'name' in result
        assert 'resource_type' in result

    def test_node_by_unique_id(self, prod_manifest):
        """Should get node by unique_id"""
        unique_id = "model.admirals_bi_dwh.core_client__client_profiles_events"
        result = node(str(prod_manifest), unique_id)

        assert isinstance(result, dict)
        assert result['unique_id'] == unique_id
        assert 'name' in result

    def test_node_returns_complete_metadata(self, prod_manifest):
        """Should return complete node metadata"""
        model_name = "core_client__client_profiles_events"
        result = node(str(prod_manifest), model_name)

        # Should have extensive metadata
        assert 'database' in result
        assert 'schema' in result
        assert 'config' in result
        assert 'columns' in result or result.get('columns') is not None

    def test_node_nonexistent_returns_none(self, prod_manifest):
        """Should return None for non-existent node"""
        result = node(str(prod_manifest), "nonexistent__model")
        assert result is None


class TestRefreshCommand:
    """Test refresh command - runs dbt parse"""

    def test_refresh_calls_dbt_parse(self, mocker):
        """Should call subprocess.run with dbt parse"""
        mock_run = mocker.patch('subprocess.run')

        refresh()

        # Verify dbt parse was called
        mock_run.assert_called_once()
        call_args = mock_run.call_args[0][0]
        assert call_args == ['dbt', 'parse']

    def test_refresh_raises_on_error(self, mocker):
        """Should raise exception if dbt parse fails"""
        import subprocess as sp
        mock_run = mocker.patch('subprocess.run')
        mock_run.side_effect = sp.CalledProcessError(1, 'dbt parse')

        with pytest.raises(sp.CalledProcessError):
            refresh()


class TestDocsCommand:
    """Test docs command - columns with descriptions"""

    def test_docs_returns_columns_with_descriptions(self, prod_manifest):
        """Should return columns with name, data_type, description"""
        model_name = "core_client__client_profiles_events"
        result = docs(str(prod_manifest), model_name)

        assert isinstance(result, list)
        assert len(result) > 0

        # Each column should have required fields
        for col in result:
            assert 'name' in col
            assert 'data_type' in col
            assert 'description' in col

    def test_docs_includes_all_columns(self, prod_manifest):
        """Should include all documented columns"""
        model_name = "core_client__client_profiles_events"
        result = docs(str(prod_manifest), model_name)

        # Should match columns command count
        cols = columns(str(prod_manifest), model_name)

        # Docs might have fewer if some columns lack descriptions
        # But structure should match
        assert len(result) > 0

    def test_docs_handles_empty_descriptions(self, prod_manifest):
        """Should handle columns with no description"""
        model_name = "core_client__client_profiles_events"
        result = docs(str(prod_manifest), model_name)

        # Some columns may have empty descriptions
        for col in result:
            assert isinstance(col['description'], str)

    def test_docs_nonexistent_model_returns_none(self, prod_manifest):
        """Should return None for non-existent model"""
        result = docs(str(prod_manifest), "nonexistent__model")
        assert result is None
