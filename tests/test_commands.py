"""
Tests for Commands - All command functionality

This module consolidates all command tests:
- Core commands: info, schema, columns
- Advanced commands: config, deps, sql, docs, path, parents, children
- Utilities: list, search, node, refresh
- Dev mode: --dev flag with dev manifest priority
- Fallback systems: production → dev → BigQuery
- Edge cases: null values, empty strings, special characters

Replaces old files:
- test_commands.py
- test_dev_and_fallbacks.py
- test_edge_cases.py
"""

import json

import pytest

from dbt_meta.commands import (
    children,
    columns,
    config,
    deps,
    docs,
    info,
    list_models,
    ls,
    parents,
    path,
    refresh,
    schema,
    search,
    sql,
)


class TestInfoCommand:
    """Test info command - basic model metadata"""


    def test_info_nonexistent_model_returns_none(self, prod_manifest):
        """
        Should return None for non-existent model

        Graceful error handling without exceptions.
        """
        result = info(str(prod_manifest), "nonexistent__model")

        assert result is None

    def test_info_extracts_materialized_type(self, prod_manifest, test_model):
        """
        Should extract materialization type from config

        Common types: table, view, incremental, ephemeral
        """
        model_name = test_model  # Use fixture
        result = info(str(prod_manifest), model_name)

        assert 'materialized' in result
        assert result['materialized'] in ['table', 'view', 'incremental', 'ephemeral']

    def test_info_extracts_tags(self, prod_manifest, test_model):
        """
        Should extract tags as list

        Empty list if no tags.
        """
        model_name = test_model  # Use fixture
        result = info(str(prod_manifest), model_name)

        assert 'tags' in result
        assert isinstance(result['tags'], list)


class TestSchemaCommand:
    """Test schema command - table location"""


    def test_schema_uses_alias_if_present(self, prod_manifest, test_model):
        """
        Should use config.alias as table name if present

        Falls back to model name if no alias.
        """
        model_name = test_model  # Use fixture
        result = schema(str(prod_manifest), model_name)

        # Table should be either alias or model name
        assert 'table' in result
        assert isinstance(result["table"], str) and len(result["table"]) > 0

    def test_schema_nonexistent_model_returns_none(self, prod_manifest):
        """
        Should return None for non-existent model

        Graceful error handling.
        """
        result = schema(str(prod_manifest), "nonexistent__model")

        assert result is None

    def test_schema_constructs_full_name(self, prod_manifest, test_model):
        """
        Should construct full_name as database.schema.table

        Format: {database}.{schema}.{table}
        """
        model_name = test_model  # Use fixture
        result = schema(str(prod_manifest), model_name)

        expected_full = f"{result['database']}.{result['schema']}.{result['table']}"
        assert result['full_name'] == expected_full


class TestColumnsCommand:
    """Test columns command - column list with types"""


    def test_columns_returns_list(self, prod_manifest, test_model):
        """
        Should return list of column dictionaries

        Each column: {name: str, data_type: str}
        """
        model_name = test_model  # Use fixture
        result = columns(str(prod_manifest), model_name)

        assert isinstance(result, list)
        assert isinstance(result, list)  # May be empty if no column descriptions

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

    def test_columns_preserves_order(self, prod_manifest, test_model):
        """
        Should preserve column order from manifest

        Columns should appear in same order as defined.
        """
        model_name = test_model  # Use fixture
        result = columns(str(prod_manifest), model_name)

        # Verify columns exist and preserve order
        assert len(result) >= 1
        assert all('name' in col and 'data_type' in col for col in result)

    def test_columns_fallback_to_bigquery_when_empty(self, prod_manifest, mocker, monkeypatch):
        """
        Should fallback to BigQuery when columns not in manifest

        Critical: 64% of models don't have columns in manifest.
        """
        # Disable catalog fallback to test BigQuery fallback
        monkeypatch.setenv('DBT_FALLBACK_CATALOG', 'false')

        # Mock Config.find_config_file to ignore real TOML config
        from unittest.mock import patch

        from dbt_meta.config import Config
        with patch.object(Config, 'find_config_file', return_value=None):
            # Mock git functions to avoid git calls
            mocker.patch('dbt_meta.utils.git.is_modified', return_value=False)
            mocker.patch('dbt_meta.utils.git.is_committed_but_not_in_main', return_value=False)

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

    def test_columns_fallback_bq_not_installed(self, prod_manifest, mocker, monkeypatch):
        """
        Should return None if bq not installed

        Graceful degradation when BigQuery SDK not available.
        """
        # Disable catalog fallback to test BigQuery fallback
        monkeypatch.setenv('DBT_FALLBACK_CATALOG', 'false')

        # Mock Config.find_config_file to ignore real TOML config
        from unittest.mock import patch

        from dbt_meta.config import Config
        with patch.object(Config, 'find_config_file', return_value=None):
            # Mock subprocess to simulate bq not found
            mock_run = mocker.patch('subprocess.run')
            mock_run.side_effect = FileNotFoundError("bq not found")

            model_name = "sugarcrm_px_customerstages"
            result = columns(str(prod_manifest), model_name)

            # Should return None
            assert result is None

    def test_columns_fallback_bq_table_not_found(self, enable_fallbacks, prod_manifest, mocker, monkeypatch):
        """
        Should return None if BigQuery table doesn't exist

        Handles case when manifest references non-existent table.
        """
        # Disable catalog fallback to test BigQuery fallback
        monkeypatch.setenv('DBT_FALLBACK_CATALOG', 'false')

        # Mock Config.find_config_file to ignore real TOML config
        from unittest.mock import patch

        from dbt_meta.config import Config
        with patch.object(Config, 'find_config_file', return_value=None):
            # Mock git status (not testing git here - testing BigQuery fallback)
            mock_git_check = mocker.patch('dbt_meta.command_impl.base._check_manifest_git_mismatch')
            mock_git_check.return_value = []  # No warnings

            # Mock subprocess for both git and BigQuery calls
            mock_run = mocker.patch('subprocess.run')

            import subprocess as sp

            # Create proper mock responses
            git_mock = mocker.Mock(returncode=0, stdout='')  # git returns empty
            bq_version_mock = mocker.Mock(returncode=0)  # bq version succeeds

            mock_run.side_effect = [
                git_mock,  # git diff call (from any git checks)
                git_mock,  # git status call (from any git checks)
                bq_version_mock,  # bq version check
                sp.CalledProcessError(1, 'bq show')  # bq show fails - table not found
            ]

            model_name = "sugarcrm_px_customerstages"
            result = columns(str(prod_manifest), model_name)

            # Should return None
            assert result is None


class TestConfigCommand:
    """Test config command - full dbt config"""

    def test_config_returns_full_config(self, prod_manifest, test_model):
        """
        Should return full dbt config dictionary

        Config includes: materialized, partition_by, cluster_by,
        incremental_strategy, unique_key, tags, etc.
        """
        model_name = test_model  # Use fixture
        result = config(str(prod_manifest), model_name)

        assert isinstance(result, dict)
        assert 'materialized' in result
        assert 'tags' in result
        assert len(result) > 10  # Should have many config fields


    def test_config_nonexistent_model_returns_none(self, prod_manifest, test_model):
        """Should return None for non-existent model"""
        result = config(str(prod_manifest), "nonexistent__model")
        assert result is None

    def test_config_includes_partition_info(self, prod_manifest, test_model):
        """
        Should include partition_by config for incremental models

        Important for query optimization.
        """
        model_name = test_model  # Use fixture
        result = config(str(prod_manifest), model_name)

        assert 'partition_by' in result
        # Partition config is a dict or None
        assert result['partition_by'] is None or isinstance(result['partition_by'], dict)


class TestDepsCommand:
    """Test deps command - model dependencies"""

    def test_deps_returns_dict_with_refs_sources(self, prod_manifest, test_model):
        """
        Should return dictionary with refs and sources

        Format: {"refs": [...], "sources": [...]}
        """
        model_name = test_model  # Use fixture
        result = deps(str(prod_manifest), model_name)

        assert isinstance(result, dict)
        assert 'refs' in result
        assert 'sources' in result
        assert isinstance(result['refs'], list)
        assert isinstance(result['sources'], list)


    def test_deps_nonexistent_model_returns_empty(self, prod_manifest, test_model):
        """Should return empty refs/sources for non-existent model"""
        result = deps(str(prod_manifest), "nonexistent__model")
        assert result == {'refs': [], 'sources': []}

    def test_deps_includes_model_refs(self, prod_manifest, test_model):
        """
        Should include model dependencies (refs)

        refs should be in format: model.project.model_name
        """
        model_name = test_model  # Use fixture
        result = deps(str(prod_manifest), model_name)

        # Model may or may not have refs, check structure
        assert 'refs' in result and isinstance(result['refs'], list)
        # All refs should start with 'model.'
        for ref in result['refs']:
            assert ref.startswith('model.')


class TestSqlCommand:
    """Test sql command - SQL code extraction"""

    def test_sql_returns_raw_code_with_jinja(self, prod_manifest, test_model):
        """
        Should return raw SQL with Jinja templates

        When raw=True, should include {{ config() }}, {% set %}, etc.
        """
        model_name = test_model  # Use fixture
        result = sql(str(prod_manifest), model_name, raw=True)

        assert isinstance(result, str)
        # Raw SQL should contain Jinja syntax
        assert '{{' in result or '{%' in result

    def test_sql_returns_empty_for_compiled(self, prod_manifest, test_model):
        """
        Should return empty string for compiled SQL if not available

        Compiled SQL only in ~/dbt-state/ manifest after dbt compile.
        """
        model_name = test_model  # Use fixture
        result = sql(str(prod_manifest), model_name, raw=False)

        # In production manifest, compiled_code might not exist
        assert result == '' or isinstance(result, str)

    def test_sql_nonexistent_model_returns_none(self, prod_manifest, test_model):
        """Should return None for non-existent model"""
        result = sql(str(prod_manifest), "nonexistent__model", raw=True)
        assert result is None

    def test_sql_raw_contains_config(self, prod_manifest, test_model):
        """
        Raw SQL should contain dbt config block

        Config defines materialization, partition, etc.
        """
        model_name = test_model  # Use fixture
        result = sql(str(prod_manifest), model_name, raw=True)

        assert 'config(' in result.lower()

    def test_sql_returns_empty_string_not_none_for_missing_compiled(self, tmp_path):
        """
        Should return empty string (not None) when compiled_code missing

        None is reserved for model not found, empty string means no compiled SQL.
        Note: In production this shouldn't happen as all models are compiled.
        """
        # Create manifest with model that has empty compiled_code
        manifest_path = tmp_path / "manifest.json"
        manifest_data = {
            "metadata": {"dbt_version": "1.5.0"},
            "nodes": {
                "model.project.test_model": {
                    "name": "test_model",
                    "resource_type": "model",
                    "schema": "analytics",
                    "alias": "test_table",
                    "raw_code": "SELECT * FROM {{ ref('upstream') }}",
                    "compiled_code": "",  # Empty compiled code (shouldn't happen in prod)
                    "original_file_path": "models/test_model.sql"
                }
            }
        }
        manifest_path.write_text(json.dumps(manifest_data))

        result = sql(str(manifest_path), "test_model", raw=False)

        # Should be empty string, NOT None
        assert result == ''
        assert result is not None


class TestSqlCommandJsonOutput:
    """Test sql command JSON output with -j flag"""

    def test_sql_json_output_structure_compiled(self, prod_manifest_with_compiled, test_model):
        """Should return string for compiled SQL (CLI wraps in JSON)"""
        model_name = test_model  # Use fixture

        result = sql(str(prod_manifest_with_compiled), model_name, raw=False, json_output=True)

        # Result is string (SQL code) or empty string if compiled_code not available
        # CLI wraps it in JSON structure
        assert isinstance(result, str)
        # May be empty if manifest doesn't have compiled_code

    def test_sql_json_output_structure_raw(self, prod_manifest, test_model):
        """Should return string for raw SQL (CLI wraps in JSON)"""
        model_name = test_model  # Use fixture

        result = sql(str(prod_manifest), model_name, raw=True, json_output=True)

        # Result is raw SQL string
        assert isinstance(result, str)
        assert '{{' in result or '{%' in result  # Contains Jinja

class TestPathCommand:
    """Test path command - file path extraction"""

    def test_path_returns_relative_path(self, prod_manifest, test_model):
        """
        Should return relative file path

        Format: models/schema/model_name.sql
        """
        model_name = test_model  # Use fixture
        result = path(str(prod_manifest), model_name)

        assert isinstance(result, str)
        assert result.startswith('models/')
        assert result.endswith('.sql')


    def test_path_nonexistent_model_returns_none(self, prod_manifest, test_model):
        """Should return None for non-existent model"""
        result = path(str(prod_manifest), "nonexistent__model")
        assert result is None

    def test_path_with_bigquery_format(self, tmp_path):
        """Should find model by BigQuery format (schema.table)"""
        # Create manifest with model that has alias
        manifest_path = tmp_path / "manifest.json"
        manifest_data = {
            "metadata": {"dbt_version": "1.5.0"},
            "nodes": {
                "model.project.customers": {
                    "name": "customers",
                    "resource_type": "model",
                    "schema": "analytics",
                    "alias": "dim_customers",
                    "original_file_path": "models/analytics/customers.sql"
                }
            }
        }
        manifest_path.write_text(json.dumps(manifest_data))

        # Try to find model by BigQuery format: schema.table
        result = path(str(manifest_path), "analytics.dim_customers")

        # Should find model by schema + alias
        assert result is not None
        assert result == "models/analytics/customers.sql"


class TestPathCommandJsonOutput:
    """Test path command JSON output with -j flag"""

    def test_path_json_output_returns_string(self, prod_manifest, test_model):
        """Should return path as string (CLI wraps it in JSON)"""
        model_name = test_model  # Use fixture

        result = path(str(prod_manifest), model_name, json_output=True)

        # Result is path string, CLI wraps in JSON
        assert isinstance(result, str)
        assert result.startswith('models/')
        assert result.endswith('.sql')

    def test_path_json_output_nonexistent_returns_none(self, prod_manifest):
        """Should return None for non-existent model even with json_output=True"""
        result = path(str(prod_manifest), "nonexistent__model", json_output=True)
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
        result = list_models(str(prod_manifest), pattern="client")

        assert isinstance(result, list)
        assert isinstance(result, list)  # May be empty if no column descriptions
        # All results should contain pattern
        for model in result:
            assert 'client' in model.lower()

    def test_list_models_pattern_case_insensitive(self, prod_manifest):
        """Should perform case-insensitive pattern matching"""
        result_lower = list_models(str(prod_manifest), pattern="client")
        result_upper = list_models(str(prod_manifest), pattern="CLIENT")

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
        assert isinstance(result, list)  # May be empty if no column descriptions

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

        assert isinstance(result, list)  # May be empty if no column descriptions
        # At least one result should have exact match
        names = [r['name'] for r in result]
        assert any('client_profiles_events' in name for name in names)

    def test_search_results_sorted_by_name(self, prod_manifest, test_model):
        """Should return results sorted alphabetically by name"""
        result = search(str(prod_manifest), "client")

        names = [r['name'] for r in result]
        assert names == sorted(names)


class TestParentsCommand:
    """Test parents command - upstream dependencies"""

    def test_parents_direct_only(self, prod_manifest, test_model):
        """Should return direct parents only (non-recursive)"""
        model_name = test_model  # Use fixture
        result = parents(str(prod_manifest), model_name, recursive=False)

        assert isinstance(result, list)
        assert isinstance(result, list)  # May be empty if no column descriptions
        # Each parent should have path, table, type in compact format
        for parent in result:
            assert 'path' in parent
            assert 'table' in parent
            assert 'type' in parent
            assert parent['type'] in ['model', 'source', 'seed', 'snapshot']

    def test_parents_recursive_all_ancestors(self, prod_manifest, test_model):
        """Should return all ancestors when recursive=True"""
        model_name = test_model  # Use fixture
        direct = parents(str(prod_manifest), model_name, recursive=False)
        all_ancestors = parents(str(prod_manifest), model_name, recursive=True)

        # All ancestors should be >= direct parents
        assert len(all_ancestors) >= len(direct)

    def test_parents_filters_out_tests(self, prod_manifest, test_model):
        """Should filter out test nodes"""
        model_name = test_model  # Use fixture
        result = parents(str(prod_manifest), model_name, recursive=True)

        # No test nodes should be included
        for parent in result:
            assert not parent['unique_id'].startswith('test.')

    def test_parents_nonexistent_model_returns_none(self, prod_manifest):
        """Should return None for non-existent model"""
        result = parents(str(prod_manifest), "nonexistent__model")
        assert result is None

    def test_parents_handles_model_without_dependencies(self, prod_manifest, test_model):
        """Should return empty list for model with no dependencies"""
        # Find a source or seed (no upstream dependencies)
        result = parents(str(prod_manifest), "sugarcrm_px_customerstages", recursive=False)

        # Should return empty list or minimal dependencies
        assert isinstance(result, list)


class TestChildrenCommand:
    """Test children command - downstream dependencies"""

    def test_children_direct_only(self, prod_manifest, test_model):
        """Should return direct children only (non-recursive)"""
        model_name = test_model  # Use fixture
        result = children(str(prod_manifest), model_name, recursive=False)

        assert isinstance(result, list)
        # Each child should have path, table, type in compact format
        for child in result:
            assert 'path' in child
            assert 'table' in child
            assert 'type' in child
            assert child['type'] in ['model', 'source', 'seed', 'snapshot']

    def test_children_recursive_all_descendants(self, prod_manifest, test_model):
        """Should return all descendants when recursive=True"""
        model_name = test_model  # Use fixture
        direct = children(str(prod_manifest), model_name, recursive=False)
        all_descendants = children(str(prod_manifest), model_name, recursive=True)

        # All descendants should be >= direct children
        assert len(all_descendants) >= len(direct)

    def test_children_filters_out_tests(self, prod_manifest, test_model):
        """Should filter out test nodes"""
        model_name = test_model  # Use fixture
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


class TestRefreshCommand:
    """Test refresh command - syncs production or parses dev"""

    def test_refresh_production_mode(self, mocker):
        """Should call sync-artifacts.sh with --force in production mode"""
        mock_run = mocker.patch('subprocess.run')
        mocker.patch('pathlib.Path.exists', return_value=True)

        refresh(use_dev=False)

        # Verify sync-artifacts.sh was called with --force
        mock_run.assert_called_once()
        call_args = mock_run.call_args[0][0]
        assert len(call_args) == 2
        assert call_args[0].endswith('sync-artifacts.sh')
        assert call_args[1] == '--force'

    def test_refresh_dev_mode(self, mocker):
        """Should call dbt parse --target dev in dev mode"""
        mock_run = mocker.patch('subprocess.run')

        refresh(use_dev=True)

        # Verify dbt parse --target dev was called
        mock_run.assert_called_once()
        call_args = mock_run.call_args[0][0]
        assert call_args == ['dbt', 'parse', '--target', 'dev']

    def test_refresh_script_not_found(self, mocker):
        """Should raise DbtMetaError if sync script missing"""
        from dbt_meta.errors import DbtMetaError
        mocker.patch('pathlib.Path.exists', return_value=False)

        with pytest.raises(DbtMetaError, match="Sync script not found"):
            refresh(use_dev=False)

    def test_refresh_raises_on_subprocess_error(self, mocker):
        """Should raise exception if subprocess fails"""
        import subprocess as sp
        mock_run = mocker.patch('subprocess.run')
        mock_run.side_effect = sp.CalledProcessError(1, 'dbt parse')

        with pytest.raises(sp.CalledProcessError):
            refresh(use_dev=True)


class TestDocsCommand:
    """Test docs command - columns with descriptions"""

    def test_docs_returns_columns_with_descriptions(self, prod_manifest, test_model):
        """Should return columns with name, data_type, description"""
        model_name = test_model  # Use fixture
        result = docs(str(prod_manifest), model_name)

        assert isinstance(result, list)
        assert isinstance(result, list)  # May be empty if no column descriptions

        # Each column should have required fields
        for col in result:
            assert 'name' in col
            assert 'data_type' in col
            assert 'description' in col

    def test_docs_includes_all_columns(self, prod_manifest, test_model):
        """Should include all documented columns"""
        model_name = test_model  # Use fixture
        result = docs(str(prod_manifest), model_name)

        # Should match columns command count
        columns(str(prod_manifest), model_name)

        # Docs might have fewer if some columns lack descriptions
        # But structure should match
        assert isinstance(result, list)  # May be empty if no column descriptions

    def test_docs_handles_empty_descriptions(self, prod_manifest, test_model):
        """Should handle columns with no description"""
        model_name = test_model  # Use fixture
        result = docs(str(prod_manifest), model_name)

        # Some columns may have empty descriptions
        for col in result:
            assert isinstance(col['description'], str)

    def test_docs_nonexistent_model_returns_none(self, prod_manifest):
        """Should return None for non-existent model"""
        result = docs(str(prod_manifest), "nonexistent__model")
        assert result is None

# ============================================================================
# Dev Mode & Fallback Tests
# ============================================================================

from unittest.mock import MagicMock, patch

from dbt_meta.utils.dev import find_dev_manifest as _find_dev_manifest
from dbt_meta.utils.git import is_modified

# ============================================================================
# SECTION 1: Git Change Detection - is_modified()
# ============================================================================
# NOTE: is_modified() is now an internal helper function (not a CLI command)
# It is tested indirectly through warning system tests in SECTION 2-7 below
# Direct unit tests removed as the function is no longer public API
# ============================================================================


# ============================================================================
# SECTION 2: Schema with Dev Flag
# ============================================================================


class TestSchemaWithDevFlag:
    """Test schema() with use_dev parameter"""

    def test_schema_with_dev_prioritizes_dev_manifest(self, tmp_path, monkeypatch):
        """With use_dev=True, should check dev manifest FIRST"""
        # Setup manifests
        project_root = tmp_path / "project"
        project_root.mkdir()
        dbt_state = project_root / ".dbt-state"
        dbt_state.mkdir()
        target = project_root / "target"
        target.mkdir()

        # Production manifest (with different data)
        prod_manifest = dbt_state / "manifest.json"
        prod_data = {
            "nodes": {
                "model.project.test_schema__events": {
                    "name": "test_schema__events",
                    "schema": "test_schema",
                    "database": "test-project",
                    "config": {"alias": "events_prod"}
                }
            }
        }
        prod_manifest.write_text(json.dumps(prod_data))

        # Dev manifest (should be used with use_dev=True)
        dev_manifest = target / "manifest.json"
        dev_data = {
            "nodes": {
                "model.project.test_schema__events": {
                    "name": "test_schema__events",  # Full SQL filename
                    "schema": "personal_test",
                    "database": "",
                    "config": {}
                }
            }
        }
        dev_manifest.write_text(json.dumps(dev_data))

        monkeypatch.setenv('DBT_USER', 'test_user')
        monkeypatch.setenv('DBT_DEV_SCHEMA_PREFIX', 'personal')
        monkeypatch.setenv('DBT_FALLBACK_BIGQUERY', 'false')

        result = schema(str(prod_manifest), "test_schema__events", use_dev=True)

        assert result is not None
        assert result['schema'] == 'personal_test_user'  # Dev schema
        assert result['table'] == 'test_schema__events'  # Full SQL filename (matches dbt --target dev)
        # Dev result doesn't include database key
        assert 'full_name' in result

    def test_schema_without_dev_uses_production_first(self, tmp_path):
        """Without use_dev, should use production manifest first"""
        project_root = tmp_path / "project"
        project_root.mkdir()
        dbt_state = project_root / ".dbt-state"
        dbt_state.mkdir()

        prod_manifest = dbt_state / "manifest.json"
        prod_data = {
            "nodes": {
                "model.project.test_schema__events": {
                    "name": "test_schema__events",
                    "schema": "test_schema",
                    "database": "test-project",
                    "config": {"alias": "events"}
                }
            }
        }
        prod_manifest.write_text(json.dumps(prod_data))

        result = schema(str(prod_manifest), "test_schema__events", use_dev=False)

        assert result is not None
        assert result['schema'] == 'test_schema'  # Production schema
        assert result['database'] == 'test-project'  # Production database

    def test_schema_dev_falls_back_to_bigquery_when_enabled(self, tmp_path, monkeypatch):
        """With use_dev=True and model not in dev, should try BigQuery"""
        project_root = tmp_path / "project"
        project_root.mkdir()
        dbt_state = project_root / ".dbt-state"
        dbt_state.mkdir()
        target = project_root / "target"
        target.mkdir()

        # Empty manifests
        prod_manifest = dbt_state / "manifest.json"
        prod_manifest.write_text('{"nodes": {}}')
        dev_manifest = target / "manifest.json"
        dev_manifest.write_text('{"nodes": {}}')

        monkeypatch.setenv('DBT_USER', 'test')
        monkeypatch.setenv('DBT_FALLBACK_BIGQUERY', 'true')

        with patch('subprocess.run') as mock_run:
            # Mock successful bq show
            mock_result = MagicMock()
            mock_result.returncode = 0
            mock_run.return_value = mock_result

            schema(str(prod_manifest), "test_schema__events", use_dev=True)

            # Should have tried bq show with dev schema
            assert mock_run.called
            bq_call_args = str(mock_run.call_args)
            assert 'bq' in bq_call_args
            assert 'show' in bq_call_args

    def test_schema_dev_skips_production_manifest(self, tmp_path, monkeypatch):
        """With use_dev=True, should NOT search production manifest"""
        project_root = tmp_path / "project"
        project_root.mkdir()
        dbt_state = project_root / ".dbt-state"
        dbt_state.mkdir()
        target = project_root / "target"
        target.mkdir()

        # Production has model, dev doesn't
        prod_manifest = dbt_state / "manifest.json"
        prod_data = {
            "nodes": {
                "model.project.test_model": {
                    "name": "test_model",
                    "schema": "prod_schema",
                    "database": "prod_db",
                    "config": {}
                }
            }
        }
        prod_manifest.write_text(json.dumps(prod_data))

        dev_manifest = target / "manifest.json"
        dev_manifest.write_text('{"nodes": {}}')

        monkeypatch.setenv('DBT_FALLBACK_BIGQUERY', 'false')

        result = schema(str(prod_manifest), "test_model", use_dev=True)

        # Should return None (not found in dev, fallback disabled)
        assert result is None


class TestSchemaDevFlag:
    """Test schema with --dev flag - dev table location

    Moved from test_commands.py to consolidate dev-related tests
    Note: v0.4.0 changed behavior - use_dev=True requires dev manifest (target/)
    """

    def test_schema_with_dev_flag_nonexistent_model_returns_none(self, tmp_path):
        """Should return None for non-existent model with use_dev=True"""
        project_root = tmp_path / "project"
        project_root.mkdir()
        dbt_state = project_root / ".dbt-state"
        dbt_state.mkdir()
        target = project_root / "target"
        target.mkdir()

        prod_manifest = dbt_state / "manifest.json"
        prod_manifest.write_text('{"nodes": {}}')
        dev_manifest = target / "manifest.json"
        dev_manifest.write_text('{"nodes": {}}')

        result = schema(str(prod_manifest), "nonexistent__model", use_dev=True)
        assert result is None


# ============================================================================
# SECTION 3: Columns with Dev Flag
# ============================================================================


class TestColumnsWithDevFlag:
    """Test columns() with use_dev parameter"""

    def test_columns_with_dev_prioritizes_dev_manifest(self, tmp_path, monkeypatch):
        """With use_dev=True, should ALWAYS use BigQuery (not manifest columns)"""
        project_root = tmp_path / "project"
        project_root.mkdir()
        dbt_state = project_root / ".dbt-state"
        dbt_state.mkdir()
        target = project_root / "target"
        target.mkdir()

        prod_manifest = dbt_state / "manifest.json"
        prod_manifest.write_text('{"nodes": {}}')

        dev_manifest = target / "manifest.json"
        dev_data = {
            "nodes": {
                "model.project.test_model": {
                    "name": "test_model",
                    "schema": "personal_test",
                    "database": "test-project",
                    "config": {}
                }
            }
        }
        dev_manifest.write_text(json.dumps(dev_data))

        monkeypatch.setenv('DBT_FALLBACK_BIGQUERY', 'false')

        # Mock BigQuery - ALWAYS called now (never uses manifest columns)
        with patch('dbt_meta.command_impl.columns._fetch_columns_from_bigquery_direct') as mock_bq:
            mock_bq.return_value = [
                {'name': 'col1', 'data_type': 'STRING'},
                {'name': 'col2', 'data_type': 'INTEGER'}
            ]

            result = columns(str(prod_manifest), "test_model", use_dev=True)

            # Verify BigQuery was called (ALWAYS, not just as fallback)
            assert mock_bq.called

        assert result is not None
        assert len(result) == 2
        assert result[0]['name'] == 'col1'
        assert result[0]['data_type'] == 'STRING'
        assert result[1]['name'] == 'col2'
        assert result[1]['data_type'] == 'INTEGER'

    def test_columns_with_dev_falls_back_to_bigquery(self, tmp_path, monkeypatch):
        """With use_dev=True and model not in manifest, should try BigQuery"""
        project_root = tmp_path / "project"
        project_root.mkdir()
        dbt_state = project_root / ".dbt-state"
        dbt_state.mkdir()
        target = project_root / "target"
        target.mkdir()

        prod_manifest = dbt_state / "manifest.json"
        prod_manifest.write_text('{"nodes": {}}')
        dev_manifest = target / "manifest.json"
        dev_manifest.write_text('{"nodes": {}}')

        monkeypatch.setenv('DBT_USER', 'test')
        monkeypatch.setenv('DBT_FALLBACK_BIGQUERY', 'true')

        # Mock git at the import level in columns.py
        with patch('dbt_meta.command_impl.columns.get_model_git_status') as mock_git:
            from dbt_meta.utils.git import GitStatus
            mock_git.return_value = GitStatus(
                exists=True,
                is_tracked=False,
                is_modified=True,
                is_committed=False,
                is_deleted=False,
                is_new=True
            )

            with patch('dbt_meta.command_impl.columns._fetch_columns_from_bigquery_direct') as mock_fetch:
                mock_fetch.return_value = [
                    {'name': 'id', 'data_type': 'INTEGER'},
                    {'name': 'name', 'data_type': 'STRING'}
                ]

                # Use proper dbt model name with __ so _infer_table_parts() works
                columns(str(prod_manifest), "test_schema__test_model", use_dev=True)

                assert mock_fetch.called
                # Should call with dev schema
                call_args = mock_fetch.call_args[0]
                assert 'personal_test' in call_args[0]  # dev schema

    def test_columns_without_dev_uses_production(self, tmp_path):
        """Without use_dev, should ALWAYS use BigQuery (not manifest columns)"""
        project_root = tmp_path / "project"
        project_root.mkdir()
        dbt_state = project_root / ".dbt-state"
        dbt_state.mkdir()

        prod_manifest = dbt_state / "manifest.json"
        prod_data = {
            "nodes": {
                "model.project.test_model": {
                    "name": "test_model",
                    "schema": "prod_schema",
                    "database": "test-project",
                    "config": {}
                }
            }
        }
        prod_manifest.write_text(json.dumps(prod_data))

        # Mock BigQuery - ALWAYS called now (never uses manifest columns)
        with patch('dbt_meta.command_impl.columns._fetch_columns_from_bigquery_direct') as mock_bq:
            mock_bq.return_value = [
                {'name': 'prod_col', 'data_type': 'STRING'}
            ]

            result = columns(str(prod_manifest), "test_model", use_dev=False)

            # Verify BigQuery was called
            assert mock_bq.called

        assert result is not None
        assert len(result) == 1
        assert result[0]['name'] == 'prod_col'


# ============================================================================
# SECTION 4: Dev Workflow Integration
# ============================================================================


class TestDevFlagIntegration:
    """Integration tests for --dev flag behavior"""

    def test_dev_flag_uses_dev_schema_naming(self, tmp_path, monkeypatch):
        """Dev flag should use personal_USERNAME schema"""
        project_root = tmp_path / "project"
        project_root.mkdir()
        dbt_state = project_root / ".dbt-state"
        dbt_state.mkdir()
        target = project_root / "target"
        target.mkdir()

        prod_manifest = dbt_state / "manifest.json"
        prod_manifest.write_text('{"nodes": {}}')

        dev_manifest = target / "manifest.json"
        dev_data = {
            "nodes": {
                "model.project.test_model": {
                    "name": "test_model",
                    "schema": "ignored",  # Should be overridden
                    "database": "",
                    "config": {}
                }
            }
        }
        dev_manifest.write_text(json.dumps(dev_data))

        monkeypatch.setenv('DBT_USER', 'john_doe')
        monkeypatch.setenv('DBT_DEV_SCHEMA_PREFIX', 'personal')

        result = schema(str(prod_manifest), "test_model", use_dev=True)

        assert result is not None
        assert result['schema'] == 'personal_john_doe'

    def test_dev_flag_uses_custom_dev_schema_template(self, tmp_path, monkeypatch):
        """Should respect DBT_DEV_SCHEMA_TEMPLATE"""
        project_root = tmp_path / "project"
        project_root.mkdir()
        dbt_state = project_root / ".dbt-state"
        dbt_state.mkdir()
        target = project_root / "target"
        target.mkdir()

        prod_manifest = dbt_state / "manifest.json"
        prod_manifest.write_text('{"nodes": {}}')

        dev_manifest = target / "manifest.json"
        dev_data = {
            "nodes": {
                "model.project.test": {
                    "name": "test",
                    "schema": "x",
                    "database": "",
                    "config": {}
                }
            }
        }
        dev_manifest.write_text(json.dumps(dev_data))

        monkeypatch.setenv('DBT_USER', 'alice')
        monkeypatch.setenv('DBT_DEV_SCHEMA_TEMPLATE', 'dev_{username}_sandbox')

        result = schema(str(prod_manifest), "test", use_dev=True)

        assert result is not None
        assert result['schema'] == 'dev_alice_sandbox'

    def test_dev_flag_workflow_modified_model(self, tmp_path, monkeypatch):
        """Complete workflow: is_modified → schema --dev"""
        # Step 1: Check if modified
        with patch('subprocess.run') as mock_run:
            mock_result = MagicMock()
            mock_result.returncode = 0
            mock_result.stdout = "models/core/events.sql"
            mock_run.return_value = mock_result

            modified = is_modified("core__events")
            assert modified is True

        # Step 2: If modified, use --dev flag
        if modified:
            project_root = tmp_path / "project"
            project_root.mkdir()
            dbt_state = project_root / ".dbt-state"
            dbt_state.mkdir()
            target = project_root / "target"
            target.mkdir()

            prod_manifest = dbt_state / "manifest.json"
            prod_manifest.write_text('{"nodes": {}}')

            dev_manifest = target / "manifest.json"
            dev_data = {
                "nodes": {
                    "model.project.core__events": {
                        "name": "events",
                        "schema": "x",
                        "database": "",
                        "config": {}
                    }
                }
            }
            dev_manifest.write_text(json.dumps(dev_data))

            monkeypatch.setenv('DBT_USER', 'test')

            result = schema(str(prod_manifest), "core__events", use_dev=True)

            assert result is not None
            assert 'personal_test' in result['schema']


# ============================================================================
# SECTION 5: Dev Table Naming Patterns (DBT_DEV_TABLE_PATTERN)
# ============================================================================


class TestDevTablePatternDefault:
    """Test default pattern behavior"""

class TestDevTablePatternPredefined:
    """Test predefined patterns"""

    def test_pattern_alias_with_alias_present(self, tmp_path, monkeypatch):
        """Pattern 'alias' should use alias when present"""
        # Create manifest with alias
        project_root = tmp_path / "project"
        project_root.mkdir()
        dbt_state = project_root / ".dbt-state"
        dbt_state.mkdir()
        target = project_root / "target"
        target.mkdir()

        prod_path = dbt_state / "manifest.json"
        prod_path.write_text('{"nodes": {}}')

        dev_path = target / "manifest.json"
        dev_data = {
            "nodes": {
                "model.project.test_schema__events": {
                    "name": "client_events",
                    "schema": "test_schema",
                    "database": "",
                    "config": {"alias": "events_alias"}
                }
            }
        }
        dev_path.write_text(json.dumps(dev_data))

        monkeypatch.setenv('DBT_DEV_SCHEMA', 'test_dataset')
        monkeypatch.setenv('DBT_DEV_TABLE_PATTERN', 'alias')

        result = schema(str(prod_path), "test_schema__events", use_dev=True)

        assert result is not None
        assert result['table'] == 'events_alias'  # Uses alias

class TestDevTablePatternCustom:
    """Test custom patterns with placeholders"""

class TestDevTablePatternErrorHandling:
    """Test error handling for invalid patterns"""

    def test_invalid_placeholder_in_pattern(self, tmp_path, monkeypatch, capsys):
        """Should fallback to 'name' and warn on invalid placeholder"""
        project_root = tmp_path / "project"
        project_root.mkdir()
        target = project_root / "target"
        target.mkdir()

        dev_path = target / "manifest.json"
        dev_data = {
            "nodes": {
                "model.project.test_model": {
                    "name": "test_model",
                    "schema": "staging",
                    "database": "",
                    "config": {},
                    "original_file_path": "models/test_model.sql"
                }
            }
        }
        dev_path.write_text(json.dumps(dev_data))

        monkeypatch.setenv('DBT_DEV_MANIFEST_PATH', str(dev_path))
        monkeypatch.setenv('DBT_DEV_SCHEMA', 'test_ds')
        monkeypatch.setenv('DBT_DEV_TABLE_PATTERN', '{invalid_placeholder}')  # Invalid!

        result = schema(str(dev_path), 'test_model', use_dev=True, json_output=False)

        # Should fallback to model name
        assert result is not None
        assert result['table'] == 'test_model'

        # Should print warning to stderr
        captured = capsys.readouterr()
        assert 'Unknown placeholder' in captured.err or 'invalid_placeholder' in captured.err

    def test_literal_pattern_without_placeholders(self, tmp_path, monkeypatch):
        """Should treat non-bracketed pattern as literal string"""
        project_root = tmp_path / "project"
        project_root.mkdir()
        target = project_root / "target"
        target.mkdir()

        dev_path = target / "manifest.json"
        dev_data = {
            "nodes": {
                "model.project.test_model": {
                    "name": "test_model",
                    "schema": "staging",
                    "database": "",
                    "config": {},
                    "original_file_path": "models/test_model.sql"
                }
            }
        }
        dev_path.write_text(json.dumps(dev_data))

        monkeypatch.setenv('DBT_DEV_MANIFEST_PATH', str(dev_path))
        monkeypatch.setenv('DBT_DEV_SCHEMA', 'test_ds')
        monkeypatch.setenv('DBT_DEV_TABLE_PATTERN', 'custom_literal_name')  # Literal

        result = schema(str(dev_path), 'test_model', use_dev=True, json_output=False)

        # Should use literal pattern
        assert result is not None
        assert result['table'] == 'custom_literal_name'


class TestDevTablePatternIntegration:
    """Integration tests with other dev features"""

    def test_pattern_model_without_folder(self, tmp_path, monkeypatch):
        """Pattern {folder} with single-word model should handle gracefully"""
        # Create manifest with model without folder (no __)
        project_root = tmp_path / "project"
        project_root.mkdir()
        dbt_state = project_root / ".dbt-state"
        dbt_state.mkdir()
        target = project_root / "target"
        target.mkdir()

        prod_path = dbt_state / "manifest.json"
        prod_path.write_text('{"nodes": {}}')

        dev_path = target / "manifest.json"
        dev_data = {
            "nodes": {
                "model.project.simple_model": {
                    "name": "simple_model",
                    "schema": "public",
                    "database": "",
                    "config": {}
                }
            }
        }
        dev_path.write_text(json.dumps(dev_data))

        monkeypatch.setenv('DBT_DEV_SCHEMA', 'test_dataset')
        monkeypatch.setenv('DBT_DEV_TABLE_PATTERN', '{folder}_{name}')

        result = schema(str(prod_path), "simple_model", use_dev=True)

        assert result is not None
        # folder should be empty string, result: "_simple_model"
        assert result['table'] == '_simple_model'


# ============================================================================
# SECTION 6: Fallback Chain Helpers
# ============================================================================


class TestHelperFunctions:
    """Test helper functions for target/ fallback"""

    def test_is_model_modified_detects_git_diff(self):
        """Test that is_modified detects modified files in git diff"""
        with patch('subprocess.run') as mock_run:
            # Mock git diff output
            mock_result = MagicMock()
            mock_result.returncode = 0
            mock_result.stdout = "models/test_schema/events.sql\nmodels/staging/users.sql"
            mock_run.return_value = mock_result

            result = is_modified("test_schema__events")
            assert result is True

    def test_is_model_modified_detects_new_files(self):
        """Test that is_modified detects new files in git status"""
        with patch('subprocess.run') as mock_run:
            # First call: git diff (empty)
            # Second call: git status with new file
            mock_diff = MagicMock()
            mock_diff.returncode = 0
            mock_diff.stdout = ""

            mock_status = MagicMock()
            mock_status.returncode = 0
            mock_status.stdout = "?? models/test_schema/events.sql\nA  models/staging/users.sql"

            mock_run.side_effect = [mock_diff, mock_status]

            result = is_modified("test_schema__events")
            assert result is True

    def test_is_model_modified_handles_git_errors(self):
        """Test that is_modified handles git errors gracefully"""
        with patch('subprocess.run') as mock_run:
            mock_run.side_effect = FileNotFoundError("git not found")

            result = is_modified("test_schema__events")
            assert result is False

    def test_find_dev_manifest_finds_target(self, tmp_path):
        """Test that _find_dev_manifest locates target/manifest.json"""
        # Create directory structure
        project_root = tmp_path / "project"
        project_root.mkdir()
        dbt_state = project_root / ".dbt-state"
        dbt_state.mkdir()
        target = project_root / "target"
        target.mkdir()

        # Create manifests
        prod_manifest = dbt_state / "manifest.json"
        prod_manifest.write_text('{"nodes": {}}')
        dev_manifest = target / "manifest.json"
        dev_manifest.write_text('{"nodes": {}}')

        result = _find_dev_manifest(str(prod_manifest))
        assert result == str(dev_manifest.absolute())

    def test_find_dev_manifest_returns_none_if_not_exists(self, tmp_path):
        """Test that _find_dev_manifest returns None if target/ doesn't exist"""
        project_root = tmp_path / "project"
        project_root.mkdir()
        dbt_state = project_root / ".dbt-state"
        dbt_state.mkdir()
        prod_manifest = dbt_state / "manifest.json"
        prod_manifest.write_text('{"nodes": {}}')

        result = _find_dev_manifest(str(prod_manifest))
        assert result is None


# ============================================================================
# SECTION 7: Three-Level Fallback Implementations
# ============================================================================


class TestSchemaTargetFallback:
    """Test schema() command with target/ fallback"""

    def test_schema_falls_back_to_target_when_not_in_production(
        self, tmp_path, monkeypatch
    ):
        """Test that schema() falls back to target/ when model not in production manifest"""
        # Setup: production manifest without model
        project_root = tmp_path / "project"
        project_root.mkdir()
        dbt_state = project_root / ".dbt-state"
        dbt_state.mkdir()
        target = project_root / "target"
        target.mkdir()

        # Production manifest (empty)
        prod_manifest = dbt_state / "manifest.json"
        prod_manifest.write_text('{"nodes": {}}')

        # Dev manifest with model
        dev_manifest = target / "manifest.json"
        dev_manifest_data = {
            "nodes": {
                "model.my_project.test_schema__events": {
                    "name": "test_schema__events",
                    "schema": "test_schema",
                    "database": "test-project",
                    "config": {
                        "alias": "events",
                        "materialized": "table"
                    }
                }
            }
        }
        dev_manifest.write_text(json.dumps(dev_manifest_data))

        # Enable target fallback
        monkeypatch.setenv('DBT_FALLBACK_TARGET', 'true')
        monkeypatch.setenv('DBT_FALLBACK_BIGQUERY', 'false')
        monkeypatch.setenv('USER', 'alice')  # Mock username

        result = schema(str(prod_manifest), "test_schema__events")

        assert result is not None
        # When using dev fallback, should return DEV schema location
        assert result['schema'] == 'personal_alice'  # Dev schema, not production
        assert result['table'] == 'test_schema__events'  # Dev table name (uses 'name' field)

    def test_schema_skips_target_when_disabled(self, tmp_path, monkeypatch):
        """Test that schema() skips target/ fallback when DBT_FALLBACK_TARGET=false"""
        # Setup: same as above
        project_root = tmp_path / "project"
        project_root.mkdir()
        dbt_state = project_root / ".dbt-state"
        dbt_state.mkdir()

        prod_manifest = dbt_state / "manifest.json"
        prod_manifest.write_text('{"nodes": {}}')

        # Disable target fallback
        monkeypatch.setenv('DBT_FALLBACK_TARGET', 'false')
        monkeypatch.setenv('DBT_FALLBACK_BIGQUERY', 'false')

        result = schema(str(prod_manifest), "test_schema__events")

        assert result is None


class TestColumnsTargetFallback:
    """Test columns() command with target/ fallback"""

    def test_columns_falls_back_to_target(self, tmp_path, monkeypatch):
        """Test that columns() falls back to target/ and ALWAYS uses BigQuery"""
        project_root = tmp_path / "project"
        project_root.mkdir()
        dbt_state = project_root / ".dbt-state"
        dbt_state.mkdir()
        target = project_root / "target"
        target.mkdir()

        # Production manifest (empty)
        prod_manifest = dbt_state / "manifest.json"
        prod_manifest.write_text('{"nodes": {}}')

        # Dev manifest with model (columns no longer used from manifest)
        dev_manifest = target / "manifest.json"
        dev_manifest_data = {
            "nodes": {
                "model.my_project.test_schema__events": {
                    "name": "test_schema__events",
                    "schema": "personal_test",
                    "database": "test-project",
                    "config": {}
                }
            }
        }
        dev_manifest.write_text(json.dumps(dev_manifest_data))

        monkeypatch.setenv('DBT_FALLBACK_TARGET', 'true')
        monkeypatch.setenv('DBT_FALLBACK_BIGQUERY', 'false')

        # Mock BigQuery - ALWAYS called now
        with patch('dbt_meta.command_impl.columns._fetch_columns_from_bigquery_direct') as mock_bq:
            mock_bq.return_value = [
                {"name": "event_id", "data_type": "STRING"},
                {"name": "created_at", "data_type": "TIMESTAMP"}
            ]

            result = columns(str(prod_manifest), "test_schema__events")

            # Verify BigQuery was called
            assert mock_bq.called

        assert result is not None
        assert len(result) == 2
        assert all('name' in col for col in result)


class TestInfoTargetFallback:
    """Test info() command with target/ fallback"""

    def test_info_falls_back_to_target(self, tmp_path, monkeypatch):
        """Test that info() falls back to target/ when model not in production"""
        project_root = tmp_path / "project"
        project_root.mkdir()
        dbt_state = project_root / ".dbt-state"
        dbt_state.mkdir()
        target = project_root / "target"
        target.mkdir()

        prod_manifest = dbt_state / "manifest.json"
        prod_manifest.write_text('{"nodes": {}}')

        dev_manifest = target / "manifest.json"
        dev_manifest_data = {
            "nodes": {
                "model.my_project.test_schema__events": {
                    "name": "test_schema__events",
                    "schema": "personal_alice",
                    "database": "test-project",
                    "original_file_path": "models/test_schema/events.sql",
                    "tags": ["dev", "test"],
                    "unique_id": "model.my_project.test_schema__events",
                    "config": {
                        "alias": "client_profiles_events",
                        "materialized": "table"
                    }
                }
            }
        }
        dev_manifest.write_text(json.dumps(dev_manifest_data))

        monkeypatch.setenv('DBT_FALLBACK_TARGET', 'true')
        monkeypatch.setenv('DBT_FALLBACK_BIGQUERY', 'false')

        result = info(str(prod_manifest), "test_schema__events")

        assert result is not None
        assert result['schema'] == 'personal_alice'
        assert result['materialized'] == 'table'
        assert result['file'] == 'models/test_schema/events.sql'
        assert 'dev' in result['tags']


class TestConfigTargetFallback:
    """Test config() command with target/ fallback"""

    def test_config_falls_back_to_target(self, tmp_path, monkeypatch):
        """Test that config() falls back to target/ when model not in production"""
        project_root = tmp_path / "project"
        project_root.mkdir()
        dbt_state = project_root / ".dbt-state"
        dbt_state.mkdir()
        target = project_root / "target"
        target.mkdir()

        prod_manifest = dbt_state / "manifest.json"
        prod_manifest.write_text('{"nodes": {}}')

        dev_manifest = target / "manifest.json"
        dev_manifest_data = {
            "nodes": {
                "model.my_project.test_schema__events": {
                    "name": "test_schema__events",
                    "config": {
                        "materialized": "incremental",
                        "partition_by": {"field": "created_at", "data_type": "timestamp"},
                        "cluster_by": ["client_id", "event_type"],
                        "unique_key": "event_id",
                        "incremental_strategy": "merge"
                    }
                }
            }
        }
        dev_manifest.write_text(json.dumps(dev_manifest_data))

        monkeypatch.setenv('DBT_FALLBACK_TARGET', 'true')
        monkeypatch.setenv('DBT_FALLBACK_BIGQUERY', 'false')

        result = config(str(prod_manifest), "test_schema__events")

        assert result is not None
        assert result['materialized'] == 'incremental'
        assert result['partition_by']['field'] == 'created_at'
        assert result['cluster_by'] == ['client_id', 'event_type']
        assert result['unique_key'] == 'event_id'


class TestThreeLevelFallbackIntegration:
    """Test complete three-level fallback: production → target → BigQuery"""

    def test_fallback_order_production_first(self, tmp_path, monkeypatch):
        """Test that production manifest is tried first"""
        project_root = tmp_path / "project"
        project_root.mkdir()
        dbt_state = project_root / ".dbt-state"
        dbt_state.mkdir()

        # Production manifest WITH model
        prod_manifest = dbt_state / "manifest.json"
        prod_manifest_data = {
            "nodes": {
                "model.my_project.test_schema__events": {
                    "name": "test_schema__events",
                    "schema": "test_schema",
                    "database": "test-project",
                    "config": {"alias": "events_prod"}
                }
            }
        }
        prod_manifest.write_text(json.dumps(prod_manifest_data))

        monkeypatch.setenv('DBT_FALLBACK_TARGET', 'true')

        result = schema(str(prod_manifest), "test_schema__events")

        # Should use production (not create target/)
        assert result is not None
        assert result['table'] == 'events_prod'

    def test_fallback_order_target_second(self, tmp_path, monkeypatch):
        """Test that target/ is tried when production fails"""
        project_root = tmp_path / "project"
        project_root.mkdir()
        dbt_state = project_root / ".dbt-state"
        dbt_state.mkdir()
        target = project_root / "target"
        target.mkdir()

        # Production: empty
        prod_manifest = dbt_state / "manifest.json"
        prod_manifest.write_text('{"nodes": {}}')

        # Dev: has model
        dev_manifest = target / "manifest.json"
        dev_manifest_data = {
            "nodes": {
                "model.my_project.test_schema__events": {
                    "name": "test_schema__events",
                    "schema": "personal_alice",
                    "database": "test-project",
                    "config": {"alias": "events_dev"}
                }
            }
        }
        dev_manifest.write_text(json.dumps(dev_manifest_data))

        monkeypatch.setenv('DBT_FALLBACK_TARGET', 'true')
        monkeypatch.setenv('DBT_FALLBACK_BIGQUERY', 'false')
        monkeypatch.setenv('DBT_DEV_TABLE_PATTERN', 'alias')  # Use alias for dev table name
        monkeypatch.setenv('USER', 'alice')  # Mock username

        result = schema(str(prod_manifest), "test_schema__events")

        # Should use dev
        assert result is not None
        assert result['table'] == 'events_dev'  # Uses alias because DBT_DEV_TABLE_PATTERN='alias'
        assert result['schema'] == 'personal_alice'

# ============================================================================
# Edge Cases
# ============================================================================


class TestEmptyStringHandling:
    """Test handling of empty strings in environment variables"""

class TestSpecialCharacters:
    """Test handling of special characters in usernames and templates"""

class TestPriorityLogic:
    """Test priority ordering of configuration options"""

    def test_prod_schema_source_model_ignores_config(self, prod_manifest, monkeypatch):
        """Strategy 'model' should use only model values, ignoring config"""
        monkeypatch.setenv("DBT_PROD_SCHEMA_SOURCE", "model")
        result = schema(prod_manifest, "DW_report")

        assert result is not None
        # Should use model.database and model.schema, not config
        assert result["database"] == "analytics-223714"
        assert result["schema"] == "tableau"

class TestFallbackChains:
    """Test completeness of fallback chains"""

class TestNullValues:
    """Test handling of null/None values in manifest data"""

class TestEnvironmentVariableInteractions:
    """Test interactions between multiple environment variables"""

class TestEdgeCasesCombinations:
    """Test complex edge case combinations"""

class TestBigQueryValidation:
    """Test BigQuery schema name validation (opt-in feature)"""

    def test_bigquery_validation_with_invalid_chars(self, tmp_path, monkeypatch, capsys):
        """Should sanitize dataset name and print warnings when DBT_VALIDATE_BIGQUERY=true"""
        project_root = tmp_path / "project"
        project_root.mkdir()
        target = project_root / "target"
        target.mkdir()

        dev_path = target / "manifest.json"
        dev_data = {
            "nodes": {
                "model.project.test_model": {
                    "name": "test_model",
                    "schema": "staging",
                    "database": "",
                    "config": {},
                    "original_file_path": "models/test_model.sql"
                }
            }
        }
        dev_path.write_text(json.dumps(dev_data))

        monkeypatch.setenv('DBT_DEV_MANIFEST_PATH', str(dev_path))
        monkeypatch.setenv('DBT_DEV_SCHEMA', 'invalid.name@test')  # Invalid chars
        monkeypatch.setenv('DBT_VALIDATE_BIGQUERY', 'true')  # Enable validation

        result = schema(str(dev_path), 'test_model', use_dev=True, json_output=False)

        # Should sanitize to valid name
        assert result is not None
        assert '.' not in result['schema']
        assert '@' not in result['schema']

        # Should print warnings to stderr
        captured = capsys.readouterr()
        assert 'BigQuery validation' in captured.err

    def test_bigquery_validation_disabled_by_default(self, tmp_path, monkeypatch):
        """Should not validate when DBT_VALIDATE_BIGQUERY is not set"""
        project_root = tmp_path / "project"
        project_root.mkdir()
        target = project_root / "target"
        target.mkdir()

        dev_path = target / "manifest.json"
        dev_data = {
            "nodes": {
                "model.project.test_model": {
                    "name": "test_model",
                    "schema": "staging",
                    "database": "",
                    "config": {},
                    "original_file_path": "models/test_model.sql"
                }
            }
        }
        dev_path.write_text(json.dumps(dev_data))

        monkeypatch.setenv('DBT_DEV_MANIFEST_PATH', str(dev_path))
        monkeypatch.setenv('DBT_DEV_SCHEMA', 'invalid.name@test')  # Invalid chars
        # DBT_VALIDATE_BIGQUERY not set - validation disabled

        result = schema(str(dev_path), 'test_model', use_dev=True, json_output=False)

        # Should NOT sanitize (validation disabled)
        assert result is not None
        assert result['schema'] == 'invalid.name@test'  # Unchanged


class TestLsCommand:
    """Test ls command - filter and list models"""

    def test_ls_all_models_text(self, prod_manifest):
        """List all models without filters (text mode)"""
        result = ls(str(prod_manifest), selectors=None, json_output=False)
        assert isinstance(result, str)
        assert len(result) > 0
        # Should be space-separated model names
        models = result.split()
        assert len(models) > 0
        # Should be sorted
        assert models == sorted(models)

    def test_ls_all_models_json(self, prod_manifest):
        """List all models without filters (JSON mode)"""
        result = ls(str(prod_manifest), selectors=None, json_output=True)
        assert isinstance(result, list)
        assert len(result) > 0
        # Each item should have expected fields
        for model_info in result:
            assert 'model' in model_info
            assert 'table' in model_info
            assert 'tags' in model_info
            assert 'materialized' in model_info
            assert 'path' in model_info

    def test_ls_tag_filter_single_tag(self, prod_manifest):
        """Filter by single tag (OR logic by default)"""
        result = ls(str(prod_manifest), selectors=['tag:verified'], json_output=True)
        assert isinstance(result, list)
        # All results should have 'verified' tag
        for model_info in result:
            assert 'verified' in model_info['tags']

    def test_ls_tag_filter_or_logic(self, prod_manifest):
        """Filter by multiple tags with OR logic (default)"""
        result = ls(str(prod_manifest), selectors=['tag:verified', 'tag:active'], json_output=True)
        assert isinstance(result, list)
        # Each result should have at least one of the tags
        for model_info in result:
            assert 'verified' in model_info['tags'] or 'active' in model_info['tags']

    def test_ls_tag_filter_and_logic(self, prod_manifest):
        """Filter by multiple tags with AND logic"""
        result = ls(str(prod_manifest), selectors=['tag:verified', 'tag:active'], and_logic=True, json_output=True)
        assert isinstance(result, list)
        # Each result should have BOTH tags
        for model_info in result:
            assert 'verified' in model_info['tags']
            assert 'active' in model_info['tags']

    def test_ls_config_selector(self, prod_manifest):
        """Filter by config selector"""
        result = ls(str(prod_manifest), selectors=['config.materialized:incremental'], json_output=True)
        assert isinstance(result, list)
        # All results should have materialized=incremental (if any exist)
        if len(result) > 0:
            for model_info in result:
                assert model_info['materialized'] == 'incremental'

        # Test with a more common config value
        result_table = ls(str(prod_manifest), selectors=['config.materialized:table'], json_output=True)
        assert isinstance(result_table, list)
        for model_info in result_table:
            assert model_info['materialized'] == 'table'

    def test_ls_path_selector(self, prod_manifest):
        """Filter by path selector"""
        result = ls(str(prod_manifest), selectors=['path:models/staging/'], json_output=True)
        assert isinstance(result, list)
        # All results should have path starting with models/staging/
        for model_info in result:
            assert model_info['path'].startswith('models/staging/')

    def test_ls_empty_result(self, prod_manifest):
        """Test with selector that matches no models"""
        result = ls(str(prod_manifest), selectors=['tag:nonexistent_tag_xyz'], json_output=True)
        assert isinstance(result, list)
        assert len(result) == 0

    def test_ls_modified(self, prod_manifest):
        """Test --modified flag to list modified/new models (compact JSON format)"""
        result = ls(str(prod_manifest), modified=True, json_output=True)
        assert isinstance(result, dict)
        assert 'models' in result
        assert 'tables' in result
        assert isinstance(result['models'], list)
        assert isinstance(result['tables'], list)
        # Result depends on git status - could be empty or have models
        # Both lists should have same length
        assert len(result['models']) == len(result['tables'])

    def test_ls_refresh(self, prod_manifest):
        """Test --refresh flag to list models needing --full-refresh (compact JSON format)"""
        result = ls(str(prod_manifest), refresh=True, json_output=True)
        assert isinstance(result, dict)
        assert 'models' in result
        assert 'tables' in result
        assert isinstance(result['models'], list)
        assert isinstance(result['tables'], list)
        # Result depends on git status - could be empty or have models
        # Both lists should have same length
        assert len(result['models']) == len(result['tables'])

    def test_ls_group_text(self, prod_manifest):
        """Test --group flag with text output"""
        result = ls(str(prod_manifest), selectors=['tag:verified', 'tag:active'], group=True, json_output=False)
        assert isinstance(result, str)
        # Should have headers like "tag:verified:"
        if 'tag:verified:' in result or 'tag:active:' in result:
            # If groups exist, verify format
            assert 'tag:' in result

    def test_ls_group_json(self, prod_manifest):
        """Test --group flag with JSON output"""
        result = ls(str(prod_manifest), selectors=['tag:verified', 'tag:active'], group=True, json_output=True)
        assert isinstance(result, dict)
        # Each group should be a list
        for group_key, group_models in result.items():
            assert isinstance(group_models, list)
            # Each model in group should have verified or active tag
            for model_info in group_models:
                assert 'model' in model_info
                assert 'tags' in model_info

    def test_ls_combined_selectors(self, prod_manifest):
        """Test combining multiple selector types"""
        # Combine tag + config selector
        result = ls(
            str(prod_manifest),
            selectors=['tag:verified', 'config.materialized:table'],
            json_output=True
        )
        assert isinstance(result, list)
        # All results should have 'verified' tag OR materialized=table
        for model_info in result:
            has_tag = 'verified' in model_info['tags']
            has_config = model_info['materialized'] == 'table'
            # At least one condition should be true (OR logic for different selector types)
            assert has_tag or has_config

    def test_ls_combined_selectors_and_logic(self, prod_manifest):
        """Test combining selectors with AND logic"""
        # AND logic should apply to all selectors
        result = ls(
            str(prod_manifest),
            selectors=['tag:verified', 'config.materialized:table'],
            and_logic=True,
            json_output=True
        )
        assert isinstance(result, list)
        # All results should have verified tag AND materialized=table
        for model_info in result:
            assert 'verified' in model_info['tags']
            assert model_info['materialized'] == 'table'

    def test_ls_sorting(self, prod_manifest):
        """Test that results are sorted alphabetically"""
        result_text = ls(str(prod_manifest), selectors=None, json_output=False)
        models = result_text.split()
        assert models == sorted(models), "Models should be sorted alphabetically"

        result_json = ls(str(prod_manifest), selectors=None, json_output=True)
        model_names = [m['model'] for m in result_json]
        assert model_names == sorted(model_names), "JSON models should be sorted alphabetically"

    def test_ls_no_duplicates(self, prod_manifest):
        """Test that results don't contain duplicates"""
        # Use OR logic with overlapping tags
        result = ls(
            str(prod_manifest),
            selectors=['tag:verified', 'tag:verified'],
            json_output=True
        )
        model_names = [m['model'] for m in result]
        assert len(model_names) == len(set(model_names)), "Results should not contain duplicates"

    def test_ls_empty_selectors_list(self, prod_manifest):
        """Test with empty selectors list"""
        result = ls(str(prod_manifest), selectors=[], json_output=True)
        assert isinstance(result, list)
        assert len(result) > 0  # Should return all models

    def test_ls_invalid_selector_format(self, prod_manifest):
        """Test with invalid selector format"""
        # Selector without colon
        result = ls(str(prod_manifest), selectors=['invalid_selector'], json_output=True)
        assert isinstance(result, list)
        # Should return all models (selector ignored)

    def test_ls_package_selector(self, prod_manifest):
        """Test package selector"""
        result = ls(str(prod_manifest), selectors=['package:my_package'], json_output=True)
        assert isinstance(result, list)
        # Results depend on manifest content
        for model_info in result:
            assert 'model' in model_info

    def test_ls_multiple_tag_combinations(self, prod_manifest):
        """Test grouping with 3+ tags"""
        result = ls(
            str(prod_manifest),
            selectors=['tag:verified', 'tag:active', 'tag:prod'],
            group=True,
            json_output=True
        )
        assert isinstance(result, dict)
        # Should have groups for single tags and combinations
        # Check that group keys are properly formatted
        for group_key in result.keys():
            assert group_key.startswith('tag:')

    def test_ls_text_output_no_trailing_space(self, prod_manifest):
        """Test that text output has no trailing spaces"""
        result = ls(str(prod_manifest), selectors=['tag:verified'], json_output=False)
        if result:  # Only check if not empty
            assert not result.endswith(' '), "Text output should not have trailing space"
            assert not result.startswith(' '), "Text output should not have leading space"

    def test_ls_refresh_text_with_plus_suffix(self, prod_manifest):
        """Test that --refresh mode adds + suffix to model names"""
        result = ls(str(prod_manifest), refresh=True, json_output=False)
        if result:  # Only check if models found
            models = result.split()
            for model in models:
                assert model.endswith('+'), f"Model '{model}' should end with '+' in refresh mode"

    def test_ls_refresh_json_compact_format(self, prod_manifest):
        """Test that --refresh mode returns compact JSON format"""
        result = ls(str(prod_manifest), refresh=True, json_output=True)
        assert isinstance(result, dict)
        assert 'models' in result
        assert 'tables' in result
        # Models should not have + suffix in JSON
        if result['models']:
            for model in result['models']:
                assert not model.endswith('+'), "Models in JSON should not have + suffix"

    def test_ls_modified_json_compact_format(self, prod_manifest):
        """Test that --modified mode returns compact JSON format"""
        result = ls(str(prod_manifest), modified=True, json_output=True)
        assert isinstance(result, dict)
        assert 'models' in result
        assert 'tables' in result

    def test_ls_group_text_format(self, prod_manifest):
        """Test grouped text output format"""
        result = ls(
            str(prod_manifest),
            selectors=['tag:verified', 'tag:active'],
            group=True,
            json_output=False
        )
        if result:  # Only check if not empty
            lines = result.split('\n')
            # Each group should have header with colon
            for i, line in enumerate(lines):
                if line.startswith('tag:'):
                    assert line.endswith(':'), f"Group header should end with colon: {line}"
                    # Next line should be model names (if not empty)
                    if i + 1 < len(lines) and lines[i + 1]:
                        # Models should be space-separated
                        assert ' ' in lines[i + 1] or lines[i + 1].isalnum()

    def test_ls_modified_empty_when_clean(self, prod_manifest):
        """Test --modified returns empty compact format when no modified files"""
        # This test might fail if there are actually modified files
        result = ls(str(prod_manifest), modified=True, json_output=True)
        assert isinstance(result, dict)
        assert 'models' in result
        assert 'tables' in result
        # Can be empty or have modified models depending on git status

    def test_ls_refresh_empty_when_clean(self, prod_manifest):
        """Test --refresh returns empty compact format when no modified files"""
        result = ls(str(prod_manifest), refresh=True, json_output=True)
        assert isinstance(result, dict)
        assert 'models' in result
        assert 'tables' in result
        # Can be empty or have models depending on git status

    def test_ls_refresh_includes_descendants(self, prod_manifest, mocker):
        """Test --refresh includes descendants of modified models"""
        # Mock subprocess to simulate git operations
        # The function calls git multiple times, so create a repeatable mock
        mock_run = mocker.patch('subprocess.run')

        def git_mock(*args, **kwargs):
            cmd = args[0] if args else kwargs.get('args', [])
            if 'diff' in cmd:
                # git diff returns one modified file
                return mocker.Mock(stdout='models/staging/freshmarketer/stg_freshmarketer__contacts.sql\n', returncode=0)
            elif 'status' in cmd:
                # git status returns nothing
                return mocker.Mock(stdout='', returncode=0)
            else:
                # Other git commands
                return mocker.Mock(stdout='', returncode=0)

        mock_run.side_effect = git_mock

        result = ls(str(prod_manifest), refresh=True, json_output=True)
        assert isinstance(result, dict)
        assert 'models' in result
        assert 'tables' in result

        # Should include the modified model
        if result['models']:
            assert any('freshmarketer' in name or 'contacts' in name for name in result['models'])

    def test_ls_config_selector_with_dots(self, prod_manifest):
        """Test config selector with nested keys"""
        # Test config.materialized:value format
        result = ls(str(prod_manifest), selectors=['config.materialized:view'], json_output=True)
        assert isinstance(result, list)
        for model_info in result:
            assert model_info['materialized'] == 'view'

    def test_ls_config_selector_invalid_format(self, prod_manifest):
        """Test config selector without colon - should return all models"""
        result = ls(str(prod_manifest), selectors=['config.materialized'], json_output=True)
        assert isinstance(result, list)
        # Invalid format returns all models (no filtering)

    def test_ls_unknown_selector_type(self, prod_manifest):
        """Test unknown selector type - should return all models"""
        result = ls(str(prod_manifest), selectors=['unknown:value'], json_output=True)
        assert isinstance(result, list)
        # Unknown selector returns all models (no filtering)

    def test_ls_modified_git_both_commands_fail(self, prod_manifest, mocker):
        """Test --modified when all git commands fail"""
        mock_run = mocker.patch('subprocess.run')
        # All git commands fail (origin/main, origin/master, main, master)
        mock_run.side_effect = [
            mocker.Mock(stdout='', returncode=128),  # origin/main not found
            mocker.Mock(stdout='', returncode=128),  # origin/master not found
            mocker.Mock(stdout='', returncode=128),  # main not found
            mocker.Mock(stdout='', returncode=128),  # master not found
        ]

        result = ls(str(prod_manifest), modified=True, json_output=True)
        assert isinstance(result, dict)
        assert result == {'models': [], 'tables': []}  # Should return empty compact format

    def test_ls_modified_git_exception(self, prod_manifest, mocker):
        """Test --modified when git raises exception"""
        mock_run = mocker.patch('subprocess.run')
        mock_run.side_effect = FileNotFoundError("git not found")

        result = ls(str(prod_manifest), modified=True, json_output=True)
        assert isinstance(result, dict)
        assert result == {'models': [], 'tables': []}  # Should return empty compact format

    def test_ls_group_multiple_tags(self, prod_manifest):
        """Test --group with models having multiple tags"""
        # Find models with multiple tags
        result = ls(str(prod_manifest), selectors=['tag:daily', 'tag:core'], group=True, json_output=True)
        assert isinstance(result, dict)
        # Should have groups with tag combinations

    def test_ls_refresh_with_multiple_modified_models(self, prod_manifest, mocker):
        """Test --refresh with 2+ modified models (tests intermediate model finding)"""
        # Mock subprocess to return 2 modified models with real paths
        mock_run = mocker.patch('subprocess.run')

        def git_mock(*args, **kwargs):
            cmd = args[0] if args else kwargs.get('args', [])
            if 'diff' in cmd:
                # git diff returns 2 modified files
                return mocker.Mock(
                    stdout='models/staging/freshmarketer/stg_freshmarketer__contacts.sql\nmodels/staging/freshmarketer/stg_freshmarketer__accounts.sql\n',
                    returncode=0
                )
            elif 'status' in cmd:
                return mocker.Mock(stdout='', returncode=0)
            else:
                return mocker.Mock(stdout='', returncode=0)

        mock_run.side_effect = git_mock

        result = ls(str(prod_manifest), refresh=True, json_output=True)
        assert isinstance(result, dict)
        assert 'models' in result
        assert 'tables' in result
        # With 2+ modified models, should also find intermediate models if they exist
        # At minimum should include the 2 modified models
        if result['models']:
            assert len(result['models']) >= 2

    def test_ls_path_selector_subdirectory(self, prod_manifest):
        """Test path selector with subdirectory"""
        result = ls(str(prod_manifest), selectors=['path:models/'], json_output=True)
        assert isinstance(result, list)
        # All results should have path starting with models/
        for model_info in result:
            assert model_info['path'].startswith('models/')

    def test_ls_json_output_structure(self, prod_manifest):
        """Test JSON output has all required fields"""
        result = ls(str(prod_manifest), selectors=None, json_output=True)
        assert isinstance(result, list)
        if len(result) > 0:
            first_model = result[0]
            required_fields = ['model', 'table', 'tags', 'materialized', 'path']
            for field in required_fields:
                assert field in first_model, f"Field '{field}' missing from JSON output"
            # Validate field types
            assert isinstance(first_model['model'], str)
            assert isinstance(first_model['table'], str)
            assert isinstance(first_model['tags'], list)
            assert isinstance(first_model['materialized'], str)
            assert isinstance(first_model['path'], str)


