"""
Tests for Dev Environments & Fallback Systems

This module consolidates tests for dev mode and fallback logic:
- --dev flag functionality: Prioritizes dev manifest over production
- Dev table naming patterns: DBT_DEV_TABLE_PATTERN with 6 placeholders
- Three-level fallback: production → target/ → BigQuery
- Git change detection: is_modified() for detecting new/changed models

Replaces old files:
- test_dev_flag.py
- test_dev_table_pattern.py
- test_target_fallback.py
- TestSchemaDevFlag class from test_commands.py
"""

import os
import pytest
import json
from pathlib import Path
from datetime import datetime
from unittest.mock import patch, MagicMock
from dbt_meta.commands import (
    schema, columns, info, config,
    is_modified, _find_dev_manifest, _build_dev_table_name
)


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
                "model.project.core_client__events": {
                    "name": "core_client__events",
                    "schema": "core_client",
                    "database": "admirals-bi-dwh",
                    "config": {"alias": "events_prod"}
                }
            }
        }
        prod_manifest.write_text(json.dumps(prod_data))

        # Dev manifest (should be used with use_dev=True)
        dev_manifest = target / "manifest.json"
        dev_data = {
            "nodes": {
                "model.project.core_client__events": {
                    "name": "events",  # filename, not alias
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

        result = schema(str(prod_manifest), "core_client__events", use_dev=True)

        assert result is not None
        assert result['schema'] == 'personal_test_user'  # Dev schema
        assert result['table'] == 'events'  # Filename, not alias
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
                "model.project.core_client__events": {
                    "name": "core_client__events",
                    "schema": "core_client",
                    "database": "admirals-bi-dwh",
                    "config": {"alias": "events"}
                }
            }
        }
        prod_manifest.write_text(json.dumps(prod_data))

        result = schema(str(prod_manifest), "core_client__events", use_dev=False)

        assert result is not None
        assert result['schema'] == 'core_client'  # Production schema
        assert result['database'] == 'admirals-bi-dwh'  # Production database

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

            result = schema(str(prod_manifest), "core_client__events", use_dev=True)

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

    def test_schema_with_dev_flag_returns_personal_schema(self, tmp_path, monkeypatch):
        """Should return personal_USERNAME schema when use_dev=True"""
        # Create manifest structure
        project_root = tmp_path / "project"
        project_root.mkdir()
        dbt_state = project_root / ".dbt-state"
        dbt_state.mkdir()
        target = project_root / "target"
        target.mkdir()

        # Production manifest (not used with use_dev=True)
        prod_manifest = dbt_state / "manifest.json"
        prod_manifest.write_text('{"nodes": {}}')

        # Dev manifest (should be used)
        dev_manifest = target / "manifest.json"
        dev_data = {
            "nodes": {
                "model.project.core_client__client_profiles_events": {
                    "name": "client_profiles_events",
                    "schema": "core_client",
                    "database": "",
                    "config": {}
                }
            }
        }
        dev_manifest.write_text(json.dumps(dev_data))

        monkeypatch.setenv('DBT_USER', 'pavel_filianin')
        result = schema(str(prod_manifest), "core_client__client_profiles_events", use_dev=True)

        assert isinstance(result, dict)
        assert result['schema'] == 'personal_pavel_filianin'
        assert 'table' in result
        assert 'full_name' in result

    def test_schema_with_dev_flag_uses_model_name_not_alias(self, tmp_path, monkeypatch):
        """Should use model name (filename), NOT alias with use_dev=True"""
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
                "model.project.core_client__client_profiles_events": {
                    "name": "client_profiles_events",  # filename, NOT alias
                    "schema": "core_client",
                    "database": "",
                    "config": {"alias": "events"}  # alias should be ignored
                }
            }
        }
        dev_manifest.write_text(json.dumps(dev_data))

        monkeypatch.setenv('DBT_USER', 'pavel_filianin')
        result = schema(str(prod_manifest), "core_client__client_profiles_events", use_dev=True)

        # Table should be model name (filename), not alias
        assert result['table'] == 'client_profiles_events'
        assert result['full_name'] == 'personal_pavel_filianin.client_profiles_events'

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
        """With use_dev=True, should get columns from dev manifest"""
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
                    "columns": {
                        "col1": {"name": "col1", "data_type": "STRING"},
                        "col2": {"name": "col2", "data_type": "INTEGER"}
                    }
                }
            }
        }
        dev_manifest.write_text(json.dumps(dev_data))

        monkeypatch.setenv('DBT_FALLBACK_BIGQUERY', 'false')

        result = columns(str(prod_manifest), "test_model", use_dev=True)

        assert result is not None
        assert len(result) == 2
        assert result[0]['name'] == 'col1'
        assert result[0]['data_type'] == 'STRING'
        assert result[1]['name'] == 'col2'
        assert result[1]['data_type'] == 'INTEGER'

    def test_columns_with_dev_falls_back_to_bigquery(self, tmp_path, monkeypatch):
        """With use_dev=True and no columns in dev, should try BigQuery"""
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

        with patch('dbt_meta.commands._fetch_columns_from_bigquery_direct') as mock_fetch:
            mock_fetch.return_value = [
                {'name': 'id', 'data_type': 'INTEGER'},
                {'name': 'name', 'data_type': 'STRING'}
            ]

            # Use proper dbt model name with __ so _infer_table_parts() works
            result = columns(str(prod_manifest), "core_client__test_model", use_dev=True)

            assert mock_fetch.called
            # Should call with dev schema
            call_args = mock_fetch.call_args[0]
            assert 'personal_test' in call_args[0]  # dev schema

    def test_columns_without_dev_uses_production(self, tmp_path):
        """Without use_dev, should use production manifest"""
        project_root = tmp_path / "project"
        project_root.mkdir()
        dbt_state = project_root / ".dbt-state"
        dbt_state.mkdir()

        prod_manifest = dbt_state / "manifest.json"
        prod_data = {
            "nodes": {
                "model.project.test_model": {
                    "name": "test_model",
                    "columns": {
                        "prod_col": {"name": "prod_col", "data_type": "STRING"}
                    }
                }
            }
        }
        prod_manifest.write_text(json.dumps(prod_data))

        result = columns(str(prod_manifest), "test_model", use_dev=False)

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

    def test_default_pattern_uses_filename(self, dev_manifest_setup, monkeypatch):
        """Default pattern should use model filename (name)"""
        monkeypatch.setenv('DBT_DEV_DATASET', 'test_dataset')
        # Don't set DBT_DEV_TABLE_PATTERN - should default to "name"

        result = schema(str(dev_manifest_setup), "core_client__client_profiles_events", use_dev=True)

        assert result is not None
        assert result['schema'] == 'test_dataset'
        # Should use filename (model.name), not alias
        assert result['table'] == 'client_profiles_events'


class TestDevTablePatternPredefined:
    """Test predefined patterns"""

    def test_pattern_name(self, dev_manifest_setup, monkeypatch):
        """Pattern 'name' should use model filename"""
        monkeypatch.setenv('DBT_DEV_DATASET', 'test_dataset')
        monkeypatch.setenv('DBT_DEV_TABLE_PATTERN', 'name')

        result = schema(str(dev_manifest_setup), "core_client__client_profiles_events", use_dev=True)

        assert result is not None
        assert result['table'] == 'client_profiles_events'

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
                "model.project.core_client__events": {
                    "name": "client_events",
                    "schema": "core_client",
                    "database": "",
                    "config": {"alias": "events_alias"}
                }
            }
        }
        dev_path.write_text(json.dumps(dev_data))

        monkeypatch.setenv('DBT_DEV_DATASET', 'test_dataset')
        monkeypatch.setenv('DBT_DEV_TABLE_PATTERN', 'alias')

        result = schema(str(prod_path), "core_client__events", use_dev=True)

        assert result is not None
        assert result['table'] == 'events_alias'  # Uses alias

    def test_pattern_alias_without_alias(self, dev_manifest_setup, monkeypatch):
        """Pattern 'alias' should fallback to name when no alias"""
        monkeypatch.setenv('DBT_DEV_DATASET', 'test_dataset')
        monkeypatch.setenv('DBT_DEV_TABLE_PATTERN', 'alias')

        result = schema(str(dev_manifest_setup), "core_client__client_profiles_events", use_dev=True)

        assert result is not None
        # No alias in fixture, should fallback to name
        assert result['table'] == 'client_profiles_events'


class TestDevTablePatternCustom:
    """Test custom patterns with placeholders"""

    def test_pattern_username_name(self, dev_manifest_setup, monkeypatch):
        """Pattern '{username}_{name}' should work"""
        monkeypatch.setenv('DBT_DEV_DATASET', 'test_dataset')
        monkeypatch.setenv('DBT_DEV_TABLE_PATTERN', '{username}_{name}')
        monkeypatch.setenv('DBT_USER', 'testuser')

        result = schema(str(dev_manifest_setup), "core_client__client_profiles_events", use_dev=True)

        assert result is not None
        assert result['table'] == 'testuser_client_profiles_events'

    def test_pattern_tmp_name(self, dev_manifest_setup, monkeypatch):
        """Pattern 'tmp_{name}' should work"""
        monkeypatch.setenv('DBT_DEV_DATASET', 'test_dataset')
        monkeypatch.setenv('DBT_DEV_TABLE_PATTERN', 'tmp_{name}')

        result = schema(str(dev_manifest_setup), "core_client__client_profiles_events", use_dev=True)

        assert result is not None
        assert result['table'] == 'tmp_client_profiles_events'

    def test_pattern_folder_name(self, dev_manifest_setup, monkeypatch):
        """Pattern '{folder}_{name}' should use model folder"""
        monkeypatch.setenv('DBT_DEV_DATASET', 'test_dataset')
        monkeypatch.setenv('DBT_DEV_TABLE_PATTERN', '{folder}_{name}')

        result = schema(str(dev_manifest_setup), "core_client__client_profiles_events", use_dev=True)

        assert result is not None
        # folder = "core_client" from "core_client__client_profiles_events"
        assert result['table'] == 'core_client_client_profiles_events'

    def test_pattern_model_name(self, dev_manifest_setup, monkeypatch):
        """Pattern '{model_name}' should use full model name"""
        monkeypatch.setenv('DBT_DEV_DATASET', 'test_dataset')
        monkeypatch.setenv('DBT_DEV_TABLE_PATTERN', '{model_name}')

        result = schema(str(dev_manifest_setup), "core_client__client_profiles_events", use_dev=True)

        assert result is not None
        # model_name includes '__'
        assert result['table'] == 'core_client__client_profiles_events'

    def test_pattern_name_date(self, dev_manifest_setup, monkeypatch):
        """Pattern '{name}_{date}' should include current date"""
        monkeypatch.setenv('DBT_DEV_DATASET', 'test_dataset')
        monkeypatch.setenv('DBT_DEV_TABLE_PATTERN', '{name}_{date}')

        result = schema(str(dev_manifest_setup), "core_client__client_profiles_events", use_dev=True)

        assert result is not None
        # Check format: name_YYYYMMDD
        today = datetime.now().strftime('%Y%m%d')
        assert result['table'] == f'client_profiles_events_{today}'


class TestDevTablePatternErrorHandling:
    """Test error handling for invalid patterns"""

    def test_invalid_placeholder_fallback_to_name(self, dev_manifest_setup, monkeypatch, capsys):
        """Invalid placeholder should warn and fallback to name"""
        monkeypatch.setenv('DBT_DEV_DATASET', 'test_dataset')
        monkeypatch.setenv('DBT_DEV_TABLE_PATTERN', '{invalid_placeholder}_{name}')

        result = schema(str(dev_manifest_setup), "core_client__client_profiles_events", use_dev=True)

        assert result is not None
        # Should fallback to name
        assert result['table'] == 'client_profiles_events'

        # Should print warning
        captured = capsys.readouterr()
        assert 'Unknown placeholder' in captured.err
        assert 'invalid_placeholder' in captured.err

    def test_literal_string_pattern(self, dev_manifest_setup, monkeypatch):
        """Literal string (no placeholders) should be used as-is"""
        monkeypatch.setenv('DBT_DEV_DATASET', 'test_dataset')
        monkeypatch.setenv('DBT_DEV_TABLE_PATTERN', 'custom_literal_table')

        result = schema(str(dev_manifest_setup), "core_client__client_profiles_events", use_dev=True)

        assert result is not None
        assert result['table'] == 'custom_literal_table'


class TestDevTablePatternIntegration:
    """Integration tests with other dev features"""

    def test_pattern_works_with_info_command(self, dev_manifest_setup, monkeypatch):
        """Pattern should work with info command"""
        monkeypatch.setenv('DBT_DEV_DATASET', 'test_dataset')
        monkeypatch.setenv('DBT_DEV_TABLE_PATTERN', '{username}_{name}')
        monkeypatch.setenv('DBT_USER', 'testuser')

        result = info(str(dev_manifest_setup), "core_client__client_profiles_events", use_dev=True)

        assert result is not None
        assert result['schema'] == 'test_dataset'
        assert result['table'] == 'testuser_client_profiles_events'

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

        monkeypatch.setenv('DBT_DEV_DATASET', 'test_dataset')
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
            mock_result.stdout = "models/core_client/events.sql\nmodels/staging/users.sql"
            mock_run.return_value = mock_result

            result = is_modified("core_client__events")
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
            mock_status.stdout = "?? models/core_client/events.sql\nA  models/staging/users.sql"

            mock_run.side_effect = [mock_diff, mock_status]

            result = is_modified("core_client__events")
            assert result is True

    def test_is_model_modified_handles_git_errors(self):
        """Test that is_modified handles git errors gracefully"""
        with patch('subprocess.run') as mock_run:
            mock_run.side_effect = FileNotFoundError("git not found")

            result = is_modified("core_client__events")
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
                "model.my_project.core_client__events": {
                    "name": "core_client__events",
                    "schema": "core_client",
                    "database": "admirals-bi-dwh",
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

        result = schema(str(prod_manifest), "core_client__events")

        assert result is not None
        assert result['schema'] == 'core_client'
        assert result['table'] == 'events'

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

        result = schema(str(prod_manifest), "core_client__events")

        assert result is None


class TestColumnsTargetFallback:
    """Test columns() command with target/ fallback"""

    def test_columns_falls_back_to_target(self, tmp_path, monkeypatch):
        """Test that columns() falls back to target/ when model not in production"""
        project_root = tmp_path / "project"
        project_root.mkdir()
        dbt_state = project_root / ".dbt-state"
        dbt_state.mkdir()
        target = project_root / "target"
        target.mkdir()

        # Production manifest (empty)
        prod_manifest = dbt_state / "manifest.json"
        prod_manifest.write_text('{"nodes": {}}')

        # Dev manifest with model and columns
        dev_manifest = target / "manifest.json"
        dev_manifest_data = {
            "nodes": {
                "model.my_project.core_client__events": {
                    "name": "core_client__events",
                    "columns": {
                        "event_id": {"name": "event_id", "data_type": "STRING"},
                        "created_at": {"name": "created_at", "data_type": "TIMESTAMP"}
                    }
                }
            }
        }
        dev_manifest.write_text(json.dumps(dev_manifest_data))

        monkeypatch.setenv('DBT_FALLBACK_TARGET', 'true')
        monkeypatch.setenv('DBT_FALLBACK_BIGQUERY', 'false')

        result = columns(str(prod_manifest), "core_client__events")

        assert result is not None
        assert len(result) == 2
        assert result[0]['name'] == 'event_id'
        assert result[1]['name'] == 'created_at'


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
                "model.my_project.core_client__events": {
                    "name": "core_client__events",
                    "schema": "personal_pavel_filianin",
                    "database": "admirals-bi-dwh",
                    "original_file_path": "models/core_client/events.sql",
                    "tags": ["dev", "test"],
                    "unique_id": "model.my_project.core_client__events",
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

        result = info(str(prod_manifest), "core_client__events")

        assert result is not None
        assert result['schema'] == 'personal_pavel_filianin'
        assert result['materialized'] == 'table'
        assert result['file'] == 'models/core_client/events.sql'
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
                "model.my_project.core_client__events": {
                    "name": "core_client__events",
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

        result = config(str(prod_manifest), "core_client__events")

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
                "model.my_project.core_client__events": {
                    "name": "core_client__events",
                    "schema": "core_client",
                    "database": "admirals-bi-dwh",
                    "config": {"alias": "events_prod"}
                }
            }
        }
        prod_manifest.write_text(json.dumps(prod_manifest_data))

        monkeypatch.setenv('DBT_FALLBACK_TARGET', 'true')

        result = schema(str(prod_manifest), "core_client__events")

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
                "model.my_project.core_client__events": {
                    "name": "core_client__events",
                    "schema": "personal_pavel_filianin",
                    "database": "admirals-bi-dwh",
                    "config": {"alias": "events_dev"}
                }
            }
        }
        dev_manifest.write_text(json.dumps(dev_manifest_data))

        monkeypatch.setenv('DBT_FALLBACK_TARGET', 'true')
        monkeypatch.setenv('DBT_FALLBACK_BIGQUERY', 'false')

        result = schema(str(prod_manifest), "core_client__events")

        # Should use dev
        assert result is not None
        assert result['table'] == 'events_dev'
        assert result['schema'] == 'personal_pavel_filianin'
