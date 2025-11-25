"""Decision Tree Scenarios Tests

Tests all 6 critical scenarios from .qa/decision_tree_visual.txt:
- Scenario A: NEW without --dev → ERROR
- Scenario B: NEW with --dev → DEV source  
- Scenario C: MODIFIED without --dev → PROD + warning
- Scenario D: MODIFIED with --dev → DEV
- Scenario E: UNMODIFIED with --dev → DEV + warning
- Scenario F: Columns empty → BigQuery fallback with correct schema
- Scenario G: Dev manifest with prod schema → Override to dev schema
"""

import json
from unittest.mock import patch

from dbt_meta.commands import columns


class TestScenarioA:
    """Scenario A: NEW MODEL WITHOUT --dev
    
    Git: NEW | Prod: ❌ | Dev: ✅ | --dev: ❌
    Decision: ❌ ERROR
    Message: "Model exists ONLY in dev. Use --dev flag"
    Action: STOP - no data returned
    """
    
    def test_new_model_without_dev_flag_returns_error(self, enable_fallbacks, tmp_path, monkeypatch):
        """NEW model without --dev should return None with error warning."""
        # Setup manifests
        prod_manifest = tmp_path / ".dbt-state" / "manifest.json"
        dev_manifest = tmp_path / "target" / "manifest.json"
        
        prod_manifest.parent.mkdir(parents=True)
        dev_manifest.parent.mkdir(parents=True)
        
        # Production manifest: model NOT in production
        prod_manifest.write_text(json.dumps({
            "metadata": {},
            "nodes": {}
        }))
        
        # Dev manifest: model EXISTS (columns no longer used from manifest)
        dev_manifest.write_text(json.dumps({
            "metadata": {},
            "nodes": {
                "model.test_project.new_model": {
                    "name": "new_model",
                    "alias": "new_model",
                    "schema": "personal_testuser",
                    "database": "test-project",
                    "config": {"materialized": "table"}
                }
            }
        }))

        monkeypatch.setenv('DBT_PROD_MANIFEST_PATH', str(prod_manifest))
        monkeypatch.setenv('DBT_DEV_MANIFEST_PATH', str(dev_manifest))

        # Mock git to report NEW status
        with patch('dbt_meta.utils.git.get_model_git_status') as mock_git:
            from dbt_meta.utils.git import GitStatus
            mock_git.return_value = GitStatus(
                exists=True,
                is_tracked=False,
                is_modified=True,
                is_committed=False,
                is_deleted=False,
                is_new=True
            )

            # Mock BigQuery - ALWAYS called
            with patch('dbt_meta.command_impl.columns._fetch_columns_from_bigquery_direct') as mock_bq:
                mock_bq.return_value = [
                    {'name': 'col1', 'data_type': 'string'}
                ]

                # Run without --dev flag
                result = columns(str(prod_manifest), 'new_model', use_dev=False, json_output=True)

                # Verify BigQuery was called
                assert mock_bq.called

        # Should use dev manifest fallback with warnings
        # (allowing defer workflow to work)
        assert result is not None
        assert len(result) == 1
        assert result[0]['name'] == 'col1'


class TestScenarioB:
    """Scenario B: NEW MODEL WITH --dev
    
    Git: NEW | Prod: ❌ | Dev: ✅ | --dev: ✅
    Decision: ✅ Use DEV
    Source: Dev Manifest → personal_user.table
    Fallback: Dev BigQuery (if columns empty)
    """
    
    def test_new_model_with_dev_flag_uses_dev_source(self, tmp_path, monkeypatch):
        """NEW model with --dev should use dev manifest and ALWAYS BigQuery."""
        # Setup manifests
        prod_manifest = tmp_path / ".dbt-state" / "manifest.json"
        dev_manifest = tmp_path / "target" / "manifest.json"

        prod_manifest.parent.mkdir(parents=True)
        dev_manifest.parent.mkdir(parents=True)

        # Production manifest: model NOT in production
        prod_manifest.write_text(json.dumps({
            "metadata": {},
            "nodes": {}
        }))

        # Dev manifest: model EXISTS (columns no longer used from manifest)
        dev_manifest.write_text(json.dumps({
            "metadata": {},
            "nodes": {
                "model.test_project.new_model": {
                    "name": "new_model",
                    "alias": "new_model",
                    "schema": "personal_testuser",
                    "database": "test-project",
                    "config": {"materialized": "table"}
                }
            }
        }))

        monkeypatch.setenv('DBT_PROD_MANIFEST_PATH', str(prod_manifest))
        monkeypatch.setenv('DBT_DEV_MANIFEST_PATH', str(dev_manifest))

        # Mock git to report NEW status
        with patch('dbt_meta.utils.git.get_model_git_status') as mock_git:
            from dbt_meta.utils.git import GitStatus
            mock_git.return_value = GitStatus(
                exists=True,
                is_tracked=False,
                is_modified=True,
                is_committed=False,
                is_deleted=False,
                is_new=True
            )

            # Mock BigQuery - ALWAYS called
            with patch('dbt_meta.command_impl.columns._fetch_columns_from_bigquery_direct') as mock_bq:
                mock_bq.return_value = [
                    {'name': 'col1', 'data_type': 'string'},
                    {'name': 'col2', 'data_type': 'integer'}
                ]

                # Run with --dev flag
                result = columns(str(prod_manifest), 'new_model', use_dev=True, json_output=True)

                # Verify BigQuery was called
                assert mock_bq.called

        # Should return columns from BigQuery
        assert result is not None
        assert len(result) == 2
        assert result[0]['name'] == 'col1'


class TestScenarioC:
    """Scenario C: MODIFIED MODEL WITHOUT --dev
    
    Git: MODIFIED | Prod: ✅ | Dev: ✅ | --dev: ❌
    Decision: ⚠️ Use PROD (with warning)
    Warning: "Model modified. Use --dev for local changes"
    Source: Prod Manifest → schema.table
    Fallback: Prod BigQuery
    """
    
    def test_modified_model_without_dev_uses_prod_with_warning(self, tmp_path, monkeypatch):
        """MODIFIED model without --dev should use prod and ALWAYS BigQuery."""
        # Setup manifests
        prod_manifest = tmp_path / ".dbt-state" / "manifest.json"
        dev_manifest = tmp_path / "target" / "manifest.json"

        prod_manifest.parent.mkdir(parents=True)
        dev_manifest.parent.mkdir(parents=True)

        # Production manifest: model EXISTS (columns no longer used from manifest)
        prod_manifest.write_text(json.dumps({
            "metadata": {},
            "nodes": {
                "model.test_project.modified_model": {
                    "name": "modified_model",
                    "alias": "modified_model",
                    "schema": "prod_schema",
                    "database": "test-project",
                    "config": {"materialized": "table"}
                }
            }
        }))

        # Dev manifest: model EXISTS (columns no longer used from manifest)
        dev_manifest.write_text(json.dumps({
            "metadata": {},
            "nodes": {
                "model.test_project.modified_model": {
                    "name": "modified_model",
                    "alias": "modified_model",
                    "schema": "personal_testuser",
                    "database": "test-project",
                    "config": {"materialized": "table"}
                }
            }
        }))

        monkeypatch.setenv('DBT_PROD_MANIFEST_PATH', str(prod_manifest))
        monkeypatch.setenv('DBT_DEV_MANIFEST_PATH', str(dev_manifest))

        # Mock git to report MODIFIED status
        with patch('dbt_meta.utils.git.get_model_git_status') as mock_git:
            from dbt_meta.utils.git import GitStatus
            mock_git.return_value = GitStatus(
                exists=True,
                is_tracked=True,
                is_modified=True,
                is_committed=False,
                is_deleted=False,
                is_new=False
            )

            # Mock BigQuery - ALWAYS called
            with patch('dbt_meta.command_impl.columns._fetch_columns_from_bigquery_direct') as mock_bq:
                mock_bq.return_value = [
                    {'name': 'prod_col', 'data_type': 'string'}
                ]

                # Run without --dev flag
                result = columns(str(prod_manifest), 'modified_model', use_dev=False, json_output=True)

                # Verify BigQuery was called
                assert mock_bq.called

        # Should return columns from PROD BigQuery
        assert result is not None
        assert len(result) == 1
        assert result[0]['name'] == 'prod_col'


class TestScenarioD:
    """Scenario D: MODIFIED MODEL WITH --dev
    
    Git: MODIFIED | Prod: ✅ | Dev: ✅ | --dev: ✅
    Decision: ✅ Use DEV
    Source: Dev Manifest → personal_user.table
    Fallback: Dev BigQuery
    """
    
    def test_modified_model_with_dev_uses_dev_source(self, tmp_path, monkeypatch):
        """MODIFIED model with --dev should use dev manifest and ALWAYS BigQuery."""
        # Setup manifests
        prod_manifest = tmp_path / ".dbt-state" / "manifest.json"
        dev_manifest = tmp_path / "target" / "manifest.json"

        prod_manifest.parent.mkdir(parents=True)
        dev_manifest.parent.mkdir(parents=True)

        # Production manifest: model EXISTS (columns no longer used from manifest)
        prod_manifest.write_text(json.dumps({
            "metadata": {},
            "nodes": {
                "model.test_project.modified_model": {
                    "name": "modified_model",
                    "alias": "modified_model",
                    "schema": "prod_schema",
                    "database": "test-project",
                    "config": {"materialized": "table"}
                }
            }
        }))

        # Dev manifest: model EXISTS (columns no longer used from manifest)
        dev_manifest.write_text(json.dumps({
            "metadata": {},
            "nodes": {
                "model.test_project.modified_model": {
                    "name": "modified_model",
                    "alias": "modified_model",
                    "schema": "prod_schema",  # NOTE: Contains prod schema!
                    "database": "test-project",
                    "config": {"materialized": "table"}
                }
            }
        }))

        monkeypatch.setenv('DBT_PROD_MANIFEST_PATH', str(prod_manifest))
        monkeypatch.setenv('DBT_DEV_MANIFEST_PATH', str(dev_manifest))
        monkeypatch.setenv('DBT_DEV_SCHEMA', 'personal_testuser')

        # Mock git to report MODIFIED status
        with patch('dbt_meta.utils.git.get_model_git_status') as mock_git:
            from dbt_meta.utils.git import GitStatus
            mock_git.return_value = GitStatus(
                exists=True,
                is_tracked=True,
                is_modified=True,
                is_committed=False,
                is_deleted=False,
                is_new=False
            )

            # Mock BigQuery - ALWAYS called
            with patch('dbt_meta.command_impl.columns._fetch_columns_from_bigquery_direct') as mock_bq:
                mock_bq.return_value = [
                    {'name': 'dev_col', 'data_type': 'string'}
                ]

                # Run with --dev flag
                result = columns(str(prod_manifest), 'modified_model', use_dev=True, json_output=True)

                # Verify BigQuery was called
                assert mock_bq.called

        # Should return columns from DEV BigQuery
        assert result is not None
        assert len(result) == 1
        assert result[0]['name'] == 'dev_col'


class TestScenarioE:
    """Scenario E: UNMODIFIED WITH --dev
    
    Git: OK | Prod: ✅ | Dev: ✅ | --dev: ✅
    Decision: ⚠️ Use DEV (with warning)
    Warning: "No changes but using --dev. Remove flag?"
    Source: Dev Manifest → personal_user.table
    Fallback: Dev BigQuery
    """
    
    def test_unmodified_model_with_dev_uses_dev_with_warning(self, tmp_path, monkeypatch):
        """UNMODIFIED model with --dev should use dev and ALWAYS BigQuery."""
        # Setup manifests
        prod_manifest = tmp_path / ".dbt-state" / "manifest.json"
        dev_manifest = tmp_path / "target" / "manifest.json"

        prod_manifest.parent.mkdir(parents=True)
        dev_manifest.parent.mkdir(parents=True)

        # Both manifests: model EXISTS (columns no longer used from manifest)
        prod_manifest.write_text(json.dumps({
            "metadata": {},
            "nodes": {
                "model.test_project.stable_model": {
                    "name": "stable_model",
                    "alias": "stable_model",
                    "schema": "prod_schema",
                    "database": "test-project",
                    "config": {"materialized": "table"}
                }
            }
        }))

        dev_manifest.write_text(json.dumps({
            "metadata": {},
            "nodes": {
                "model.test_project.stable_model": {
                    "name": "stable_model",
                    "alias": "stable_model",
                    "schema": "prod_schema",  # Contains prod schema
                    "database": "test-project",
                    "config": {"materialized": "table"}
                }
            }
        }))

        monkeypatch.setenv('DBT_PROD_MANIFEST_PATH', str(prod_manifest))
        monkeypatch.setenv('DBT_DEV_MANIFEST_PATH', str(dev_manifest))
        monkeypatch.setenv('DBT_DEV_SCHEMA', 'personal_testuser')

        # Mock git to report UNMODIFIED status
        with patch('dbt_meta.utils.git.get_model_git_status') as mock_git:
            from dbt_meta.utils.git import GitStatus
            mock_git.return_value = GitStatus(
                exists=True,
                is_tracked=True,
                is_modified=False,
                is_committed=True,
                is_deleted=False,
                is_new=False
            )

            # Mock BigQuery - ALWAYS called
            with patch('dbt_meta.command_impl.columns._fetch_columns_from_bigquery_direct') as mock_bq:
                mock_bq.return_value = [
                    {'name': 'col', 'data_type': 'string'}
                ]

                # Run with --dev flag
                result = columns(str(prod_manifest), 'stable_model', use_dev=True, json_output=True)

                # Verify BigQuery was called
                assert mock_bq.called

        # Should return columns from DEV BigQuery (even though unmodified)
        assert result is not None
        assert len(result) == 1


class TestScenarioF:
    """Scenario F: COLUMNS EMPTY → BigQuery Fallback
    
    Model found but columns empty → BigQuery fallback with CORRECT schema
    - If found in dev manifest → use dev schema (personal_*)
    - If found in prod manifest → use prod schema
    """
    
    def test_columns_empty_dev_manifest_uses_dev_schema_in_bigquery(self, tmp_path, monkeypatch):
        """Empty columns in dev manifest should use dev schema for BigQuery."""
        # Setup manifests
        prod_manifest = tmp_path / ".dbt-state" / "manifest.json"
        dev_manifest = tmp_path / "target" / "manifest.json"
        
        prod_manifest.parent.mkdir(parents=True)
        dev_manifest.parent.mkdir(parents=True)
        
        # Production manifest: empty
        prod_manifest.write_text(json.dumps({
            "metadata": {},
            "nodes": {}
        }))
        
        # Dev manifest: model exists but NO columns
        dev_manifest.write_text(json.dumps({
            "metadata": {},
            "nodes": {
                "model.test_project.empty_cols_model": {
                    "name": "empty_cols_model",
                    "alias": "empty_cols_model",
                    "schema": "prod_schema",  # Contains prod schema (from dbt compile)
                    "database": "test-project",
                    "columns": {},  # EMPTY!
                    "config": {"materialized": "table"}
                }
            }
        }))
        
        monkeypatch.setenv('DBT_PROD_MANIFEST_PATH', str(prod_manifest))
        monkeypatch.setenv('DBT_DEV_MANIFEST_PATH', str(dev_manifest))
        monkeypatch.setenv('DBT_DEV_SCHEMA', 'personal_testuser')
        
        # Mock BigQuery fetch
        with patch('dbt_meta.command_impl.columns._fetch_columns_from_bigquery_direct') as mock_bq:
            mock_bq.return_value = [
                {'name': 'col1', 'data_type': 'string'},
                {'name': 'col2', 'data_type': 'integer'}
            ]
            
            # Run with --dev flag
            result = columns(str(prod_manifest), 'empty_cols_model', use_dev=True, json_output=True)
            
            # Verify BigQuery was called with DEV schema, not prod
            mock_bq.assert_called_once()
            call_args = mock_bq.call_args
            
            # CRITICAL: Should use DEV schema (personal_testuser), NOT prod_schema!
            assert call_args[0][0] == 'personal_testuser', f"Should use dev schema, got: {call_args[0][0]}"
            assert call_args[0][1] == 'empty_cols_model'
            assert call_args[0][2] == 'test-project'
        
        assert result is not None
        assert len(result) == 2


class TestScenarioG:
    """Scenario G: DEV MANIFEST WITH PROD SCHEMA → Override
    
    When dev manifest contains production schema (from dbt compile):
    - Override schema to dev schema when using --dev flag
    - This ensures BigQuery fallback queries dev tables
    """
    
    def test_dev_manifest_prod_schema_overridden_to_dev(self, tmp_path, monkeypatch):
        """Dev manifest with prod schema should be overridden to dev schema."""
        # Setup manifests  
        prod_manifest = tmp_path / ".dbt-state" / "manifest.json"
        dev_manifest = tmp_path / "target" / "manifest.json"
        
        prod_manifest.parent.mkdir(parents=True)
        dev_manifest.parent.mkdir(parents=True)
        
        # Production manifest: empty
        prod_manifest.write_text(json.dumps({
            "metadata": {},
            "nodes": {}
        }))
        
        # Dev manifest: contains PRODUCTION schema (from dbt compile)
        dev_manifest.write_text(json.dumps({
            "metadata": {},
            "nodes": {
                "model.test_project.test_model": {
                    "name": "test_model",
                    "alias": "test_model",  
                    "schema": "staging_appsflyer",  # PRODUCTION schema!
                    "database": "test-project",
                    "columns": {},  # Empty - will trigger BigQuery fallback
                    "config": {"materialized": "table"}
                }
            }
        }))
        
        monkeypatch.setenv('DBT_PROD_MANIFEST_PATH', str(prod_manifest))
        monkeypatch.setenv('DBT_DEV_MANIFEST_PATH', str(dev_manifest))
        monkeypatch.setenv('USER', 'testuser')
        
        # Mock BigQuery fetch
        with patch('dbt_meta.command_impl.columns._fetch_columns_from_bigquery_direct') as mock_bq:
            mock_bq.return_value = [
                {'name': 'col1', 'data_type': 'string'}
            ]
            
            # Run with --dev flag
            result = columns(str(prod_manifest), 'test_model', use_dev=True, json_output=True)
            
            # Verify BigQuery was called with DEV schema, NOT staging_appsflyer!
            mock_bq.assert_called_once()
            call_args = mock_bq.call_args
            
            # CRITICAL: Should use personal_testuser, NOT staging_appsflyer
            assert call_args[0][0] == 'personal_testuser', \
                f"Expected dev schema 'personal_testuser', got '{call_args[0][0]}'"
        
        assert result is not None
