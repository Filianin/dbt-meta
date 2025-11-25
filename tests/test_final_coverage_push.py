"""Final coverage push - target remaining gaps to reach 95%.

Focus on uncovered lines in:
- deps.py (lines 50-54, 70)
- model_state.py (lines 138, 159-163)
- git.py (lines 188-202, 271-279, 293-296, 380, 409-413)
- columns.py, schema.py, base.py edge cases
"""

import subprocess
from unittest.mock import patch

import pytest

from dbt_meta.utils.git import GitStatus, _find_sql_file_fast
from dbt_meta.utils.model_state import ModelState, detect_model_state


class TestDepsEdgeCases:
    """Cover deps.py lines 50-54, 70."""

    def test_deps_model_not_found_returns_empty_dict(self, tmp_path, monkeypatch):
        """Test deps command returns empty dict when model not found."""
        from dbt_meta.commands import deps

        # Empty manifests
        prod_manifest = tmp_path / "manifest.json"
        prod_manifest.write_text('{"metadata": {}, "nodes": {}}')

        monkeypatch.setenv('DBT_FALLBACK_TARGET', 'false')
        monkeypatch.setenv('DBT_FALLBACK_BIGQUERY', 'false')

        result = deps(str(prod_manifest), 'nonexistent_model', use_dev=False, json_output=False)

        # Should return empty dict (refs and sources lists) for nonexistent model
        assert result == {'refs': [], 'sources': []}


class TestModelStateEdgeCases:
    """Cover model_state.py lines 138, 159-163."""

    def test_detect_modified_uncommitted(self):
        """Test MODIFIED_UNCOMMITTED state detection."""
        git_status = GitStatus(
            exists=True,
            is_tracked=True,
            is_modified=True,  # Key: file is modified
            is_committed=False,
            is_deleted=False,
            is_new=False
        )

        # Model in both manifests, file modified, NOT in deprecated folder
        state = detect_model_state(
            model_name="test_model",
            in_prod_manifest=True,
            in_dev_manifest=True,
            git_status=git_status,
            model={'config': {}},
            file_path="models/core/test_model.sql"  # NOT deprecated
        )

        # Should detect as MODIFIED_UNCOMMITTED or MODIFIED_IN_DEV
        assert state in (ModelState.MODIFIED_UNCOMMITTED, ModelState.MODIFIED_IN_DEV)

    def test_new_uncommitted_state(self):
        """Test NEW_UNCOMMITTED state detection."""
        git_status = GitStatus(
            exists=True,
            is_tracked=False,  # Untracked
            is_modified=False,
            is_committed=False,
            is_deleted=False,
            is_new=True
        )

        state = detect_model_state(
            model_name="new_model",
            in_prod_manifest=False,
            in_dev_manifest=False,  # Not in any manifest
            git_status=git_status,
            model={'config': {}},
            file_path="models/core/new_model.sql"
        )

        # Should be NEW_UNCOMMITTED
        assert state == ModelState.NEW_UNCOMMITTED


class TestGitUtilsEdgeCases:
    """Cover git.py uncovered lines."""

    def test_find_sql_file_fast_not_found(self):
        """Test _find_sql_file_fast when file not found."""
        with patch('pathlib.Path.glob', return_value=[]):
            result = _find_sql_file_fast("nonexistent_model")

            assert result is None

    def test_git_status_with_timeout_error(self):
        """Test git status handles timeout errors gracefully."""
        from dbt_meta.utils.git import get_model_git_status

        with patch('dbt_meta.utils.git._find_sql_file_fast', return_value="models/test.sql"):
            with patch('subprocess.run', side_effect=subprocess.TimeoutExpired('git', 5)):
                status = get_model_git_status("test_model")

                # Should return safe defaults
                assert status.exists is True
                assert status.is_tracked is False
                assert status.is_modified is False


class TestColumnsSchemaBaseEdgeCases:
    """Cover remaining edge cases in columns, schema, base modules."""

    def test_columns_model_not_found_returns_none(self, tmp_path, monkeypatch):
        """Test columns command returns None for nonexistent model."""
        from dbt_meta.commands import columns

        prod_manifest = tmp_path / "manifest.json"
        prod_manifest.write_text('{"metadata": {}, "nodes": {}}')

        monkeypatch.setenv('DBT_FALLBACK_TARGET', 'false')
        monkeypatch.setenv('DBT_FALLBACK_BIGQUERY', 'false')

        result = columns(str(prod_manifest), 'nonexistent', use_dev=False, json_output=False)

        assert result is None

    def test_schema_model_not_found_returns_none(self, tmp_path, monkeypatch):
        """Test schema command returns None for nonexistent model."""
        from dbt_meta.commands import schema

        prod_manifest = tmp_path / "manifest.json"
        prod_manifest.write_text('{"metadata": {}, "nodes": {}}')

        monkeypatch.setenv('DBT_FALLBACK_TARGET', 'false')
        monkeypatch.setenv('DBT_FALLBACK_BIGQUERY', 'false')

        result = schema(str(prod_manifest), 'nonexistent', use_dev=False, json_output=False)

        assert result is None


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
