"""Additional tests to improve coverage for utils modules.

Focus on uncovered code paths in:
- model_state.py: state detection edge cases
- bigquery.py: retry logic and error handling
- git.py: git operations edge cases
"""


import pytest

from dbt_meta.utils.bigquery import _should_retry, sanitize_bigquery_name
from dbt_meta.utils.git import GitStatus, validate_path
from dbt_meta.utils.model_state import ModelState, detect_model_state


class TestModelStateCoverage:
    """Tests for model_state.py uncovered lines (93, 100, 105-110, 138, 159-163)."""

    def test_deprecated_disabled_state(self):
        """Test DEPRECATED_DISABLED state detection."""
        git_status = GitStatus(
            exists=True,
            is_tracked=True,
            is_modified=False,
            is_committed=True,
            is_deleted=False,
            is_new=False
        )

        # Model with enabled=false
        model = {
            'config': {
                'enabled': False
            }
        }

        state = detect_model_state(
            model_name="test_model",
            in_prod_manifest=True,
            in_dev_manifest=True,
            git_status=git_status,
            model=model,
            file_path="models/test/test_model.sql"
        )

        assert state == ModelState.DEPRECATED_DISABLED

    def test_deprecated_folder_state(self):
        """Test DEPRECATED_FOLDER state detection."""
        git_status = GitStatus(
            exists=True,
            is_tracked=True,
            is_modified=False,
            is_committed=True,
            is_deleted=False,
            is_new=False
        )

        state = detect_model_state(
            model_name="test_model",
            in_prod_manifest=True,
            in_dev_manifest=True,
            git_status=git_status,
            model={'config': {}},
            file_path="models/deprecated/test_model.sql"
        )

        assert state == ModelState.DEPRECATED_FOLDER

    def test_renamed_new_state(self):
        """Test RENAMED_NEW state detection."""
        git_status = GitStatus(
            exists=True,
            is_tracked=True,
            is_modified=False,
            is_committed=False,
            is_deleted=False,
            is_new=False,
            is_renamed=True,
            renamed_from="models/old/old_model.sql",
            renamed_to="models/new/new_model.sql"
        )

        state = detect_model_state(
            model_name="new_model",
            in_prod_manifest=False,
            in_dev_manifest=True,
            git_status=git_status,
            model={'config': {}},
            file_path="models/new/new_model.sql"
        )

        assert state == ModelState.RENAMED_NEW

    def test_renamed_old_state(self):
        """Test RENAMED_OLD state detection."""
        git_status = GitStatus(
            exists=False,
            is_tracked=True,
            is_modified=False,
            is_committed=True,
            is_deleted=True,
            is_new=False,
            is_renamed=True,
            renamed_from="models/old/old_model.sql",
            renamed_to="models/new/new_model.sql"
        )

        state = detect_model_state(
            model_name="old_model",
            in_prod_manifest=True,
            in_dev_manifest=False,
            git_status=git_status,
            model={'config': {}},
            file_path="models/old/old_model.sql"
        )

        assert state == ModelState.RENAMED_OLD


class TestBigQueryCoverage:
    """Tests for bigquery.py uncovered lines."""

    def test_should_retry_with_exponential_backoff(self):
        """Test _should_retry helper function with exponential backoff."""
        import time

        # First attempt (0) should retry with 1 second wait (2^0)
        start = time.time()
        result = _should_retry(0, 3, "Test error")
        elapsed = time.time() - start

        assert result is True  # Should retry
        assert elapsed >= 1.0  # Should have waited 1 second

    def test_should_retry_final_attempt(self):
        """Test _should_retry on final attempt doesn't retry."""
        result = _should_retry(2, 3, "Test error")  # Last attempt (attempt 2 of max 3)

        assert result is False  # Should NOT retry on final attempt

    def test_sanitize_bigquery_name_with_dots(self):
        """Test sanitize_bigquery_name with dots in name."""
        name, warnings = sanitize_bigquery_name("schema.table.name", "dataset")

        assert '.' not in name  # Dots should be replaced
        assert '_' in name
        assert len(warnings) > 0  # Should warn about invalid characters

    def test_sanitize_bigquery_name_with_at_symbol(self):
        """Test sanitize_bigquery_name with @ symbol."""
        name, warnings = sanitize_bigquery_name("user@domain", "dataset")

        assert '@' not in name  # @ should be replaced
        assert '_' in name
        assert len(warnings) > 0


class TestGitCoverage:
    """Tests for git.py uncovered lines."""

    def test_validate_path_with_parent_directory_attack(self):
        """Test validate_path rejects ../.. attacks."""
        with pytest.raises(ValueError) as exc_info:
            validate_path("../../etc/passwd")

        assert "Unsafe path" in str(exc_info.value) or "traversal" in str(exc_info.value)

    def test_validate_path_with_shell_metacharacters(self):
        """Test validate_path rejects shell metacharacters."""
        with pytest.raises(ValueError) as exc_info:
            validate_path("file; rm -rf /")

        assert "Unsafe" in str(exc_info.value) or "dangerous" in str(exc_info.value)

    def test_validate_path_accepts_safe_paths(self):
        """Test validate_path accepts safe paths."""
        safe_paths = ["models/core/clients.sql", "tests/test_file.py", "README.md"]

        for path in safe_paths:
            result = validate_path(path)
            assert result == path  # Should return unchanged


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
