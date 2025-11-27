"""Tests for exception hierarchy in dbt_meta.errors module.

Consolidated from test_exception_handling.py
"""

import subprocess
from unittest.mock import MagicMock, patch

import pytest

from dbt_meta.errors import (
    BigQueryError,
    ConfigurationError,
    DbtMetaError,
    GitOperationError,
    ManifestNotFoundError,
    ManifestParseError,
    ModelNotFoundError,
)


class TestDbtMetaError:
    """Test base exception class."""

    def test_message_only(self):
        """Test exception with message only."""
        error = DbtMetaError("Something went wrong")
        assert error.message == "Something went wrong"
        assert error.suggestion is None
        assert str(error) == "Something went wrong"

    def test_message_with_suggestion(self):
        """Test exception with message and suggestion."""
        error = DbtMetaError("Something went wrong", "Try this fix")
        assert error.message == "Something went wrong"
        assert error.suggestion == "Try this fix"
        assert "Suggestion: Try this fix" in str(error)


class TestModelNotFoundError:
    """Test ModelNotFoundError."""

    def test_basic_model_not_found(self):
        """Test model not found with single location."""
        error = ModelNotFoundError("core__clients", ["production manifest"])

        assert error.model_name == "core__clients"
        assert error.searched_locations == ["production manifest"]
        assert "Model 'core__clients' not found" in error.message
        assert "production manifest" in error.suggestion

    def test_multiple_locations(self):
        """Test model not found with multiple search locations."""
        error = ModelNotFoundError(
            "staging__orders",
            ["production manifest", "dev manifest", "BigQuery"]
        )

        assert error.model_name == "staging__orders"
        assert len(error.searched_locations) == 3
        assert "production manifest" in error.suggestion
        assert "dev manifest" in error.suggestion
        assert "BigQuery" in error.suggestion

    def test_suggestion_includes_list_command(self):
        """Test that suggestion includes meta list command with schema prefix."""
        error = ModelNotFoundError("core_client__events", ["production manifest"])

        # Should suggest: meta list core_client
        assert "meta list core_client" in error.suggestion

    def test_model_without_schema_prefix(self):
        """Test model without schema prefix."""
        error = ModelNotFoundError("simple_model", ["production manifest"])

        # Should suggest: meta list
        assert "meta list" in error.suggestion


class TestManifestNotFoundError:
    """Test ManifestNotFoundError."""

    def test_single_path(self):
        """Test manifest not found with single path."""
        error = ManifestNotFoundError(["/path/to/manifest.json"])

        assert error.searched_paths == ["/path/to/manifest.json"]
        assert "manifest.json not found" in error.message
        assert "/path/to/manifest.json" in error.suggestion
        assert "dbt compile" in error.suggestion

    def test_multiple_paths(self):
        """Test manifest not found with multiple paths."""
        error = ManifestNotFoundError([
            "~/.dbt-state/manifest.json",
            "./target/manifest.json"
        ])

        assert len(error.searched_paths) == 2
        assert "~/.dbt-state/manifest.json" in error.suggestion
        assert "./target/manifest.json" in error.suggestion


class TestManifestParseError:
    """Test ManifestParseError."""

    def test_parse_error_with_details(self):
        """Test manifest parse error with details."""
        error = ManifestParseError(
            "/path/to/manifest.json",
            "Unexpected token at line 5"
        )

        assert error.path == "/path/to/manifest.json"
        assert error.parse_error == "Unexpected token at line 5"
        assert "Failed to parse manifest" in error.message
        assert "Unexpected token at line 5" in error.suggestion
        assert "dbt compile" in error.suggestion


class TestBigQueryError:
    """Test BigQueryError."""

    def test_basic_bigquery_error(self):
        """Test basic BigQuery error."""
        error = BigQueryError(
            "fetch dataset.table",
            "Table not found: dataset.table"
        )

        assert error.operation == "fetch dataset.table"
        assert error.details == "Table not found: dataset.table"
        assert "BigQuery operation failed" in error.message

    def test_table_not_found_suggestion(self):
        """Test that table not found error has appropriate suggestion."""
        error = BigQueryError(
            "fetch dataset.table",
            "Not found: Table project:dataset.table"
        )

        assert "not found" in error.suggestion.lower()
        assert "Table not found in BigQuery" in error.suggestion

    def test_permission_error_suggestion(self):
        """Test that permission error suggests auth check."""
        error = BigQueryError(
            "fetch dataset.table",
            "Permission denied: dataset.table"
        )

        assert "permission" in error.suggestion.lower() or "auth" in error.suggestion.lower()
        assert "gcloud auth list" in error.suggestion

    def test_auth_error_suggestion(self):
        """Test that authentication error suggests auth check."""
        error = BigQueryError(
            "fetch dataset.table",
            "Authentication failed"
        )

        assert "gcloud auth list" in error.suggestion


class TestGitOperationError:
    """Test GitOperationError."""

    def test_git_command_failure(self):
        """Test git command failure."""
        error = GitOperationError(
            "git diff --name-only HEAD",
            "fatal: not a git repository"
        )

        assert error.command == "git diff --name-only HEAD"
        assert error.error == "fatal: not a git repository"
        assert "Git command failed" in error.message
        assert "fatal: not a git repository" in error.suggestion


class TestConfigurationError:
    """Test ConfigurationError."""

    def test_invalid_value_without_valid_values(self):
        """Test configuration error without valid values."""
        error = ConfigurationError(
            "DBT_PROD_TABLE_NAME",
            "invalid_value"
        )

        assert error.config_key == "DBT_PROD_TABLE_NAME"
        assert error.invalid_value == "invalid_value"
        assert error.valid_values is None
        assert "Invalid configuration" in error.message
        assert "DBT_PROD_TABLE_NAME" in error.message

    def test_invalid_value_with_valid_values(self):
        """Test configuration error with valid values list."""
        error = ConfigurationError(
            "DBT_PROD_TABLE_NAME",
            "wrong",
            ["alias_or_name", "name", "alias"]
        )

        assert error.config_key == "DBT_PROD_TABLE_NAME"
        assert error.invalid_value == "wrong"
        assert error.valid_values == ["alias_or_name", "name", "alias"]
        assert "Invalid configuration" in error.message
        assert "alias_or_name" in error.suggestion
        assert "name" in error.suggestion
        assert "alias" in error.suggestion


class TestInheritance:
    """Test exception inheritance."""

    def test_all_inherit_from_base(self):
        """Test that all custom exceptions inherit from DbtMetaError."""
        exceptions = [
            ModelNotFoundError("model", []),
            ManifestNotFoundError([]),
            ManifestParseError("path", "error"),
            BigQueryError("op", "details"),
            GitOperationError("cmd", "err"),
            ConfigurationError("key", "val"),
        ]

        for exc in exceptions:
            assert isinstance(exc, DbtMetaError)
            assert isinstance(exc, Exception)

    def test_catchable_with_base_exception(self):
        """Test that all exceptions can be caught with DbtMetaError."""
        def raise_model_not_found():
            raise ModelNotFoundError("test", ["location"])

        with pytest.raises(DbtMetaError):
            raise_model_not_found()

        def raise_manifest_not_found():
            raise ManifestNotFoundError(["path"])

        with pytest.raises(DbtMetaError):
            raise_manifest_not_found()


# ============================================================================
# SECTION 5: Exception Handling - No Silent Failures
# ============================================================================


@pytest.mark.critical
class TestNoSilentFailures:
    """Verify all exceptions are properly handled, not silently swallowed.

    CRITICAL: Silent failures hide bugs and corrupt state.
    All exceptions must be specific and logged/handled properly.
    """

    def test_manifest_not_found_raises_specific_error(self):
        """ManifestParser should raise ManifestNotFoundError, not generic Exception."""
        from dbt_meta.manifest.parser import ManifestParser

        with pytest.raises(ManifestNotFoundError) as exc_info:
            parser = ManifestParser("/nonexistent/manifest.json")
            _ = parser.manifest  # Trigger lazy loading

        assert "not found" in str(exc_info.value).lower()

    def test_invalid_json_raises_parse_error(self):
        """Invalid JSON should raise ManifestParseError, not generic Exception."""
        import orjson

        from dbt_meta.manifest.parser import ManifestParser

        # Create invalid JSON file
        with patch('pathlib.Path.exists', return_value=True):
            with patch('builtins.open', MagicMock()):
                with patch('orjson.loads', side_effect=orjson.JSONDecodeError("Invalid JSON", "bad json", 5)):
                    parser = ManifestParser("/path/to/bad.json")

                    with pytest.raises(ManifestParseError) as exc_info:
                        _ = parser.manifest

                    assert "Invalid JSON" in str(exc_info.value)

    def test_git_timeout_returns_safe_default(self):
        """Git timeouts should return False, not raise unhandled exception."""
        from dbt_meta.utils.git import is_modified

        with patch('subprocess.run', side_effect=subprocess.TimeoutExpired('git', 5)):
            result = is_modified('some_model')
            assert result is False  # Safe default

    def test_git_file_not_found_returns_safe_default(self):
        """Git not installed should return safe defaults, not crash."""
        from dbt_meta.utils.git import is_modified

        with patch('subprocess.run', side_effect=FileNotFoundError("git not found")):
            result = is_modified('some_model')
            assert result is False  # Safe default

    def test_filesystem_permission_error_handled(self):
        """Filesystem permission errors should be caught and handled."""
        from dbt_meta.utils.dev import find_dev_manifest

        with patch('pathlib.Path.cwd', side_effect=PermissionError("Access denied")):
            result = find_dev_manifest("/some/manifest.json")
            assert result is None  # Safe default

    def test_fallback_model_not_found_raises_specific_error(self):
        """FallbackStrategy should raise ModelNotFoundError when model not found."""
        from dbt_meta.config import Config
        from dbt_meta.fallback import FallbackStrategy
        from dbt_meta.manifest.parser import ManifestParser

        config = Config.from_env()
        strategy = FallbackStrategy(config)

        mock_parser = MagicMock(spec=ManifestParser)
        mock_parser.get_model.return_value = None

        with pytest.raises(ModelNotFoundError) as exc_info:
            strategy.get_model(
                "nonexistent_model",
                prod_parser=mock_parser,
                allowed_levels=[]
            )

        assert "nonexistent_model" in str(exc_info.value)

    def test_fallback_strategy_catches_manifest_errors(self, enable_fallbacks):
        """FallbackStrategy should catch ManifestNotFoundError and ManifestParseError gracefully."""
        from dbt_meta.config import Config
        from dbt_meta.fallback import FallbackStrategy

        # Create config with fallbacks enabled
        config = Config.from_env()

        # Create mock parser that returns a model
        mock_parser = MagicMock()
        mock_parser.get_model.return_value = {'name': 'test_model', 'schema': 'test_schema'}

        strategy = FallbackStrategy(config)

        # Should not crash when dev manifest doesn't exist - continues to production
        result = strategy.get_model('test_model', mock_parser)

        assert result.found is True
        assert result.data is not None

    def test_bigquery_error_caught_in_fallback(self):
        """BigQuery errors should be caught and fallback continues."""
        from dbt_meta.config import Config
        from dbt_meta.fallback import FallbackLevel, FallbackStrategy

        config = Config.from_env()
        config.fallback_bigquery_enabled = True
        strategy = FallbackStrategy(config)

        mock_parser = MagicMock()
        mock_parser.get_model.return_value = None

        with patch.object(strategy, '_fetch_from_bigquery') as mock_bq:
            mock_bq.side_effect = subprocess.CalledProcessError(1, 'bq')

            with pytest.raises(ModelNotFoundError):
                strategy.get_model(
                    "test_model",
                    prod_parser=mock_parser,
                    allowed_levels=[FallbackLevel.BIGQUERY]
                )

            # Should have tried BigQuery despite error
            mock_bq.assert_called_once()

    def test_git_status_all_errors_return_safe_default(self):
        """All git errors should return safe GitStatus, not crash."""
        from dbt_meta.utils.git import get_model_git_status

        test_errors = [
            subprocess.TimeoutExpired('git', 5),
            OSError("File error"),
            ValueError("Parse error"),
            UnicodeDecodeError('utf-8', b'', 0, 1, "Bad unicode")
        ]

        for error in test_errors:
            with patch('dbt_meta.utils.git._find_sql_file_fast') as mock_find:
                mock_find.return_value = "models/test.sql"

                with patch('subprocess.run', side_effect=error):
                    status = get_model_git_status('test_model')

                    # Should return safe defaults
                    assert status.exists is True
                    assert status.is_tracked is False
                    assert status.is_modified is False

    def test_no_bare_except_statements_in_codebase(self):
        """Verify no 'except:' or 'except Exception:' remain (except CLI)."""
        from pathlib import Path

        allowed_files = ['cli.py']  # CLI can have broad handler as last resort

        src_dir = Path(__file__).parent.parent / 'src' / 'dbt_meta'
        bare_except_found = []

        for py_file in src_dir.rglob('*.py'):
            if py_file.name in allowed_files:
                continue

            with open(py_file) as f:
                lines = f.readlines()
                for i, line in enumerate(lines, 1):
                    # Check for bare except or except Exception
                    if line.strip().startswith('except:') or \
                       line.strip().startswith('except Exception:'):
                        bare_except_found.append(f"{py_file.name}:{i}")

        # After our fixes, this should be empty
        assert len(bare_except_found) == 0, \
            f"Broad exception handlers found in: {bare_except_found}"
