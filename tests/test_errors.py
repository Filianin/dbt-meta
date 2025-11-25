"""Tests for exception hierarchy in dbt_meta.errors module."""

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
