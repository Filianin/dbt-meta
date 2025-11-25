"""Exception hierarchy for dbt-meta CLI.

All custom exceptions inherit from DbtMetaError and provide:
- Clear error messages
- Actionable suggestions for users
- Structured data for programmatic handling
"""

from typing import Optional


class DbtMetaError(Exception):
    """Base exception for all dbt-meta errors.

    All custom exceptions should inherit from this class to enable
    consistent error handling throughout the application.

    Attributes:
        message: Human-readable error description
        suggestion: Optional actionable suggestion for fixing the error
    """

    def __init__(self, message: str, suggestion: Optional[str] = None):
        self.message = message
        self.suggestion = suggestion
        super().__init__(message)

    def __str__(self) -> str:
        """String representation includes suggestion if available."""
        if self.suggestion:
            return f"{self.message}\n\nSuggestion: {self.suggestion}"
        return self.message


class ModelNotFoundError(DbtMetaError):
    """Raised when model not found in any available source.

    This error occurs when a model cannot be found in:
    - Production manifest
    - Dev manifest (if fallback enabled)
    - BigQuery (if fallback enabled)

    Attributes:
        model_name: The requested model name
        searched_locations: List of locations where model was searched
    """

    def __init__(self, model_name: str, searched_locations: list[str]):
        self.model_name = model_name
        self.searched_locations = searched_locations

        # Extract potential schema prefix for suggestion
        schema_prefix = model_name.split('__')[0] if '__' in model_name else ''

        message = f"Model '{model_name}' not found"

        # Build suggestion with searched locations (if any)
        if searched_locations:
            suggestion = f"Searched in: {', '.join(searched_locations)}\n"
        else:
            suggestion = "No locations were searched (all fallback levels may be disabled)\n"

        suggestion += f"Try: meta list {schema_prefix}" if schema_prefix else "Try: meta list"

        super().__init__(message, suggestion)


class ManifestNotFoundError(DbtMetaError):
    """Raised when manifest.json file not found.

    This typically occurs when:
    - dbt has not been compiled yet
    - manifest path is incorrectly configured
    - manifest file was deleted

    Attributes:
        searched_paths: List of paths where manifest was searched
    """

    def __init__(self, searched_paths: list[str]):
        self.searched_paths = searched_paths

        message = "manifest.json not found"
        suggestion = (
            f"Searched: {', '.join(searched_paths)}\n"
            "Run: dbt compile or dbt build to generate manifest"
        )

        super().__init__(message, suggestion)


class ManifestParseError(DbtMetaError):
    """Raised when manifest.json exists but cannot be parsed.

    This occurs when:
    - manifest.json is corrupted
    - manifest.json has invalid JSON syntax
    - manifest.json is from incompatible dbt version

    Attributes:
        path: Path to the problematic manifest file
        parse_error: Original parsing error message
    """

    def __init__(self, path: str, parse_error: str):
        self.path = path
        self.parse_error = parse_error

        message = f"Failed to parse manifest: {path}"
        suggestion = (
            f"Parse error: {parse_error}\n"
            "Run: dbt compile to regenerate manifest"
        )

        super().__init__(message, suggestion)


class BigQueryError(DbtMetaError):
    """Raised when BigQuery operation fails.

    This occurs when:
    - BigQuery CLI (bq) is not installed
    - Authentication is not configured
    - Table does not exist in BigQuery
    - Network connectivity issues

    Attributes:
        operation: Description of the failed operation
        details: Detailed error information from BigQuery
    """

    def __init__(self, operation: str, details: str):
        self.operation = operation
        self.details = details

        message = f"BigQuery operation failed: {operation}"

        # Provide context-specific suggestions
        if "not found" in details.lower():
            suggestion = f"Table not found in BigQuery\nDetails: {details}"
        elif "permission" in details.lower() or "auth" in details.lower():
            suggestion = (
                f"Authentication issue\n"
                f"Details: {details}\n"
                f"Check: gcloud auth list"
            )
        else:
            suggestion = f"Details: {details}"

        super().__init__(message, suggestion)


class GitOperationError(DbtMetaError):
    """Raised when git operation fails.

    This occurs when:
    - git command is not available
    - Current directory is not a git repository
    - git operation times out

    Attributes:
        command: The git command that failed
        error: Error message from git
    """

    def __init__(self, command: str, error: str):
        self.command = command
        self.error = error

        message = f"Git command failed: {command}"
        suggestion = f"Error: {error}"

        super().__init__(message, suggestion)


class ConfigurationError(DbtMetaError):
    """Raised when configuration is invalid.

    This occurs when:
    - Environment variables have invalid values
    - Required configuration is missing
    - Conflicting configuration options

    Attributes:
        config_key: The problematic configuration key
        invalid_value: The invalid value provided
        valid_values: Optional list of valid values
    """

    def __init__(
        self,
        config_key: str,
        invalid_value: str,
        valid_values: Optional[list[str]] = None
    ):
        self.config_key = config_key
        self.invalid_value = invalid_value
        self.valid_values = valid_values

        message = f"Invalid configuration: {config_key}='{invalid_value}'"

        if valid_values:
            suggestion = f"Valid values: {', '.join(valid_values)}"
        else:
            suggestion = f"Check configuration documentation for {config_key}"

        super().__init__(message, suggestion)
