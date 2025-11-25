"""Test exception handling - verify no silent failures.

CRITICAL: Silent failures hide bugs and corrupt state.
All exceptions must be specific and logged/handled properly.
"""

import subprocess
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from dbt_meta.errors import ManifestNotFoundError, ManifestParseError, ModelNotFoundError
from dbt_meta.fallback import FallbackStrategy
from dbt_meta.utils.dev import find_dev_manifest
from dbt_meta.utils.git import get_model_git_status, is_modified


@pytest.mark.critical
class TestNoSilentFailures:
    """Verify all exceptions are properly handled, not silently swallowed."""

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
        with patch('subprocess.run', side_effect=subprocess.TimeoutExpired('git', 5)):
            result = is_modified('some_model')
            assert result is False  # Safe default

    def test_git_file_not_found_returns_safe_default(self):
        """Git not installed should return safe defaults, not crash."""
        with patch('subprocess.run', side_effect=FileNotFoundError("git not found")):
            result = is_modified('some_model')
            assert result is False  # Safe default

    def test_filesystem_permission_error_handled(self):
        """Filesystem permission errors should be caught and handled."""
        with patch('pathlib.Path.cwd', side_effect=PermissionError("Access denied")):
            result = find_dev_manifest("/some/manifest.json")
            assert result is None  # Safe default

    def test_fallback_model_not_found_raises_specific_error(self):
        """FallbackStrategy should raise ModelNotFoundError when model not found."""
        from dbt_meta.config import Config
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