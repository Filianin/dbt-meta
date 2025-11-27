"""Consolidated BigQuery tests.

Merged from:
- test_bigquery_final_coverage.py - Edge cases for BigQuery utilities
- test_bigquery_retry.py - Retry logic and exponential backoff
- test_path_bigquery_coverage.py - Path BigQuery format search

Coverage targets:
- BigQuery retry logic (exponential backoff, max attempts)
- Helper functions (_should_retry, sanitize_bigquery_name, infer_table_parts)
- Path command BigQuery format search
"""

import json
import subprocess
from unittest.mock import MagicMock, patch

import pytest

from dbt_meta.commands import path
from dbt_meta.utils.bigquery import (
    _should_retry,
    fetch_columns_from_bigquery_direct,
    infer_table_parts,
    sanitize_bigquery_name,
)


# ============================================================================
# SECTION 1: BigQuery Utility Functions
# ============================================================================


class TestShouldRetry:
    """Cover _should_retry edge cases."""

    def test_should_retry_with_debug_enabled(self, monkeypatch, capsys):
        """Test _should_retry prints debug message when DBT_META_DEBUG set."""
        monkeypatch.setenv('DBT_META_DEBUG', '1')

        result = _should_retry(0, 3, "API rate limit")

        assert result is True
        captured = capsys.readouterr()
        # Should print retry message to stderr
        assert "retrying in 1s" in captured.err or "API rate limit" in captured.err

    def test_should_retry_last_attempt_no_retry(self):
        """Test _should_retry returns False on last attempt."""
        result = _should_retry(2, 3, "Timeout")

        # Last attempt (attempt 2 of 3) - should not retry
        assert result is False


class TestSanitizeBigQueryName:
    """Cover sanitize_bigquery_name edge cases."""

    def test_sanitize_name_too_long(self):
        """Test sanitize with name longer than 1024 chars."""
        long_name = "a" * 1500

        sanitized, warnings = sanitize_bigquery_name(long_name)

        # Should truncate to 1024 chars
        assert len(sanitized) == 1024
        assert any("too long" in w.lower() for w in warnings)

    def test_sanitize_name_starts_with_number(self):
        """Test sanitize with name starting with number."""
        name = "123_table"

        sanitized, warnings = sanitize_bigquery_name(name, "table")

        # Should prepend underscore
        assert sanitized.startswith("_")
        assert sanitized == "_123_table"
        assert any("must start with letter or underscore" in w.lower() for w in warnings)

    def test_sanitize_name_starts_with_special_char(self):
        """Test sanitize with name starting with special character."""
        name = "@invalid"

        sanitized, _warnings = sanitize_bigquery_name(name, "table")

        # Should prepend underscore and replace @
        assert sanitized.startswith("_")
        assert "@" not in sanitized


class TestInferTableParts:
    """Cover infer_table_parts edge cases."""

    def test_infer_table_parts_multiple_underscores(self):
        """Test infer_table_parts with multiple __ separators."""
        model_name = "core__client__events__daily"

        dataset, table = infer_table_parts(model_name)

        # Should join all parts except last as dataset
        assert dataset == "core__client__events"
        assert table == "daily"

    def test_infer_table_parts_three_underscores(self):
        """Test infer_table_parts with three __ separators."""
        model_name = "staging__external__source__table"

        dataset, table = infer_table_parts(model_name)

        assert dataset == "staging__external__source"
        assert table == "table"

    def test_infer_table_parts_no_separator(self):
        """Test infer_table_parts with no __ separator."""
        model_name = "simple_table"

        dataset, table = infer_table_parts(model_name)

        # No separator - dataset is None
        assert dataset is None
        assert table == "simple_table"


# ============================================================================
# SECTION 2: BigQuery Retry Logic
# ============================================================================


@pytest.mark.unit
class TestBigQueryRetryLogic:
    """Test retry logic with exponential backoff.

    CRITICAL: These tests verify retry logic prevents data loss from transient failures.
    """

    def test_success_on_first_attempt(self):
        """Successful query on first attempt requires no retries."""
        with patch('dbt_meta.utils.bigquery.run_bq_command') as mock_bq:
            # Mock successful response
            mock_version = MagicMock()  # Version check
            mock_result = MagicMock()   # Actual query
            mock_result.stdout = '[{"name": "id", "type": "INT64"}]'

            # First call: version check, Second call: query
            mock_bq.side_effect = [mock_version, mock_result]

            columns = fetch_columns_from_bigquery_direct('test_schema', 'test_table')

            # Verify called twice (version + query)
            assert mock_bq.call_count == 2
            assert columns is not None
            assert len(columns) == 1
            assert columns[0]['name'] == 'id'

    def test_success_on_second_attempt(self):
        """Query fails once, succeeds on retry."""
        with patch('dbt_meta.utils.bigquery.run_bq_command') as mock_bq:
            with patch('time.sleep') as mock_sleep:
                # Mock responses
                mock_version = MagicMock()
                mock_result_success = MagicMock()
                mock_result_success.stdout = '[{"name": "id", "type": "INT64"}]'

                # Version check succeeds, first query fails, second succeeds
                mock_bq.side_effect = [
                    mock_version,                               # Version check
                    subprocess.CalledProcessError(1, 'bq'),     # First query: fail
                    mock_result_success                          # Second query: success
                ]

                columns = fetch_columns_from_bigquery_direct('test_schema', 'test_table')

                # Verify retry occurred (3 calls: version + 2 query attempts)
                assert mock_bq.call_count == 3
                # Verify exponential backoff: wait 1 second (2^0)
                mock_sleep.assert_called_once_with(1)
                assert columns is not None

    def test_retry_on_called_process_error(self):
        """CalledProcessError triggers retry with exponential backoff."""
        with patch('dbt_meta.utils.bigquery.run_bq_command') as mock_bq:
            with patch('time.sleep') as mock_sleep:
                # Version check succeeds, all query attempts fail
                mock_version = MagicMock()
                mock_bq.side_effect = [
                    mock_version,                               # Version check
                    subprocess.CalledProcessError(1, 'bq'),     # Attempt 1
                    subprocess.CalledProcessError(1, 'bq'),     # Attempt 2
                    subprocess.CalledProcessError(1, 'bq')      # Attempt 3
                ]

                columns = fetch_columns_from_bigquery_direct('test_schema', 'test_table')

                # Verify max retries (1 version + 3 query attempts)
                assert mock_bq.call_count == 4
                # Verify exponential backoff: 1s, 2s (2^0, 2^1)
                assert mock_sleep.call_count == 2
                mock_sleep.assert_any_call(1)  # 2^0
                mock_sleep.assert_any_call(2)  # 2^1
                assert columns is None

    def test_retry_on_timeout_expired(self):
        """TimeoutExpired triggers retry with exponential backoff."""
        with patch('dbt_meta.utils.bigquery.run_bq_command') as mock_bq:
            with patch('time.sleep') as mock_sleep:
                # Version check succeeds, all timeouts
                mock_version = MagicMock()
                mock_bq.side_effect = [
                    mock_version,                               # Version check
                    subprocess.TimeoutExpired('bq', 10),        # Attempt 1
                    subprocess.TimeoutExpired('bq', 10),        # Attempt 2
                    subprocess.TimeoutExpired('bq', 10)         # Attempt 3
                ]

                columns = fetch_columns_from_bigquery_direct('test_schema', 'test_table')

                # Verify retries (1 version + 3 attempts)
                assert mock_bq.call_count == 4
                assert mock_sleep.call_count == 2
                assert columns is None

    def test_exponential_backoff_timing(self):
        """Exponential backoff follows 2^attempt pattern."""
        with patch('dbt_meta.utils.bigquery.run_bq_command') as mock_bq:
            with patch('time.sleep') as mock_sleep:
                mock_version = MagicMock()
                mock_bq.side_effect = [
                    mock_version,
                    subprocess.CalledProcessError(1, 'bq'),
                    subprocess.CalledProcessError(1, 'bq'),
                    subprocess.CalledProcessError(1, 'bq')
                ]

                fetch_columns_from_bigquery_direct('test_schema', 'test_table')

                # Verify exponential backoff sequence
                # Attempt 0 fails → sleep(2^0 = 1)
                # Attempt 1 fails → sleep(2^1 = 2)
                # Attempt 2 fails → no sleep (last attempt)
                sleep_calls = [call_args[0][0] for call_args in mock_sleep.call_args_list]
                assert sleep_calls == [1, 2]

    def test_max_attempts_respected(self):
        """Retry stops after max_attempts even if still failing."""
        with patch('dbt_meta.utils.bigquery.run_bq_command') as mock_bq:
            with patch('time.sleep'):
                # Version check + infinite failures
                mock_version = MagicMock()
                errors = [subprocess.CalledProcessError(1, 'bq')] * 100
                mock_bq.side_effect = [mock_version, *errors]

                columns = fetch_columns_from_bigquery_direct('test_schema', 'test_table')

                # Should stop at max_retries (1 version + 3 attempts = 4), not continue
                assert mock_bq.call_count == 4
                assert columns is None

    def test_all_attempts_fail_returns_none(self):
        """All retries exhausted returns None, not exception."""
        with patch('dbt_meta.utils.bigquery.run_bq_command') as mock_bq:
            with patch('time.sleep'):
                mock_version = MagicMock()
                mock_bq.side_effect = [
                    mock_version,
                    subprocess.CalledProcessError(1, 'bq'),
                    subprocess.CalledProcessError(1, 'bq'),
                    subprocess.CalledProcessError(1, 'bq')
                ]

                # Should return None, not raise exception
                columns = fetch_columns_from_bigquery_direct('test_schema', 'test_table')

                assert columns is None

    def test_retry_with_different_errors(self):
        """Mix of different retryable errors handled correctly."""
        with patch('dbt_meta.utils.bigquery.run_bq_command') as mock_bq:
            with patch('time.sleep') as mock_sleep:
                mock_version = MagicMock()
                mock_result_success = MagicMock()
                mock_result_success.stdout = '[{"name": "id", "type": "INT64"}]'

                # Fail with different errors, then succeed
                mock_bq.side_effect = [
                    mock_version,                               # Version check
                    subprocess.CalledProcessError(1, 'bq'),      # CalledProcessError
                    subprocess.TimeoutExpired('bq', 10),         # TimeoutExpired
                    mock_result_success                          # Success
                ]

                columns = fetch_columns_from_bigquery_direct('test_schema', 'test_table')

                # Verify retry occurred for both error types (1 version + 3 attempts)
                assert mock_bq.call_count == 4
                assert mock_sleep.call_count == 2
                assert columns is not None


@pytest.mark.unit
class TestBigQueryRetryEdgeCases:
    """Test edge cases in retry logic."""

    def test_json_parse_error_no_retry(self):
        """JSON parse error does not retry (not transient)."""
        with patch('dbt_meta.utils.bigquery.run_bq_command') as mock_bq:
            with patch('time.sleep') as mock_sleep:
                # Version check + invalid JSON
                mock_version = MagicMock()
                mock_result = MagicMock()
                mock_result.stdout = 'invalid json'

                mock_bq.side_effect = [mock_version, mock_result]

                columns = fetch_columns_from_bigquery_direct('test_schema', 'test_table')

                # Should NOT retry on JSON parse error (1 version + 1 query)
                assert mock_bq.call_count == 2
                assert mock_sleep.call_count == 0
                assert columns is None

    def test_empty_columns_no_retry(self):
        """Empty column list returns successfully without retry."""
        with patch('dbt_meta.utils.bigquery.run_bq_command') as mock_bq:
            mock_version = MagicMock()
            mock_result = MagicMock()
            mock_result.stdout = '[]'

            mock_bq.side_effect = [mock_version, mock_result]

            columns = fetch_columns_from_bigquery_direct('test_schema', 'test_table')

            # Should not retry on empty but valid response
            assert mock_bq.call_count == 2
            assert columns is not None
            assert len(columns) == 0

    def test_retry_preserves_error_context(self):
        """Error messages are preserved across retries."""
        with patch('dbt_meta.utils.bigquery.run_bq_command') as mock_bq:
            with patch('time.sleep'):
                mock_version = MagicMock()
                # Fail with specific error code
                error = subprocess.CalledProcessError(
                    returncode=403,
                    cmd='bq query'
                )
                mock_bq.side_effect = [mock_version, error, error, error]

                columns = fetch_columns_from_bigquery_direct('test_schema', 'test_table')

                # Verify all retries used same error (1 version + 3 attempts)
                assert mock_bq.call_count == 4
                assert columns is None


@pytest.mark.integration
class TestBigQueryRetryIntegration:
    """Integration tests for retry logic."""

    def test_retry_with_real_subprocess_mock(self):
        """Test retry with realistic subprocess behavior."""
        with patch('dbt_meta.utils.bigquery.run_bq_command') as mock_bq:
            with patch('time.sleep'):
                # Simulate real subprocess behavior
                mock_version = MagicMock()
                success_result = MagicMock()
                success_result.stdout = '[{"name": "id", "type": "INT64"}]'

                mock_bq.side_effect = [
                    mock_version,                               # Version check
                    subprocess.CalledProcessError(1, ['bq']),   # First query fails
                    success_result                               # Second query succeeds
                ]

                columns = fetch_columns_from_bigquery_direct('test_schema', 'test_table')

                # Verify retry worked with real subprocess mock
                assert mock_bq.call_count == 3
                assert columns is not None


# ============================================================================
# SECTION 3: Path Command BigQuery Format Search
# ============================================================================


class TestPathBigQueryFormatEdgeCases:
    """Cover path.py BigQuery format search edge cases."""

    def test_path_bigquery_format_not_in_dev_mode(self, tmp_path, monkeypatch):
        """Test BigQuery format search returns None when use_dev=False."""
        prod_manifest = tmp_path / "manifest.json"
        prod_manifest.write_text('{"metadata": {}, "nodes": {}}')

        monkeypatch.setenv('DBT_FALLBACK_TARGET', 'false')

        # BigQuery format with use_dev=False should not search
        result = path(str(prod_manifest), 'schema.table', use_dev=False, json_output=False)

        # Should return None (not use_dev)
        assert result is None

    def test_path_bigquery_format_dev_manifest_not_found(self, tmp_path, monkeypatch):
        """Test BigQuery format search returns None when dev manifest missing."""
        prod_manifest = tmp_path / "manifest.json"
        prod_manifest.write_text('{"metadata": {}, "nodes": {}}')

        monkeypatch.setenv('DBT_PROD_MANIFEST_PATH', str(prod_manifest))
        monkeypatch.setenv('DBT_DEV_MANIFEST_PATH', str(tmp_path / "nonexistent" / "manifest.json"))

        # Mock find_dev_manifest to return None
        with patch('dbt_meta.utils.dev.find_dev_manifest', return_value=None):
            result = path(str(prod_manifest), 'schema.table', use_dev=True, json_output=False)

            # Should return None (no dev manifest)
            assert result is None

    def test_path_bigquery_format_single_part_name(self, tmp_path, monkeypatch):
        """Test BigQuery format with single part returns None."""
        prod_manifest = tmp_path / "manifest.json"
        dev_manifest = tmp_path / "dev_manifest.json"

        prod_manifest.write_text('{"metadata": {}, "nodes": {}}')
        dev_manifest.write_text('{"metadata": {}, "nodes": {}}')

        monkeypatch.setenv('DBT_PROD_MANIFEST_PATH', str(prod_manifest))
        monkeypatch.setenv('DBT_DEV_MANIFEST_PATH', str(dev_manifest))

        with patch('dbt_meta.utils.dev.find_dev_manifest', return_value=str(dev_manifest)):
            # Single part name (no dot) should return None
            result = path(str(prod_manifest), 'tablename', use_dev=True, json_output=False)

            # Should return None (len(parts) < 2)
            assert result is None

    def test_path_prod_bigquery_format_no_match(self, enable_fallbacks, tmp_path, monkeypatch):
        """Test production BigQuery format search with no matches."""
        prod_manifest = tmp_path / "manifest.json"
        prod_manifest.write_text(json.dumps({
            "metadata": {},
            "nodes": {
                "model.test.my_model": {
                    "name": "my_model",
                    "schema": "core",
                    "alias": "my_table",
                    "resource_type": "model",
                    "original_file_path": "models/core/my_model.sql"
                }
            }
        }))

        monkeypatch.setenv('DBT_PROD_MANIFEST_PATH', str(prod_manifest))
        monkeypatch.setenv('DBT_FALLBACK_TARGET', 'false')

        # Search for non-matching BigQuery format
        result = path(str(prod_manifest), 'different_schema.different_table', use_dev=False, json_output=False)

        # Should return None (no match found in prod manifest)
        assert result is None

    def test_path_prod_bigquery_format_match_by_alias(self, enable_fallbacks, prod_manifest):
        """Test production BigQuery format matches by alias."""
        from dbt_meta.manifest.parser import ManifestParser

        parser = ManifestParser(str(prod_manifest))
        nodes = parser.manifest.get('nodes', {})

        # Find model with alias
        for _node_id, node_data in nodes.items():
            if node_data.get('resource_type') == 'model':
                schema = node_data.get('schema')
                alias = node_data.get('alias') or node_data.get('config', {}).get('alias')
                if schema and alias:
                    # Test BigQuery format: schema.alias
                    result = path(str(prod_manifest), f'{schema}.{alias}', use_dev=False, json_output=False)

                    if result:
                        assert 'sql' in result or '.sql' in result
                        break


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
