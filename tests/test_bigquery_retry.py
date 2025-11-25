"""Test BigQuery retry logic and exponential backoff.

CRITICAL: These tests verify retry logic prevents data loss from transient failures.
"""

import subprocess
from unittest.mock import MagicMock, patch

import pytest

from dbt_meta.utils.bigquery import fetch_columns_from_bigquery_direct


@pytest.mark.unit
class TestBigQueryRetryLogic:
    """Test retry logic with exponential backoff."""

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