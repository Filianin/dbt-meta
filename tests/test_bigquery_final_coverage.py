"""Tests to cover remaining gaps in bigquery.py.

Target lines: 35, 64-65, 94-95, 127-131, 170-171, 199-201, 221, 291, 314
"""

import pytest
import os
from unittest.mock import patch, Mock
from dbt_meta.utils.bigquery import (
    _should_retry,
    sanitize_bigquery_name,
    infer_table_parts
)


class TestShouldRetry:
    """Cover _should_retry edge cases."""

    def test_should_retry_with_debug_enabled(self, monkeypatch, capsys):
        """Test _should_retry prints debug message when DBT_META_DEBUG set (line 35)."""
        monkeypatch.setenv('DBT_META_DEBUG', '1')

        # Correct parameter order: attempt, max_retries, error_msg
        result = _should_retry(0, 3, "API rate limit")

        assert result is True
        captured = capsys.readouterr()
        # Should print retry message to stderr
        assert "retrying in 1s" in captured.err or "API rate limit" in captured.err

    def test_should_retry_last_attempt_no_retry(self):
        """Test _should_retry returns False on last attempt (line 38-39)."""
        # Correct parameter order: attempt, max_retries, error_msg
        result = _should_retry(2, 3, "Timeout")

        # Last attempt (attempt 2 of 3) - should not retry
        assert result is False


class TestSanitizeBigQueryName:
    """Cover sanitize_bigquery_name edge cases."""

    def test_sanitize_name_too_long(self):
        """Test sanitize with name longer than 1024 chars (lines 64-65)."""
        long_name = "a" * 1500

        sanitized, warnings = sanitize_bigquery_name(long_name)

        # Should truncate to 1024 chars
        assert len(sanitized) == 1024
        assert any("too long" in w.lower() for w in warnings)

    def test_sanitize_name_starts_with_number(self):
        """Test sanitize with name starting with number (lines 94-95)."""
        name = "123_table"

        sanitized, warnings = sanitize_bigquery_name(name, "table")

        # Should prepend underscore
        assert sanitized.startswith("_")
        assert sanitized == "_123_table"
        assert any("must start with letter or underscore" in w.lower() for w in warnings)

    def test_sanitize_name_starts_with_special_char(self):
        """Test sanitize with name starting with special character (lines 94-95)."""
        name = "@invalid"

        sanitized, warnings = sanitize_bigquery_name(name, "table")

        # Should prepend underscore and replace @
        assert sanitized.startswith("_")
        assert "@" not in sanitized


class TestInferTableParts:
    """Cover infer_table_parts edge cases."""

    def test_infer_table_parts_multiple_underscores(self):
        """Test infer_table_parts with multiple __ separators (lines 127-131)."""
        model_name = "core__client__events__daily"

        dataset, table = infer_table_parts(model_name)

        # Should join all parts except last as dataset
        assert dataset == "core__client__events"
        assert table == "daily"

    def test_infer_table_parts_three_underscores(self):
        """Test infer_table_parts with three __ separators (lines 127-131)."""
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


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
