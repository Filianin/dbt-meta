"""Static-check tests for ValidateCommand — no BigQuery dry-run involved."""

from __future__ import annotations

from dbt_meta.command_impl.validate import _find_unguarded_max_partition


def test_guarded_max_partition_returns_no_lines():
    sql = """
        {% if is_incremental() %}
        WHERE event_date >= _dbt_max_partition
        {% endif %}
    """
    assert _find_unguarded_max_partition(sql) == []


def test_unguarded_max_partition_reports_line():
    sql = "SELECT * FROM t\nWHERE event_date >= _dbt_max_partition\n"
    lines = _find_unguarded_max_partition(sql)
    assert lines == [2]


def test_max_partition_inside_unrelated_if_is_flagged():
    sql = """{% if var('foo') %}
WHERE d >= _dbt_max_partition
{% endif %}
"""
    lines = _find_unguarded_max_partition(sql)
    assert lines == [2]


def test_nested_guard_works():
    sql = """{% if some_var %}
{% if is_incremental() %}
WHERE d >= _dbt_max_partition
{% endif %}
{% endif %}
"""
    assert _find_unguarded_max_partition(sql) == []


def test_comment_strip_does_not_count():
    sql = "{# example: _dbt_max_partition usage #}\nSELECT 1\n"
    assert _find_unguarded_max_partition(sql) == []


def test_multiple_unguarded_occurrences():
    sql = (
        "SELECT _dbt_max_partition\n"
        "FROM t\n"
        "WHERE x > _dbt_max_partition\n"
    )
    assert _find_unguarded_max_partition(sql) == [1, 3]
