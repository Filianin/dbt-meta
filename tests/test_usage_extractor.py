"""Tests for ColumnUsageExtractor (WHERE/JOIN/GROUP/ORDER/QUALIFY/Window)."""

import pytest

from dbt_meta.usage import ColumnUsageExtractor, UsageEvent


@pytest.fixture
def ext():
    return ColumnUsageExtractor(dialect="bigquery")


def _filter(events, *, clause=None, column=None):
    out = events
    if clause:
        out = [e for e in out if e.clause == clause]
    if column:
        out = [e for e in out if e.column == column]
    return out


class TestSimpleWhere:
    def test_eq_literal(self, ext):
        sql = "SELECT 1 FROM proj.ds.core_sessions cs WHERE cs.client_id = 5"
        events = ext.extract(sql, "m", {"core_sessions"})
        wh = _filter(events, clause="where", column="client_id")
        assert len(wh) == 1
        assert wh[0].operator == "eq"
        assert wh[0].selectivity == "literal"

    def test_in_literal_list(self, ext):
        sql = "SELECT 1 FROM proj.ds.t cs WHERE cs.country IN ('EE', 'LV')"
        events = ext.extract(sql, "m", {"t"})
        wh = _filter(events, clause="where", column="country")
        assert len(wh) == 1
        assert wh[0].operator == "in"
        assert wh[0].selectivity == "literal"

    def test_in_subquery(self, ext):
        sql = "SELECT 1 FROM proj.ds.t cs WHERE cs.client_id IN (SELECT id FROM other)"
        events = ext.extract(sql, "m", {"t"})
        wh = _filter(events, clause="where", column="client_id")
        assert len(wh) == 1
        assert wh[0].operator == "in"
        assert wh[0].selectivity == "subquery"

    def test_between_is_range(self, ext):
        sql = "SELECT 1 FROM proj.ds.t cs WHERE cs.d BETWEEN '2026-01-01' AND '2026-02-01'"
        events = ext.extract(sql, "m", {"t"})
        wh = _filter(events, clause="where", column="d")
        assert len(wh) == 1
        assert wh[0].operator == "between"
        assert wh[0].selectivity == "range"

    def test_function_wrapped_marked_fn(self, ext):
        sql = "SELECT 1 FROM proj.ds.t cs WHERE COALESCE(cs.x, cs.y) = 5"
        events = ext.extract(sql, "m", {"t"})
        # operator now reflects the actual comparison (``eq``); the
        # function wrap is recorded in ``wrapping_fn`` so advisors can
        # decide per-BQ-semantics whether the wrap defeats pruning.
        wh = [e for e in _filter(events, clause="where") if e.column in {"x", "y"}]
        assert {e.column for e in wh} == {"x", "y"}
        for e in wh:
            assert e.operator == "eq"
            assert e.wrapping_fn == "coalesce"


class TestJoin:
    def test_join_on_column(self, ext):
        sql = """
            SELECT 1
            FROM proj.ds.core_sessions cs
            JOIN proj.ds.events e ON e.session_id = cs.session_id
        """
        events = ext.extract(sql, "m", {"core_sessions"})
        joins = _filter(events, clause="join")
        # Only cs.session_id matters — the e.session_id is a different table
        assert len(joins) == 1
        assert joins[0].column == "session_id"
        assert joins[0].operator == "eq"

    def test_join_on_function_wrapped(self, ext):
        sql = """
            SELECT 1
            FROM proj.ds.core_sessions cs
            JOIN proj.ds.events e ON UPPER(e.id) = UPPER(cs.id)
        """
        events = ext.extract(sql, "m", {"core_sessions"})
        joins = _filter(events, clause="join", column="id")
        assert len(joins) == 1
        # Comparison is ``eq``; ``upper`` is recorded as the wrapper.
        assert joins[0].operator == "eq"
        assert joins[0].wrapping_fn == "upper"


class TestGroupOrderQualify:
    def test_group_by_no_operator(self, ext):
        sql = "SELECT cs.country, COUNT(*) FROM proj.ds.t cs GROUP BY cs.country"
        events = ext.extract(sql, "m", {"t"})
        groups = _filter(events, clause="group_by")
        assert len(groups) == 1
        assert groups[0].column == "country"
        assert groups[0].operator == "none"

    def test_qualify_window_partition_and_order(self, ext):
        sql = """
            SELECT 1 FROM proj.ds.t cs
            QUALIFY ROW_NUMBER() OVER (PARTITION BY cs.x ORDER BY cs.y DESC) = 1
        """
        events = ext.extract(sql, "m", {"t"})
        # Window cols reported only once each via partition_by / order_by
        assert _filter(events, clause="partition_by", column="x")
        assert _filter(events, clause="order_by", column="y")
        # Qualify clause itself should NOT report x/y (would be a duplicate)
        assert _filter(events, clause="qualify") == []


class TestCteIsolation:
    def test_cte_with_same_alias_does_not_match(self, ext):
        # Upstream we care about is `core_sessions`. The SQL also has a CTE
        # called `cs` referencing a different table — its columns must not
        # be reported under our target.
        sql = """
            WITH cs AS (SELECT id, val FROM proj.ds.unrelated)
            SELECT cs.id FROM cs WHERE cs.val = 5
        """
        events = ext.extract(sql, "m", {"core_sessions"})
        # Nothing should match — alias `cs` resolves to CTE, not our target
        assert events == []

    def test_target_aliased_separately_from_cte(self, ext):
        # Both a CTE and a real reference to our target — only the real
        # reference's columns should appear.
        sql = """
            WITH stale AS (SELECT id FROM proj.ds.unrelated)
            SELECT t.amount
            FROM proj.ds.core_sessions t
            JOIN stale s ON s.id = t.id
            WHERE t.amount > 100
        """
        events = ext.extract(sql, "m", {"core_sessions"})
        wh = _filter(events, clause="where", column="amount")
        assert wh and wh[0].operator == "gt"


class TestSelectClause:
    def test_select_projection_recorded(self, ext):
        sql = "SELECT cs.a, cs.b FROM proj.ds.t cs"
        events = ext.extract(sql, "m", {"t"})
        cols = {e.column for e in _filter(events, clause="select")}
        assert cols == {"a", "b"}


class TestEdgeCases:
    def test_empty_sql_returns_empty(self, ext):
        assert ext.extract("", "m", {"t"}) == []
        assert ext.extract("   \n", "m", {"t"}) == []

    def test_no_target_keys_returns_empty(self, ext):
        sql = "SELECT cs.x FROM proj.ds.t cs WHERE cs.x = 1"
        assert ext.extract(sql, "m", set()) == []

    def test_target_not_referenced_returns_empty(self, ext):
        sql = "SELECT 1 FROM proj.ds.unrelated u WHERE u.x = 1"
        assert ext.extract(sql, "m", {"core_sessions"}) == []

    def test_invalid_sql_returns_empty(self, ext):
        # Garbage that sqlglot can't parse — must not raise
        events = ext.extract("@#$ NOT VALID SQL @#$", "m", {"t"})
        assert events == []

    def test_qualified_match_database_schema_table(self, ext):
        sql = "SELECT cs.x FROM `db1`.`ds1`.`my_table` cs WHERE cs.x = 1"
        events = ext.extract(sql, "m", {"db1.ds1.my_table"})
        assert _filter(events, clause="where", column="x")

    def test_qualified_match_schema_table(self, ext):
        sql = "SELECT cs.x FROM ds1.my_table cs WHERE cs.x = 1"
        events = ext.extract(sql, "m", {"ds1.my_table"})
        assert _filter(events, clause="where", column="x")

    def test_no_alias_uses_bare_name(self, ext):
        # SQL without AS — alias is the table name itself
        sql = "SELECT my_table.x FROM proj.ds.my_table WHERE my_table.x = 1"
        events = ext.extract(sql, "m", {"my_table"})
        assert _filter(events, clause="where", column="x")


class TestUsageEventDataclass:
    def test_event_is_hashable_frozen(self):
        ev = UsageEvent(
            column="x", clause="where", operator="eq",
            selectivity="literal", downstream_model="m"
        )
        # frozen=True → hashable
        assert hash(ev) == hash(ev)
        with pytest.raises(Exception):
            ev.column = "y"  # type: ignore[misc]
