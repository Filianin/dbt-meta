"""Tests for the resolve_column_target seam (step 3 of the resolver unification).

resolve_column_target answers a *different* question than resolve_location: the
``columns`` command chooses its physical BigQuery table by git *state*
(freshness), trusting the model's already-resolved fields rather than
re-deriving from config. This matrix locks that behaviour in one place.
"""

import warnings

import pytest

from dbt_meta.command_impl.resolver import ColumnTarget, resolve_column_target
from dbt_meta.utils.model_state import ModelState


@pytest.fixture(autouse=True)
def _dev_schema(monkeypatch):
    monkeypatch.setenv("DBT_DEV_SCHEMA", "personal_tester")
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=DeprecationWarning)
        yield


DEV_MODEL = {"name": "test__events", "schema": "personal_tester", "database": "", "config": {"alias": "events"}}
PROD_MODEL = {"name": "test__events", "schema": "core", "database": "proj", "alias": "events"}


# --------------------------------------------------------------------------
# model present
# --------------------------------------------------------------------------


class TestModelPresent:
    def test_use_dev_uses_model_schema_and_filename(self):
        t = resolve_column_target(ModelState.MODIFIED_IN_DEV, DEV_MODEL, None, "test__events", True)
        assert t == ColumnTarget(database="", schema="personal_tester", table="test__events", is_dev=True)

    def test_prod_prefers_prod_model(self):
        dev = {"name": "test__events", "schema": "personal_tester", "database": ""}
        t = resolve_column_target(ModelState.MODIFIED_UNCOMMITTED, dev, PROD_MODEL, "test__events", False)
        assert t == ColumnTarget(database="proj", schema="core", table="events", is_dev=False)

    def test_prod_falls_back_to_model_when_no_prod_model(self):
        t = resolve_column_target(ModelState.PROD_STABLE, PROD_MODEL, None, "test__events", False)
        assert t == ColumnTarget(database="proj", schema="core", table="events", is_dev=False)

    def test_prod_table_uses_name_when_no_alias(self):
        model = {"name": "some_model", "schema": "core", "database": "proj"}
        t = resolve_column_target(ModelState.PROD_STABLE, model, None, "some_model", False)
        assert t.table == "some_model"


# --------------------------------------------------------------------------
# model absent — location by git state
# --------------------------------------------------------------------------


class TestModelAbsent:
    def test_modified_uncommitted_uses_prod_model(self):
        t = resolve_column_target(ModelState.MODIFIED_UNCOMMITTED, None, PROD_MODEL, "test__events", False)
        assert t == ColumnTarget(database="proj", schema="core", table="events", is_dev=False)

    def test_modified_uncommitted_infers_when_no_prod_model(self, monkeypatch):
        monkeypatch.setattr(
            "dbt_meta.command_impl.resolver.infer_table_parts", lambda name: ("core", "model")
        )
        t = resolve_column_target(ModelState.MODIFIED_UNCOMMITTED, None, None, "core__model", False)
        assert t == ColumnTarget(database=None, schema="core", table="model", is_dev=False)

    def test_modified_uncommitted_with_dev_returns_none(self):
        # --dev + no model → not a MODIFIED_UNCOMMITTED prod-fetch case.
        assert resolve_column_target(ModelState.MODIFIED_UNCOMMITTED, None, None, "m", True) is None

    def test_new_uncommitted_uses_dev_schema(self):
        t = resolve_column_target(ModelState.NEW_UNCOMMITTED, None, None, "new_model", False)
        assert t == ColumnTarget(database=None, schema="personal_tester", table="new_model", is_dev=True)

    def test_new_committed_uses_dev_schema(self):
        t = resolve_column_target(ModelState.NEW_COMMITTED, None, None, "new_model", False)
        assert t == ColumnTarget(database=None, schema="personal_tester", table="new_model", is_dev=True)

    def test_new_state_uses_dev_even_with_use_dev(self):
        t = resolve_column_target(ModelState.NEW_UNCOMMITTED, None, None, "new_model", True)
        assert t is not None and t.is_dev

    @pytest.mark.parametrize(
        "state",
        [
            ModelState.PROD_STABLE,
            ModelState.NOT_FOUND,
            ModelState.DELETED_DEPLOYED,
            ModelState.MODIFIED_COMMITTED,
            ModelState.NEW_IN_DEV,
        ],
    )
    def test_unfetchable_states_return_none(self, state):
        assert resolve_column_target(state, None, None, "m", False) is None
