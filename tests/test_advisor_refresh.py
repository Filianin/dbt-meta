"""Tests for RefreshAdvisor (column-aware --full-refresh planner)."""

from pathlib import Path
from unittest.mock import patch

import pytest

from dbt_meta.usage import RefreshAdvisor, changed_models_from_git
from dbt_meta.usage.advisor_refresh import (
    _infer_project_root,
    _read_disk_compiled,
    _run_bulk_dbt_compile,
)


def _model(short, *, sql, alias=None, materialized="table",
           unique_key=None, partition_by=None, original_file_path=None,
           depends_on=None):
    config = {"materialized": materialized}
    if unique_key:
        config["unique_key"] = unique_key
    if partition_by:
        config["partition_by"] = partition_by
    return f"model.pkg.{short}", {
        "name": short,
        "alias": alias or short,
        "schema": "ds",
        "database": "proj",
        "compiled_code": sql,
        "config": config,
        "package_name": "pkg",
        "resource_type": "model",
        "depends_on": {"nodes": depends_on or []},
        "original_file_path": original_file_path or f"models/{short}.sql",
    }


def _build_manifest(target, downstreams):
    """Build a manifest with target → downstream relations.

    Auto-fills ``depends_on.nodes`` of each downstream to point at the target,
    matching dbt's real manifest format.
    """
    target_uid = target[0]
    target_node = dict(target[1])
    nodes = {target_uid: target_node}
    child_map = {target_uid: [d[0] for d in downstreams]}
    for uid, payload in downstreams:
        node = dict(payload)
        deps = (node.get("depends_on") or {}).get("nodes") or []
        if target_uid not in deps:
            deps = [*deps, target_uid]
        node["depends_on"] = {"nodes": deps}
        nodes[uid] = node
        child_map.setdefault(uid, [])
    return {"nodes": nodes, "sources": {}, "child_map": child_map}


class TestRefreshSkipsUnusedColumn:
    def test_skips_when_column_not_referenced(self):
        target = _model("upstream", sql="SELECT 1", alias="t")
        ds = _model("ds_uses_other", sql="SELECT t.other_col FROM proj.ds.t t")
        manifest = _build_manifest(target, [ds])

        advisor = RefreshAdvisor(manifest)
        plan = advisor.plan({"upstream": {"changed_col"}})

        skipped_models = {d.model for d in plan.can_skip}
        assert "ds_uses_other" in skipped_models

    def test_skips_models_that_dont_reference_target(self):
        target = _model("upstream", sql="SELECT 1", alias="t")
        ds = _model("ds_other_table", sql="SELECT * FROM proj.ds.unrelated u WHERE u.x = 1")
        manifest = _build_manifest(target, [ds])

        advisor = RefreshAdvisor(manifest)
        plan = advisor.plan({"upstream": {"any_col"}})
        skipped = {d.model for d in plan.can_skip}
        assert "ds_other_table" in skipped


class TestRefreshFullRefresh:
    def test_changed_model_itself_in_full_refresh(self):
        target = _model("upstream", sql="SELECT 1", alias="t")
        manifest = _build_manifest(target, [])
        advisor = RefreshAdvisor(manifest)
        plan = advisor.plan({"upstream": None})
        full = {d.model for d in plan.needs_full_refresh}
        assert "upstream" in full

    def test_non_incremental_downstream_full_refresh(self):
        target = _model("upstream", sql="SELECT 1", alias="t")
        ds = _model(
            "ds_table",
            sql="SELECT t.changed_col FROM proj.ds.t t",
            materialized="table",
        )
        manifest = _build_manifest(target, [ds])
        advisor = RefreshAdvisor(manifest)
        plan = advisor.plan({"upstream": {"changed_col"}})
        full = {d.model for d in plan.needs_full_refresh}
        assert "ds_table" in full

    def test_unique_key_collision_forces_full_refresh(self):
        target = _model("upstream", sql="SELECT 1", alias="t")
        ds = _model(
            "ds_inc",
            sql="SELECT t.id FROM proj.ds.t t",
            materialized="incremental",
            unique_key="id",
        )
        manifest = _build_manifest(target, [ds])
        advisor = RefreshAdvisor(manifest)
        plan = advisor.plan({"upstream": {"id"}})
        full = {d.model for d in plan.needs_full_refresh}
        assert "ds_inc" in full

    def test_partition_key_collision_forces_full_refresh(self):
        target = _model("upstream", sql="SELECT 1", alias="t")
        ds = _model(
            "ds_inc",
            sql="SELECT t.event_date FROM proj.ds.t t",
            materialized="incremental",
            partition_by={"field": "event_date", "data_type": "date"},
        )
        manifest = _build_manifest(target, [ds])
        advisor = RefreshAdvisor(manifest)
        plan = advisor.plan({"upstream": {"event_date"}})
        full = {d.model for d in plan.needs_full_refresh}
        assert "ds_inc" in full

    def test_select_star_triggers_full_refresh(self):
        target = _model("upstream", sql="SELECT 1", alias="t")
        ds = _model(
            "ds_inc",
            sql="SELECT * FROM proj.ds.t t",
            materialized="incremental",
        )
        manifest = _build_manifest(target, [ds])
        advisor = RefreshAdvisor(manifest)
        plan = advisor.plan({"upstream": None})  # whole-model change
        full = {d.model for d in plan.needs_full_refresh}
        assert "ds_inc" in full


class TestRefreshIncremental:
    def test_incremental_with_safe_change_stays_incremental(self):
        target = _model("upstream", sql="SELECT 1", alias="t")
        ds = _model(
            "ds_inc",
            sql="SELECT t.changed_col FROM proj.ds.t t",
            materialized="incremental",
            unique_key="other_id",
        )
        manifest = _build_manifest(target, [ds])
        advisor = RefreshAdvisor(manifest)
        plan = advisor.plan({"upstream": {"changed_col"}})
        inc = {d.model for d in plan.needs_incremental}
        assert "ds_inc" in inc


class TestChainPropagation:
    """Verify that affectedness propagates through intermediate models.

    Setup: changed (A) → intermediate (B) → leaf (C). C never directly
    references A; under the old direct-only check it would be skipped.
    With chain-aware propagation, C must be detected as affected because
    B's output is affected and C consumes B's output.
    """

    def _chain_manifest(self, *, b_sql: str, c_sql: str):
        a = ("model.pkg.a", {
            "name": "a", "alias": "a", "schema": "ds", "database": "proj",
            "compiled_code": "SELECT 1 AS x", "config": {},
            "package_name": "pkg",
            "depends_on": {"nodes": []},
            "resource_type": "model",
        })
        b = ("model.pkg.b", {
            "name": "b", "alias": "b", "schema": "ds", "database": "proj",
            "compiled_code": b_sql, "config": {},
            "package_name": "pkg",
            "depends_on": {"nodes": ["model.pkg.a"]},
            "resource_type": "model",
        })
        c = ("model.pkg.c", {
            "name": "c", "alias": "c", "schema": "ds", "database": "proj",
            "compiled_code": c_sql, "config": {},
            "package_name": "pkg",
            "depends_on": {"nodes": ["model.pkg.b"]},
            "resource_type": "model",
        })
        return {
            "nodes": {a[0]: a[1], b[0]: b[1], c[0]: c[1]},
            "sources": {},
            "child_map": {a[0]: [b[0]], b[0]: [c[0]], c[0]: []},
        }

    def test_three_level_chain_select_passthrough(self):
        # B selects A.x; C selects B.x → both should land in full_refresh
        manifest = self._chain_manifest(
            b_sql="SELECT t.x FROM proj.ds.a t",
            c_sql="SELECT t.x FROM proj.ds.b t",
        )
        plan = RefreshAdvisor(manifest).plan({"a": {"x"}})
        full = {d.model for d in plan.needs_full_refresh}
        assert "b" in full
        assert "c" in full  # ← chain propagation
        assert {d.model for d in plan.can_skip} == set()

    def test_chain_breaks_when_intermediate_drops_column(self):
        # B selects A.x but emits only y; C reads B.y — change to A.x must
        # NOT mark C as affected (B dropped x from its output).
        # NOTE: heuristic — V1 still treats B as affected and propagates
        # the *upstream* col name {x}; C.y won't intersect {x} so C stays
        # in skip if there's no other reference.
        manifest = self._chain_manifest(
            b_sql="SELECT 1 AS y FROM proj.ds.a t WHERE t.x > 0",  # uses x in WHERE only
            c_sql="SELECT t.y FROM proj.ds.b t",
        )
        plan = RefreshAdvisor(manifest).plan({"a": {"x"}})
        # B uses x in WHERE → B is affected (full)
        full = {d.model for d in plan.needs_full_refresh}
        assert "b" in full
        # C only sees y; doesn't reference x → skipped
        skipped = {d.model for d in plan.can_skip}
        assert "c" in skipped

    def test_select_star_propagates_through_chain(self):
        # B selects * from A; C selects b.col — any A change must reach C
        manifest = self._chain_manifest(
            b_sql="SELECT * FROM proj.ds.a",
            c_sql="SELECT t.col FROM proj.ds.b t",
        )
        plan = RefreshAdvisor(manifest).plan({"a": {"x"}})
        full = {d.model for d in plan.needs_full_refresh}
        assert "b" in full  # SELECT * means b is whole-row affected
        assert "c" in full  # propagated through whole-row

    def test_orphan_branch_skipped(self):
        # D depends on a different upstream that wasn't changed
        m = self._chain_manifest(
            b_sql="SELECT t.x FROM proj.ds.a t",
            c_sql="SELECT t.x FROM proj.ds.b t",
        )
        m["nodes"]["model.pkg.unrelated"] = {
            "name": "unrelated", "alias": "unrelated", "schema": "ds", "database": "proj",
            "compiled_code": "SELECT 1 AS y", "config": {}, "package_name": "pkg",
            "depends_on": {"nodes": []}, "resource_type": "model",
        }
        m["nodes"]["model.pkg.d"] = {
            "name": "d", "alias": "d", "schema": "ds", "database": "proj",
            "compiled_code": "SELECT t.y FROM proj.ds.unrelated t", "config": {},
            "package_name": "pkg",
            "depends_on": {"nodes": ["model.pkg.unrelated"]},
            "resource_type": "model",
        }
        m["child_map"]["model.pkg.unrelated"] = []
        # d not in transitive of a → not surfaced anywhere
        plan = RefreshAdvisor(m).plan({"a": {"x"}})
        all_models = {d.model for d in plan.needs_full_refresh + plan.needs_incremental + plan.can_skip}
        assert "d" not in all_models  # not even in scope


class TestProjectRootInference:
    def test_returns_none_when_path_is_none(self):
        assert _infer_project_root(None) is None

    def test_returns_none_when_no_dbt_project_yml(self, tmp_path):
        manifest = tmp_path / "manifest.json"
        manifest.write_text("{}")
        assert _infer_project_root(str(manifest)) is None

    def test_walks_up_to_find_dbt_project_yml(self, tmp_path):
        (tmp_path / "dbt_project.yml").write_text("name: test\n")
        target_dir = tmp_path / "target"
        target_dir.mkdir()
        manifest = target_dir / "manifest.json"
        manifest.write_text("{}")
        assert _infer_project_root(str(manifest)) == str(tmp_path)

    def test_falls_back_to_cwd_when_manifest_outside_project(self, tmp_path, monkeypatch):
        # Simulates: prod manifest at ~/dbt-state/manifest.json + cwd inside reports/
        outside = tmp_path / "dbt-state"
        outside.mkdir()
        manifest = outside / "manifest.json"
        manifest.write_text("{}")
        project = tmp_path / "reports"
        project.mkdir()
        (project / "dbt_project.yml").write_text("name: reports\n")
        (project / "target").mkdir()
        monkeypatch.chdir(project)
        assert _infer_project_root(str(manifest)) == str(project)

    def test_cwd_fallback_requires_target_directory(self, tmp_path, monkeypatch):
        # A stray dbt_project.yml without target/ next to it is NOT enough —
        # protects against false positives like ~/Projects/dbt_project.yml.
        stray = tmp_path / "stray"
        stray.mkdir()
        (stray / "dbt_project.yml").write_text("name: stray\n")
        # No 'target/' directory here.
        monkeypatch.chdir(stray)
        assert _infer_project_root(None) is None


class TestDiskCompiledRead:
    def test_reads_existing_file(self, tmp_path):
        compiled = tmp_path / "target" / "compiled" / "test_pkg" / "models" / "x.sql"
        compiled.parent.mkdir(parents=True)
        compiled.write_text("SELECT compiled_sql_here")
        node = {"package_name": "test_pkg", "original_file_path": "models/x.sql"}
        assert "compiled_sql_here" in _read_disk_compiled(str(tmp_path), node)

    def test_falls_back_to_unique_id_for_package(self, tmp_path):
        compiled = tmp_path / "target" / "compiled" / "test_pkg" / "models" / "x.sql"
        compiled.parent.mkdir(parents=True)
        compiled.write_text("SELECT 1")
        node = {"unique_id": "model.test_pkg.x", "original_file_path": "models/x.sql"}
        assert _read_disk_compiled(str(tmp_path), node) == "SELECT 1"

    def test_returns_empty_when_missing_file(self, tmp_path):
        node = {"package_name": "p", "original_file_path": "models/x.sql"}
        assert _read_disk_compiled(str(tmp_path), node) == ""

    def test_returns_empty_for_empty_file(self, tmp_path):
        compiled = tmp_path / "target" / "compiled" / "p" / "x.sql"
        compiled.parent.mkdir(parents=True)
        compiled.write_text("")
        node = {"package_name": "p", "original_file_path": "x.sql"}
        assert _read_disk_compiled(str(tmp_path), node) == ""

    def test_returns_empty_when_missing_metadata(self, tmp_path):
        # No package_name and no derivable unique_id
        node = {"original_file_path": "models/x.sql"}
        assert _read_disk_compiled(str(tmp_path), node) == ""


class TestRefreshDiskFallback:
    def test_uses_disk_compiled_when_manifest_empty(self, tmp_path):
        # Set up: target with empty compiled_code in manifest, but disk has SQL
        (tmp_path / "dbt_project.yml").write_text("name: test\n")
        compiled_dir = tmp_path / "target" / "compiled" / "pkg" / "models"
        compiled_dir.mkdir(parents=True)
        (compiled_dir / "ds.sql").write_text("SELECT t.changed_col FROM proj.ds.upstream t")
        (compiled_dir / "upstream.sql").write_text("SELECT 1")
        manifest_path = tmp_path / "target" / "manifest.json"
        manifest_path.write_text("{}")  # not actually loaded — passed in as data

        # Build manifest in-memory
        target = ("model.pkg.upstream", {
            "name": "upstream",
            "alias": "upstream",
            "schema": "ds",
            "database": "proj",
            "package_name": "pkg",
            "compiled_code": "",   # empty in manifest
            "original_file_path": "models/upstream.sql",
            "config": {},
            "depends_on": {"nodes": []},
            "resource_type": "model",
        })
        ds = ("model.pkg.ds", {
            "name": "ds",
            "alias": "ds",
            "schema": "ds",
            "database": "proj",
            "package_name": "pkg",
            "compiled_code": "",   # empty — must come from disk
            "original_file_path": "models/ds.sql",
            "config": {"materialized": "table"},
            "depends_on": {"nodes": [target[0]]},
            "resource_type": "model",
        })
        manifest = {
            "nodes": {target[0]: target[1], ds[0]: ds[1]},
            "sources": {},
            "child_map": {target[0]: [ds[0]], ds[0]: []},
        }

        advisor = RefreshAdvisor(
            manifest,
            manifest_path=str(manifest_path),
            auto_compile=False,  # disable subprocess; rely on disk only
        )
        plan = advisor.plan({"upstream": {"changed_col"}})
        full_models = {d.model for d in plan.needs_full_refresh}
        # ds_inc references changed_col via disk-read SQL → must be full_refresh
        assert "ds" in full_models


class TestBulkCompileTrigger:
    def test_skipped_when_no_project_root(self):
        manifest = {
            "nodes": {
                "model.pkg.up": {
                    "name": "up", "alias": "up", "schema": "s", "database": "d",
                    "compiled_code": "SELECT 1", "config": {},
                    "depends_on": {"nodes": []}, "resource_type": "model",
                },
                "model.pkg.ds": {
                    "name": "ds", "alias": "ds", "schema": "s", "database": "d",
                    "compiled_code": "",  # missing
                    "config": {"materialized": "table"},
                    "depends_on": {"nodes": ["model.pkg.up"]},
                    "resource_type": "model",
                },
            },
            "sources": {},
            "child_map": {"model.pkg.up": ["model.pkg.ds"], "model.pkg.ds": []},
        }
        # No manifest_path → no project root → bulk compile must NOT be attempted
        advisor = RefreshAdvisor(manifest, auto_compile=True)
        with patch("dbt_meta.usage.advisor_refresh._run_bulk_dbt_compile") as mock_run:
            advisor.plan({"up": None})
        mock_run.assert_not_called()

    def test_triggered_when_majority_missing(self, tmp_path):
        (tmp_path / "dbt_project.yml").write_text("name: test\n")
        manifest_path = tmp_path / "target" / "manifest.json"
        manifest_path.parent.mkdir()
        manifest_path.write_text("{}")
        # 5 downstream, all missing compiled_code, no disk fallback
        nodes = {
            "model.pkg.up": {
                "name": "up", "alias": "up", "schema": "s", "database": "d",
                "compiled_code": "SELECT 1", "config": {},
                "depends_on": {"nodes": []}, "resource_type": "model",
            }
        }
        child_map = {"model.pkg.up": []}
        for i in range(5):
            uid = f"model.pkg.ds{i}"
            nodes[uid] = {
                "name": f"ds{i}", "alias": f"ds{i}", "schema": "s", "database": "d",
                "compiled_code": "",
                "config": {"materialized": "table"},
                "package_name": "pkg",
                "original_file_path": f"models/ds{i}.sql",
                "depends_on": {"nodes": ["model.pkg.up"]},
                "resource_type": "model",
            }
            child_map["model.pkg.up"].append(uid)
            child_map[uid] = []
        manifest = {"nodes": nodes, "sources": {}, "child_map": child_map}

        advisor = RefreshAdvisor(
            manifest, manifest_path=str(manifest_path), auto_compile=True,
        )
        with patch(
            "dbt_meta.usage.advisor_refresh._run_bulk_dbt_compile",
            return_value=(False, "dbt not configured"),
        ) as mock_run:
            plan = advisor.plan({"up": None})

        mock_run.assert_called_once()
        # Failed compile → warning recorded
        assert any("bulk dbt compile failed" in w for w in plan.warnings)

    def test_triggered_when_any_downstream_missing_compiled_code(self, tmp_path):
        # Regression: old sample-based heuristic skipped compile when the
        # first 20 downstream models had SQL but later ones didn't. Verify
        # the new logic scans the entire downstream set and triggers compile
        # for as little as one missing model.
        (tmp_path / "dbt_project.yml").write_text("name: test\n")
        manifest_path = tmp_path / "target" / "manifest.json"
        manifest_path.parent.mkdir()
        manifest_path.write_text("{}")

        nodes = {
            "model.pkg.up": {
                "name": "up", "alias": "up", "schema": "s", "database": "d",
                "compiled_code": "SELECT 1", "config": {},
                "depends_on": {"nodes": []}, "resource_type": "model",
            }
        }
        child_map = {"model.pkg.up": []}
        # 22 downstream: 21 already compiled, only the last one missing
        for i in range(22):
            uid = f"model.pkg.ds{i:02d}"
            nodes[uid] = {
                "name": f"ds{i:02d}", "alias": f"ds{i:02d}", "schema": "s", "database": "d",
                "compiled_code": "" if i == 21 else "SELECT 1",
                "config": {"materialized": "table"},
                "package_name": "pkg",
                "original_file_path": f"models/ds{i:02d}.sql",
                "depends_on": {"nodes": ["model.pkg.up"]},
                "resource_type": "model",
            }
            child_map["model.pkg.up"].append(uid)
            child_map[uid] = []
        manifest = {"nodes": nodes, "sources": {}, "child_map": child_map}

        advisor = RefreshAdvisor(
            manifest, manifest_path=str(manifest_path), auto_compile=True,
        )
        with patch(
            "dbt_meta.usage.advisor_refresh._run_bulk_dbt_compile",
            return_value=(True, None),
        ) as mock_run:
            advisor.plan({"up": None})

        mock_run.assert_called_once()
        called_models = mock_run.call_args[0][0]
        # Only the missing model is in the --select list (not all 22)
        assert called_models == ["ds21"]


class TestBulkDbtCompileLauncher:
    def test_returns_failure_when_dbt_missing_from_path(self, tmp_path, monkeypatch):
        monkeypatch.setenv("PATH", "")  # ensure dbt cannot be found
        ok, err = _run_bulk_dbt_compile(["m1"], str(tmp_path))
        assert ok is False
        assert "dbt CLI not found" in (err or "")


class TestChangedModelsFromGit:
    def test_resolves_modified_paths_to_models(self):
        manifest = {
            "nodes": {
                "model.p.m1": {"original_file_path": "models/m1.sql", "resource_type": "model"},
                "model.p.m2": {"original_file_path": "models/m2.sql", "resource_type": "model"},
                "model.p.m3": {"original_file_path": "models/m3.sql", "resource_type": "model"},
            }
        }
        out = changed_models_from_git(["models/m1.sql", "models/m3.sql"], manifest)
        assert set(out.keys()) == {"m1", "m3"}
        assert all(v is None for v in out.values())

    def test_ignores_paths_not_in_manifest(self):
        manifest = {"nodes": {"model.p.m1": {"original_file_path": "models/m1.sql"}}}
        out = changed_models_from_git(["models/unknown.sql"], manifest)
        assert out == {}


class TestRefreshCliAllModelsMissing:
    """CLI exits non-zero when none of the supplied models exist.

    Regression: previously the CLI ran the advisor anyway, which emitted
    an empty summary plus a buried "not in manifest" warning, then exited
    0 — readable as "success / nothing to do" by shells and CI.
    """

    def _setup(self, tmp_path):
        import json as _json

        manifest = {
            "nodes": {
                "model.pkg.existing_model": {
                    "name": "existing_model",
                    "alias": "existing_model",
                    "schema": "ds",
                    "database": "proj",
                    "compiled_code": "SELECT 1",
                    "config": {"materialized": "table"},
                    "package_name": "pkg",
                    "resource_type": "model",
                    "depends_on": {"nodes": []},
                    "original_file_path": "models/existing_model.sql",
                }
            },
            "sources": {},
            "child_map": {"model.pkg.existing_model": []},
            "parent_map": {"model.pkg.existing_model": []},
        }
        manifest_path = tmp_path / "manifest.json"
        manifest_path.write_text(_json.dumps(manifest))
        return str(manifest_path)

    def _runner(self):
        from typer.testing import CliRunner

        from dbt_meta.cli import app

        return CliRunner(), app

    def test_exits_nonzero_when_all_models_missing(self, tmp_path):
        manifest_path = self._setup(tmp_path)
        runner, app = self._runner()
        result = runner.invoke(
            app,
            [
                "optimize", "refresh",
                "ghost_one", "ghost_two",
                "--no-compile",
                "--manifest", manifest_path,
            ],
        )
        assert result.exit_code == 1
        assert "None of the specified models exist" in result.output

    def test_emits_json_error_when_all_missing(self, tmp_path):
        import json as _json

        manifest_path = self._setup(tmp_path)
        runner, app = self._runner()
        result = runner.invoke(
            app,
            [
                "optimize", "refresh",
                "ghost",
                "--no-compile",
                "--manifest", manifest_path,
                "-j",
            ],
        )
        assert result.exit_code == 1
        payload = _json.loads(result.stdout)
        assert "None of the specified models exist" in payload["error"]

    def test_exits_zero_when_at_least_one_model_valid(self, tmp_path):
        manifest_path = self._setup(tmp_path)
        runner, app = self._runner()
        result = runner.invoke(
            app,
            [
                "optimize", "refresh",
                "existing_model", "ghost",
                "--no-compile",
                "--manifest", manifest_path,
            ],
        )
        assert result.exit_code == 0
        # The known model is still planned; only the unknown one warns.
        assert "existing_model" in result.output


class TestRefreshGitUntrackedDetection:
    """``meta optimize refresh -m`` must walk into untracked directories.

    Regression: ``git status --porcelain`` defaults to collapsing untracked
    directories to a single entry (``?? models/new_dir/``), which never
    matches any model's ``original_file_path``. Without ``-uall`` /
    ``--untracked-files=all`` new models in fresh directories silently
    fall out of the plan.
    """

    def test_git_status_called_with_untracked_files_all(self, tmp_path, monkeypatch):
        import json as _json
        import subprocess

        from typer.testing import CliRunner

        from dbt_meta.cli import app

        manifest = {
            "nodes": {
                "model.pkg.brand_new": {
                    "name": "brand_new",
                    "alias": "brand_new",
                    "schema": "ds",
                    "database": "proj",
                    "compiled_code": "SELECT 1",
                    "config": {"materialized": "table"},
                    "package_name": "pkg",
                    "resource_type": "model",
                    "depends_on": {"nodes": []},
                    "original_file_path": "models/new_dir/brand_new.sql",
                }
            },
            "sources": {},
            "child_map": {"model.pkg.brand_new": []},
            "parent_map": {"model.pkg.brand_new": []},
        }
        manifest_path = tmp_path / "manifest.json"
        manifest_path.write_text(_json.dumps(manifest))

        captured_calls: list[list[str]] = []

        class _FakeResult:
            def __init__(self, stdout: str = "", returncode: int = 0):
                self.stdout = stdout
                self.stderr = ""
                self.returncode = returncode

        def _fake_run(cmd, capture_output=False, text=False, timeout=None, **kwargs):
            captured_calls.append(list(cmd))
            if cmd[:2] != ["git", "diff"] and cmd[:3] != ["git", "status", "--porcelain"]:
                return _FakeResult()
            if cmd[:2] == ["git", "diff"]:
                # Empty committed/uncommitted diffs.
                return _FakeResult(stdout="")
            # git status --porcelain ... — emulate the -uall expansion by
            # returning the individual .sql file inside the new directory.
            assert "--untracked-files=all" in cmd, (
                f"refresh -m must request per-file untracked listing, got: {cmd!r}"
            )
            return _FakeResult(stdout="?? models/new_dir/brand_new.sql\n")

        monkeypatch.setattr(subprocess, "run", _fake_run)

        runner = CliRunner()
        result = runner.invoke(
            app,
            [
                "optimize", "refresh",
                "-m",
                "--no-compile",
                "--manifest", str(manifest_path),
                "--base", "origin/master",
            ],
        )
        assert result.exit_code == 0, result.output
        # The model from the untracked directory must show up as
        # untracked-sourced in the plan output.
        assert "brand_new" in result.output
        assert "untracked" in result.output
        # And the assertion inside the fake `run` confirmed --untracked-files=all
        # was actually passed; this guards against accidental regression.
        assert any("--untracked-files=all" in call for call in captured_calls)
