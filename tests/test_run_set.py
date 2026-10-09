"""Tests for ``meta run-set``: the closed ``--select`` list for ``defer run``.

Invariant: RunSet(T) = T ∪ every node on a path from a branch-modified
ancestor down to a target, unmodified intermediates included; modified
nodes that feed no target are left out.
"""

import json
import os
import subprocess
from pathlib import Path

import pytest

from dbt_meta.command_impl import run_set as run_set_mod
from dbt_meta.command_impl.run_set import RunSetCommand
from dbt_meta.errors import DbtMetaError
from dbt_meta.manifest.graph import ManifestGraph
from dbt_meta.utils import get_cached_parser
from dbt_meta.utils.git import branch_changed_files

P = "model.p."


def _model(name, parents=(), materialized="table", patch=None, macros=(), config=None):
    cfg = {"materialized": materialized}
    cfg.update(config or {})
    return {
        "original_file_path": f"models/{name}.sql",
        "patch_path": f"p://models/{patch}" if patch else None,
        "depends_on": {"nodes": list(parents), "macros": list(macros)},
        "config": cfg,
    }


def _seed(name):
    return {
        "original_file_path": f"seeds/{name}.csv",
        "depends_on": {"nodes": []},
        "config": {"materialized": "seed"},
    }


def _write(tmp_path, nodes, name="manifest.json", macros=None):
    path = tmp_path / name
    path.write_text(json.dumps({"nodes": nodes, "macros": macros or {}}))
    return str(path)


def _changed(*names):
    return {f"models/{n}.sql": "uncommitted" for n in names}


def _run(manifest, files, targets=None, prod=None, **kwargs):
    get_cached_parser.cache_clear()
    cmd = RunSetCommand(manifest, prod, files, parse=False)
    return cmd.execute(targets, **kwargs)


def _names(result):
    return [e["model"] for e in result["run_set"]]


@pytest.fixture(autouse=True)
def _cwd(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)


@pytest.fixture
def chain(tmp_path):
    """m2 (mod) → x → m1 (mod) → report; m3 (mod) unrelated."""
    return _write(
        tmp_path,
        {
            P + "m2": _model("m2"),
            P + "x": _model("x", [P + "m2"], materialized="incremental"),
            P + "m1": _model("m1", [P + "x"]),
            P + "report": _model("report", [P + "m1"]),
            P + "m3": _model("m3"),
        },
    )


class TestInvariant:
    def test_chain_includes_unmodified_intermediate(self, chain):
        r = _run(chain, _changed("m2", "m1", "m3"), ["m1"])
        assert _names(r) == ["m2", "x", "m1"]
        assert r["command"] == "defer run -s m2 x m1"
        roles = {e["model"]: e for e in r["run_set"]}
        assert roles["x"]["role"] == "intermediate"
        assert roles["x"]["materialized"] == "incremental"
        assert roles["m2"]["role"] == "modified"
        assert roles["m1"] == {
            "model": "m1",
            "role": "target",
            "modified": True,
            "resource_type": "model",
            "materialized": "table",
        }

    def test_unrelated_modified_is_ignored(self, chain):
        r = _run(chain, _changed("m2", "m1", "m3"), ["m1"])
        assert r["ignored_modified"] == ["m3"]
        assert r["modified_upstream"] == ["m1", "m2"]
        assert "m3" not in _names(r)

    def test_target_without_modified_ancestors(self, chain):
        r = _run(chain, {}, ["m1"])
        assert _names(r) == ["m1"]

    def test_inspection_target_downstream(self, chain):
        r = _run(chain, _changed("m2"), ["report"])
        assert _names(r) == ["m2", "x", "m1", "report"]
        assert r["run_set"][-1]["modified"] is False

    def test_all_targets_every_modified(self, chain):
        r = _run(chain, _changed("m2", "m1", "m3"), use_all=True)
        assert set(_names(r)) == {"m2", "x", "m1", "m3"}
        assert r["ignored_modified"] == []

    def test_diamond_both_branches(self, tmp_path):
        path = _write(
            tmp_path,
            {
                P + "a": _model("a"),
                P + "b": _model("b"),
                P + "xa": _model("xa", [P + "a"]),
                P + "xb": _model("xb", [P + "b"]),
                P + "t": _model("t", [P + "xa", P + "xb"]),
            },
        )
        r = _run(path, _changed("a", "b"), ["t"])
        assert set(_names(r)) == {"a", "b", "xa", "xb", "t"}

    def test_diamond_only_modified_branch(self, tmp_path):
        path = _write(
            tmp_path,
            {
                P + "a": _model("a"),
                P + "xa": _model("xa", [P + "a"]),
                P + "xb": _model("xb", [P + "a"]),
                P + "m": _model("m", [P + "xa"]),
                P + "t": _model("t", [P + "m", P + "xb"]),
            },
        )
        r = _run(path, _changed("m"), ["t"])
        assert _names(r) == ["m", "t"]

    def test_modified_ephemeral_not_emitted_but_path_kept(self, tmp_path):
        path = _write(
            tmp_path,
            {
                P + "e": _model("e", materialized="ephemeral"),
                P + "x": _model("x", [P + "e"]),
                P + "t": _model("t", [P + "x"]),
            },
        )
        r = _run(path, _changed("e"), ["t"])
        assert _names(r) == ["x", "t"]

    def test_ephemeral_intermediate_traversed(self, tmp_path):
        path = _write(
            tmp_path,
            {
                P + "m": _model("m"),
                P + "e": _model("e", [P + "m"], materialized="ephemeral"),
                P + "t": _model("t", [P + "e"]),
            },
        )
        r = _run(path, _changed("m"), ["t"])
        assert _names(r) == ["m", "t"]

    def test_ephemeral_target_warns(self, tmp_path):
        path = _write(tmp_path, {P + "e": _model("e", materialized="ephemeral")})
        r = _run(path, {}, ["e"])
        assert _names(r) == []
        assert r["command"] == ""
        assert any("ephemeral" in w for w in r["warnings"])

    def _snapshot_chain(self, tmp_path):
        return _write(
            tmp_path,
            {
                P + "m": _model("m"),
                "snapshot.p.snap": {
                    "original_file_path": "snapshots/snap.sql",
                    "depends_on": {"nodes": [P + "m"]},
                    "config": {"materialized": "snapshot"},
                },
                P + "x": _model("x", ["snapshot.p.snap"]),
                P + "t": _model("t", [P + "x"]),
            },
        )

    def test_snapshot_on_path_traversed_not_emitted(self, tmp_path):
        r = _run(self._snapshot_chain(tmp_path), _changed("m"), ["t"])
        assert _names(r) == ["m", "x", "t"]
        assert r["command"] == "defer run -s m x t"
        warning = next(w for w in r["warnings"] if w.startswith("snapshot snap"))
        assert "m (modified) → snap → x → t" in warning
        assert "defer snapshot -s snap" in warning

    def test_snapshot_on_path_check_reports_intermediate_only(self, tmp_path):
        r = _run(self._snapshot_chain(tmp_path), _changed("m"), ["m", "t"], check=True)
        assert [x["model"] for x in r["missing"]] == ["x"]
        r = _run(self._snapshot_chain(tmp_path), _changed("m"), ["m", "x", "t"], check=True)
        assert r["closed"] is True

    def test_snapshot_is_not_a_target(self, tmp_path):
        with pytest.raises(DbtMetaError, match="not in the dev manifest"):
            _run(self._snapshot_chain(tmp_path), {}, ["snap"])

    def test_changed_snapshot_file_is_warning_not_modified(self, tmp_path):
        r = _run(self._snapshot_chain(tmp_path), {"snapshots/snap.sql": "uncommitted"}, ["t"])
        assert _names(r) == ["t"]
        assert r["modified"] == []
        assert any("snapshot changed: snap" in w for w in r["warnings"])

    def test_modified_seed_gets_seed_command(self, tmp_path):
        path = _write(
            tmp_path,
            {
                "seed.p.s": _seed("s"),
                P + "x": _model("x", ["seed.p.s"]),
                P + "t": _model("t", [P + "x"]),
            },
        )
        r = _run(path, {"seeds/s.csv": "uncommitted"}, ["t"])
        assert _names(r) == ["s", "x", "t"]
        assert r["run_set"][0]["resource_type"] == "seed"
        assert r["command"] == "defer seed -s s && defer run -s x t"

    def test_unknown_target_is_an_error(self, chain):
        with pytest.raises(DbtMetaError, match="not in the dev manifest"):
            _run(chain, {}, ["nope"])


class TestModifiedDetection:
    def test_untracked_new_model(self, tmp_path):
        path = _write(
            tmp_path,
            {
                P + "new": _model("new"),
                P + "t": _model("t", [P + "new"]),
            },
        )
        r = _run(path, {"models/new.sql": "untracked"}, ["t"])
        assert _names(r) == ["new", "t"]

    def test_deleted_model_warns(self, chain):
        r = _run(chain, {"models/gone.sql": "committed"}, ["m1"])
        assert any("gone.sql: deleted" in w for w in r["warnings"])

    def test_sql_missing_from_manifest_warns(self, chain, tmp_path):
        (tmp_path / "models").mkdir()
        (tmp_path / "models" / "fresh.sql").write_text("select 1")
        r = _run(chain, {"models/fresh.sql": "untracked"}, ["m1"])
        assert any("not in the dev manifest" in w for w in r["warnings"])

    def _yml_project(self, tmp_path, dev_cfg, prod_cfg, prod_has_t=True):
        dev = _write(
            tmp_path,
            {
                P + "m": _model("m", patch="schema.yml", config=dev_cfg),
                P + "t": _model("t", [P + "m"]),
            },
        )
        prod_nodes = (
            {P + "m": _model("m", patch="schema.yml", config=prod_cfg)} if prod_has_t else {}
        )
        prod = _write(tmp_path, prod_nodes, name="prod.json")
        return dev, prod

    def test_yml_config_change_counts(self, tmp_path):
        dev, prod = self._yml_project(tmp_path, {"cluster_by": ["a"]}, {})
        r = _run(dev, {"models/schema.yml": "uncommitted"}, ["t"], prod=prod)
        assert _names(r) == ["m", "t"]

    def test_yml_description_only_does_not_count(self, tmp_path):
        dev, prod = self._yml_project(tmp_path, {}, {})
        r = _run(dev, {"models/schema.yml": "uncommitted"}, ["t"], prod=prod)
        assert _names(r) == ["t"]

    def test_yml_node_new_to_prod_counts(self, tmp_path):
        dev, prod = self._yml_project(tmp_path, {}, {}, prod_has_t=False)
        r = _run(dev, {"models/schema.yml": "uncommitted"}, ["t"], prod=prod)
        assert _names(r) == ["m", "t"]

    def test_yml_without_prod_manifest_counts_and_warns(self, tmp_path):
        dev, _ = self._yml_project(tmp_path, {}, {})
        r = _run(
            dev, {"models/schema.yml": "uncommitted"}, ["t"], prod=str(tmp_path / "missing.json")
        )
        assert _names(r) == ["m", "t"]
        assert any("prod manifest not found" in w for w in r["warnings"])

    def test_docs_md_ignored(self, chain):
        r = _run(chain, {"models/docs/m2.md": "uncommitted"}, ["m1"])
        assert _names(r) == ["m1"]
        assert r["modified"] == []

    def test_changed_macro_warns_only(self, tmp_path):
        path = _write(
            tmp_path,
            {
                P + "a": _model("a", macros=["macro.p.fx"]),
                P + "t": _model("t", [P + "a"]),
                P + "other": _model("other", macros=["macro.p.fx"]),
            },
            macros={"macro.p.fx": {"original_file_path": "macros/fx.sql"}},
        )
        r = _run(path, {"macros/fx.sql": "uncommitted"}, ["t"])
        assert _names(r) == ["t"]
        assert r["macro_warnings"] == [{"macro": "fx", "models": ["a"]}]

    def test_snapshot_warns(self, tmp_path):
        path = _write(
            tmp_path,
            {
                "snapshot.p.snap": {
                    "original_file_path": "snapshots/snap.sql",
                    "depends_on": {"nodes": []},
                },
                P + "t": _model("t"),
            },
        )
        r = _run(path, {"snapshots/snap.sql": "uncommitted"}, ["t"])
        assert any("snapshot changed: snap" in w for w in r["warnings"])


class TestCheck:
    def test_incomplete_list_reports_missing_with_chain(self, chain):
        r = _run(chain, _changed("m2", "m1"), ["m2", "m1"], check=True)
        assert r["closed"] is False
        assert r["missing"] == [{"model": "x", "reason": "on path m2 (modified) → x → m1"}]
        assert r["command"] == "defer run -s m2 x m1"

    def test_single_target_misses_parent_and_intermediate(self, chain):
        r = _run(chain, _changed("m2", "m1"), ["m1"], check=True)
        assert [m["model"] for m in r["missing"]] == ["m2", "x"]

    def test_closed_list(self, chain):
        r = _run(chain, _changed("m2", "m1", "m3"), ["m2", "x", "m1"], check=True)
        assert r["closed"] is True
        assert r["missing"] == []

    def test_extra_models_allowed(self, chain):
        r = _run(chain, _changed("m2", "m1", "m3"), ["m2", "x", "m1", "m3"], check=True)
        assert r["closed"] is True

    @pytest.mark.parametrize(
        "selector", ["tag:daily", "m1+", "+m1", "@m1", "path:models", "m1,m2", "m*"]
    )
    def test_selector_syntax_rejected(self, chain, selector):
        with pytest.raises(DbtMetaError, match="pass explicit model names"):
            _run(chain, {}, [selector], check=True)

    def test_seed_not_required_in_check(self, tmp_path):
        path = _write(
            tmp_path,
            {
                "seed.p.s": _seed("s"),
                P + "t": _model("t", ["seed.p.s"]),
            },
        )
        r = _run(path, {"seeds/s.csv": "uncommitted"}, ["t"], check=True)
        assert r["closed"] is True
        assert r["seed_warnings"] and "defer seed -s s" in r["seed_warnings"][0]


class TestFreshness:
    @pytest.fixture
    def project(self, tmp_path):
        (tmp_path / "dbt_project.yml").write_text("name: p\n")
        (tmp_path / "models").mkdir()
        (tmp_path / "models" / "m1.sql").write_text("select 1")
        target = tmp_path / "target"
        target.mkdir()
        path = _write(target, {P + "m1": _model("m1")})
        return tmp_path, path

    def _cmd(self, path, files=None):
        get_cached_parser.cache_clear()
        return RunSetCommand(path, None, files or {}, parse=True)

    def test_newer_non_model_file_does_not_trigger_parse(self, project, monkeypatch):
        root, path = project
        old = os.path.getmtime(path) - 100
        os.utime(root / "models" / "m1.sql", (old, old))
        (root / "models" / "notes.md").write_text("# docs")  # newer, but not .sql/.yml
        calls = []
        monkeypatch.setattr(
            run_set_mod, "run_dbt_parse", lambda *a, **k: calls.append(a) or (True, None)
        )
        self._cmd(path).execute(["m1"])
        assert calls == []

    def test_fresh_manifest_skips_parse(self, project, monkeypatch):
        root, path = project
        old = os.path.getmtime(path) - 100
        os.utime(root / "models" / "m1.sql", (old, old))
        calls = []
        monkeypatch.setattr(
            run_set_mod, "run_dbt_parse", lambda *a, **k: calls.append(a) or (True, None)
        )
        self._cmd(path).execute(["m1"])
        assert calls == []

    def test_newer_sql_triggers_parse(self, project, monkeypatch):
        root, path = project
        new = os.path.getmtime(path) + 100
        os.utime(root / "models" / "m1.sql", (new, new))
        calls = []
        monkeypatch.setattr(
            run_set_mod, "run_dbt_parse", lambda *a, **k: calls.append(a) or (True, None)
        )
        self._cmd(path).execute(["m1"])
        assert len(calls) == 1

    def test_changed_sql_missing_from_manifest_triggers_parse(self, project, monkeypatch):
        root, path = project
        old = os.path.getmtime(path) - 100
        (root / "models" / "m2.sql").write_text("select 2")
        for f in ("m1.sql", "m2.sql"):
            os.utime(root / "models" / f, (old, old))
        calls = []
        monkeypatch.setattr(
            run_set_mod, "run_dbt_parse", lambda *a, **k: calls.append(a) or (True, None)
        )
        self._cmd(path, {"models/m2.sql": "untracked"}).execute(["m1"])
        assert len(calls) == 1

    def test_parse_failure_is_an_error(self, project, monkeypatch):
        root, path = project
        new = os.path.getmtime(path) + 100
        os.utime(root / "models" / "m1.sql", (new, new))
        monkeypatch.setattr(
            run_set_mod, "run_dbt_parse", lambda *a, **k: (False, "Compilation Error\nboom")
        )
        with pytest.raises(DbtMetaError, match="dbt parse` failed"):
            self._cmd(path).execute(["m1"])

    def test_missing_manifest_without_parse(self, tmp_path):
        get_cached_parser.cache_clear()
        with pytest.raises(DbtMetaError, match="dev manifest not found"):
            RunSetCommand(str(tmp_path / "none.json"), None, {}, parse=False).execute(["m1"])

    def test_missing_manifest_triggers_parse(self, project, monkeypatch):
        root, path = project
        os.remove(path)

        def _parse(*a, **k):
            _write(root / "target", {P + "m1": _model("m1")})
            return True, None

        monkeypatch.setattr(run_set_mod, "run_dbt_parse", _parse)
        r = self._cmd(path).execute(["m1"])
        assert _names(r) == ["m1"]

    def test_no_project_root_skips_parse(self, tmp_path, monkeypatch):
        path = _write(tmp_path, {P + "m1": _model("m1")})
        monkeypatch.setattr(run_set_mod, "infer_project_root", lambda _p: None)
        monkeypatch.setattr(
            run_set_mod, "run_dbt_parse", lambda *a, **k: pytest.fail("parse called")
        )
        r = self._cmd(path).execute(["m1"])
        assert _names(r) == ["m1"]


class TestEdgeCells:
    def test_duplicate_targets_deduplicated(self, chain):
        r = _run(chain, _changed("m2"), ["m1", "m1"])
        assert r["targets"] == ["m1"]
        assert _names(r) == ["m2", "x", "m1"]

    @pytest.mark.parametrize("name", ["m1; rm -rf /", "модель", "", "M1"])
    def test_odd_names_are_unknown_not_executed(self, chain, name):
        with pytest.raises(DbtMetaError, match="not in the dev manifest"):
            _run(chain, {}, [name])

    def test_all_with_no_changes_is_empty(self, chain):
        r = _run(chain, {}, use_all=True)
        assert r["targets"] == [] and r["run_set"] == [] and r["command"] == ""

    def test_all_with_only_seed_change_targets_the_seed(self, tmp_path):
        path = _write(tmp_path, {"seed.p.s": _seed("s"), P + "t": _model("t", ["seed.p.s"])})
        r = _run(path, {"seeds/s.csv": "committed"}, use_all=True)
        assert r["targets"] == ["s"]
        assert r["ignored_modified"] == []
        assert r["run_set"][0]["role"] == "target"

    def test_empty_manifest(self, tmp_path):
        path = _write(tmp_path, {})
        with pytest.raises(DbtMetaError, match="not in the dev manifest"):
            _run(path, {}, ["m1"])

    def test_node_without_depends_on(self, tmp_path):
        path = _write(
            tmp_path, {P + "bare": {"original_file_path": "models/bare.sql", "config": {}}}
        )
        r = _run(path, _changed("bare"), ["bare"])
        assert _names(r) == ["bare"]
        assert r["run_set"][0]["materialized"] == ""

    def test_parent_outside_graph_ignored(self, tmp_path):
        path = _write(tmp_path, {P + "t": _model("t", ["source.p.raw.events", "model.other.gone"])})
        r = _run(path, {}, ["t"])
        assert _names(r) == ["t"]

    def test_large_chain(self, tmp_path):
        n = 2000
        nodes = {P + "n0": _model("n0")}
        for i in range(1, n):
            nodes[P + f"n{i}"] = _model(f"n{i}", [P + f"n{i - 1}"])
        path = _write(tmp_path, nodes)
        r = _run(path, _changed("n0"), [f"n{n - 1}"], check=True)
        assert len(r["run_set"]) == n
        assert len(r["missing"]) == n - 1
        assert r["run_set"][0]["model"] == "n0"


def _git(repo, *args):
    subprocess.run(["git", *args], cwd=repo, check=True, capture_output=True)


class TestBranchChangedFiles:
    @pytest.fixture
    def repo(self, tmp_path, monkeypatch):
        _git(tmp_path, "init", "-q", "-b", "main")
        _git(tmp_path, "config", "user.email", "t@example.com")
        _git(tmp_path, "config", "user.name", "t")
        (tmp_path / "base.sql").write_text("1")
        (tmp_path / "staged.sql").write_text("1")
        (tmp_path / "unstaged.sql").write_text("1")
        _git(tmp_path, "add", ".")
        _git(tmp_path, "commit", "-q", "-m", "base")
        _git(tmp_path, "checkout", "-q", "-b", "feature")
        (tmp_path / "committed.sql").write_text("1")
        _git(tmp_path, "add", ".")
        _git(tmp_path, "commit", "-q", "-m", "feature")
        (tmp_path / "staged.sql").write_text("2")
        _git(tmp_path, "add", "staged.sql")
        (tmp_path / "unstaged.sql").write_text("2")
        (tmp_path / "new_dir").mkdir()
        (tmp_path / "new_dir" / "fresh.sql").write_text("1")
        monkeypatch.chdir(tmp_path)
        return tmp_path

    def test_all_sources(self, repo):
        changes = branch_changed_files()
        assert changes.base == "main"
        assert changes.files == {
            "committed.sql": "committed",
            "staged.sql": "uncommitted",
            "unstaged.sql": "uncommitted",
            "new_dir/fresh.sql": "untracked",
        }

    def test_explicit_missing_base(self, repo):
        with pytest.raises(DbtMetaError, match="'nope' not found"):
            branch_changed_files("nope")

    def test_git_not_installed(self, tmp_path, monkeypatch):
        def _boom(*a, **k):
            raise FileNotFoundError("git")

        monkeypatch.setattr(subprocess, "run", _boom)
        changes = branch_changed_files()
        assert changes == type(changes)(files={}, base=None)

    def test_outside_git(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        changes = branch_changed_files()
        assert changes.base is None
        assert changes.files == {}

    def test_blank_lines_and_empty_untracked_entries_are_skipped(self, monkeypatch):
        from dbt_meta.utils import git as git_mod

        outputs = {
            "diff": "a.sql\n\nb.sql\n",
            "status": "?? \n?? c.sql\n M a.sql\n\n",
        }

        def _fake(cmd):
            if cmd[1] == "status":
                return 0, outputs["status"]
            if cmd[2].endswith("...HEAD"):
                return 0, outputs["diff"]
            return 0, "\nb.sql\n\n"

        monkeypatch.setattr(git_mod, "_git_out", _fake)
        changes = branch_changed_files()
        assert changes.base == git_mod.DEFAULT_BASE_BRANCHES[0]
        assert changes.files == {"a.sql": "committed", "b.sql": "uncommitted", "c.sql": "untracked"}


class TestCli:
    @pytest.fixture
    def invoke(self, chain, monkeypatch, tmp_path):
        from typer.testing import CliRunner

        from dbt_meta.cli import app
        from dbt_meta.utils import git as git_mod

        monkeypatch.setattr(
            git_mod,
            "branch_changed_files",
            lambda base=None: git_mod.BranchChanges(files=_changed("m2", "m1", "m3"), base="main"),
        )
        monkeypatch.setenv("DBT_PROD_MANIFEST_PATH", str(tmp_path / "absent.json"))
        runner = CliRunner()

        def _invoke(*args):
            get_cached_parser.cache_clear()
            return runner.invoke(app, ["run-set", "--manifest", chain, "--no-parse", *args])

        return _invoke

    def test_json_shape(self, invoke):
        r = invoke("-j", "m1")
        assert r.exit_code == 0, r.output
        data = json.loads(r.output)
        assert data["command"] == "defer run -s m2 x m1"
        assert data["git"] == {"base_branch": "main"}
        assert set(data) >= {
            "targets",
            "modified",
            "ignored_modified",
            "run_set",
            "missing",
            "macro_warnings",
        }

    def test_text_output(self, invoke):
        r = invoke("m1")
        assert r.exit_code == 0, r.output
        assert "intermediate (unmodified)" in r.output
        assert "Run:  defer run -s m2 x m1" in r.output

    def test_check_exit_2(self, invoke):
        r = invoke("--check", "m1")
        assert r.exit_code == 2
        assert "Not closed" in r.output
        assert "x: on path m2 (modified) → x → m1" in r.output

    def test_check_exit_0(self, invoke):
        r = invoke("--check", "m2", "x", "m1")
        assert r.exit_code == 0
        assert "Closed" in r.output

    def test_check_selector_exit_1(self, invoke):
        r = invoke("--check", "-j", "m1+")
        assert r.exit_code == 1
        assert "pass explicit model names" in json.loads(r.output)["error"]

    @pytest.mark.parametrize("args", [(), ("--all", "m1"), ("--all", "--check")])
    def test_argument_validation(self, invoke, args):
        assert invoke(*args).exit_code == 1


REPORTS_MANIFEST = Path("~/Projects/reports/target/manifest.json").expanduser()


@pytest.mark.integration
@pytest.mark.skipif(not REPORTS_MANIFEST.exists(), reason="reports dev manifest not available")
def test_real_reports_manifest():
    """Injected change two levels above a leaf: the intermediate is pulled in."""
    get_cached_parser.cache_clear()
    manifest = json.loads(REPORTS_MANIFEST.read_text())
    g = ManifestGraph(manifest)
    for target in sorted(g.nodes):
        mids = [p for p in g.parents[target] if p.startswith("model.") and g.parents[p]]
        if not mids or g.is_ephemeral(target):
            continue
        mid = mids[0]
        top = next(
            (p for p in g.parents[mid] if p.startswith("model.") and not g.is_ephemeral(p)), None
        )
        if top and not g.is_ephemeral(mid):
            break
    else:
        pytest.skip("no 3-level chain found")
    files = {g.nodes[top]["original_file_path"]: "uncommitted"}
    r = RunSetCommand(str(REPORTS_MANIFEST), None, files, parse=False).execute(
        [top.split(".")[-1], target.split(".")[-1]],
        check=True,
    )
    assert r["closed"] is False
    assert mid.split(".")[-1] in [m["model"] for m in r["missing"]]


class TestReviewFixes:
    """Regressions found in review: lone modified seed under --all, disabled models."""

    def test_all_includes_a_lone_modified_seed(self, tmp_path):
        path = _write(tmp_path, {"seed.p.s": _seed("s"), P + "x": _model("x", ["seed.p.s"])})
        r = _run(path, {"seeds/s.csv": "uncommitted"}, use_all=True)
        assert _names(r) == ["s"]
        assert r["command"] == "defer seed -s s"

    def test_disabled_model_does_not_mark_manifest_stale(self, tmp_path, monkeypatch):
        (tmp_path / "dbt_project.yml").write_text("name: p")
        (tmp_path / "models").mkdir()
        (tmp_path / "models" / "off.sql").write_text("select 1")
        path = tmp_path / "target" / "manifest.json"
        path.parent.mkdir()
        path.write_text(
            json.dumps(
                {
                    "nodes": {P + "t": _model("t")},
                    "disabled": {P + "off": [_model("off")]},
                    "macros": {},
                }
            )
        )
        calls = []
        monkeypatch.setattr(
            run_set_mod, "run_dbt_parse", lambda *a, **k: calls.append(a) or (True, None)
        )
        get_cached_parser.cache_clear()
        r = RunSetCommand(str(path), None, {"models/off.sql": "uncommitted"}).execute(["t"])
        assert calls == []
        assert r["warnings"] == ["models/off.sql: disabled (enabled: false) — not built"]


class TestUncoveredBranches:
    def test_reason_chain_without_a_path_is_empty(self):
        g = ManifestGraph({"nodes": {P + "a": _model("a"), P + "b": _model("b")}})
        assert run_set_mod.reason_chain(g, P + "b", P + "a", P + "b") == ""

    def test_node_without_file_path_is_never_matched(self):
        from dbt_meta.manifest.changes import detect_modified

        node = _model("a")
        node["original_file_path"] = ""
        manifest = {"nodes": {P + "a": node}}
        modified, macros, warnings = detect_modified(
            {"": "committed", "models/a.sql": "committed"}, manifest, None, ManifestGraph(manifest)
        )
        assert modified == {}
        assert macros == {}
        assert warnings == []
