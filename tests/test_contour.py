"""Unit tests for the trust-contour invariant check (``meta contour``).

The invariant: a model tagged ``primary`` or ``verified`` must not have an
ancestor in a non-staging layer that is untagged or tagged ``review`` /
``deprecating``. ``mirror`` models and ``staging`` models are exempt.
"""

import json

import pytest

from dbt_meta.command_impl.contour import ContourCommand


def _node(path, tier=None, parents=()):
    node = {
        "path": path,
        "original_file_path": "models/" + path,
        "depends_on": {"nodes": list(parents)},
        "config": {},
    }
    if tier:
        node["config"]["meta"] = {"tier": tier}
    return node


def _manifest(nodes):
    return {"nodes": nodes}


@pytest.fixture
def write_manifest(tmp_path):
    def _write(nodes):
        path = tmp_path / "manifest.json"
        path.write_text(json.dumps(_manifest(nodes)))
        return str(path)
    return _write


def _run(path, **kwargs):
    from dbt_meta.utils import get_cached_parser

    get_cached_parser.cache_clear() if hasattr(get_cached_parser, "cache_clear") else None
    return ContourCommand(object(), path, **kwargs).execute()


class TestInvariant:
    def test_clean_project_has_no_violations(self, write_manifest):
        path = write_manifest({
            "model.p.stg_a": _node("staging/a/stg_a.sql"),
            "model.p.core_a": _node("core/a/core_a.sql", "verified", ["model.p.stg_a"]),
            "model.p.core_b": _node("core/a/core_b.sql", "primary", ["model.p.core_a"]),
        })
        result = _run(path)
        assert result["ok"] is True
        assert result["summary"]["models_checked"] == 2

    def test_untagged_ancestor_is_a_violation(self, write_manifest):
        path = write_manifest({
            "model.p.core_a": _node("core/a/core_a.sql"),
            "model.p.core_b": _node("core/a/core_b.sql", "primary", ["model.p.core_a"]),
        })
        result = _run(path)
        assert result["ok"] is False
        assert result["violations"][0]["model"] == "core_b"
        assert result["violations"][0]["ancestors"][0]["tier"] == "none"

    def test_review_ancestor_is_a_violation(self, write_manifest):
        path = write_manifest({
            "model.p.core_a": _node("core/a/core_a.sql", "review"),
            "model.p.core_b": _node("core/a/core_b.sql", "verified", ["model.p.core_a"]),
        })
        assert _run(path)["summary"]["violations"] == 1

    def test_staging_ancestor_is_exempt(self, write_manifest):
        path = write_manifest({
            "model.p.stg_a": _node("staging/a/stg_a.sql"),
            "model.p.core_b": _node("core/a/core_b.sql", "primary", ["model.p.stg_a"]),
        })
        assert _run(path)["ok"] is True

    def test_mirror_ancestor_is_exempt(self, write_manifest):
        path = write_manifest({
            "model.p.core_m": _node("core/a/core_m.sql", "mirror"),
            "model.p.core_b": _node("core/a/core_b.sql", "primary", ["model.p.core_m"]),
        })
        assert _run(path)["ok"] is True

    def test_violation_is_transitive(self, write_manifest):
        path = write_manifest({
            "model.p.core_a": _node("core/a/core_a.sql", "review"),
            "model.p.core_b": _node("core/a/core_b.sql", "verified", ["model.p.core_a"]),
            "model.p.core_c": _node("core/a/core_c.sql", "primary", ["model.p.core_b"]),
        })
        offenders = {v["model"] for v in _run(path)["violations"]}
        assert offenders == {"core_b", "core_c"}

    def test_untagged_model_is_never_checked(self, write_manifest):
        path = write_manifest({
            "model.p.core_a": _node("core/a/core_a.sql", "review"),
            "model.p.core_b": _node("core/a/core_b.sql", None, ["model.p.core_a"]),
        })
        result = _run(path)
        assert result["ok"] is True
        assert result["summary"]["models_checked"] == 0

    def test_cycle_does_not_hang(self, write_manifest):
        path = write_manifest({
            "model.p.core_a": _node("core/a/core_a.sql", "primary", ["model.p.core_b"]),
            "model.p.core_b": _node("core/a/core_b.sql", "primary", ["model.p.core_a"]),
        })
        assert _run(path)["summary"]["violations"] == 0


class TestWarnings:
    def test_missing_doc_and_tests_are_warnings_not_violations(self, write_manifest):
        path = write_manifest({
            "model.p.core_a": _node("core/a/core_a.sql", "primary"),
        })
        result = _run(path)
        assert result["ok"] is True
        assert result["warnings"][0]["missing"] == ["decision doc", "0 tests (min 1)"]

    def test_min_tests_zero_silences_the_test_warning(self, write_manifest):
        path = write_manifest({
            "model.p.core_a": _node("core/a/core_a.sql", "primary"),
        })
        result = _run(path, min_tests=0)
        assert result["warnings"][0]["missing"] == ["decision doc"]

    def test_tests_are_counted_from_the_manifest(self, write_manifest):
        path = write_manifest({
            "model.p.core_a": _node("core/a/core_a.sql", "primary"),
            "test.p.t1": {"depends_on": {"nodes": ["model.p.core_a"]}},
        })
        result = _run(path, min_tests=1)
        assert result["warnings"][0]["missing"] == ["decision doc"]


class TestSummary:
    def test_tier_counts_cover_every_value(self, write_manifest):
        path = write_manifest({
            "model.p.a": _node("core/a/a.sql", "primary"),
            "model.p.b": _node("core/a/b.sql", "review"),
            "model.p.c": _node("core/a/c.sql", "mirror"),
            "model.p.d": _node("core/a/d.sql", "mirror"),
        })
        assert _run(path)["summary"]["tiers"] == {"mirror": 2, "primary": 1, "review": 1}


class TestDecisionDoc:
    def test_doc_next_to_the_model_clears_the_warning(self, tmp_path):
        project = tmp_path / "proj"
        (project / "models" / "core" / "a" / "docs").mkdir(parents=True)
        (project / "dbt_project.yml").write_text("name: p\n")
        (project / "models" / "core" / "a" / "docs" / "core_a.md").write_text("why")
        (project / "target").mkdir()
        manifest = project / "target" / "manifest.json"
        manifest.write_text(json.dumps(_manifest({
            "model.p.core_a": _node("core/a/core_a.sql", "primary"),
            "test.p.t1": {"depends_on": {"nodes": ["model.p.core_a"]}},
        })))
        result = _run(str(manifest))
        assert result["warnings"] == []

    def test_model_without_sql_path_gets_the_doc_warning(self, write_manifest):
        node = _node("core/a/core_a.sql", "primary")
        node["original_file_path"] = ""
        path = write_manifest({"model.p.core_a": node})
        assert _run(path)["warnings"][0]["missing"][0] == "decision doc"

    def test_non_sql_path_gets_the_doc_warning(self, write_manifest):
        node = _node("core/a/core_a.py", "primary")
        node["original_file_path"] = "models/core/a/core_a.py"
        path = write_manifest({"model.p.core_a": node})
        assert _run(path)["warnings"][0]["missing"][0] == "decision doc"


class TestInvariantEdges:
    def test_deprecating_ancestor_is_a_violation(self, write_manifest):
        path = write_manifest({
            "model.p.core_a": _node("core/a/core_a.sql", "deprecating"),
            "model.p.core_b": _node("core/a/core_b.sql", "primary", ["model.p.core_a"]),
        })
        violation = _run(path)["violations"][0]
        assert violation["ancestors"][0]["tier"] == "deprecating"
        assert violation["ancestors"][0]["path"] == "models/core/a/core_a.sql"

    def test_intermediate_ancestor_is_not_exempt(self, write_manifest):
        path = write_manifest({
            "model.p.int_a": _node("intermediate/a/int_a.sql"),
            "model.p.core_b": _node("core/a/core_b.sql", "verified", ["model.p.int_a"]),
        })
        assert _run(path)["summary"]["violations"] == 1

    def test_non_string_tier_is_ignored(self, write_manifest):
        node = _node("core/a/core_a.sql")
        node["config"]["meta"] = {"tier": ["primary"]}
        path = write_manifest({"model.p.core_a": node})
        result = _run(path)
        assert result["summary"]["tiers"] == {}
        assert result["summary"]["models_checked"] == 0

    def test_empty_path_is_not_exempt(self, write_manifest):
        parent = _node("core/a/core_a.sql")
        parent["path"] = ""
        path = write_manifest({
            "model.p.core_a": parent,
            "model.p.core_b": _node("core/a/core_b.sql", "primary", ["model.p.core_a"]),
        })
        assert _run(path)["summary"]["violations"] == 1

    def test_non_model_parents_are_ignored(self, write_manifest):
        path = write_manifest({
            "model.p.core_b": _node(
                "core/a/core_b.sql", "primary",
                ["source.p.raw.tbl", "seed.p.s1"],
            ),
        })
        assert _run(path)["ok"] is True

    def test_unknown_parent_id_is_skipped(self, write_manifest):
        path = write_manifest({
            "model.p.core_b": _node("core/a/core_b.sql", "primary", ["model.p.ghost"]),
        })
        assert _run(path)["ok"] is True

    def test_violations_are_ranked_by_unreviewed_count(self, write_manifest):
        path = write_manifest({
            "model.p.core_x": _node("core/a/core_x.sql"),
            "model.p.core_y": _node("core/a/core_y.sql"),
            "model.p.few": _node("core/a/few.sql", "primary", ["model.p.core_x"]),
            "model.p.many": _node(
                "core/a/many.sql", "primary", ["model.p.core_x", "model.p.core_y"],
            ),
        })
        ranked = [v["model"] for v in _run(path)["violations"]]
        assert ranked == ["many", "few"]

    def test_shared_ancestors_are_cached_once(self, write_manifest):
        path = write_manifest({
            "model.p.core_root": _node("core/a/core_root.sql", "review"),
            "model.p.core_mid": _node("core/a/core_mid.sql", "verified", ["model.p.core_root"]),
            "model.p.core_leaf": _node("core/a/core_leaf.sql", "primary", ["model.p.core_mid"]),
        })
        result = _run(path)
        by_model = {v["model"]: v["unreviewed_ancestors"] for v in result["violations"]}
        assert by_model == {"core_mid": 1, "core_leaf": 1}


class TestTestCounting:
    def test_min_tests_above_one_still_warns(self, write_manifest):
        path = write_manifest({
            "model.p.core_a": _node("core/a/core_a.sql", "primary"),
            "test.p.t1": {"depends_on": {"nodes": ["model.p.core_a"]}},
        })
        result = _run(path, min_tests=2)
        assert "1 tests (min 2)" in result["warnings"][0]["missing"]

    def test_tests_on_other_models_do_not_count(self, write_manifest):
        path = write_manifest({
            "model.p.core_a": _node("core/a/core_a.sql", "primary"),
            "model.p.core_b": _node("core/a/core_b.sql"),
            "test.p.t1": {"depends_on": {"nodes": ["model.p.core_b"]}},
            "test.p.t2": {"depends_on": {"nodes": ["source.p.raw.tbl"]}},
        })
        result = _run(path)
        assert "0 tests (min 1)" in result["warnings"][0]["missing"]

    def test_warnings_are_sorted_by_model(self, write_manifest):
        path = write_manifest({
            "model.p.zz": _node("core/a/zz.sql", "primary"),
            "model.p.aa": _node("core/a/aa.sql", "verified"),
        })
        assert [w["model"] for w in _run(path)["warnings"]] == ["aa", "zz"]


class TestCli:
    """The CLI wiring: exit codes, JSON payload, text rendering."""

    def setup_method(self):
        from typer.testing import CliRunner

        self.runner = CliRunner()

    def _invoke(self, args, result, mocker):
        mocker.patch(
            'dbt_meta.command_impl.contour.ContourCommand.__init__', return_value=None,
        )
        mocker.patch(
            'dbt_meta.command_impl.contour.ContourCommand.execute',
            return_value=result,
        )
        mocker.patch('dbt_meta.cli.get_manifest_path', return_value=('/m.json', False))
        from dbt_meta.cli import app

        return self.runner.invoke(app, ['contour', *args])

    @staticmethod
    def _payload(violations=(), warnings=(), tiers=None):
        return {
            'ok': not violations,
            'summary': {
                'models_checked': 2,
                'violations': len(violations),
                'warnings': len(warnings),
                'tiers': tiers if tiers is not None else {'primary': 2},
            },
            'violations': list(violations),
            'warnings': list(warnings),
        }

    def test_clean_contour_exits_zero(self, mocker):
        out = self._invoke([], self._payload(), mocker)
        assert out.exit_code == 0
        assert "No violations" in out.stdout

    def test_violation_exits_one_and_names_the_model(self, mocker):
        violation = {
            'model': 'core_b', 'tier': 'primary', 'path': 'models/core/a/core_b.sql',
            'unreviewed_ancestors': 1,
            'ancestors': [{'model': 'core_a', 'tier': 'review', 'path': ''}],
        }
        out = self._invoke([], self._payload([violation]), mocker)
        assert out.exit_code == 1
        assert 'core_b' in out.stdout and 'core_a' in out.stdout

    def test_long_ancestor_list_is_truncated(self, mocker):
        ancestors = [
            {'model': f'core_{i}', 'tier': 'none', 'path': ''} for i in range(7)
        ]
        violation = {
            'model': 'core_b', 'tier': 'primary', 'path': '',
            'unreviewed_ancestors': 7, 'ancestors': ancestors,
        }
        out = self._invoke([], self._payload([violation]), mocker)
        assert 'and 2 more' in out.stdout
        assert 'core_6' not in out.stdout

    def test_warnings_are_hidden_without_the_flag(self, mocker):
        warning = {'model': 'core_a', 'tier': 'primary', 'missing': ['decision doc']}
        out = self._invoke([], self._payload(warnings=[warning]), mocker)
        assert 'decision doc' not in out.stdout

    def test_warnings_flag_shows_them(self, mocker):
        warning = {'model': 'core_a', 'tier': 'primary', 'missing': ['decision doc']}
        out = self._invoke(['-w'], self._payload(warnings=[warning]), mocker)
        assert 'decision doc' in out.stdout

    def test_untagged_project_reports_no_tagged_models(self, mocker):
        out = self._invoke([], self._payload(tiers={}), mocker)
        assert 'no tagged models' in out.stdout

    def test_json_output_is_the_raw_result(self, mocker):
        payload = self._payload()
        out = self._invoke(['-j'], payload, mocker)
        assert out.exit_code == 0
        assert json.loads(out.stdout) == payload

    def test_json_output_still_exits_one_on_violation(self, mocker):
        violation = {
            'model': 'core_b', 'tier': 'primary', 'path': '',
            'unreviewed_ancestors': 1, 'ancestors': [],
        }
        out = self._invoke(['-j'], self._payload([violation]), mocker)
        assert out.exit_code == 1
        assert json.loads(out.stdout)['ok'] is False

    def test_min_tests_reaches_the_command(self, mocker):
        mocker.patch('dbt_meta.cli.get_manifest_path', return_value=('/m.json', False))
        init = mocker.patch(
            'dbt_meta.command_impl.contour.ContourCommand.__init__', return_value=None,
        )
        mocker.patch(
            'dbt_meta.command_impl.contour.ContourCommand.execute',
            return_value=self._payload(),
        )
        from dbt_meta.cli import app

        out = self.runner.invoke(app, ['contour', '--min-tests', '3'])
        assert out.exit_code == 0
        assert init.call_args[0][2] == 3


class TestAncestorCache:
    def test_second_lookup_reuses_the_cached_set(self, write_manifest):
        path = write_manifest({
            "model.p.core_a": _node("core/a/core_a.sql"),
            "model.p.core_b": _node("core/a/core_b.sql", "primary", ["model.p.core_a"]),
        })
        command = ContourCommand(object(), path)
        assert command._ancestors("model.p.core_b") == {"model.p.core_a"}
        first = command._graph.ancestors("model.p.core_b")
        assert command._graph.ancestors("model.p.core_b") is first
