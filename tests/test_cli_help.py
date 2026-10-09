"""Top-level ``meta -h``: every command is listed and the page stays short."""

import typer.main
from typer.testing import CliRunner

from dbt_meta import cli
from dbt_meta.cli import HELP_GROUPS, app


def _registered() -> dict[str, list[str]]:
    """Top-level command name -> subcommand names (empty for plain commands)."""
    root = typer.main.get_command(app)
    out: dict[str, list[str]] = {}
    for name, cmd in root.commands.items():  # type: ignore[attr-defined]
        if cmd.hidden:
            continue
        out[name] = sorted(getattr(cmd, "commands", {}))
    return out


def _listed() -> dict[str, str]:
    return {name: desc for _, rows in HELP_GROUPS for name, desc in rows}


def _gaps(listed: dict[str, str], registered: dict[str, list[str]]) -> list[str]:
    """Commands or subcommands registered but not listed, or listed but gone."""
    gaps = sorted(set(listed) ^ set(registered))
    for family, subs in registered.items():
        if subs and family in listed:
            names = sorted(s.strip() for s in listed[family].split("·"))
            gaps += [f"{family} {s}" for s in sorted(set(names) ^ set(subs))]
    return gaps


def test_every_command_and_subcommand_is_listed():
    assert _gaps(_listed(), _registered()) == []


def test_gap_check_catches_missing_and_stale_entries():
    listed = _listed()
    del listed["run-set"]
    listed["ghost"] = "Removed command"
    listed["settings"] = "init · show · path"
    assert _gaps(listed, _registered()) == ["ghost", "run-set", "settings validate"]


def test_bare_invocation_shows_the_overview():
    result = CliRunner().invoke(app, [])
    assert "Model metadata" in result.output


def test_no_command_listed_twice():
    names = [name for _, rows in HELP_GROUPS for name, _ in rows]
    assert len(names) == len(set(names))


def test_help_fits_one_screen(monkeypatch):
    # The module console reads its width from the terminal / COLUMNS at
    # import; pin it so the check does not depend on where pytest runs.
    monkeypatch.setattr(cli.console, "_width", 80)
    result = CliRunner().invoke(app, ["-h"], env={"COLUMNS": "80"})
    assert result.exit_code == 0
    lines = result.output.splitlines()
    assert len(lines) <= 60
    assert max(len(line) for line in lines) <= 80
    assert "Model metadata" in result.output
    assert "Global flags" not in result.output
    for flag in ("-a, --all", "-m, --modified", "--base", "--no-compile"):
        assert flag in result.output
    assert "--min-gb" not in result.output
