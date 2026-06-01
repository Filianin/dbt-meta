"""Regression: under -j, stdout = exactly one JSON document; diagnostics go to stderr."""

from __future__ import annotations

import json
import subprocess
import sys


def _run(args: list[str], env: dict | None = None) -> tuple[int, str, str]:
    """Invoke `meta` via the module entrypoint and capture stdout/stderr separately."""
    proc = subprocess.run(
        [sys.executable, "-m", "dbt_meta.cli", *args],
        capture_output=True,
        text=True,
        env=env,
    )
    return proc.returncode, proc.stdout, proc.stderr


def _assert_clean_json_stdout(stdout: str) -> object:
    """Parse stdout as a single JSON document; fail with a readable diff on noise."""
    assert stdout.strip(), "stdout was empty under -j"
    try:
        return json.loads(stdout)
    except json.JSONDecodeError as e:
        raise AssertionError(
            f"stdout is not a single JSON document: {e}\n"
            f"--- stdout ---\n{stdout!r}"
        ) from None


def test_json_stdout_clean_on_dev_manifest_missing(tmp_path, monkeypatch):
    """When --dev is used and dev manifest is missing under -j: stdout = {'error': ...} only."""
    # Force dev manifest miss by pointing at a non-existent path.
    monkeypatch.setenv("DBT_DEV_MANIFEST_PATH", str(tmp_path / "no-such-manifest.json"))
    monkeypatch.setenv("DBT_PROD_MANIFEST_PATH", str(tmp_path / "no-such-prod.json"))

    code, stdout, stderr = _run(["info", "any_model", "--dev", "-j"])

    assert code == 1
    payload = _assert_clean_json_stdout(stdout)
    assert isinstance(payload, dict) and "error" in payload
    assert "Dev manifest not found" in payload["error"]
    # stderr stays empty under -j when the error is structured.
    assert stderr == "", f"stderr should be empty under -j, got: {stderr!r}"


def test_toplevel_json_flag_is_recognised(tmp_path, monkeypatch):
    """`meta -j <cmd> ...` must behave like `meta <cmd> ... -j`."""
    monkeypatch.setenv("DBT_DEV_MANIFEST_PATH", str(tmp_path / "no-such.json"))
    monkeypatch.setenv("DBT_PROD_MANIFEST_PATH", str(tmp_path / "also-no-such.json"))
    code, stdout, stderr = _run(["-j", "info", "any_model", "--dev"])
    assert code == 1
    payload = _assert_clean_json_stdout(stdout)
    assert isinstance(payload, dict) and "error" in payload
    assert stderr == "", f"stderr should be empty under top-level -j, got: {stderr!r}"


def test_toplevel_and_subcommand_json_compose(tmp_path, monkeypatch):
    """Passing `-j` at both positions must not break parsing (idempotent OR)."""
    monkeypatch.setenv("DBT_DEV_MANIFEST_PATH", str(tmp_path / "no.json"))
    monkeypatch.setenv("DBT_PROD_MANIFEST_PATH", str(tmp_path / "also-no.json"))
    code, stdout, _ = _run(["-j", "info", "any_model", "--dev", "-j"])
    assert code == 1
    payload = _assert_clean_json_stdout(stdout)
    assert "error" in payload


def test_text_mode_dev_missing_writes_to_stderr(tmp_path, monkeypatch):
    """Without -j, the same error goes to stderr (not stdout) for human consumption."""
    monkeypatch.setenv("DBT_DEV_MANIFEST_PATH", str(tmp_path / "nope.json"))
    monkeypatch.setenv("DBT_PROD_MANIFEST_PATH", str(tmp_path / "also-nope.json"))

    code, stdout, stderr = _run(["info", "any_model", "--dev"])

    assert code == 1
    # stdout must NOT carry the error in text mode either (so `| jq` on accidental
    # plain mode still gives a clean parse error rather than a half-baked doc).
    assert stdout == "", f"stdout should be empty, got: {stdout!r}"
    assert "Dev manifest not found" in stderr
