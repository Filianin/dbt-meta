"""Refresh command for dbt-meta."""

from __future__ import annotations

import subprocess
from pathlib import Path

from dbt_meta.errors import DbtMetaError
from dbt_meta.utils.compiled_sql import infer_project_root, run_dbt_parse


class RefreshCommand:
    """Refresh dbt artifacts (manifest.json + catalog.json).

    Dev mode (use_dev=True):
      Parses the dbt project containing the cwd to <project>/target/manifest.json
      Runs: dbt parse --target dev, via the project venv's dbt

    Production mode (use_dev=False):
      Syncs production artifacts via ~/.claude/scripts/sync-artifacts.sh

    Raises:
        DbtMetaError: If sync script not found, or no dbt project / dbt parse fails
        subprocess.CalledProcessError: If subprocess fails
    """

    def __init__(self, use_dev: bool = False):
        self.use_dev = use_dev

    def execute(self) -> None:
        if self.use_dev:
            # Walking up from <cwd>/dbt_project.yml checks the cwd itself first
            # and, unlike the cwd fallback, does not require target/ to exist.
            project_root = infer_project_root(str(Path.cwd() / "dbt_project.yml"))
            if project_root is None:
                raise DbtMetaError(
                    "No dbt project found (dbt_project.yml) in the current directory or its parents",
                    suggestion="Run meta refresh --dev from inside the dbt project",
                )
            print("Parsing local dbt project...")
            ok, err = run_dbt_parse(project_root)
            if not ok:
                raise DbtMetaError(f"dbt parse failed: {err}")
            print(f"✅ Local manifest refreshed ({project_root}/target/manifest.json)")
        else:
            script_path = Path.home() / '.claude' / 'scripts' / 'sync-artifacts.sh'
            if not script_path.exists():
                raise DbtMetaError(
                    f"Sync script not found: {script_path}",
                    suggestion="Install sync-artifacts.sh in ~/.claude/scripts/",
                )
            print("Syncing production artifacts from remote storage...")
            subprocess.run([str(script_path), '--force'], check=True)
            print("✅ Production artifacts synced (~/dbt-state/)")
