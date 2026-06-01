"""Refresh command for dbt-meta."""

from __future__ import annotations

import subprocess
from pathlib import Path

from dbt_meta.errors import DbtMetaError


class RefreshCommand:
    """Refresh dbt artifacts (manifest.json + catalog.json).

    Dev mode (use_dev=True):
      Parses local dbt project to ./target/manifest.json
      Runs: dbt parse --target dev

    Production mode (use_dev=False):
      Syncs production artifacts via ~/.claude/scripts/sync-artifacts.sh

    Raises:
        DbtMetaError: If sync script not found
        subprocess.CalledProcessError: If subprocess fails
    """

    def __init__(self, use_dev: bool = False):
        self.use_dev = use_dev

    def execute(self) -> None:
        if self.use_dev:
            print("Parsing local dbt project...")
            subprocess.run(['dbt', 'parse', '--target', 'dev'], check=True)
            print("✅ Local manifest refreshed (./target/manifest.json)")
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
