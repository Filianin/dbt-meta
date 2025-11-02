"""
Tests for ManifestFinder - 8-level priority manifest search

Following TDD: These tests are written FIRST, then implementation.
"""

import pytest
from pathlib import Path
from dbt_meta.manifest.finder import ManifestFinder


class TestManifestFinder:
    """Test 8-level priority manifest search logic"""

    def test_priority_1_explicit_env_var(self, tmp_path, monkeypatch):
        """
        Priority 1: DBT_MANIFEST_PATH environment variable

        Should find manifest when DBT_MANIFEST_PATH is set,
        regardless of other locations.
        """
        manifest_path = tmp_path / "custom" / "manifest.json"
        manifest_path.parent.mkdir(parents=True)
        manifest_path.write_text('{"metadata": {}}')

        monkeypatch.setenv("DBT_MANIFEST_PATH", str(manifest_path))

        finder = ManifestFinder()
        assert finder.find() == str(manifest_path.absolute())

    def test_priority_2_dbt_state_current_dir(self, tmp_path, monkeypatch):
        """
        Priority 2: ./.dbt-state/manifest.json

        Should find production manifest in current directory.
        """
        monkeypatch.chdir(tmp_path)

        manifest_path = tmp_path / ".dbt-state" / "manifest.json"
        manifest_path.parent.mkdir(parents=True)
        manifest_path.write_text('{"metadata": {}}')

        finder = ManifestFinder()
        assert finder.find() == str(manifest_path.absolute())

    def test_priority_3_target_current_dir(self, tmp_path, monkeypatch):
        """
        Priority 3: ./target/manifest.json

        Should find dev manifest in current directory
        when .dbt-state doesn't exist.
        """
        monkeypatch.chdir(tmp_path)

        manifest_path = tmp_path / "target" / "manifest.json"
        manifest_path.parent.mkdir(parents=True)
        manifest_path.write_text('{"metadata": {}}')

        finder = ManifestFinder()
        assert finder.find() == str(manifest_path.absolute())

    def test_priority_4_dbt_project_path_dbt_state(self, tmp_path, monkeypatch):
        """
        Priority 4: $DBT_PROJECT_PATH/.dbt-state/manifest.json

        Should find production manifest in DBT_PROJECT_PATH
        when not in current directory.
        """
        project_path = tmp_path / "dbt_project"
        manifest_path = project_path / ".dbt-state" / "manifest.json"
        manifest_path.parent.mkdir(parents=True)
        manifest_path.write_text('{"metadata": {}}')

        # Create other_dir before chdir
        other_dir = tmp_path / "other_dir"
        other_dir.mkdir()

        monkeypatch.setenv("DBT_PROJECT_PATH", str(project_path))
        monkeypatch.chdir(other_dir)

        finder = ManifestFinder()
        assert finder.find() == str(manifest_path.absolute())

    def test_production_prioritized_over_dev(self, tmp_path, monkeypatch):
        """
        CRITICAL: .dbt-state has priority over target

        When both production (.dbt-state) and dev (target) manifests exist,
        must always prefer production manifest.
        """
        monkeypatch.chdir(tmp_path)

        # Create both manifests
        prod_manifest = tmp_path / ".dbt-state" / "manifest.json"
        dev_manifest = tmp_path / "target" / "manifest.json"

        prod_manifest.parent.mkdir(parents=True)
        dev_manifest.parent.mkdir(parents=True)

        prod_manifest.write_text('{"metadata": {"env": "prod"}}')
        dev_manifest.write_text('{"metadata": {"env": "dev"}}')

        finder = ManifestFinder()
        found_path = finder.find()

        # MUST find production manifest
        assert found_path == str(prod_manifest.absolute())

    def test_raises_when_no_manifest_found(self, tmp_path, monkeypatch):
        """
        Should raise clear error when no manifest found

        Error message must explain all searched locations.
        """
        # Clear env vars that could point to manifests
        monkeypatch.delenv("DBT_MANIFEST_PATH", raising=False)
        monkeypatch.delenv("DBT_PROJECT_PATH", raising=False)

        monkeypatch.chdir(tmp_path)

        finder = ManifestFinder()

        with pytest.raises(FileNotFoundError, match="No manifest.json found"):
            finder.find()

    def test_finds_absolute_path(self, tmp_path, monkeypatch):
        """
        Should always return absolute path

        Even when manifest is found via relative path,
        return value must be absolute.
        """
        monkeypatch.chdir(tmp_path)

        manifest_path = tmp_path / "target" / "manifest.json"
        manifest_path.parent.mkdir(parents=True)
        manifest_path.write_text('{"metadata": {}}')

        finder = ManifestFinder()
        found = Path(finder.find())

        assert found.is_absolute()
        assert found.exists()

    def test_priority_5_dbt_project_path_target(self, tmp_path, monkeypatch):
        """
        Priority 5: $DBT_PROJECT_PATH/target/manifest.json

        Should find dev manifest in DBT_PROJECT_PATH
        when .dbt-state doesn't exist.
        """
        project_path = tmp_path / "dbt_project"
        manifest_path = project_path / "target" / "manifest.json"
        manifest_path.parent.mkdir(parents=True)
        manifest_path.write_text('{"metadata": {}}')

        other_dir = tmp_path / "other_dir"
        other_dir.mkdir()

        monkeypatch.setenv("DBT_PROJECT_PATH", str(project_path))
        monkeypatch.chdir(other_dir)

        finder = ManifestFinder()
        assert finder.find() == str(manifest_path.absolute())

    def test_priority_6_search_upward_production(self, tmp_path, monkeypatch):
        """
        Priority 6: Search upward for .dbt-state/manifest.json

        Should find production manifest in parent directory.
        """
        # Clear env vars to skip priorities 1-5
        monkeypatch.delenv("DBT_MANIFEST_PATH", raising=False)
        monkeypatch.delenv("DBT_PROJECT_PATH", raising=False)

        # Create manifest in parent
        parent_manifest = tmp_path / ".dbt-state" / "manifest.json"
        parent_manifest.parent.mkdir(parents=True)
        parent_manifest.write_text('{"metadata": {}}')

        # Change to subdirectory
        subdir = tmp_path / "nested" / "deep"
        subdir.mkdir(parents=True)
        monkeypatch.chdir(subdir)

        finder = ManifestFinder()
        assert finder.find() == str(parent_manifest.absolute())

    def test_priority_7_search_upward_dev(self, tmp_path, monkeypatch):
        """
        Priority 7: Search upward for target/manifest.json

        Should find dev manifest in parent directory
        when .dbt-state doesn't exist.
        """
        # Clear env vars to skip priorities 1-5
        monkeypatch.delenv("DBT_MANIFEST_PATH", raising=False)
        monkeypatch.delenv("DBT_PROJECT_PATH", raising=False)

        # Create manifest in parent
        parent_manifest = tmp_path / "target" / "manifest.json"
        parent_manifest.parent.mkdir(parents=True)
        parent_manifest.write_text('{"metadata": {}}')

        # Change to subdirectory
        subdir = tmp_path / "nested" / "deep"
        subdir.mkdir(parents=True)
        monkeypatch.chdir(subdir)

        finder = ManifestFinder()
        assert finder.find() == str(parent_manifest.absolute())
