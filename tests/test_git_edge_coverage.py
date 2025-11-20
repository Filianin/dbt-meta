"""Tests to cover git.py edge cases and error handling.

Target lines: 188-202, 271-279, 293-296, 380, 409-413
"""

import pytest
import subprocess
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
from dbt_meta.utils.git import _find_sql_file_fast, get_model_git_status, GitStatus


class TestGitFilesystemErrors:
    """Cover git.py filesystem error handling (lines 188-202)."""

    def test_find_sql_file_fast_permission_error(self):
        """Test _find_sql_file_fast handles PermissionError (lines 199-202)."""
        with patch('pathlib.Path.cwd') as mock_cwd:
            mock_path = MagicMock()
            mock_cwd.return_value = mock_path

            # Simulate PermissionError when accessing models dir
            mock_path.__truediv__.return_value.rglob.side_effect = PermissionError("Access denied")

            result = _find_sql_file_fast("test_model")

            # Should return None on permission error
            assert result is None

    def test_find_sql_file_fast_os_error(self):
        """Test _find_sql_file_fast handles OSError (lines 199-202)."""
        with patch('pathlib.Path.cwd') as mock_cwd:
            mock_path = MagicMock()
            mock_cwd.return_value = mock_path

            # Simulate OSError when searching
            mock_path.__truediv__.return_value.rglob.side_effect = OSError("Disk error")

            result = _find_sql_file_fast("test_model")

            # Should return None on OS error
            assert result is None

    def test_find_sql_file_fast_with_many_files(self):
        """Test _find_sql_file_fast stops at 1000 files (lines 189-190)."""
        with patch('pathlib.Path.cwd') as mock_cwd:
            mock_path = MagicMock()
            mock_cwd.return_value = mock_path

            # Create 1500 mock SQL files (exceeds 1000 limit)
            mock_files = []
            for i in range(1500):
                mock_file = MagicMock()
                mock_file.stem = f"model_{i}"
                mock_files.append(mock_file)

            mock_path.__truediv__.return_value.rglob.return_value = iter(mock_files)

            result = _find_sql_file_fast("nonexistent_model")

            # Should return None after hitting 1000 file limit
            assert result is None

    def test_find_sql_file_fast_exact_match(self):
        """Test _find_sql_file_fast finds exact stem match (lines 194-195)."""
        # Create mock file that matches
        mock_file = MagicMock()
        mock_file.stem = "my_model"
        mock_file.__str__.return_value = "models/core/my_model.sql"

        # Mock models directory
        mock_models_dir = MagicMock()
        mock_models_dir.exists.return_value = True
        mock_models_dir.rglob.return_value = iter([mock_file])

        # Patch Path at the module level where it's used
        with patch('dbt_meta.utils.git.Path') as mock_path_class:
            # When Path('models') is called, return our mock directory
            mock_path_class.return_value = mock_models_dir

            result = _find_sql_file_fast("my_model")

            # Should find the file
            assert result == "models/core/my_model.sql"


class TestGitStatusEdgeCases:
    """Cover git status edge cases."""

    def test_git_status_with_unicode_decode_error(self):
        """Test git status handles UnicodeDecodeError."""
        with patch('dbt_meta.utils.git._find_sql_file_fast', return_value="models/test.sql"):
            with patch('subprocess.run') as mock_run:
                # Simulate unicode decode error
                mock_run.side_effect = UnicodeDecodeError('utf-8', b'', 0, 1, "Bad encoding")

                status = get_model_git_status("test_model")

                # Should return safe defaults
                assert status.exists is True
                assert status.is_tracked is False
                assert status.is_modified is False

    def test_git_status_with_value_error(self):
        """Test git status handles ValueError."""
        with patch('dbt_meta.utils.git._find_sql_file_fast', return_value="models/test.sql"):
            with patch('subprocess.run') as mock_run:
                # Simulate value error during parsing
                mock_run.side_effect = ValueError("Parse error")

                status = get_model_git_status("test_model")

                # Should return safe defaults
                assert status.exists is True
                assert status.is_tracked is False

    def test_git_status_with_file_not_found_error(self):
        """Test git status handles FileNotFoundError (git not installed)."""
        with patch('dbt_meta.utils.git._find_sql_file_fast', return_value="models/test.sql"):
            with patch('subprocess.run', side_effect=FileNotFoundError("git not found")):
                status = get_model_git_status("test_model")

                # Should return safe defaults
                assert status.exists is True
                assert status.is_tracked is False
                assert status.is_modified is False


class TestGitDiffParsing:
    """Cover git diff parsing edge cases."""

    def test_git_status_untracked_file(self):
        """Test git status detects untracked files."""
        with patch('dbt_meta.utils.git._find_sql_file_fast', return_value="models/new_model.sql"):
            with patch('subprocess.run') as mock_run:
                # Mock BOTH subprocess calls:
                # 1st call: git status (returns ??)
                # 2nd call: git log (returns empty = not committed)
                mock_run.side_effect = [
                    Mock(returncode=0, stdout="?? models/new_model.sql"),  # git status
                    Mock(returncode=0, stdout="")  # git log (empty = not in history)
                ]

                status = get_model_git_status("new_model")

                # Should detect as untracked
                assert status.exists is True
                assert status.is_tracked is False
                assert status.is_new is True

    def test_git_status_deleted_file(self):
        """Test git status detects deleted files."""
        with patch('dbt_meta.utils.git._find_sql_file_fast', return_value="models/deleted.sql"):
            with patch('subprocess.run') as mock_run:
                # Mock git status showing deleted file
                mock_run.return_value = Mock(
                    returncode=0,
                    stdout=" D models/deleted.sql"
                )

                status = get_model_git_status("deleted")

                # Should detect as deleted
                assert status.is_deleted is True


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
