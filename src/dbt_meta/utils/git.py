"""Git operations for dbt-meta.

This module handles git-related operations:
- Checking if model files are modified
- Detecting git/manifest mismatches
"""

import subprocess
from typing import Dict, List, Optional


__all__ = ['is_modified', 'check_manifest_git_mismatch']


def is_modified(model_name: str) -> bool:
    """Check if model file is modified in git (new or changed).

    Uses git diff to detect if the model's SQL file has uncommitted changes.

    Args:
        model_name: dbt model name (e.g., "core_client__events")

    Returns:
        True if model is new or modified, False otherwise or if git check fails

    Example:
        >>> is_modified('core_client__events')
        True  # If models/core/client/events.sql is modified
    """
    try:
        # Extract table name from model_name
        # Inline implementation to avoid circular import
        if '__' not in model_name:
            table = model_name
        else:
            parts = model_name.split('__')
            table = parts[-1]

        # Check git diff for modified files
        result = subprocess.run(
            ['git', 'diff', '--name-only', 'HEAD'],
            capture_output=True,
            text=True,
            timeout=5
        )

        if result.returncode == 0:
            # Check if any modified file contains the table name
            modified_files = result.stdout.splitlines()
            for file_path in modified_files:
                if table in file_path and file_path.endswith('.sql'):
                    return True

        # Check git status for new files (untracked)
        result = subprocess.run(
            ['git', 'status', '--porcelain'],
            capture_output=True,
            text=True,
            timeout=5
        )

        if result.returncode == 0:
            # Check for new files (starting with ??)
            status_lines = result.stdout.splitlines()
            for line in status_lines:
                if line.startswith('??') or line.startswith('A '):
                    if table in line and line.endswith('.sql'):
                        return True

        return False

    except (subprocess.TimeoutExpired, FileNotFoundError, Exception):
        # If git check fails, assume not modified (safe default)
        return False


def check_manifest_git_mismatch(
    model_name: str,
    use_dev: bool,
    dev_manifest_found: Optional[str] = None
) -> List[Dict[str, str]]:
    """Check git status and return structured warnings.

    Returns list of warning objects that can be output as JSON (with -j) or text (without -j).

    Warning types:
    - git_mismatch: Model modified in git but querying production
    - dev_without_changes: Using --dev but model not modified
    - dev_manifest_missing: Using --dev but dev manifest not found

    Args:
        model_name: dbt model name (e.g., "core_client__events")
        use_dev: Whether --dev flag was used
        dev_manifest_found: Path to dev manifest if found, None otherwise

    Returns:
        List of warning dictionaries with keys: type, severity, message, suggestion (optional)

    Example:
        >>> warnings = check_manifest_git_mismatch('core__clients', use_dev=False)
        >>> if warnings:
        ...     print(warnings[0]['message'])
        Model 'core__clients' IS modified in git
    """
    warnings = []
    modified = is_modified(model_name)

    # Case 1: Using --dev but model NOT modified
    if use_dev and not modified:
        warnings.append({
            "type": "dev_without_changes",
            "severity": "warning",
            "message": f"Model '{model_name}' NOT modified in git, but using --dev flag",
            "detail": "Dev table may not exist or may be outdated",
            "suggestion": "Remove --dev flag to query production table"
        })

    # Case 2: NOT using --dev but model IS modified
    elif not use_dev and modified:
        warnings.append({
            "type": "git_mismatch",
            "severity": "warning",
            "message": f"Model '{model_name}' IS modified in git",
            "detail": "Querying production table, but local changes exist",
            "suggestion": "Use --dev flag to query dev table"
        })

    # Case 3: Using --dev but dev manifest not found
    if use_dev and dev_manifest_found is None:
        warnings.append({
            "type": "dev_manifest_missing",
            "severity": "error",
            "message": "Dev manifest (target/manifest.json) not found",
            "detail": "Dev table cannot be queried without manifest",
            "suggestion": f"Run 'defer run --select {model_name}' to build dev table"
        })

    return warnings
