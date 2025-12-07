"""Git operations for dbt-meta.

This module handles git-related operations:
- Checking if model files are modified
- Detecting git/manifest mismatches
- Full git status detection for model state tracking
"""

from __future__ import annotations

import subprocess
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dbt_meta.manifest.parser import ManifestParser

__all__ = ['GitStatus', 'check_manifest_git_mismatch', 'get_model_git_status', 'is_modified', 'validate_path']


def validate_path(path: str) -> str:
    """Validate path is safe for subprocess execution.

    Prevents command injection by rejecting dangerous patterns.

    Args:
        path: Path to validate

    Returns:
        Validated path (unchanged if safe)

    Raises:
        ValueError: If path contains dangerous patterns

    Example:
        >>> validate_path("models/core/clients.sql")
        'models/core/clients.sql'

        >>> validate_path("../../etc/passwd")
        ValueError: Unsafe path contains parent directory traversal: ../../etc/passwd
    """
    if not path:
        raise ValueError("Path cannot be empty")

    # Check for directory traversal
    if '..' in path:
        raise ValueError(f"Unsafe path contains parent directory traversal: {path}")

    # Check for command injection characters
    dangerous_chars = [';', '&', '|', '`', '$', '(', ')', '{', '}', '<', '>', '\n', '\r']
    for char in dangerous_chars:
        if char in path:
            raise ValueError(f"Unsafe path contains shell metacharacter '{char}': {path}")

    # Check for absolute paths outside project (security risk)
    if path.startswith('/') and not path.startswith('/Users/'):
        raise ValueError(f"Unsafe absolute path outside user directory: {path}")

    return path


@dataclass
class GitStatus:
    """Git status of a model file.

    Attributes:
        exists: File exists on disk
        is_tracked: Git knows about the file
        is_modified: Has uncommitted changes
        is_committed: Committed to git history
        is_deleted: Deleted from disk
        is_new: Untracked or newly added
        is_renamed: File was renamed (git mv)
        renamed_from: Old filename (if renamed)
        renamed_to: New filename (if renamed)
    """
    exists: bool
    is_tracked: bool
    is_modified: bool
    is_committed: bool
    is_deleted: bool
    is_new: bool
    is_renamed: bool = False
    renamed_from: str | None = None
    renamed_to: str | None = None


def is_committed_but_not_in_main(model_name: str) -> bool:
    """Check if model file is committed in current branch but not in main/master.

    Compares current branch with main/master to detect committed but not merged changes.

    Args:
        model_name: dbt model name (e.g., "core_client__events")

    Returns:
        True if model is in current branch but not in main/master, False otherwise

    Example:
        >>> is_committed_but_not_in_main('core_client__events')
        True  # If models/core/client/events.sql is committed but not merged
    """
    try:
        # Extract table name from model_name
        if '__' not in model_name:
            table = model_name
        else:
            parts = model_name.split('__')
            table = parts[-1]

        # Try different branch names in order of likelihood
        for base_branch in ['origin/main', 'origin/master', 'main', 'master']:
            result = subprocess.run(
                ['git', 'diff', f'{base_branch}...HEAD', '--name-only'],
                capture_output=True,
                text=True,
                timeout=5
            )

            if result.returncode == 0:
                # Check if any changed file contains the table name OR full model name
                changed_files = result.stdout.splitlines()
                for file_path in changed_files:
                    if (
                        (f"/{table}.sql" in file_path or file_path == f"{table}.sql" or
                         f"/{model_name}.sql" in file_path or file_path == f"{model_name}.sql")
                        and file_path.endswith('.sql')
                    ):
                        return True
                # Found branch, no match - return False
                return False

        # No base branch found
        return False

    except (subprocess.TimeoutExpired, FileNotFoundError, subprocess.CalledProcessError, OSError):
        # If git check fails, assume not committed (safe default)
        return False


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
            # Check if any modified file contains the table name OR full model name
            modified_files = result.stdout.splitlines()
            for file_path in modified_files:
                # Match by table name (e.g., user_devices.sql)
                # OR by full model name (e.g., core_google_events__user_devices.sql)
                # Use exact filename match to avoid false positives
                if (
                    (f"/{table}.sql" in file_path or file_path == f"{table}.sql" or
                     f"/{model_name}.sql" in file_path or file_path == f"{model_name}.sql")
                    and file_path.endswith('.sql')
                ):
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
                # Match by table name OR full model name
                if (
                    (line.startswith('??') or line.startswith('A '))
                    and (f"/{table}.sql" in line or line.endswith(f" {table}.sql") or
                         f"/{model_name}.sql" in line or line.endswith(f" {model_name}.sql"))
                    and '.sql' in line
                ):
                    return True

        return False

    except (subprocess.TimeoutExpired, FileNotFoundError, subprocess.CalledProcessError, OSError):
        # If git check fails, assume not modified (safe default)
        return False


@lru_cache(maxsize=128)
def _find_sql_file_fast(model_name: str) -> str | None:
    """Find .sql file in models/ directory with performance bounds.

    Quick filesystem check to verify if model file exists.
    Used to detect files that exist but weren't compiled into manifest.

    Args:
        model_name: dbt model name (e.g., "stg_appsflyer__in_app_events_postbacks")

    Returns:
        Relative path to .sql file or None if not found

    Performance:
        - LRU cached for repeated calls
        - Maximum 1000 files searched (safety limit)
        - Returns None if models/ directory doesn't exist

    Example:
        >>> _find_sql_file_fast('stg_appsflyer__upload_log')
        'models/staging/appsflyer/stg_appsflyer__upload_log.sql'
    """
    try:
        # Extract table name from model_name (e.g., "stg_appsflyer__upload_log" → "upload_log")
        # Note: Some models use full name as filename, so try both
        table_name = model_name.split('__')[-1] if '__' in model_name else model_name

        # Check if models/ directory exists in current working directory
        models_dir = Path('models')
        if not models_dir.exists():
            return None

        # Search with performance bound (max 1000 files)
        for i, sql_file in enumerate(models_dir.rglob('*.sql')):
            if i >= 1000:  # Safety limit to prevent runaway search
                return None

            # Match by filename stem (without .sql extension)
            # Try exact match with table name or full model name
            if sql_file.stem == table_name or sql_file.stem == model_name:
                return str(sql_file)

        return None

    except (OSError, PermissionError):
        # If filesystem check fails, return None (safe default)
        # This can happen if models/ directory is inaccessible
        return None


def check_manifest_git_mismatch(
    model_name: str,
    use_dev: bool,
    dev_manifest_found: str | None = None,
    prod_parser: ManifestParser | None = None,
    dev_parser: ManifestParser | None = None
) -> list[dict[str, str]]:
    """Check git status and return structured warnings.

    Returns list of warning objects that can be output as JSON (with -j) or text (without -j).

    Warning types:
    - new_model: Model exists ONLY in dev manifest, not in production (CRITICAL)
    - git_mismatch: Model modified in git but querying production
    - dev_without_changes: Using --dev but model not modified
    - dev_manifest_missing: Using --dev but dev manifest not found

    Args:
        model_name: dbt model name (e.g., "core_client__events")
        use_dev: Whether --dev flag was used
        dev_manifest_found: Path to dev manifest if found, None otherwise
        prod_parser: Production manifest parser (optional, for new model detection)
        dev_parser: Dev manifest parser (optional, for new model detection)

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
    committed = is_committed_but_not_in_main(model_name)

    # CRITICAL: Check for NEW MODEL state (only in dev, not in prod)
    # This must be detected FIRST before generic "modified" checks
    # NEW model is determined by manifest state, NOT git status
    # Model in dev but NOT in prod = NEW MODEL (regardless of git status)
    if prod_parser and dev_parser and not use_dev:
        try:
            in_prod = prod_parser.get_model(model_name) is not None
            in_dev = dev_parser.get_model(model_name) is not None

            # Case: New model (only in dev, not in production)
            # This can be either:
            #  1. NEW model in feature branch (not merged to master)
            #  2. DEV model from defer run (legitimate fallback scenario)
            # We can't definitively distinguish these cases, so we let fallback proceed
            # and only add a warning (not an error that blocks fallback)
            if not in_prod and in_dev and modified:
                # Only warn if file is modified (likely a new model in development)
                # If file not modified, it's probably a defer build (let fallback proceed silently)
                warnings.append({
                    "type": "new_model_candidate",
                    "severity": "warning",
                    "message": f"Model '{model_name}' exists in dev manifest but NOT in production",
                    "detail": "This may be a new model or a defer-built model",
                    "suggestion": "Use --dev flag to explicitly query dev table if this is a new model"
                })
                # NO early return - let fallback proceed for defer scenarios

            # Case: Using --dev but model NOT in dev manifest
            # This happens when user wants to query dev table but hasn't built it yet
            if use_dev and not in_dev:
                warnings.append({
                    "type": "model_not_in_dev",
                    "severity": "error",
                    "message": f"Model '{model_name}' NOT found in dev manifest",
                    "detail": "Using --dev flag but model not built in dev environment",
                    "suggestion": f"Run 'defer run --select {model_name}' to build model in dev"
                })
                # Early return - can't proceed without dev manifest
                return warnings

            # Case: File exists but NOT compiled into manifest
            # This happens when dbt compile fails due to SQL errors, missing deps, etc.
            if modified and not in_prod and not in_dev:
                warnings.append({
                    "type": "file_not_compiled",
                    "severity": "error",
                    "message": "Model file detected in git but NOT in manifest",
                    "detail": "File exists but compilation likely failed",
                    "suggestion": f"Run 'dbt compile --select {model_name}' and check for errors.\nPossible causes: SQL syntax error, missing dependencies, disabled in dbt_project.yml"
                })
                # Early return - this is also critical
                return warnings
        except (AttributeError, KeyError, TypeError):
            # If parser check fails (missing methods/keys), continue with normal flow
            # This can happen if manifest structure is different or parser is None
            pass

    # Case 1: Using --dev but model NOT modified OR committed
    if use_dev and not modified and not committed:
        # Model is clean (not modified, not committed in branch)
        warnings.append({
            "type": "dev_without_changes",
            "severity": "warning",
            "message": f"Model '{model_name}' has no changes in current branch, but using --dev flag",
            "detail": "Dev table may not exist or may be outdated",
            "suggestion": "Remove --dev flag to query production table"
        })
    elif use_dev and not modified and committed:
        # Model is committed but not merged to main (no local changes)
        warnings.append({
            "type": "dev_committed_not_merged",
            "severity": "info",
            "message": f"Model '{model_name}' is committed but not merged to main",
            "detail": "Querying dev table with committed changes (not in production yet)",
            "suggestion": "Changes are in your branch but not in production"
        })

    # Case 2: NOT using --dev but model IS modified (uncommitted changes)
    elif not use_dev and modified:
        warnings.append({
            "type": "git_mismatch",
            "severity": "warning",
            "message": f"Model '{model_name}' is modified in git",
            "detail": "Querying production table, but local changes exist",
            "suggestion": "Use --dev flag to query dev table"
        })

    # Case 3: NOT using --dev but model IS committed (no uncommitted changes)
    elif not use_dev and not modified and committed:
        warnings.append({
            "type": "git_committed",
            "severity": "info",
            "message": f"Model '{model_name}' is committed but not merged to main",
            "detail": "Querying production table (may not have your branch changes)",
            "suggestion": "Use --dev flag to query dev table if changes were built with defer"
        })

    # Case 4: Using --dev but dev manifest not found
    if use_dev and dev_manifest_found is None:
        warnings.append({
            "type": "dev_manifest_missing",
            "severity": "error",
            "message": "Dev manifest (target/manifest.json) not found",
            "detail": "Dev table cannot be queried without manifest",
            "suggestion": f"Run 'defer run --select {model_name}' to build dev table"
        })

    return warnings


def get_model_git_status(model_name: str, file_path: str | None = None) -> GitStatus:
    """Detect complete git status of model file.

    Process:
    1. Use file_path from manifest OR find .sql file from model_name
    2. Check if file exists on disk
    3. Check git status via 'git status --porcelain'
    4. Check git history via 'git log --all -- path'

    Args:
        model_name: Model name in dbt format (e.g., 'core_client__events')
        file_path: Optional file path from manifest (e.g., 'models/staging/model.sql')
                  If provided, uses this instead of searching for file

    Returns:
        GitStatus with all flags set

    Example:
        >>> status = get_model_git_status('core_client__events')
        >>> # With manifest path
        >>> status = get_model_git_status('core_client__events', 'models/core/events.sql')
    """
    # Track whether file_path was provided (from manifest) or found (from filesystem)
    file_path_from_manifest = file_path is not None

    # Use provided file_path or find it
    if not file_path:
        file_path = _find_sql_file_fast(model_name)

    if not file_path:
        # File not found via search
        return GitStatus(
            exists=False,
            is_tracked=False,
            is_modified=False,
            is_committed=False,
            is_deleted=False,
            is_new=False
        )

    # Check if file exists on disk (ONLY if file_path from manifest)
    # If from _find_sql_file_fast, it already checked existence
    if file_path_from_manifest:
        file_exists = Path(file_path).exists()
        if not file_exists:
            # File in manifest but not on disk = deleted
            return GitStatus(
                exists=False,
                is_tracked=False,
                is_modified=False,
                is_committed=False,
                is_deleted=True,
                is_new=False
            )

    # Check git status
    try:
        # Validate path for safety before using in subprocess
        safe_path = validate_path(file_path)

        # Get git status for the file
        result = subprocess.run(
            ['git', 'status', '--porcelain', safe_path],
            capture_output=True,
            text=True,
            timeout=5
        )

        if result.returncode != 0:
            # Git command failed, return minimal status
            return GitStatus(
                exists=True,
                is_tracked=False,
                is_modified=False,
                is_committed=False,
                is_deleted=False,
                is_new=False
            )

        status_line = result.stdout.strip()

        # Parse git status codes
        # ?? = untracked (new)
        # M  = modified, staged
        #  M = modified, unstaged
        # MM = modified, staged and unstaged
        # A  = added (new, staged)
        # D  = deleted
        # R  = renamed (format: "R  old_path -> new_path")
        is_new = status_line.startswith('??') or status_line.startswith('A ')
        is_modified = 'M' in status_line or status_line.startswith('A ')
        is_deleted = 'D' in status_line
        is_renamed = status_line.startswith('R ')
        renamed_from = None
        renamed_to = None

        # Parse rename information if present
        if is_renamed and ' -> ' in status_line:
            # Format: "R  old_path -> new_path"
            parts = status_line.split(' -> ')
            if len(parts) == 2:
                # Remove 'R  ' prefix from old_path
                renamed_from = parts[0][3:].strip()
                renamed_to = parts[1].strip()

        # Check if file is in git history (committed)
        log_result = subprocess.run(
            ['git', 'log', '--all', '--', safe_path],
            capture_output=True,
            text=True,
            timeout=5
        )

        is_committed = bool(log_result.stdout.strip())
        is_tracked = is_committed or (bool(status_line) and not is_new)

        return GitStatus(
            exists=True,
            is_tracked=is_tracked,
            is_modified=is_modified,
            is_committed=is_committed,
            is_deleted=is_deleted,
            is_new=is_new,
            is_renamed=is_renamed,
            renamed_from=renamed_from,
            renamed_to=renamed_to
        )

    except subprocess.TimeoutExpired:
        # Timeout - file exists but can't determine git status
        return GitStatus(
            exists=True,
            is_tracked=False,
            is_modified=False,
            is_committed=False,
            is_deleted=False,
            is_new=False
        )
    except (OSError, ValueError, UnicodeDecodeError):
        # Any other error - safe fallback
        # OSError: file system issues
        # ValueError: git output parsing issues
        # UnicodeDecodeError: non-UTF8 file names
        return GitStatus(
            exists=True,
            is_tracked=False,
            is_modified=False,
            is_committed=False,
            is_deleted=False,
            is_new=False
        )
