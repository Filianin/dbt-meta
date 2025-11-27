"""Model state detection for dbt-meta.

This module detects model lifecycle state based on git status and manifest presence.
Used to provide context-aware messages and suggestions for the columns command.
"""

from enum import Enum
from typing import Any, Optional

from dbt_meta.utils.git import GitStatus

__all__ = ['ModelState', 'detect_model_state']


class ModelState(Enum):
    """Model lifecycle state.

    States map to 17 scenarios documented in .qa/model_states_detailed.md
    """
    # NEW models (not in production manifest)
    NEW_UNCOMMITTED = "new_uncommitted"          # Untracked file
    NEW_COMMITTED = "new_committed"              # Committed, not in prod
    NEW_IN_DEV = "new_in_dev_manifest"           # In dev manifest only

    # MODIFIED models (in production manifest with changes)
    MODIFIED_UNCOMMITTED = "modified_uncommitted"  # Local changes
    MODIFIED_COMMITTED = "modified_committed"      # Committed changes
    MODIFIED_IN_DEV = "modified_in_dev_manifest"   # In dev manifest

    # PROD models (stable, in production)
    PROD_STABLE = "prod_stable"                  # Clean, in prod

    # DELETED models
    DELETED_LOCALLY = "deleted_locally"          # Deleted, still in prod
    DELETED_DEPLOYED = "deleted_deployed"        # Removed from prod

    # DEPRECATED models
    DEPRECATED_DISABLED = "deprecated_disabled"  # enabled: false
    DEPRECATED_FOLDER = "deprecated_folder"      # In deprecated/

    # RENAMED models
    RENAMED_NEW = "renamed_new"                  # New name after rename
    RENAMED_OLD = "renamed_old"                  # Old name after rename

    # NOT FOUND
    NOT_FOUND = "not_found"                      # Doesn't exist


def detect_model_state(
    model_name: str,
    in_prod_manifest: bool,
    in_dev_manifest: bool,
    git_status: GitStatus,
    model: Optional[dict[str, Any]] = None,
    file_path: Optional[str] = None
) -> ModelState:
    """Detect model state based on manifest presence and git status.

    Decision tree:
    1. Check deprecated status (config.enabled, folder)
    2. Check git file existence and status
    3. Check manifest presence (prod/dev)
    4. Combine to determine state

    Args:
        model_name: Model name (e.g., 'core_client__events')
        in_prod_manifest: True if model exists in production manifest
        in_dev_manifest: True if model exists in dev manifest
        git_status: GitStatus object with file status
        model: Optional model dict from manifest (for config.enabled check)
        file_path: Optional file path (for deprecated/ folder check)

    Returns:
        ModelState enum value representing current state

    Examples:
        >>> # NEW uncommitted model
        >>> git = GitStatus(exists=True, is_new=True, is_tracked=False, ...)
        >>> state = detect_model_state("new_model", False, False, git)
        >>> assert state == ModelState.NEW_UNCOMMITTED

        >>> # PROD stable model
        >>> git = GitStatus(exists=True, is_committed=True, is_modified=False, ...)
        >>> state = detect_model_state("prod_model", True, False, git)
        >>> assert state == ModelState.PROD_STABLE
    """
    # DEPRECATED models (check first - highest priority)
    # 1. Check config.enabled = false
    if model is not None:
        config = model.get('config', {})
        if isinstance(config, dict) and config.get('enabled') is False:
            return ModelState.DEPRECATED_DISABLED

    # 2. Check deprecated/ folder
    if file_path is not None:
        # Normalize path separators
        normalized_path = file_path.replace('\\', '/')
        if '/deprecated/' in normalized_path or normalized_path.startswith('deprecated/'):
            return ModelState.DEPRECATED_FOLDER

    # RENAMED models (check before deleted - rename can coexist with other states)
    if git_status.is_renamed and git_status.renamed_from and git_status.renamed_to:
        # If file_path or model_name matches the old path - this is the OLD name
        if file_path and git_status.renamed_from in file_path:
            return ModelState.RENAMED_OLD
        # Otherwise, this is the NEW name
        return ModelState.RENAMED_NEW

    # DELETED models (file deleted from disk)
    if git_status.is_deleted or not git_status.exists:
        if in_prod_manifest:
            return ModelState.DELETED_LOCALLY  # File deleted but still in prod manifest
        # File doesn't exist and not in prod - model was never deployed or fully removed
        return ModelState.NOT_FOUND

    # NEW models (not in production manifest)
    if not in_prod_manifest:
        if git_status.is_new:
            # Untracked file
            return ModelState.NEW_UNCOMMITTED

        if git_status.is_committed:
            # Committed but not deployed
            if in_dev_manifest:
                return ModelState.NEW_IN_DEV
            return ModelState.NEW_COMMITTED

        # File exists (we know from line 84 check) but not in git and not in prod
        # This is likely a new untracked file that failed to be detected as "new"
        # or a file in a non-git directory
        if git_status.exists:
            return ModelState.NEW_UNCOMMITTED

        # Only return NOT_FOUND if we truly can't categorize it
        return ModelState.NOT_FOUND

    # MODIFIED models (in production manifest with changes)
    if in_prod_manifest:
        if git_status.is_modified:
            # Has uncommitted local changes
            if in_dev_manifest:
                return ModelState.MODIFIED_IN_DEV
            return ModelState.MODIFIED_UNCOMMITTED
        # No modifications → stable
        return ModelState.PROD_STABLE

    # Fallback for edge cases
    return ModelState.NOT_FOUND
