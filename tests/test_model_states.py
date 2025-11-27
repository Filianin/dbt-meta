"""Test all 14 model lifecycle states.

CRITICAL: These tests verify correct state detection which determines:
- Which BigQuery schema to query (prod vs dev)
- Which manifest to use
- What warnings to show

Failure in state detection = wrong data queried!
"""

import pytest

from dbt_meta.utils.git import GitStatus
from dbt_meta.utils.model_state import ModelState, detect_model_state


@pytest.mark.critical
class TestAllModelStates:
    """Test all 14 model states documented in the architecture."""

    def test_new_uncommitted(self):
        """New model, not committed to git."""
        git = GitStatus(
            exists=True,
            is_tracked=False,
            is_modified=False,
            is_committed=False,
            is_deleted=False,
            is_new=True
        )
        state = detect_model_state("new_model", False, False, git)
        assert state == ModelState.NEW_UNCOMMITTED

    def test_new_committed(self):
        """New model, committed but not in production."""
        git = GitStatus(
            exists=True,
            is_tracked=True,
            is_modified=False,
            is_committed=True,
            is_deleted=False,
            is_new=False
        )
        state = detect_model_state("new_model", False, False, git)
        assert state == ModelState.NEW_COMMITTED

    def test_new_in_dev(self):
        """New model in dev manifest only."""
        git = GitStatus(
            exists=True,
            is_tracked=True,
            is_modified=False,
            is_committed=True,
            is_deleted=False,
            is_new=False
        )
        state = detect_model_state("new_model", False, True, git)
        assert state == ModelState.NEW_IN_DEV

    def test_modified_uncommitted(self):
        """Model in prod with uncommitted changes."""
        git = GitStatus(
            exists=True,
            is_tracked=True,
            is_modified=True,
            is_committed=False,
            is_deleted=False,
            is_new=False
        )
        state = detect_model_state("mod_model", True, False, git)
        assert state == ModelState.MODIFIED_UNCOMMITTED

    def test_modified_uncommitted_not_in_dev(self):
        """Model in prod with uncommitted changes, not compiled in dev."""
        git = GitStatus(
            exists=True,
            is_tracked=True,
            is_modified=True,
            is_committed=True,
            is_deleted=False,
            is_new=False
        )
        state = detect_model_state("mod_model", True, False, git)
        assert state == ModelState.MODIFIED_UNCOMMITTED

    def test_modified_in_dev(self):
        """Model in prod with uncommitted changes, compiled in dev."""
        git = GitStatus(
            exists=True,
            is_tracked=True,
            is_modified=True,  # Has uncommitted changes
            is_committed=True,
            is_deleted=False,
            is_new=False
        )
        state = detect_model_state("mod_model", True, True, git)
        assert state == ModelState.MODIFIED_IN_DEV

    def test_prod_stable(self):
        """Model in production, no changes."""
        git = GitStatus(
            exists=True,
            is_tracked=True,
            is_modified=False,
            is_committed=True,
            is_deleted=False,
            is_new=False
        )
        state = detect_model_state("stable_model", True, False, git)
        assert state == ModelState.PROD_STABLE

    def test_deleted_locally(self):
        """Model deleted locally but still in prod."""
        git = GitStatus(
            exists=False,
            is_tracked=True,
            is_modified=False,
            is_committed=False,
            is_deleted=True,
            is_new=False
        )
        state = detect_model_state("deleted_model", True, False, git)
        assert state == ModelState.DELETED_LOCALLY

    def test_deleted_deployed_was_wrong(self):
        """CRITICAL FIX: Model deleted and NOT in prod should be NOT_FOUND, not DELETED_DEPLOYED."""
        git = GitStatus(
            exists=False,
            is_tracked=False,
            is_modified=False,
            is_committed=False,
            is_deleted=True,
            is_new=False
        )
        state = detect_model_state("never_existed", False, False, git)
        # BEFORE FIX: Would incorrectly return ModelState.DELETED_DEPLOYED
        # AFTER FIX: Correctly returns NOT_FOUND
        assert state == ModelState.NOT_FOUND

    def test_not_found(self):
        """Model doesn't exist anywhere."""
        git = GitStatus(
            exists=False,
            is_tracked=False,
            is_modified=False,
            is_committed=False,
            is_deleted=False,
            is_new=False
        )
        state = detect_model_state("unknown_model", False, False, git)
        assert state == ModelState.NOT_FOUND

    def test_edge_case_exists_but_not_new(self):
        """CRITICAL FIX: File exists but git status doesn't show as new."""
        git = GitStatus(
            exists=True,  # File exists
            is_tracked=False,
            is_modified=False,
            is_committed=False,
            is_deleted=False,
            is_new=False  # Git doesn't report as new (edge case)
        )
        state = detect_model_state("edge_model", False, False, git)
        # BEFORE FIX: Would return NOT_FOUND even though file exists
        # AFTER FIX: Correctly returns NEW_UNCOMMITTED
        assert state == ModelState.NEW_UNCOMMITTED

    def test_committed_in_prod_clean(self):
        """Model committed and in prod, no local changes."""
        git = GitStatus(
            exists=True,
            is_tracked=True,
            is_modified=False,  # No local changes
            is_committed=True,
            is_deleted=False,
            is_new=False
        )
        state = detect_model_state("prod_model", True, False, git)
        assert state == ModelState.PROD_STABLE

    def test_new_file_tracked_but_not_in_manifest(self):
        """File added to git but not in any manifest."""
        git = GitStatus(
            exists=True,
            is_tracked=True,  # Added to git
            is_modified=False,
            is_committed=True,
            is_deleted=False,
            is_new=False
        )
        state = detect_model_state("added_model", False, False, git)
        assert state == ModelState.NEW_COMMITTED

    def test_file_in_both_manifests_no_changes(self):
        """Model in both prod and dev manifests, no changes - should be PROD_STABLE."""
        git = GitStatus(
            exists=True,
            is_tracked=True,
            is_modified=False,  # No uncommitted changes
            is_committed=True,
            is_deleted=False,
            is_new=False
        )
        state = detect_model_state("both_model", True, True, git)
        # No modifications → PROD_STABLE (being in dev manifest doesn't matter)
        assert state == ModelState.PROD_STABLE