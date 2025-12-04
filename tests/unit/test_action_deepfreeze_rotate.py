"""Test deepfreeze Rotate action"""
# pylint: disable=attribute-defined-outside-init
from unittest import TestCase
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime
import pytest

from curator.actions.deepfreeze.rotate import Rotate
from curator.actions.deepfreeze.helpers import Settings, Repository
from curator.actions.deepfreeze.constants import STATUS_INDEX
from curator.actions.deepfreeze.exceptions import MissingIndexError, PreconditionError, ActionException


class TestDeepfreezeRotate(TestCase):
    """Test Deepfreeze Rotate action"""

    def setUp(self):
        """Set up test fixtures"""
        self.client = Mock()
        # Mock ILM get_lifecycle to return empty dict by default
        self.client.ilm.get_lifecycle.return_value = {}
        self.mock_settings = Settings(
            repo_name_prefix="deepfreeze",
            bucket_name_prefix="deepfreeze",
            base_path_prefix="snapshots",
            rotate_by="path",
            style="oneup",
            last_suffix="000001"
        )
        self.mock_latest_repo = Repository(
            name="deepfreeze-000001",
            bucket="deepfreeze",
            base_path="snapshots-000001",
            is_mounted=True,
            is_thawed=False
        )

    def test_init_defaults(self):
        """Test Rotate initialization with default values"""
        with patch('curator.actions.deepfreeze.rotate.get_settings', return_value=self.mock_settings):
            with patch('curator.actions.deepfreeze.rotate.get_matching_repo_names', return_value=["deepfreeze-000001"]):
                with patch('curator.actions.deepfreeze.rotate.get_next_suffix', return_value="000002"):
                    with patch('curator.actions.deepfreeze.rotate.s3_client_factory') as mock_factory:
                        with patch('curator.actions.deepfreeze.rotate.get_policies_for_repo') as mock_policies:
                            mock_s3 = Mock()
                            mock_factory.return_value = mock_s3
                            mock_policies.return_value = {"test-policy": {}}
                            self.client.indices.exists.return_value = True

                            rotate = Rotate(self.client)

                            assert rotate.client == self.client
                            assert rotate.s3 == mock_s3
                            assert rotate.settings == self.mock_settings
                            assert rotate.latest_repo == "deepfreeze-000001"
                            assert rotate.keep == 6  # default value

    def test_calculate_new_names_rotate_by_path_oneup(self):
        """Test name calculation for path rotation with oneup style"""
        with patch('curator.actions.deepfreeze.rotate.get_settings', return_value=self.mock_settings):
            with patch('curator.actions.deepfreeze.rotate.get_matching_repo_names', return_value=["deepfreeze-000001"]):
                with patch('curator.actions.deepfreeze.rotate.get_next_suffix', return_value="000002"):
                    with patch('curator.actions.deepfreeze.rotate.s3_client_factory'):
                        with patch('curator.actions.deepfreeze.rotate.get_policies_for_repo') as mock_policies:
                            mock_policies.return_value = {"test-policy": {}}  # Mock at least one policy
                            self.client.indices.exists.return_value = True
                            rotate = Rotate(self.client)

                            assert rotate.new_repo_name == "deepfreeze-000002"
                            assert rotate.new_bucket_name == "deepfreeze"
                            assert rotate.base_path == "snapshots-000002"

    def test_calculate_new_names_rotate_by_bucket(self):
        """Test name calculation for bucket rotation"""
        settings = Settings(
            repo_name_prefix="deepfreeze",
            bucket_name_prefix="deepfreeze",
            base_path_prefix="snapshots",
            rotate_by="bucket",
            style="oneup",
            last_suffix="000003"
        )

        with patch('curator.actions.deepfreeze.rotate.get_settings', return_value=settings):
            with patch('curator.actions.deepfreeze.rotate.get_matching_repo_names', return_value=["deepfreeze-000003"]):
                with patch('curator.actions.deepfreeze.rotate.get_next_suffix', return_value="000004"):
                    with patch('curator.actions.deepfreeze.rotate.s3_client_factory'):
                        with patch('curator.actions.deepfreeze.rotate.get_policies_for_repo') as mock_policies:
                            mock_policies.return_value = {"test-policy": {}}
                            self.client.indices.exists.return_value = True
                            rotate = Rotate(self.client)

                            assert rotate.new_repo_name == "deepfreeze-000004"
                            assert rotate.new_bucket_name == "deepfreeze-000004"
                            assert rotate.base_path == "snapshots"

    def test_calculate_new_names_monthly_style(self):
        """Test name calculation with monthly style"""
        settings = Settings(
            repo_name_prefix="deepfreeze",
            bucket_name_prefix="deepfreeze",
            base_path_prefix="snapshots",
            rotate_by="path",
            style="monthly",
            last_suffix="2024.02"
        )

        with patch('curator.actions.deepfreeze.rotate.get_settings', return_value=settings):
            with patch('curator.actions.deepfreeze.rotate.get_matching_repo_names', return_value=["deepfreeze-2024.02"]):
                with patch('curator.actions.deepfreeze.rotate.get_next_suffix', return_value="2024.03"):
                    with patch('curator.actions.deepfreeze.rotate.s3_client_factory'):
                        with patch('curator.actions.deepfreeze.rotate.get_policies_for_repo') as mock_policies:
                            mock_policies.return_value = {"test-policy": {}}
                            self.client.indices.exists.return_value = True
                            rotate = Rotate(self.client)

                            assert rotate.new_repo_name == "deepfreeze-2024.03"
                            assert rotate.base_path == "snapshots-2024.03"

    def test_check_preconditions_missing_index(self):
        """Test preconditions check when status index is missing"""
        from elasticsearch8 import NotFoundError

        with patch('curator.actions.deepfreeze.rotate.get_settings') as mock_get_settings:
            mock_get_settings.side_effect = MissingIndexError("Status index missing")

            with pytest.raises(MissingIndexError):
                Rotate(self.client)

    def test_check_preconditions_new_repo_exists(self):
        """Test preconditions check when new repository already exists"""
        # Return repo list that includes the new repo name that will be calculated
        with patch('curator.actions.deepfreeze.rotate.get_settings', return_value=self.mock_settings):
            with patch('curator.actions.deepfreeze.rotate.get_matching_repo_names', return_value=["deepfreeze-000001", "deepfreeze-000002"]):
                with patch('curator.actions.deepfreeze.rotate.get_next_suffix', return_value="000002"):
                    with patch('curator.actions.deepfreeze.rotate.s3_client_factory'):
                        self.client.indices.exists.return_value = True
                        from curator.exceptions import RepositoryException
                        with pytest.raises(RepositoryException, match="already exists"):
                            Rotate(self.client)

    def test_check_preconditions_success(self):
        """Test successful preconditions check"""
        with patch('curator.actions.deepfreeze.rotate.get_settings', return_value=self.mock_settings):
            with patch('curator.actions.deepfreeze.rotate.get_matching_repo_names', return_value=["deepfreeze-000001"]):
                with patch('curator.actions.deepfreeze.rotate.get_next_suffix', return_value="000002"):
                    with patch('curator.actions.deepfreeze.rotate.s3_client_factory') as mock_factory:
                        with patch('curator.actions.deepfreeze.rotate.get_policies_for_repo') as mock_policies:
                            mock_s3 = Mock()
                            mock_factory.return_value = mock_s3
                            mock_policies.return_value = {"test-policy": {}}
                            self.client.indices.exists.return_value = True

                            # Should not raise any exceptions
                            rotate = Rotate(self.client)
                            assert rotate is not None

    def test_update_ilm_policies_creates_versioned_policies(self):
        """Test that update_ilm_policies creates versioned policies instead of modifying existing ones"""
        with patch('curator.actions.deepfreeze.rotate.get_settings', return_value=self.mock_settings):
            with patch('curator.actions.deepfreeze.rotate.get_matching_repo_names', return_value=["deepfreeze-000001"]):
                with patch('curator.actions.deepfreeze.rotate.get_next_suffix', return_value="000002"):
                    with patch('curator.actions.deepfreeze.rotate.s3_client_factory'):
                        with patch('curator.actions.deepfreeze.rotate.get_policies_for_repo') as mock_get_policies:
                            with patch('curator.actions.deepfreeze.rotate.create_versioned_ilm_policy') as mock_create:
                                with patch('curator.actions.deepfreeze.rotate.get_composable_templates') as mock_get_composable:
                                    with patch('curator.actions.deepfreeze.rotate.get_index_templates') as mock_get_templates:
                                        with patch('curator.actions.deepfreeze.rotate.update_template_ilm_policy') as mock_update_template:
                                            self.client.indices.exists.return_value = True

                                            # Mock policy that references the old repo
                                            mock_get_policies.return_value = {
                                                "my-policy": {
                                                    "policy": {
                                                        "phases": {
                                                            "cold": {
                                                                "actions": {
                                                                    "searchable_snapshot": {
                                                                        "snapshot_repository": "deepfreeze-000001"
                                                                    }
                                                                }
                                                            }
                                                        }
                                                    }
                                                }
                                            }

                                            mock_create.return_value = "my-policy-000002"
                                            mock_get_composable.return_value = {"index_templates": []}
                                            mock_get_templates.return_value = {}

                                            rotate = Rotate(self.client)
                                            rotate.update_ilm_policies(dry_run=False)

                                            # Verify versioned policy was created
                                            mock_create.assert_called_once()
                                            call_args = mock_create.call_args
                                            assert call_args[0][1] == "my-policy"  # base policy name
                                            assert call_args[0][3] == "deepfreeze-000002"  # new repo name
                                            assert call_args[0][4] == "000002"  # suffix

    def test_update_ilm_policies_updates_templates(self):
        """Test that update_ilm_policies updates index templates to use new versioned policies"""
        with patch('curator.actions.deepfreeze.rotate.get_settings', return_value=self.mock_settings):
            with patch('curator.actions.deepfreeze.rotate.get_matching_repo_names', return_value=["deepfreeze-000001"]):
                with patch('curator.actions.deepfreeze.rotate.get_next_suffix', return_value="000002"):
                    with patch('curator.actions.deepfreeze.rotate.s3_client_factory'):
                        with patch('curator.actions.deepfreeze.rotate.get_policies_for_repo') as mock_get_policies:
                            with patch('curator.actions.deepfreeze.rotate.create_versioned_ilm_policy') as mock_create:
                                with patch('curator.actions.deepfreeze.rotate.get_composable_templates') as mock_get_composable:
                                    with patch('curator.actions.deepfreeze.rotate.get_index_templates') as mock_get_templates:
                                        with patch('curator.actions.deepfreeze.rotate.update_template_ilm_policy') as mock_update_template:
                                            self.client.indices.exists.return_value = True

                                            mock_get_policies.return_value = {
                                                "my-policy": {"policy": {"phases": {}}}
                                            }
                                            mock_create.return_value = "my-policy-000002"

                                            # Mock templates
                                            mock_get_composable.return_value = {
                                                "index_templates": [{"name": "logs-template"}]
                                            }
                                            mock_get_templates.return_value = {"metrics-template": {}}
                                            mock_update_template.return_value = True

                                            rotate = Rotate(self.client)
                                            rotate.update_ilm_policies(dry_run=False)

                                            # Verify templates were updated (both composable and legacy)
                                            assert mock_update_template.call_count >= 2

    def test_update_ilm_policies_dry_run(self):
        """Test that update_ilm_policies dry-run mode doesn't create policies"""
        with patch('curator.actions.deepfreeze.rotate.get_settings', return_value=self.mock_settings):
            with patch('curator.actions.deepfreeze.rotate.get_matching_repo_names', return_value=["deepfreeze-000001"]):
                with patch('curator.actions.deepfreeze.rotate.get_next_suffix', return_value="000002"):
                    with patch('curator.actions.deepfreeze.rotate.s3_client_factory'):
                        with patch('curator.actions.deepfreeze.rotate.get_policies_for_repo') as mock_get_policies:
                            with patch('curator.actions.deepfreeze.rotate.create_versioned_ilm_policy') as mock_create:
                                with patch('curator.actions.deepfreeze.rotate.get_composable_templates') as mock_get_composable:
                                    with patch('curator.actions.deepfreeze.rotate.get_index_templates') as mock_get_templates:
                                        self.client.indices.exists.return_value = True

                                        mock_get_policies.return_value = {
                                            "my-policy": {"policy": {"phases": {}}}
                                        }
                                        mock_get_composable.return_value = {"index_templates": []}
                                        mock_get_templates.return_value = {}

                                        rotate = Rotate(self.client)
                                        rotate.update_ilm_policies(dry_run=True)

                                        # Verify no policies were created in dry-run
                                        mock_create.assert_not_called()

    def test_cleanup_policies_for_repo(self):
        """Test cleanup_policies_for_repo deletes policies with matching suffix"""
        with patch('curator.actions.deepfreeze.rotate.get_settings', return_value=self.mock_settings):
            with patch('curator.actions.deepfreeze.rotate.get_matching_repo_names', return_value=["deepfreeze-000001"]):
                with patch('curator.actions.deepfreeze.rotate.get_next_suffix', return_value="000002"):
                    with patch('curator.actions.deepfreeze.rotate.s3_client_factory'):
                        with patch('curator.actions.deepfreeze.rotate.get_policies_for_repo') as mock_policies:
                            with patch('curator.actions.deepfreeze.rotate.get_policies_by_suffix') as mock_get_by_suffix:
                                with patch('curator.actions.deepfreeze.rotate.is_policy_safe_to_delete') as mock_is_safe:
                                    mock_policies.return_value = {"test-policy": {}}
                                    self.client.indices.exists.return_value = True

                                    # Mock policies with suffix 000001
                                    mock_get_by_suffix.return_value = {
                                        "my-policy-000001": {"policy": {}},
                                        "other-policy-000001": {"policy": {}}
                                    }
                                    mock_is_safe.return_value = True

                                    rotate = Rotate(self.client)
                                    rotate.cleanup_policies_for_repo("deepfreeze-000001", dry_run=False)

                                    # Verify policies were deleted
                                    assert self.client.ilm.delete_lifecycle.call_count == 2
                                    self.client.ilm.delete_lifecycle.assert_any_call(name="my-policy-000001")
                                    self.client.ilm.delete_lifecycle.assert_any_call(name="other-policy-000001")

    def test_cleanup_policies_for_repo_skips_in_use(self):
        """Test cleanup_policies_for_repo skips policies still in use"""
        with patch('curator.actions.deepfreeze.rotate.get_settings', return_value=self.mock_settings):
            with patch('curator.actions.deepfreeze.rotate.get_matching_repo_names', return_value=["deepfreeze-000001"]):
                with patch('curator.actions.deepfreeze.rotate.get_next_suffix', return_value="000002"):
                    with patch('curator.actions.deepfreeze.rotate.s3_client_factory'):
                        with patch('curator.actions.deepfreeze.rotate.get_policies_for_repo') as mock_policies:
                            with patch('curator.actions.deepfreeze.rotate.get_policies_by_suffix') as mock_get_by_suffix:
                                with patch('curator.actions.deepfreeze.rotate.is_policy_safe_to_delete') as mock_is_safe:
                                    mock_policies.return_value = {"test-policy": {}}
                                    self.client.indices.exists.return_value = True

                                    mock_get_by_suffix.return_value = {
                                        "my-policy-000001": {"policy": {}}
                                    }
                                    # Policy is still in use
                                    mock_is_safe.return_value = False

                                    rotate = Rotate(self.client)
                                    rotate.cleanup_policies_for_repo("deepfreeze-000001", dry_run=False)

                                    # Verify policy was NOT deleted
                                    self.client.ilm.delete_lifecycle.assert_not_called()

    def test_cleanup_policies_for_repo_dry_run(self):
        """Test cleanup_policies_for_repo dry-run mode doesn't delete policies"""
        with patch('curator.actions.deepfreeze.rotate.get_settings', return_value=self.mock_settings):
            with patch('curator.actions.deepfreeze.rotate.get_matching_repo_names', return_value=["deepfreeze-000001"]):
                with patch('curator.actions.deepfreeze.rotate.get_next_suffix', return_value="000002"):
                    with patch('curator.actions.deepfreeze.rotate.s3_client_factory'):
                        with patch('curator.actions.deepfreeze.rotate.get_policies_for_repo') as mock_policies:
                            with patch('curator.actions.deepfreeze.rotate.get_policies_by_suffix') as mock_get_by_suffix:
                                with patch('curator.actions.deepfreeze.rotate.is_policy_safe_to_delete') as mock_is_safe:
                                    mock_policies.return_value = {"test-policy": {}}
                                    self.client.indices.exists.return_value = True

                                    mock_get_by_suffix.return_value = {
                                        "my-policy-000001": {"policy": {}}
                                    }
                                    mock_is_safe.return_value = True

                                    rotate = Rotate(self.client)
                                    rotate.cleanup_policies_for_repo("deepfreeze-000001", dry_run=True)

                                    # Verify no policies were deleted in dry-run
                                    self.client.ilm.delete_lifecycle.assert_not_called()

    def test_unmount_oldest_repos_calls_cleanup(self):
        """Test that unmount_oldest_repos calls cleanup_policies_for_repo"""
        with patch('curator.actions.deepfreeze.rotate.get_settings', return_value=self.mock_settings):
            with patch('curator.actions.deepfreeze.rotate.get_matching_repo_names', return_value=["deepfreeze-000002", "deepfreeze-000001"]):
                with patch('curator.actions.deepfreeze.rotate.get_next_suffix', return_value="000003"):
                    with patch('curator.actions.deepfreeze.rotate.s3_client_factory'):
                        with patch('curator.actions.deepfreeze.rotate.get_policies_for_repo') as mock_policies:
                            with patch('curator.actions.deepfreeze.rotate.unmount_repo') as mock_unmount:
                                with patch('curator.actions.deepfreeze.rotate.push_to_glacier'):
                                    with patch('curator.actions.deepfreeze.rotate.Repository') as mock_repo_class:
                                        mock_policies.return_value = {"test-policy": {}}
                                        self.client.indices.exists.return_value = True

                                        mock_repo = Mock()
                                        mock_repo.name = "deepfreeze-000001"
                                        mock_repo.thaw_state = "frozen"  # Make sure repo is not thawed
                                        mock_repo_class.from_elasticsearch.return_value = mock_repo

                                        rotate = Rotate(self.client, keep="1")

                                        with patch.object(rotate, 'cleanup_policies_for_repo') as mock_cleanup:
                                            with patch.object(rotate, 'is_thawed', return_value=False):
                                                rotate.unmount_oldest_repos(dry_run=False)

                                                # Verify cleanup was called for the unmounted repo
                                                mock_cleanup.assert_called_once_with("deepfreeze-000001", dry_run=False)

    def test_unmount_oldest_repos_sets_thaw_state_frozen(self):
        """
        Test that unmount_oldest_repos properly sets thaw_state to 'frozen' after push_to_glacier.

        This is a regression test for the bug where repositories were pushed to Glacier
        but their metadata still showed thaw_state='active' instead of 'frozen'.
        """
        with patch('curator.actions.deepfreeze.rotate.get_settings', return_value=self.mock_settings):
            with patch('curator.actions.deepfreeze.rotate.get_matching_repo_names', return_value=["deepfreeze-000002", "deepfreeze-000001"]):
                with patch('curator.actions.deepfreeze.rotate.get_next_suffix', return_value="000003"):
                    with patch('curator.actions.deepfreeze.rotate.s3_client_factory'):
                        with patch('curator.actions.deepfreeze.rotate.get_policies_for_repo') as mock_policies:
                            with patch('curator.actions.deepfreeze.rotate.unmount_repo') as mock_unmount:
                                with patch('curator.actions.deepfreeze.rotate.push_to_glacier'):
                                    with patch('curator.actions.deepfreeze.rotate.Repository') as mock_repo_class:
                                        mock_policies.return_value = {"test-policy": {}}
                                        self.client.indices.exists.return_value = True

                                        # Create a mock repository that will be returned by from_elasticsearch
                                        mock_repo = Mock()
                                        mock_repo.name = "deepfreeze-000001"
                                        mock_repo.thaw_state = "active"  # Initially active (bug scenario)
                                        mock_repo.is_mounted = True
                                        mock_repo.is_thawed = False
                                        mock_repo_class.from_elasticsearch.return_value = mock_repo

                                        rotate = Rotate(self.client, keep="1")

                                        with patch.object(rotate, 'cleanup_policies_for_repo'):
                                            with patch.object(rotate, 'is_thawed', return_value=False):
                                                # Run the unmount operation
                                                rotate.unmount_oldest_repos(dry_run=False)

                                                # Verify reset_to_frozen was called (which sets thaw_state='frozen')
                                                mock_repo.reset_to_frozen.assert_called_once()

                                                # Verify persist was called to save the updated state
                                                mock_repo.persist.assert_called_once_with(self.client)


