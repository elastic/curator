"""Test deepfreeze Setup action"""
# pylint: disable=attribute-defined-outside-init
from unittest import TestCase
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime
import pytest

from curator.actions.deepfreeze.setup import Setup
from curator.actions.deepfreeze.helpers import Settings, Repository
from curator.actions.deepfreeze.constants import STATUS_INDEX, SETTINGS_ID
from curator.actions.deepfreeze.exceptions import PreconditionError, ActionException
from curator.s3client import AwsS3Client


class TestDeepfreezeSetup(TestCase):
    """Test Deepfreeze Setup action"""

    def setUp(self):
        """Set up test fixtures"""
        self.client = Mock()
        self.client.indices.exists.return_value = False
        self.client.snapshot.get_repository.return_value = {}
        self.client.ilm.get_lifecycle.return_value = {}
        # Mock info() for version checking
        self.client.info.return_value = {
            'version': {
                'number': '8.0.0'
            }
        }

    def test_init_defaults(self):
        """Test Setup initialization with default values"""
        with patch('curator.actions.deepfreeze.setup.s3_client_factory') as mock_factory:
            with patch('curator.actions.deepfreeze.setup.get_matching_repo_names') as mock_get_repos:
                mock_s3 = Mock()
                mock_factory.return_value = mock_s3
                mock_get_repos.return_value = []

                setup = Setup(self.client)

                assert setup.client == self.client
                assert setup.s3 == mock_s3
                assert setup.settings.repo_name_prefix == "deepfreeze"
                assert setup.settings.bucket_name_prefix == "deepfreeze"
                assert setup.settings.base_path_prefix == "snapshots"
                assert setup.settings.canned_acl == "private"
                assert setup.settings.storage_class == "intelligent_tiering"
                assert setup.settings.provider == "aws"
                assert setup.settings.rotate_by == "path"
                assert setup.settings.style == "oneup"
                assert setup.ilm_policy_name == "deepfreeze-sample-policy"
                assert setup.create_sample_ilm_policy is False

    def test_init_custom_values(self):
        """Test Setup initialization with custom values"""
        with patch('curator.actions.deepfreeze.setup.s3_client_factory') as mock_factory:
            with patch('curator.actions.deepfreeze.setup.get_matching_repo_names') as mock_get_repos:
                mock_s3 = Mock()
                mock_factory.return_value = mock_s3
                mock_get_repos.return_value = []

                setup = Setup(
                    self.client,
                    year=2024,
                    month=3,
                    repo_name_prefix="custom-repo",
                    bucket_name_prefix="custom-bucket",
                    base_path_prefix="custom-path",
                    canned_acl="public-read",
                    storage_class="GLACIER",
                    provider="gcp",
                    rotate_by="bucket",
                    style="monthly",
                    ilm_policy_name="custom-policy",
                    create_sample_ilm_policy=True
                )

                assert setup.settings.repo_name_prefix == "custom-repo"
                assert setup.settings.bucket_name_prefix == "custom-bucket"
                assert setup.settings.base_path_prefix == "custom-path"
                assert setup.settings.canned_acl == "public-read"
                assert setup.settings.storage_class == "GLACIER"
                assert setup.settings.provider == "gcp"
                assert setup.settings.rotate_by == "bucket"
                assert setup.settings.style == "monthly"
                assert setup.ilm_policy_name == "custom-policy"
                assert setup.create_sample_ilm_policy is True

    def test_check_preconditions_status_index_exists(self):
        """Test preconditions check when status index exists"""
        self.client.indices.exists.return_value = True

        with patch('curator.actions.deepfreeze.setup.s3_client_factory'):
            with patch('curator.actions.deepfreeze.setup.get_matching_repo_names') as mock_get_repos:
                mock_get_repos.return_value = []
                setup = Setup(self.client)

                with pytest.raises(PreconditionError, match="precondition error"):
                    setup._check_preconditions()

    def test_check_preconditions_repository_exists(self):
        """Test preconditions check when repository already exists"""
        self.client.indices.exists.return_value = False
        self.client.snapshot.get_repository.return_value = {
            'deepfreeze-000001': {}
        }

        with patch('curator.actions.deepfreeze.setup.s3_client_factory'):
            with patch('curator.actions.deepfreeze.setup.get_matching_repo_names') as mock_get_repos:
                mock_get_repos.return_value = []
                setup = Setup(self.client)

                with pytest.raises(PreconditionError, match="precondition error"):
                    setup._check_preconditions()

    def test_check_preconditions_bucket_exists(self):
        """Test preconditions check when bucket already exists"""
        self.client.indices.exists.return_value = False
        self.client.snapshot.get_repository.return_value = {}

        with patch('curator.actions.deepfreeze.setup.s3_client_factory') as mock_factory:
            with patch('curator.actions.deepfreeze.setup.get_matching_repo_names') as mock_get_repos:
                mock_s3 = Mock()
                mock_s3.bucket_exists.return_value = True
                mock_factory.return_value = mock_s3
                mock_get_repos.return_value = []

                setup = Setup(self.client, rotate_by="bucket")

                with pytest.raises(PreconditionError, match="precondition error"):
                    setup._check_preconditions()

    def test_check_preconditions_success(self):
        """Test successful preconditions check"""
        self.client.indices.exists.return_value = False
        self.client.snapshot.get_repository.return_value = {}

        with patch('curator.actions.deepfreeze.setup.s3_client_factory') as mock_factory:
            with patch('curator.actions.deepfreeze.setup.get_matching_repo_names') as mock_get_repos:
                mock_s3 = Mock()
                mock_s3.bucket_exists.return_value = False
                mock_factory.return_value = mock_s3
                mock_get_repos.return_value = []

                setup = Setup(self.client)

                # Should not raise any exceptions
                setup._check_preconditions()

    def test_do_dry_run(self):
        """Test dry run mode"""
        with patch('curator.actions.deepfreeze.setup.s3_client_factory') as mock_factory:
            with patch('curator.actions.deepfreeze.setup.get_matching_repo_names') as mock_get_repos:
                with patch('curator.actions.deepfreeze.setup.create_repo') as mock_create_repo:
                    mock_s3 = Mock()
                    mock_s3.bucket_exists.return_value = False
                    mock_factory.return_value = mock_s3
                    mock_get_repos.return_value = []

                    setup = Setup(self.client)
                    setup.do_dry_run()

                    # Should call create_repo with dry_run=True
                    mock_create_repo.assert_called_once()
                    call_args = mock_create_repo.call_args
                    assert call_args.kwargs.get('dry_run') is True

    def test_do_action_success_rotate_by_path(self):
        """Test successful setup action with rotate_by='path'"""
        self.client.indices.exists.return_value = False

        with patch('curator.actions.deepfreeze.setup.s3_client_factory') as mock_factory:
            with patch('curator.actions.deepfreeze.setup.get_matching_repo_names') as mock_get_repos:
                mock_s3 = Mock()
                mock_s3.bucket_exists.return_value = False
                mock_factory.return_value = mock_s3
                mock_get_repos.return_value = []

                with patch('curator.actions.deepfreeze.setup.ensure_settings_index'):
                    with patch('curator.actions.deepfreeze.setup.save_settings'):
                        with patch('curator.actions.deepfreeze.setup.create_repo'):
                            setup = Setup(self.client, rotate_by="path")

                            setup.do_action()

                            # Should create bucket (only one for path rotation)
                            mock_s3.create_bucket.assert_called_once_with("deepfreeze")

    def test_do_action_success_rotate_by_bucket(self):
        """Test successful setup action with rotate_by='bucket'"""
        self.client.indices.exists.return_value = False

        with patch('curator.actions.deepfreeze.setup.s3_client_factory') as mock_factory:
            with patch('curator.actions.deepfreeze.setup.get_matching_repo_names') as mock_get_repos:
                mock_s3 = Mock()
                mock_s3.bucket_exists.return_value = False
                mock_factory.return_value = mock_s3
                mock_get_repos.return_value = []

                with patch('curator.actions.deepfreeze.setup.ensure_settings_index'):
                    with patch('curator.actions.deepfreeze.setup.save_settings'):
                        with patch('curator.actions.deepfreeze.setup.create_repo'):
                            setup = Setup(self.client, rotate_by="bucket")

                            setup.do_action()

                            # Should create bucket with suffix for bucket rotation
                            mock_s3.create_bucket.assert_called_once_with("deepfreeze-000001")

    def test_do_action_with_ilm_policy(self):
        """Test setup action creates ILM policy"""
        self.client.indices.exists.return_value = False

        with patch('curator.actions.deepfreeze.setup.s3_client_factory') as mock_factory:
            with patch('curator.actions.deepfreeze.setup.get_matching_repo_names') as mock_get_repos:
                mock_s3 = Mock()
                mock_s3.bucket_exists.return_value = False
                mock_factory.return_value = mock_s3
                mock_get_repos.return_value = []

                with patch('curator.actions.deepfreeze.setup.ensure_settings_index'):
                    with patch('curator.actions.deepfreeze.setup.save_settings'):
                        with patch('curator.actions.deepfreeze.setup.create_repo'):
                            with patch('curator.actions.deepfreeze.setup.create_ilm_policy') as mock_create_ilm:
                                setup = Setup(
                                    self.client,
                                    create_sample_ilm_policy=True,
                                    ilm_policy_name="test-policy"
                                )

                                setup.do_action()

                                # Should create ILM policy
                                mock_create_ilm.assert_called_once()

    def test_calculate_names_rotate_by_path(self):
        """Test name calculation for path rotation"""
        with patch('curator.actions.deepfreeze.setup.s3_client_factory'):
            with patch('curator.actions.deepfreeze.setup.get_matching_repo_names') as mock_get_repos:
                mock_get_repos.return_value = []
                setup = Setup(self.client, rotate_by="path")

                # Should calculate names correctly
                assert setup.new_repo_name == "deepfreeze-000001"
                assert setup.new_bucket_name == "deepfreeze"
                assert setup.base_path == "snapshots-000001"

    def test_calculate_names_rotate_by_bucket(self):
        """Test name calculation for bucket rotation"""
        with patch('curator.actions.deepfreeze.setup.s3_client_factory'):
            with patch('curator.actions.deepfreeze.setup.get_matching_repo_names') as mock_get_repos:
                mock_get_repos.return_value = []
                setup = Setup(self.client, rotate_by="bucket")

                # Should calculate names correctly
                assert setup.new_repo_name == "deepfreeze-000001"
                assert setup.new_bucket_name == "deepfreeze-000001"
                assert setup.base_path == "snapshots"

    def test_calculate_names_monthly_style(self):
        """Test name calculation with monthly style"""
        with patch('curator.actions.deepfreeze.setup.s3_client_factory'):
            with patch('curator.actions.deepfreeze.setup.get_matching_repo_names') as mock_get_repos:
                mock_get_repos.return_value = []
                setup = Setup(
                    self.client,
                    year=2024,
                    month=3,
                    style="monthly",
                    rotate_by="path"
                )

                assert setup.new_repo_name == "deepfreeze-2024.03"
                assert setup.base_path == "snapshots-2024.03"

    def test_action_with_existing_repo_name_fails(self):
        """Test that setup fails if repository name already exists"""
        self.client.indices.exists.return_value = False
        self.client.snapshot.get_repository.return_value = {
            'deepfreeze-000001': {}  # Repository already exists
        }

        with patch('curator.actions.deepfreeze.setup.s3_client_factory'):
            with patch('curator.actions.deepfreeze.setup.get_matching_repo_names') as mock_get_repos:
                mock_get_repos.return_value = []
                setup = Setup(self.client)

                with pytest.raises(PreconditionError, match="precondition error"):
                    setup._check_preconditions()

    def test_action_with_existing_bucket_fails(self):
        """Test that setup fails if bucket already exists for bucket rotation"""
        self.client.indices.exists.return_value = False
        self.client.snapshot.get_repository.return_value = {}

        with patch('curator.actions.deepfreeze.setup.s3_client_factory') as mock_factory:
            with patch('curator.actions.deepfreeze.setup.get_matching_repo_names') as mock_get_repos:
                mock_s3 = Mock()
                mock_s3.bucket_exists.return_value = True  # Bucket exists
                mock_factory.return_value = mock_s3
                mock_get_repos.return_value = []

                setup = Setup(self.client, rotate_by="bucket")

                with pytest.raises(PreconditionError, match="precondition error"):
                    setup._check_preconditions()

