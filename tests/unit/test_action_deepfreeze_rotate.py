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
                        mock_s3 = Mock()
                        mock_factory.return_value = mock_s3
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
                        mock_s3 = Mock()
                        mock_factory.return_value = mock_s3
                        self.client.indices.exists.return_value = True

                        # Should not raise any exceptions
                        rotate = Rotate(self.client)
                        assert rotate is not None


