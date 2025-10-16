"""Test deepfreeze Thaw action"""
# pylint: disable=attribute-defined-outside-init
from datetime import datetime, timezone
from unittest import TestCase
from unittest.mock import Mock, patch, call

from curator.actions.deepfreeze.thaw import Thaw
from curator.actions.deepfreeze.helpers import Settings, Repository


class TestDeepfreezeThaw(TestCase):
    """Test Deepfreeze Thaw action"""

    def setUp(self):
        """Set up test fixtures"""
        self.client = Mock()
        self.mock_settings = Settings(
            repo_name_prefix="deepfreeze",
            bucket_name_prefix="deepfreeze",
            base_path_prefix="snapshots",
            canned_acl="private",
            storage_class="GLACIER",
            provider="aws",
            rotate_by="path",
            style="oneup",
            last_suffix="000003",
        )

        self.start_date = "2025-01-01T00:00:00Z"
        self.end_date = "2025-01-31T23:59:59Z"

    @patch("curator.actions.deepfreeze.thaw.s3_client_factory")
    @patch("curator.actions.deepfreeze.thaw.get_settings")
    def test_init_success(self, mock_get_settings, mock_s3_factory):
        """Test Thaw initialization with valid dates"""
        mock_get_settings.return_value = self.mock_settings
        mock_s3_factory.return_value = Mock()

        thaw = Thaw(
            self.client,
            start_date=self.start_date,
            end_date=self.end_date,
        )

        assert thaw.client == self.client
        assert thaw.sync is False
        assert thaw.duration == 7
        assert thaw.retrieval_tier == "Standard"
        assert thaw.start_date.year == 2025
        assert thaw.start_date.month == 1
        assert thaw.end_date.month == 1
        mock_get_settings.assert_called_once_with(self.client)
        mock_s3_factory.assert_called_once_with("aws")

    @patch("curator.actions.deepfreeze.thaw.s3_client_factory")
    @patch("curator.actions.deepfreeze.thaw.get_settings")
    def test_init_with_custom_params(self, mock_get_settings, mock_s3_factory):
        """Test Thaw initialization with custom parameters"""
        mock_get_settings.return_value = self.mock_settings
        mock_s3_factory.return_value = Mock()

        thaw = Thaw(
            self.client,
            start_date=self.start_date,
            end_date=self.end_date,
            sync=True,
            duration=14,
            retrieval_tier="Expedited",
        )

        assert thaw.sync is True
        assert thaw.duration == 14
        assert thaw.retrieval_tier == "Expedited"

    @patch("curator.actions.deepfreeze.thaw.s3_client_factory")
    @patch("curator.actions.deepfreeze.thaw.get_settings")
    def test_init_invalid_date_format(self, mock_get_settings, mock_s3_factory):
        """Test Thaw initialization with invalid date format"""
        mock_get_settings.return_value = self.mock_settings
        mock_s3_factory.return_value = Mock()

        with self.assertRaises(ValueError) as context:
            Thaw(
                self.client,
                start_date="not-a-date",
                end_date=self.end_date,
            )

        assert "Invalid start_date" in str(context.exception)

    @patch("curator.actions.deepfreeze.thaw.s3_client_factory")
    @patch("curator.actions.deepfreeze.thaw.get_settings")
    def test_init_start_after_end(self, mock_get_settings, mock_s3_factory):
        """Test Thaw initialization with start_date after end_date"""
        mock_get_settings.return_value = self.mock_settings
        mock_s3_factory.return_value = Mock()

        with self.assertRaises(ValueError) as context:
            Thaw(
                self.client,
                start_date=self.end_date,
                end_date=self.start_date,
            )

        assert "start_date must be before or equal to end_date" in str(
            context.exception
        )

    @patch("curator.actions.deepfreeze.thaw.find_repos_by_date_range")
    @patch("curator.actions.deepfreeze.thaw.s3_client_factory")
    @patch("curator.actions.deepfreeze.thaw.get_settings")
    def test_do_dry_run_no_repos(
        self, mock_get_settings, mock_s3_factory, mock_find_repos
    ):
        """Test dry run with no matching repositories"""
        mock_get_settings.return_value = self.mock_settings
        mock_s3_factory.return_value = Mock()
        mock_find_repos.return_value = []

        thaw = Thaw(
            self.client,
            start_date=self.start_date,
            end_date=self.end_date,
        )

        thaw.do_dry_run()

        mock_find_repos.assert_called_once()

    @patch("curator.actions.deepfreeze.thaw.find_repos_by_date_range")
    @patch("curator.actions.deepfreeze.thaw.s3_client_factory")
    @patch("curator.actions.deepfreeze.thaw.get_settings")
    def test_do_dry_run_with_repos(
        self, mock_get_settings, mock_s3_factory, mock_find_repos
    ):
        """Test dry run with matching repositories"""
        mock_get_settings.return_value = self.mock_settings
        mock_s3_factory.return_value = Mock()

        mock_repos = [
            Repository(
                name="deepfreeze-000001",
                bucket="deepfreeze",
                base_path="snapshots-000001",
                start="2025-01-01T00:00:00Z",
                end="2025-01-15T23:59:59Z",
                is_mounted=False,
                is_thawed=False,
            ),
            Repository(
                name="deepfreeze-000002",
                bucket="deepfreeze",
                base_path="snapshots-000002",
                start="2025-01-16T00:00:00Z",
                end="2025-01-31T23:59:59Z",
                is_mounted=False,
                is_thawed=False,
            ),
        ]
        mock_find_repos.return_value = mock_repos

        thaw = Thaw(
            self.client,
            start_date=self.start_date,
            end_date=self.end_date,
        )

        thaw.do_dry_run()

        mock_find_repos.assert_called_once()

    @patch("curator.actions.deepfreeze.thaw.save_thaw_request")
    @patch("curator.actions.deepfreeze.thaw.find_repos_by_date_range")
    @patch("curator.actions.deepfreeze.thaw.s3_client_factory")
    @patch("curator.actions.deepfreeze.thaw.get_settings")
    def test_do_action_async_mode(
        self,
        mock_get_settings,
        mock_s3_factory,
        mock_find_repos,
        mock_save_request,
    ):
        """Test thaw action in async mode"""
        mock_get_settings.return_value = self.mock_settings
        mock_s3 = Mock()
        mock_s3_factory.return_value = mock_s3

        mock_repo = Repository(
            name="deepfreeze-000001",
            bucket="deepfreeze",
            base_path="snapshots-000001",
            start="2025-01-01T00:00:00Z",
            end="2025-01-15T23:59:59Z",
            is_mounted=False,
            is_thawed=False,
        )
        mock_find_repos.return_value = [mock_repo]

        # Mock list_objects to return some objects
        mock_s3.list_objects.return_value = [
            {"Key": "snapshots-000001/index1/data.dat"},
            {"Key": "snapshots-000001/index2/data.dat"},
        ]

        thaw = Thaw(
            self.client,
            start_date=self.start_date,
            end_date=self.end_date,
            sync=False,
        )

        thaw.do_action()

        # Should list objects and call thaw
        mock_s3.list_objects.assert_called_once_with(
            "deepfreeze", "snapshots-000001"
        )
        mock_s3.thaw.assert_called_once()

        # Should save thaw request in async mode
        mock_save_request.assert_called_once()
        args = mock_save_request.call_args[0]
        assert args[0] == self.client
        assert args[2] == [mock_repo]  # repos list
        assert args[3] == "in_progress"  # status

    @patch("curator.actions.deepfreeze.thaw.mount_repo")
    @patch("curator.actions.deepfreeze.thaw.check_restore_status")
    @patch("curator.actions.deepfreeze.thaw.find_repos_by_date_range")
    @patch("curator.actions.deepfreeze.thaw.s3_client_factory")
    @patch("curator.actions.deepfreeze.thaw.get_settings")
    def test_do_action_sync_mode(
        self,
        mock_get_settings,
        mock_s3_factory,
        mock_find_repos,
        mock_check_status,
        mock_mount_repo,
    ):
        """Test thaw action in sync mode"""
        mock_get_settings.return_value = self.mock_settings
        mock_s3 = Mock()
        mock_s3_factory.return_value = mock_s3

        mock_repo = Repository(
            name="deepfreeze-000001",
            bucket="deepfreeze",
            base_path="snapshots-000001",
            start="2025-01-01T00:00:00Z",
            end="2025-01-15T23:59:59Z",
            is_mounted=False,
            is_thawed=False,
        )
        mock_find_repos.return_value = [mock_repo]

        # Mock list_objects to return some objects
        mock_s3.list_objects.return_value = [
            {"Key": "snapshots-000001/index1/data.dat"},
        ]

        # Mock restore status to indicate completion
        mock_check_status.return_value = {
            "total": 1,
            "restored": 1,
            "in_progress": 0,
            "not_restored": 0,
            "complete": True,
        }

        thaw = Thaw(
            self.client,
            start_date=self.start_date,
            end_date=self.end_date,
            sync=True,
        )

        thaw.do_action()

        # Should list objects and call thaw
        mock_s3.list_objects.assert_called_once()
        mock_s3.thaw.assert_called_once()

        # Should check restore status and mount in sync mode
        mock_check_status.assert_called()
        mock_mount_repo.assert_called_once_with(self.client, mock_repo)

    @patch("curator.actions.deepfreeze.thaw.find_repos_by_date_range")
    @patch("curator.actions.deepfreeze.thaw.s3_client_factory")
    @patch("curator.actions.deepfreeze.thaw.get_settings")
    def test_do_action_no_repos(
        self, mock_get_settings, mock_s3_factory, mock_find_repos
    ):
        """Test thaw action with no matching repositories"""
        mock_get_settings.return_value = self.mock_settings
        mock_s3_factory.return_value = Mock()
        mock_find_repos.return_value = []

        thaw = Thaw(
            self.client,
            start_date=self.start_date,
            end_date=self.end_date,
        )

        thaw.do_action()

        mock_find_repos.assert_called_once()

    @patch("curator.actions.deepfreeze.thaw.find_repos_by_date_range")
    @patch("curator.actions.deepfreeze.thaw.s3_client_factory")
    @patch("curator.actions.deepfreeze.thaw.get_settings")
    def test_thaw_repository_already_thawed(
        self, mock_get_settings, mock_s3_factory, mock_find_repos
    ):
        """Test thawing a repository that is already thawed"""
        mock_get_settings.return_value = self.mock_settings
        mock_s3 = Mock()
        mock_s3_factory.return_value = mock_s3

        mock_repo = Repository(
            name="deepfreeze-000001",
            bucket="deepfreeze",
            base_path="snapshots-000001",
            is_mounted=True,
            is_thawed=True,
        )

        thaw = Thaw(
            self.client,
            start_date=self.start_date,
            end_date=self.end_date,
        )

        result = thaw._thaw_repository(mock_repo)

        assert result is True
        # Should not call S3 operations for already thawed repo
        mock_s3.list_objects.assert_not_called()
        mock_s3.thaw.assert_not_called()

    @patch("curator.actions.deepfreeze.thaw.s3_client_factory")
    @patch("curator.actions.deepfreeze.thaw.get_settings")
    def test_thaw_repository_s3_error(self, mock_get_settings, mock_s3_factory):
        """Test thawing a repository when S3 operations fail"""
        mock_get_settings.return_value = self.mock_settings
        mock_s3 = Mock()
        mock_s3_factory.return_value = mock_s3

        mock_repo = Repository(
            name="deepfreeze-000001",
            bucket="deepfreeze",
            base_path="snapshots-000001",
            is_mounted=False,
            is_thawed=False,
        )

        # Mock list_objects to return objects
        mock_s3.list_objects.return_value = [
            {"Key": "snapshots-000001/index1/data.dat"},
        ]

        # Mock thaw to raise an exception
        mock_s3.thaw.side_effect = Exception("S3 error")

        thaw = Thaw(
            self.client,
            start_date=self.start_date,
            end_date=self.end_date,
        )

        result = thaw._thaw_repository(mock_repo)

        assert result is False

    @patch("curator.actions.deepfreeze.thaw.check_restore_status")
    @patch("curator.actions.deepfreeze.thaw.s3_client_factory")
    @patch("curator.actions.deepfreeze.thaw.get_settings")
    @patch("curator.actions.deepfreeze.thaw.time.sleep")
    def test_wait_for_restore_success(
        self, mock_sleep, mock_get_settings, mock_s3_factory, mock_check_status
    ):
        """Test waiting for restore to complete"""
        mock_get_settings.return_value = self.mock_settings
        mock_s3 = Mock()
        mock_s3_factory.return_value = mock_s3

        mock_repo = Repository(
            name="deepfreeze-000001",
            bucket="deepfreeze",
            base_path="snapshots-000001",
        )

        # Three calls: initial, in-progress, then complete
        # (the initial call is made to get total objects count)
        mock_check_status.side_effect = [
            {
                "total": 2,
                "restored": 0,
                "in_progress": 2,
                "not_restored": 0,
                "complete": False,
            },
            {
                "total": 2,
                "restored": 1,
                "in_progress": 1,
                "not_restored": 0,
                "complete": False,
            },
            {
                "total": 2,
                "restored": 2,
                "in_progress": 0,
                "not_restored": 0,
                "complete": True,
            },
        ]

        thaw = Thaw(
            self.client,
            start_date=self.start_date,
            end_date=self.end_date,
        )

        result = thaw._wait_for_restore(mock_repo, poll_interval=1, show_progress=False)

        assert result is True
        assert mock_check_status.call_count == 3
        # Should sleep once between the second and third check
        mock_sleep.assert_called_once_with(1)

    @patch("curator.actions.deepfreeze.thaw.s3_client_factory")
    @patch("curator.actions.deepfreeze.thaw.get_settings")
    def test_do_singleton_action(self, mock_get_settings, mock_s3_factory):
        """Test singleton action execution"""
        mock_get_settings.return_value = self.mock_settings
        mock_s3_factory.return_value = Mock()

        thaw = Thaw(
            self.client,
            start_date=self.start_date,
            end_date=self.end_date,
        )

        with patch.object(thaw, "do_action") as mock_do_action:
            thaw.do_singleton_action()

            mock_do_action.assert_called_once()

    @patch("curator.actions.deepfreeze.thaw.s3_client_factory")
    @patch("curator.actions.deepfreeze.thaw.get_settings")
    @patch("curator.actions.deepfreeze.thaw.get_repositories_by_names")
    @patch("curator.actions.deepfreeze.thaw.get_thaw_request")
    def test_check_status_mode_initialization(
        self, mock_get_request, mock_get_repos, mock_get_settings, mock_s3_factory
    ):
        """Test initialization in check_status mode"""
        mock_get_settings.return_value = self.mock_settings
        mock_s3_factory.return_value = Mock()

        thaw = Thaw(
            self.client,
            check_status="test-request-id",
        )

        assert thaw.mode == "check_status"
        assert thaw.check_status == "test-request-id"

    def test_list_mode_initialization(self):
        """Test initialization in list mode"""
        thaw = Thaw(
            self.client,
            list_requests=True,
        )

        assert thaw.mode == "list"
        assert thaw.list_requests is True

    def test_create_mode_missing_dates_error(self):
        """Test error when creating thaw without dates"""
        with self.assertRaises(ValueError) as context:
            Thaw(self.client)

        assert "start_date and end_date are required" in str(context.exception)

    @patch("curator.actions.deepfreeze.thaw.update_thaw_request")
    @patch("curator.actions.deepfreeze.thaw.mount_repo")
    @patch("curator.actions.deepfreeze.thaw.check_restore_status")
    @patch("curator.actions.deepfreeze.thaw.get_repositories_by_names")
    @patch("curator.actions.deepfreeze.thaw.get_thaw_request")
    @patch("curator.actions.deepfreeze.thaw.s3_client_factory")
    @patch("curator.actions.deepfreeze.thaw.get_settings")
    def test_do_check_status_restoration_complete(
        self,
        mock_get_settings,
        mock_s3_factory,
        mock_get_request,
        mock_get_repos,
        mock_check_status,
        mock_mount_repo,
        mock_update_request,
    ):
        """Test check_status when restoration is complete"""
        mock_get_settings.return_value = self.mock_settings
        mock_s3 = Mock()
        mock_s3_factory.return_value = mock_s3

        # Mock thaw request
        mock_get_request.return_value = {
            "request_id": "test-id",
            "repos": ["deepfreeze-000001"],
            "status": "in_progress",
            "created_at": "2025-01-15T10:00:00Z",
        }

        # Mock repository
        mock_repo = Repository(
            name="deepfreeze-000001",
            bucket="deepfreeze",
            base_path="snapshots-000001",
            is_mounted=False,
            is_thawed=False,
        )
        mock_get_repos.return_value = [mock_repo]

        # Mock complete restoration status
        mock_check_status.return_value = {
            "total": 10,
            "restored": 10,
            "in_progress": 0,
            "not_restored": 0,
            "complete": True,
        }

        thaw = Thaw(self.client, check_status="test-id")
        thaw.do_check_status()

        # Should mount the repository
        mock_mount_repo.assert_called_once_with(self.client, mock_repo)
        # Should update request status to completed
        mock_update_request.assert_called_once_with(
            self.client, "test-id", status="completed"
        )

    @patch("curator.actions.deepfreeze.thaw.check_restore_status")
    @patch("curator.actions.deepfreeze.thaw.get_repositories_by_names")
    @patch("curator.actions.deepfreeze.thaw.get_thaw_request")
    @patch("curator.actions.deepfreeze.thaw.s3_client_factory")
    @patch("curator.actions.deepfreeze.thaw.get_settings")
    def test_do_check_status_restoration_in_progress(
        self,
        mock_get_settings,
        mock_s3_factory,
        mock_get_request,
        mock_get_repos,
        mock_check_status,
    ):
        """Test check_status when restoration is still in progress"""
        mock_get_settings.return_value = self.mock_settings
        mock_s3 = Mock()
        mock_s3_factory.return_value = mock_s3

        mock_get_request.return_value = {
            "request_id": "test-id",
            "repos": ["deepfreeze-000001"],
            "status": "in_progress",
            "created_at": "2025-01-15T10:00:00Z",
        }

        mock_repo = Repository(
            name="deepfreeze-000001",
            bucket="deepfreeze",
            base_path="snapshots-000001",
            is_mounted=False,
            is_thawed=False,
        )
        mock_get_repos.return_value = [mock_repo]

        # Mock in-progress restoration status
        mock_check_status.return_value = {
            "total": 10,
            "restored": 5,
            "in_progress": 5,
            "not_restored": 0,
            "complete": False,
        }

        thaw = Thaw(self.client, check_status="test-id")
        thaw.do_check_status()

        # Should check status but not mount
        mock_check_status.assert_called_once()

    @patch("curator.actions.deepfreeze.thaw.list_thaw_requests")
    def test_do_list_requests_empty(self, mock_list_requests):
        """Test listing thaw requests when none exist"""
        mock_list_requests.return_value = []

        thaw = Thaw(self.client, list_requests=True)
        thaw.do_list_requests()

        mock_list_requests.assert_called_once_with(self.client)

    @patch("curator.actions.deepfreeze.thaw.list_thaw_requests")
    def test_do_list_requests_with_data(self, mock_list_requests):
        """Test listing thaw requests with data"""
        mock_list_requests.return_value = [
            {
                "id": "request-1",
                "request_id": "request-1",
                "repos": ["deepfreeze-000001", "deepfreeze-000002"],
                "status": "in_progress",
                "created_at": "2025-01-15T10:00:00Z",
            },
            {
                "id": "request-2",
                "request_id": "request-2",
                "repos": ["deepfreeze-000003"],
                "status": "completed",
                "created_at": "2025-01-14T14:00:00Z",
            },
        ]

        thaw = Thaw(self.client, list_requests=True)
        thaw.do_list_requests()

        mock_list_requests.assert_called_once_with(self.client)

    @patch("curator.actions.deepfreeze.thaw.s3_client_factory")
    @patch("curator.actions.deepfreeze.thaw.get_settings")
    def test_mode_routing_in_do_action(self, mock_get_settings, mock_s3_factory):
        """Test that do_action routes to correct handler based on mode"""
        mock_get_settings.return_value = self.mock_settings
        mock_s3_factory.return_value = Mock()

        # Test list mode
        thaw_list = Thaw(self.client, list_requests=True)
        with patch.object(thaw_list, "do_list_requests") as mock_list:
            thaw_list.do_action()
            mock_list.assert_called_once()

        # Test check_status mode
        thaw_check = Thaw(self.client, check_status="test-id")
        with patch.object(thaw_check, "do_check_status") as mock_check:
            thaw_check.do_action()
            mock_check.assert_called_once()
