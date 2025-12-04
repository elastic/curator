"""Tests for deepfreeze action classes (Task Groups 7-13 + Task Group 18 additions)"""

import pytest
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch, PropertyMock

from deepfreeze.actions import (
    Setup,
    Status,
    Rotate,
    Thaw,
    Refreeze,
    Cleanup,
    RepairMetadata,
)
from deepfreeze.constants import (
    STATUS_INDEX,
    THAW_STATE_ACTIVE,
    THAW_STATE_FROZEN,
    THAW_STATE_THAWED,
    THAW_STATUS_COMPLETED,
    THAW_STATUS_IN_PROGRESS,
)
from deepfreeze.exceptions import (
    MissingIndexError,
    MissingSettingsError,
    PreconditionError,
)
from deepfreeze.helpers import Repository, Settings


class TestSetupAction:
    """Tests for the Setup action class"""

    def test_setup_initialization(self):
        """Test Setup action can be initialized with required parameters"""
        mock_client = MagicMock()

        # Mock s3_client_factory
        with patch("deepfreeze.actions.setup.s3_client_factory") as mock_factory:
            mock_s3 = MagicMock()
            mock_factory.return_value = mock_s3

            setup = Setup(
                client=mock_client,
                repo_name_prefix="test-repo",
                bucket_name_prefix="test-bucket",
                ilm_policy_name="test-policy",
                index_template_name="test-template",
            )

            assert setup.client == mock_client
            assert setup.settings.repo_name_prefix == "test-repo"
            assert setup.settings.bucket_name_prefix == "test-bucket"
            assert setup.ilm_policy_name == "test-policy"
            assert setup.index_template_name == "test-template"

    def test_setup_preconditions_fail_status_index_exists(self):
        """Test Setup raises PreconditionError when status index exists"""
        mock_client = MagicMock()
        mock_client.indices.exists.return_value = True
        mock_client.snapshot.get_repository.return_value = {}

        with patch("deepfreeze.actions.setup.s3_client_factory") as mock_factory:
            mock_s3 = MagicMock()
            mock_s3.bucket_exists.return_value = False
            mock_factory.return_value = mock_s3

            setup = Setup(
                client=mock_client,
                repo_name_prefix="test-repo",
                bucket_name_prefix="test-bucket",
                ilm_policy_name="test-policy",
                index_template_name="test-template",
                porcelain=True,
            )

            with pytest.raises(PreconditionError):
                setup._check_preconditions()

    def test_setup_dry_run(self):
        """Test Setup dry run doesn't create anything"""
        mock_client = MagicMock()
        mock_client.indices.exists.return_value = False
        mock_client.snapshot.get_repository.return_value = {}

        # Mock get_index_template to return template exists
        mock_client.indices.get_index_template.return_value = {
            "index_templates": [{"index_template": {}}]
        }

        # Mock cluster info to return ES 8.x (no S3 plugin check needed)
        mock_client.info.return_value = {"version": {"number": "8.10.0"}}

        with patch("deepfreeze.actions.setup.s3_client_factory") as mock_factory:
            mock_s3 = MagicMock()
            mock_s3.bucket_exists.return_value = False
            mock_factory.return_value = mock_s3

            with patch("deepfreeze.actions.setup.create_repo") as mock_create:
                setup = Setup(
                    client=mock_client,
                    repo_name_prefix="test-repo",
                    bucket_name_prefix="test-bucket",
                    ilm_policy_name="test-policy",
                    index_template_name="test-template",
                    porcelain=True,
                )

                setup.do_dry_run()

                # create_repo should be called with dry_run=True
                mock_create.assert_called_once()
                call_args = mock_create.call_args
                assert call_args[1].get("dry_run") is True or call_args[0][-1] is True

    def test_setup_do_action_success(self):
        """Test Setup do_action creates bucket and repository"""
        mock_client = MagicMock()
        mock_client.indices.exists.return_value = False
        mock_client.snapshot.get_repository.return_value = {}
        mock_client.indices.get_index_template.return_value = {
            "index_templates": [{"index_template": {}}]
        }
        mock_client.info.return_value = {"version": {"number": "8.10.0"}}

        with patch("deepfreeze.actions.setup.s3_client_factory") as mock_factory:
            mock_s3 = MagicMock()
            mock_s3.bucket_exists.return_value = False
            mock_factory.return_value = mock_s3

            with patch("deepfreeze.actions.setup.ensure_settings_index"):
                with patch("deepfreeze.actions.setup.save_settings"):
                    with patch("deepfreeze.actions.setup.create_repo"):
                        with patch("deepfreeze.actions.setup.create_or_update_ilm_policy") as mock_ilm:
                            mock_ilm.return_value = {"action": "created", "policy_body": {}}
                            with patch("deepfreeze.actions.setup.update_index_template_ilm_policy") as mock_template:
                                mock_template.return_value = {"action": "updated", "template_type": "composable"}

                                setup = Setup(
                                    client=mock_client,
                                    repo_name_prefix="test-repo",
                                    bucket_name_prefix="test-bucket",
                                    ilm_policy_name="test-policy",
                                    index_template_name="test-template",
                                    porcelain=True,
                                )

                                # Should not raise
                                setup.do_action()

                                # S3 bucket should be created
                                mock_s3.create_bucket.assert_called_once()


class TestStatusAction:
    """Tests for the Status action class"""

    def test_status_initialization(self):
        """Test Status action can be initialized"""
        mock_client = MagicMock()
        status = Status(client=mock_client, porcelain=False)

        assert status.client == mock_client
        assert status.porcelain is False

    def test_status_raises_missing_index_error(self):
        """Test Status raises MissingIndexError when status index doesn't exist"""
        mock_client = MagicMock()
        mock_client.indices.exists.return_value = False

        status = Status(client=mock_client, porcelain=True)

        with pytest.raises(MissingIndexError):
            status.do_action()

    def test_status_raises_missing_settings_error(self):
        """Test Status raises MissingSettingsError when settings not found"""
        mock_client = MagicMock()
        mock_client.indices.exists.return_value = True

        with patch("deepfreeze.actions.status.get_settings") as mock_get:
            mock_get.return_value = None

            status = Status(client=mock_client, porcelain=True)

            with pytest.raises(MissingSettingsError):
                status.do_action()


class TestRotateAction:
    """Tests for the Rotate action class"""

    def test_rotate_initialization(self):
        """Test Rotate action can be initialized"""
        mock_client = MagicMock()
        rotate = Rotate(client=mock_client, keep=2)

        assert rotate.client == mock_client
        assert rotate.keep == 2

    def test_rotate_raises_missing_index_error(self):
        """Test Rotate raises MissingIndexError when status index doesn't exist"""
        mock_client = MagicMock()
        mock_client.indices.exists.return_value = False

        rotate = Rotate(client=mock_client, porcelain=True)

        with pytest.raises(MissingIndexError):
            rotate.do_action()

    def test_rotate_dry_run_shows_what_would_happen(self):
        """Test Rotate dry run shows what would be created"""
        mock_client = MagicMock()
        mock_client.indices.exists.return_value = True

        mock_settings = Settings(
            repo_name_prefix="deepfreeze",
            bucket_name_prefix="deepfreeze",
            base_path_prefix="snapshots",
            style="oneup",
            last_suffix="000001",
        )

        with patch("deepfreeze.actions.rotate.get_settings") as mock_get:
            mock_get.return_value = mock_settings

            with patch("deepfreeze.actions.rotate.s3_client_factory") as mock_factory:
                mock_s3 = MagicMock()
                mock_factory.return_value = mock_s3

                with patch(
                    "deepfreeze.actions.rotate.get_matching_repos"
                ) as mock_repos:
                    mock_repos.return_value = []

                    rotate = Rotate(client=mock_client, porcelain=True)
                    rotate.do_dry_run()

                    # Should not have created anything
                    mock_s3.create_bucket.assert_not_called()

    def test_rotate_calculates_next_suffix(self):
        """Test Rotate correctly calculates next suffix"""
        mock_client = MagicMock()
        mock_client.indices.exists.return_value = True

        mock_settings = Settings(
            repo_name_prefix="deepfreeze",
            bucket_name_prefix="deepfreeze",
            base_path_prefix="snapshots",
            style="oneup",
            last_suffix="000005",
        )

        with patch("deepfreeze.actions.rotate.get_settings") as mock_get:
            mock_get.return_value = mock_settings

            with patch("deepfreeze.actions.rotate.s3_client_factory") as mock_factory:
                mock_s3 = MagicMock()
                mock_factory.return_value = mock_s3

                with patch("deepfreeze.actions.rotate.get_matching_repos") as mock_repos:
                    mock_repos.return_value = []

                    rotate = Rotate(client=mock_client, porcelain=True)
                    rotate._load_settings()

                    # Test the _create_new_repository method
                    new_repo, new_bucket, base_path, new_suffix = rotate._create_new_repository(dry_run=True)

                    assert new_suffix == "000006"
                    assert new_repo == "deepfreeze-000006"


class TestThawAction:
    """Tests for the Thaw action class"""

    def test_thaw_initialization(self):
        """Test Thaw action can be initialized"""
        mock_client = MagicMock()
        start = datetime(2023, 1, 1, tzinfo=timezone.utc)
        end = datetime(2023, 1, 31, tzinfo=timezone.utc)

        thaw = Thaw(
            client=mock_client,
            start_date=start,
            end_date=end,
            restore_days=7,
            retrieval_tier="Standard",
        )

        assert thaw.client == mock_client
        assert thaw.start_date == start
        assert thaw.end_date == end
        assert thaw.restore_days == 7
        assert thaw.retrieval_tier == "Standard"

    def test_thaw_list_mode(self):
        """Test Thaw can list all requests"""
        mock_client = MagicMock()
        mock_client.indices.exists.return_value = True

        mock_settings = Settings()

        with patch("deepfreeze.actions.thaw.get_settings") as mock_get:
            mock_get.return_value = mock_settings

            with patch("deepfreeze.actions.thaw.s3_client_factory") as mock_factory:
                mock_s3 = MagicMock()
                mock_factory.return_value = mock_s3

                with patch("deepfreeze.actions.thaw.list_thaw_requests") as mock_list:
                    mock_list.return_value = []

                    thaw = Thaw(client=mock_client, list_requests=True, porcelain=True)
                    thaw.do_action()

                    mock_list.assert_called_once()

    def test_thaw_raises_missing_index_error(self):
        """Test Thaw raises MissingIndexError when status index doesn't exist"""
        mock_client = MagicMock()
        mock_client.indices.exists.return_value = False

        thaw = Thaw(
            client=mock_client,
            start_date=datetime.now(timezone.utc),
            end_date=datetime.now(timezone.utc),
            porcelain=True,
        )

        with pytest.raises(MissingIndexError):
            thaw.do_action()

    def test_thaw_initiate_finds_repos(self):
        """Test Thaw initiate finds repositories by date range"""
        mock_client = MagicMock()
        mock_client.indices.exists.return_value = True

        mock_settings = Settings()
        start = datetime(2023, 1, 1, tzinfo=timezone.utc)
        end = datetime(2023, 1, 31, tzinfo=timezone.utc)

        with patch("deepfreeze.actions.thaw.get_settings") as mock_get:
            mock_get.return_value = mock_settings

            with patch("deepfreeze.actions.thaw.s3_client_factory") as mock_factory:
                mock_s3 = MagicMock()
                mock_factory.return_value = mock_s3

                with patch("deepfreeze.actions.thaw.find_repos_by_date_range") as mock_find:
                    mock_find.return_value = []  # No repos found

                    thaw = Thaw(
                        client=mock_client,
                        start_date=start,
                        end_date=end,
                        porcelain=True,
                    )
                    thaw._load_settings()

                    # Should call find_repos_by_date_range in _initiate_thaw
                    thaw._initiate_thaw(dry_run=True)
                    mock_find.assert_called_once()


class TestRefreezeAction:
    """Tests for the Refreeze action class"""

    def test_refreeze_initialization(self):
        """Test Refreeze action can be initialized"""
        mock_client = MagicMock()
        refreeze = Refreeze(client=mock_client, request_id="abc123")

        assert refreeze.client == mock_client
        assert refreeze.request_id == "abc123"

    def test_refreeze_raises_missing_index_error(self):
        """Test Refreeze raises MissingIndexError when status index doesn't exist"""
        mock_client = MagicMock()
        mock_client.indices.exists.return_value = False

        refreeze = Refreeze(client=mock_client, request_id="abc123", porcelain=True)

        with pytest.raises(MissingIndexError):
            refreeze.do_action()

    def test_refreeze_all_mode(self):
        """Test Refreeze all completed requests mode"""
        mock_client = MagicMock()
        mock_client.indices.exists.return_value = True

        mock_settings = Settings()

        with patch("deepfreeze.actions.refreeze.get_settings") as mock_get:
            mock_get.return_value = mock_settings

            with patch(
                "deepfreeze.actions.refreeze.s3_client_factory"
            ) as mock_factory:
                mock_s3 = MagicMock()
                mock_factory.return_value = mock_s3

                with patch(
                    "deepfreeze.actions.refreeze.list_thaw_requests"
                ) as mock_list:
                    mock_list.return_value = []  # No completed requests

                    refreeze = Refreeze(
                        client=mock_client, all_requests=True, porcelain=True
                    )
                    refreeze.do_action()

                    mock_list.assert_called_once()


class TestCleanupAction:
    """Tests for the Cleanup action class"""

    def test_cleanup_initialization(self):
        """Test Cleanup action can be initialized"""
        mock_client = MagicMock()
        cleanup = Cleanup(client=mock_client)

        assert cleanup.client == mock_client

    def test_cleanup_raises_missing_index_error(self):
        """Test Cleanup raises MissingIndexError when status index doesn't exist"""
        mock_client = MagicMock()
        mock_client.indices.exists.return_value = False

        cleanup = Cleanup(client=mock_client, porcelain=True)

        with pytest.raises(MissingIndexError):
            cleanup.do_action()

    def test_cleanup_dry_run_finds_expired_repos(self):
        """Test Cleanup dry run finds expired repositories"""
        mock_client = MagicMock()
        mock_client.indices.exists.return_value = True
        mock_client.ilm.get_lifecycle.return_value = {}

        mock_settings = Settings(repo_name_prefix="deepfreeze")

        with patch("deepfreeze.actions.cleanup.get_settings") as mock_get:
            mock_get.return_value = mock_settings

            with patch("deepfreeze.actions.cleanup.s3_client_factory") as mock_factory:
                mock_s3 = MagicMock()
                mock_factory.return_value = mock_s3

                with patch(
                    "deepfreeze.actions.cleanup.get_matching_repos"
                ) as mock_repos:
                    mock_repos.return_value = []

                    with patch(
                        "deepfreeze.actions.cleanup.list_thaw_requests"
                    ) as mock_list:
                        mock_list.return_value = []

                        cleanup = Cleanup(client=mock_client, porcelain=True)
                        cleanup.do_dry_run()

                        mock_repos.assert_called_once()
                        mock_list.assert_called_once()

    def test_cleanup_finds_old_thaw_requests(self):
        """Test Cleanup finds old thaw requests based on retention settings"""
        mock_client = MagicMock()
        mock_client.indices.exists.return_value = True
        mock_client.ilm.get_lifecycle.return_value = {}

        mock_settings = Settings(
            repo_name_prefix="deepfreeze",
            thaw_request_retention_days_completed=7,
        )

        old_date = datetime(2020, 1, 1, tzinfo=timezone.utc).isoformat()

        with patch("deepfreeze.actions.cleanup.get_settings") as mock_get:
            mock_get.return_value = mock_settings

            with patch("deepfreeze.actions.cleanup.s3_client_factory") as mock_factory:
                mock_s3 = MagicMock()
                mock_factory.return_value = mock_s3

                with patch("deepfreeze.actions.cleanup.get_matching_repos") as mock_repos:
                    mock_repos.return_value = []

                    with patch("deepfreeze.actions.cleanup.list_thaw_requests") as mock_list:
                        mock_list.return_value = [
                            {
                                "request_id": "old-request",
                                "status": "completed",
                                "created_at": old_date,
                            }
                        ]

                        cleanup = Cleanup(client=mock_client, porcelain=True)
                        cleanup._load_settings()

                        old_requests = cleanup._find_old_thaw_requests()
                        assert len(old_requests) == 1
                        assert old_requests[0]["request"]["request_id"] == "old-request"


class TestRepairMetadataAction:
    """Tests for the RepairMetadata action class"""

    def test_repair_metadata_initialization(self):
        """Test RepairMetadata action can be initialized"""
        mock_client = MagicMock()
        repair = RepairMetadata(client=mock_client)

        assert repair.client == mock_client

    def test_repair_metadata_raises_missing_index_error(self):
        """Test RepairMetadata raises MissingIndexError when status index doesn't exist"""
        mock_client = MagicMock()
        mock_client.indices.exists.return_value = False

        repair = RepairMetadata(client=mock_client, porcelain=True)

        with pytest.raises(MissingIndexError):
            repair.do_action()

    def test_repair_metadata_dry_run_scans_repos(self):
        """Test RepairMetadata dry run scans repositories"""
        mock_client = MagicMock()
        mock_client.indices.exists.return_value = True

        mock_settings = Settings()

        with patch("deepfreeze.actions.repair_metadata.get_settings") as mock_get:
            mock_get.return_value = mock_settings

            with patch(
                "deepfreeze.actions.repair_metadata.s3_client_factory"
            ) as mock_factory:
                mock_s3 = MagicMock()
                mock_factory.return_value = mock_s3

                with patch(
                    "deepfreeze.actions.repair_metadata.get_all_repos"
                ) as mock_repos:
                    mock_repos.return_value = []

                    repair = RepairMetadata(client=mock_client, porcelain=True)
                    repair.do_dry_run()

                    mock_repos.assert_called_once()


class TestActionInterfaceConsistency:
    """Tests to verify all actions have consistent interface"""

    @pytest.mark.parametrize(
        "action_class",
        [Setup, Status, Rotate, Thaw, Refreeze, Cleanup, RepairMetadata],
    )
    def test_action_has_do_action_method(self, action_class):
        """Test all action classes have do_action method"""
        assert hasattr(action_class, "do_action")
        assert callable(getattr(action_class, "do_action"))

    @pytest.mark.parametrize(
        "action_class",
        [Setup, Status, Rotate, Thaw, Refreeze, Cleanup, RepairMetadata],
    )
    def test_action_has_do_dry_run_method(self, action_class):
        """Test all action classes have do_dry_run method"""
        assert hasattr(action_class, "do_dry_run")
        assert callable(getattr(action_class, "do_dry_run"))


class TestNoCuratorImports:
    """Test that action modules don't import from curator"""

    @pytest.mark.parametrize(
        "module_name",
        [
            "deepfreeze.actions.setup",
            "deepfreeze.actions.status",
            "deepfreeze.actions.rotate",
            "deepfreeze.actions.thaw",
            "deepfreeze.actions.refreeze",
            "deepfreeze.actions.cleanup",
            "deepfreeze.actions.repair_metadata",
        ],
    )
    def test_no_curator_imports(self, module_name):
        """Test that action modules don't import from curator"""
        import importlib
        import inspect

        module = importlib.import_module(module_name)
        source = inspect.getsource(module)

        # Check for curator imports
        assert "from curator" not in source, f"{module_name} imports from curator"
        assert "import curator" not in source, f"{module_name} imports curator"


# ============================================================================
# Task Group 18: Additional Action Tests
# ============================================================================


class TestSetupActionAdditional:
    """Additional tests for Setup action (Task Group 18)"""

    def test_setup_with_custom_storage_class(self):
        """Test Setup with custom storage class"""
        mock_client = MagicMock()

        with patch("deepfreeze.actions.setup.s3_client_factory") as mock_factory:
            mock_s3 = MagicMock()
            mock_factory.return_value = mock_s3

            setup = Setup(
                client=mock_client,
                repo_name_prefix="test-repo",
                bucket_name_prefix="test-bucket",
                ilm_policy_name="test-policy",
                index_template_name="test-template",
                storage_class="glacier",
            )

            assert setup.settings.storage_class == "glacier"

    def test_setup_with_canned_acl(self):
        """Test Setup with canned ACL option"""
        mock_client = MagicMock()

        with patch("deepfreeze.actions.setup.s3_client_factory") as mock_factory:
            mock_s3 = MagicMock()
            mock_factory.return_value = mock_s3

            setup = Setup(
                client=mock_client,
                repo_name_prefix="test-repo",
                bucket_name_prefix="test-bucket",
                ilm_policy_name="test-policy",
                index_template_name="test-template",
                canned_acl="bucket-owner-full-control",
            )

            assert setup.settings.canned_acl == "bucket-owner-full-control"


class TestStatusActionAdditional:
    """Additional tests for Status action (Task Group 18)"""

    def test_status_porcelain_mode(self):
        """Test Status with porcelain=True produces machine-readable output"""
        mock_client = MagicMock()
        mock_client.indices.exists.return_value = True

        mock_settings = Settings(
            repo_name_prefix="deepfreeze",
            bucket_name_prefix="deepfreeze",
        )

        with patch("deepfreeze.actions.status.get_settings") as mock_get:
            mock_get.return_value = mock_settings

            with patch("deepfreeze.actions.status.s3_client_factory") as mock_factory:
                mock_s3 = MagicMock()
                mock_s3.list_buckets.return_value = []
                mock_factory.return_value = mock_s3

                with patch("deepfreeze.actions.status.get_all_repos") as mock_repos:
                    mock_repos.return_value = []

                    with patch("deepfreeze.actions.status.list_thaw_requests") as mock_thaw:
                        mock_thaw.return_value = []

                        status = Status(client=mock_client, porcelain=True)

                        # Should not raise
                        status.do_action()

    def test_status_with_porcelain_false(self):
        """Test Status with porcelain=False (human-readable output)"""
        mock_client = MagicMock()
        mock_client.indices.exists.return_value = True

        mock_settings = Settings(
            repo_name_prefix="deepfreeze",
            bucket_name_prefix="deepfreeze",
        )

        with patch("deepfreeze.actions.status.get_settings") as mock_get:
            mock_get.return_value = mock_settings

            with patch("deepfreeze.actions.status.s3_client_factory") as mock_factory:
                mock_s3 = MagicMock()
                mock_s3.list_buckets.return_value = []
                mock_factory.return_value = mock_s3

                with patch("deepfreeze.actions.status.get_all_repos") as mock_repos:
                    mock_repos.return_value = []

                    with patch("deepfreeze.actions.status.list_thaw_requests") as mock_thaw:
                        mock_thaw.return_value = []

                        status = Status(client=mock_client, porcelain=False)

                        # Should not raise
                        status.do_action()


class TestThawActionAdditional:
    """Additional tests for Thaw action (Task Group 18)"""

    def test_thaw_request_id_mode(self):
        """Test Thaw with request_id mode checks existing request"""
        mock_client = MagicMock()
        mock_client.indices.exists.return_value = True

        mock_settings = Settings()

        with patch("deepfreeze.actions.thaw.get_settings") as mock_get:
            mock_get.return_value = mock_settings

            with patch("deepfreeze.actions.thaw.s3_client_factory") as mock_factory:
                mock_s3 = MagicMock()
                mock_factory.return_value = mock_s3

                with patch("deepfreeze.actions.thaw.get_thaw_request") as mock_req:
                    mock_req.return_value = {
                        "request_id": "test-123",
                        "status": "in_progress",
                        "repos": ["repo-1"],
                    }

                    thaw = Thaw(
                        client=mock_client,
                        request_id="test-123",
                        porcelain=True,
                    )

                    # Should be able to initialize
                    assert thaw.request_id == "test-123"

    def test_thaw_with_retrieval_tier_expedited(self):
        """Test Thaw with Expedited retrieval tier"""
        mock_client = MagicMock()
        start = datetime(2023, 1, 1, tzinfo=timezone.utc)
        end = datetime(2023, 1, 31, tzinfo=timezone.utc)

        thaw = Thaw(
            client=mock_client,
            start_date=start,
            end_date=end,
            retrieval_tier="Expedited",
        )

        assert thaw.retrieval_tier == "Expedited"


class TestRefreezeActionAdditional:
    """Additional tests for Refreeze action (Task Group 18)"""

    def test_refreeze_dry_run(self):
        """Test Refreeze dry run mode"""
        mock_client = MagicMock()
        mock_client.indices.exists.return_value = True

        mock_settings = Settings()

        with patch("deepfreeze.actions.refreeze.get_settings") as mock_get:
            mock_get.return_value = mock_settings

            with patch("deepfreeze.actions.refreeze.s3_client_factory") as mock_factory:
                mock_s3 = MagicMock()
                mock_factory.return_value = mock_s3

                with patch("deepfreeze.actions.refreeze.get_thaw_request") as mock_req:
                    mock_req.return_value = {
                        "request_id": "test-123",
                        "status": "completed",
                        "repos": [],
                    }

                    refreeze = Refreeze(
                        client=mock_client,
                        request_id="test-123",
                        porcelain=True,
                    )

                    # Do dry run
                    refreeze.do_dry_run()

                    # Should not have actually refrozen anything
                    mock_s3.refreeze.assert_not_called()


class TestCleanupActionAdditional:
    """Additional tests for Cleanup action (Task Group 18)"""

    def test_cleanup_initialization_with_porcelain(self):
        """Test Cleanup can be initialized with porcelain mode"""
        mock_client = MagicMock()
        cleanup = Cleanup(client=mock_client, porcelain=True)

        assert cleanup.porcelain is True

    def test_cleanup_dry_run_shows_what_would_be_cleaned(self):
        """Test Cleanup dry run reports what would be cleaned"""
        mock_client = MagicMock()
        mock_client.indices.exists.return_value = True
        mock_client.ilm.get_lifecycle.return_value = {}

        mock_settings = Settings(repo_name_prefix="deepfreeze")

        old_date = datetime(2020, 1, 1, tzinfo=timezone.utc).isoformat()

        with patch("deepfreeze.actions.cleanup.get_settings") as mock_get:
            mock_get.return_value = mock_settings

            with patch("deepfreeze.actions.cleanup.s3_client_factory") as mock_factory:
                mock_s3 = MagicMock()
                mock_factory.return_value = mock_s3

                with patch("deepfreeze.actions.cleanup.get_matching_repos") as mock_repos:
                    # Expired repository
                    expired_repo = Repository(
                        name="deepfreeze-old",
                        bucket="bucket",
                        thaw_state="expired",
                    )
                    mock_repos.return_value = [expired_repo]

                    with patch("deepfreeze.actions.cleanup.list_thaw_requests") as mock_list:
                        mock_list.return_value = [
                            {
                                "request_id": "old-request",
                                "status": "refrozen",
                                "created_at": old_date,
                            }
                        ]

                        cleanup = Cleanup(client=mock_client, porcelain=True)
                        cleanup.do_dry_run()

                        # Should have called get_matching_repos
                        mock_repos.assert_called()


class TestRepairMetadataActionAdditional:
    """Additional tests for RepairMetadata action (Task Group 18)"""

    def test_repair_metadata_porcelain_output(self):
        """Test RepairMetadata with porcelain output"""
        mock_client = MagicMock()
        mock_client.indices.exists.return_value = True

        mock_settings = Settings()

        with patch("deepfreeze.actions.repair_metadata.get_settings") as mock_get:
            mock_get.return_value = mock_settings

            with patch("deepfreeze.actions.repair_metadata.s3_client_factory") as mock_factory:
                mock_s3 = MagicMock()
                mock_factory.return_value = mock_s3

                with patch("deepfreeze.actions.repair_metadata.get_all_repos") as mock_repos:
                    mock_repos.return_value = []

                    repair = RepairMetadata(client=mock_client, porcelain=True)

                    assert repair.porcelain is True

                    repair.do_action()
