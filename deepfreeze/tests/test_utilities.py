"""
Tests for utilities module (Task Group 6)

These tests verify that:
1. Settings operations work correctly (mock ES)
2. Repository operations work correctly (mock ES)
3. Date range operations work correctly
4. No curator imports remain in the module
5. CreateIndex is replaced with direct ES calls
6. All utility functions are accessible
"""

import pytest
import re
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch


class TestSettingsOperations:
    """Tests for settings-related utility functions"""

    def test_ensure_settings_index_creates_when_missing(self):
        """Test ensure_settings_index creates index when missing and flag is True"""
        from deepfreeze.utilities import ensure_settings_index

        mock_client = MagicMock()
        mock_client.indices.exists.return_value = False
        mock_client.indices.create.return_value = {"acknowledged": True}

        ensure_settings_index(mock_client, create_if_missing=True)

        mock_client.indices.create.assert_called_once()
        call_kwargs = mock_client.indices.create.call_args[1]
        assert call_kwargs["index"] == "deepfreeze-status"
        assert "mappings" in call_kwargs["body"]

    def test_ensure_settings_index_raises_when_missing_no_create(self):
        """Test ensure_settings_index raises MissingIndexError when missing and flag is False"""
        from deepfreeze.utilities import ensure_settings_index
        from deepfreeze.exceptions import MissingIndexError

        mock_client = MagicMock()
        mock_client.indices.exists.return_value = False

        with pytest.raises(MissingIndexError):
            ensure_settings_index(mock_client, create_if_missing=False)

    def test_ensure_settings_index_does_nothing_when_exists(self):
        """Test ensure_settings_index does nothing when index exists"""
        from deepfreeze.utilities import ensure_settings_index

        mock_client = MagicMock()
        mock_client.indices.exists.return_value = True

        ensure_settings_index(mock_client, create_if_missing=True)

        mock_client.indices.create.assert_not_called()

    def test_get_settings_returns_settings_object(self):
        """Test get_settings returns a Settings object when found"""
        from deepfreeze.utilities import get_settings
        from deepfreeze.helpers import Settings

        mock_client = MagicMock()
        mock_client.indices.exists.return_value = True
        mock_client.get.return_value = {
            "_source": {
                "doctype": "settings",
                "repo_name_prefix": "test-repo",
                "bucket_name_prefix": "test-bucket",
            }
        }

        result = get_settings(mock_client)

        assert isinstance(result, Settings)
        assert result.repo_name_prefix == "test-repo"
        assert result.bucket_name_prefix == "test-bucket"

    def test_get_settings_returns_none_when_not_found(self):
        """Test get_settings returns None when settings not found"""
        from deepfreeze.utilities import get_settings
        from elasticsearch8 import NotFoundError

        mock_client = MagicMock()
        mock_client.indices.exists.return_value = True
        mock_client.get.side_effect = NotFoundError(404, "not_found", "not found")

        result = get_settings(mock_client)

        assert result is None

    def test_save_settings_creates_new(self):
        """Test save_settings creates new document when not exists"""
        from deepfreeze.utilities import save_settings
        from deepfreeze.helpers import Settings
        from elasticsearch8 import NotFoundError

        mock_client = MagicMock()
        mock_client.get.side_effect = NotFoundError(404, "not_found", "not found")

        settings = Settings(repo_name_prefix="new-repo")

        save_settings(mock_client, settings)

        mock_client.create.assert_called_once()

    def test_save_settings_updates_existing(self):
        """Test save_settings updates document when exists"""
        from deepfreeze.utilities import save_settings
        from deepfreeze.helpers import Settings

        mock_client = MagicMock()
        mock_client.get.return_value = {"_source": {}}

        settings = Settings(repo_name_prefix="updated-repo")

        save_settings(mock_client, settings)

        mock_client.update.assert_called_once()


class TestRepositoryOperations:
    """Tests for repository-related utility functions"""

    def test_get_repository_returns_repository_when_found(self):
        """Test get_repository returns Repository when found in status index"""
        from deepfreeze.utilities import get_repository
        from deepfreeze.helpers import Repository

        mock_client = MagicMock()
        mock_client.search.return_value = {
            "hits": {
                "total": {"value": 1},
                "hits": [
                    {
                        "_id": "doc-123",
                        "_source": {
                            "name": "test-repo",
                            "bucket": "test-bucket",
                            "base_path": "test-path",
                            "doctype": "repository",
                        },
                    }
                ],
            }
        }

        result = get_repository(mock_client, "test-repo")

        assert isinstance(result, Repository)
        assert result.name == "test-repo"
        assert result.docid == "doc-123"

    def test_get_repository_returns_empty_when_not_found(self):
        """Test get_repository returns new Repository when not found"""
        from deepfreeze.utilities import get_repository
        from deepfreeze.helpers import Repository

        mock_client = MagicMock()
        mock_client.search.return_value = {
            "hits": {"total": {"value": 0}, "hits": []}
        }

        result = get_repository(mock_client, "nonexistent")

        assert isinstance(result, Repository)
        assert result.name == "nonexistent"
        assert result.docid is None

    def test_get_all_repos_returns_list(self):
        """Test get_all_repos returns list of Repository objects"""
        from deepfreeze.utilities import get_all_repos
        from deepfreeze.helpers import Repository

        mock_client = MagicMock()
        mock_client.search.return_value = {
            "hits": {
                "hits": [
                    {
                        "_id": "doc-1",
                        "_source": {
                            "name": "repo-1",
                            "bucket": "bucket-1",
                            "doctype": "repository",
                        },
                    },
                    {
                        "_id": "doc-2",
                        "_source": {
                            "name": "repo-2",
                            "bucket": "bucket-2",
                            "doctype": "repository",
                        },
                    },
                ]
            }
        }

        result = get_all_repos(mock_client)

        assert len(result) == 2
        assert all(isinstance(r, Repository) for r in result)
        assert result[0].name == "repo-1"
        assert result[1].name == "repo-2"

    def test_get_matching_repos_filters_by_prefix(self):
        """Test get_matching_repos filters repositories by prefix"""
        from deepfreeze.utilities import get_matching_repos
        from deepfreeze.helpers import Repository

        mock_client = MagicMock()
        mock_client.search.return_value = {
            "hits": {
                "hits": [
                    {
                        "_id": "doc-1",
                        "_source": {
                            "name": "deepfreeze-000001",
                            "is_mounted": True,
                            "doctype": "repository",
                        },
                    },
                    {
                        "_id": "doc-2",
                        "_source": {
                            "name": "other-repo",
                            "is_mounted": True,
                            "doctype": "repository",
                        },
                    },
                ]
            }
        }

        result = get_matching_repos(mock_client, "deepfreeze")

        assert len(result) == 1
        assert result[0].name == "deepfreeze-000001"


class TestDateOperations:
    """Tests for date-related utility functions"""

    def test_decode_date_from_string(self):
        """Test decode_date converts ISO string to datetime"""
        from deepfreeze.utilities import decode_date

        result = decode_date("2023-06-15T10:30:00+00:00")

        assert isinstance(result, datetime)
        assert result.year == 2023
        assert result.month == 6
        assert result.day == 15

    def test_decode_date_from_datetime(self):
        """Test decode_date handles datetime input"""
        from deepfreeze.utilities import decode_date

        input_dt = datetime(2023, 6, 15, 10, 30, 0, tzinfo=timezone.utc)

        result = decode_date(input_dt)

        assert result == input_dt

    def test_decode_date_adds_utc_if_naive(self):
        """Test decode_date adds UTC timezone to naive datetime"""
        from deepfreeze.utilities import decode_date

        result = decode_date("2023-06-15T10:30:00")

        assert result.tzinfo == timezone.utc

    def test_decode_date_invalid_raises(self):
        """Test decode_date raises ValueError for invalid input"""
        from deepfreeze.utilities import decode_date

        with pytest.raises(ValueError):
            decode_date(12345)  # Neither string nor datetime

    def test_get_next_suffix_oneup(self):
        """Test get_next_suffix with oneup style"""
        from deepfreeze.utilities import get_next_suffix

        result = get_next_suffix("oneup", "000041", None, None)

        assert result == "000042"

    def test_get_next_suffix_date(self):
        """Test get_next_suffix with date style"""
        from deepfreeze.utilities import get_next_suffix

        result = get_next_suffix("date", "000001", 2024, 3)

        assert result == "2024.03"

    def test_get_next_suffix_invalid_raises(self):
        """Test get_next_suffix raises for invalid style"""
        from deepfreeze.utilities import get_next_suffix

        with pytest.raises(ValueError):
            get_next_suffix("invalid_style", "000001", None, None)

    def test_find_repos_by_date_range(self):
        """Test find_repos_by_date_range queries with correct date filters"""
        from deepfreeze.utilities import find_repos_by_date_range
        from deepfreeze.helpers import Repository

        mock_client = MagicMock()
        mock_client.search.return_value = {
            "hits": {
                "hits": [
                    {
                        "_id": "doc-1",
                        "_source": {
                            "name": "repo-in-range",
                            "start": "2023-01-01T00:00:00+00:00",
                            "end": "2023-12-31T23:59:59+00:00",
                            "doctype": "repository",
                        },
                    }
                ]
            }
        }

        start = datetime(2023, 6, 1, tzinfo=timezone.utc)
        end = datetime(2023, 6, 30, tzinfo=timezone.utc)

        result = find_repos_by_date_range(mock_client, start, end)

        assert len(result) == 1
        assert isinstance(result[0], Repository)
        mock_client.search.assert_called_once()


class TestThawRequestOperations:
    """Tests for thaw request utility functions"""

    def test_save_thaw_request_creates_document(self):
        """Test save_thaw_request creates a thaw request document"""
        from deepfreeze.utilities import save_thaw_request
        from deepfreeze.helpers import Repository

        mock_client = MagicMock()
        repos = [Repository(name="repo-1"), Repository(name="repo-2")]

        save_thaw_request(
            mock_client,
            request_id="thaw-123",
            repos=repos,
            status="in_progress",
        )

        mock_client.index.assert_called_once()
        call_kwargs = mock_client.index.call_args[1]
        assert call_kwargs["id"] == "thaw-123"
        assert call_kwargs["body"]["request_id"] == "thaw-123"
        assert call_kwargs["body"]["repos"] == ["repo-1", "repo-2"]

    def test_get_thaw_request_returns_document(self):
        """Test get_thaw_request retrieves thaw request"""
        from deepfreeze.utilities import get_thaw_request

        mock_client = MagicMock()
        mock_client.get.return_value = {
            "_source": {
                "request_id": "thaw-123",
                "status": "completed",
                "repos": ["repo-1"],
            }
        }

        result = get_thaw_request(mock_client, "thaw-123")

        assert result["request_id"] == "thaw-123"
        assert result["status"] == "completed"

    def test_list_thaw_requests_returns_all(self):
        """Test list_thaw_requests returns all thaw requests"""
        from deepfreeze.utilities import list_thaw_requests

        mock_client = MagicMock()
        mock_client.search.return_value = {
            "hits": {
                "hits": [
                    {
                        "_id": "thaw-1",
                        "_source": {
                            "doctype": "thaw_request",
                            "request_id": "thaw-1",
                            "status": "completed",
                        },
                    },
                    {
                        "_id": "thaw-2",
                        "_source": {
                            "doctype": "thaw_request",
                            "request_id": "thaw-2",
                            "status": "in_progress",
                        },
                    },
                ]
            }
        }

        result = list_thaw_requests(mock_client)

        assert len(result) == 2
        assert result[0]["id"] == "thaw-1"
        assert result[1]["id"] == "thaw-2"

    def test_update_thaw_request(self):
        """Test update_thaw_request updates status"""
        from deepfreeze.utilities import update_thaw_request

        mock_client = MagicMock()

        update_thaw_request(mock_client, "thaw-123", status="completed")

        mock_client.update.assert_called_once()
        call_kwargs = mock_client.update.call_args[1]
        assert call_kwargs["id"] == "thaw-123"
        assert call_kwargs["doc"]["status"] == "completed"


class TestNoCuratorImports:
    """Verify that utilities module has no curator imports"""

    def test_no_curator_imports_in_utilities(self):
        """Test that utilities.py has no curator imports (actual import statements, not docstrings)"""
        import deepfreeze.utilities as util_module
        import inspect

        source = inspect.getsource(util_module)

        # Use regex to find actual import statements, not docstring references
        # Look for "from curator" or "import curator" at the start of a line (after any whitespace)
        import_pattern = r'^\s*(from\s+curator\b|import\s+curator\b)'
        matches = re.findall(import_pattern, source, re.MULTILINE)

        assert len(matches) == 0, f"Found curator import statements: {matches}"

    def test_module_uses_local_exceptions(self):
        """Test that utilities uses deepfreeze.exceptions"""
        from deepfreeze.utilities import ActionError, MissingIndexError
        from deepfreeze.exceptions import ActionError as DFActionError
        from deepfreeze.exceptions import MissingIndexError as DFMissingIndexError

        # Verify they are the same classes
        assert ActionError is DFActionError
        assert MissingIndexError is DFMissingIndexError


class TestAllUtilityFunctionsAccessible:
    """Verify all expected utility functions are accessible"""

    def test_repository_functions_exist(self):
        """Test repository operation functions are importable"""
        from deepfreeze.utilities import (
            create_repo,
            mount_repo,
            unmount_repo,
            get_repository,
            get_all_repos,
            get_matching_repos,
            get_matching_repo_names,
        )

        assert callable(create_repo)
        assert callable(mount_repo)
        assert callable(unmount_repo)
        assert callable(get_repository)
        assert callable(get_all_repos)
        assert callable(get_matching_repos)
        assert callable(get_matching_repo_names)

    def test_settings_functions_exist(self):
        """Test settings operation functions are importable"""
        from deepfreeze.utilities import (
            get_settings,
            save_settings,
            ensure_settings_index,
        )

        assert callable(get_settings)
        assert callable(save_settings)
        assert callable(ensure_settings_index)

    def test_date_functions_exist(self):
        """Test date operation functions are importable"""
        from deepfreeze.utilities import (
            get_timestamp_range,
            decode_date,
            find_repos_by_date_range,
        )

        assert callable(get_timestamp_range)
        assert callable(decode_date)
        assert callable(find_repos_by_date_range)

    def test_ilm_functions_exist(self):
        """Test ILM operation functions are importable"""
        from deepfreeze.utilities import (
            create_ilm_policy,
            get_ilm_policy,
            create_or_update_ilm_policy,
            create_thawed_ilm_policy,
        )

        assert callable(create_ilm_policy)
        assert callable(get_ilm_policy)
        assert callable(create_or_update_ilm_policy)
        assert callable(create_thawed_ilm_policy)

    def test_thaw_functions_exist(self):
        """Test thaw operation functions are importable"""
        from deepfreeze.utilities import (
            save_thaw_request,
            get_thaw_request,
            list_thaw_requests,
            update_thaw_request,
        )

        assert callable(save_thaw_request)
        assert callable(get_thaw_request)
        assert callable(list_thaw_requests)
        assert callable(update_thaw_request)

    def test_s3_functions_exist(self):
        """Test S3 operation functions are importable"""
        from deepfreeze.utilities import (
            push_to_glacier,
            check_restore_status,
        )

        assert callable(push_to_glacier)
        assert callable(check_restore_status)

    def test_index_functions_exist(self):
        """Test index operation functions are importable"""
        from deepfreeze.utilities import (
            find_snapshots_for_index,
            mount_snapshot_index,
            get_all_indices_in_repo,
            find_and_mount_indices_in_date_range,
        )

        assert callable(find_snapshots_for_index)
        assert callable(mount_snapshot_index)
        assert callable(get_all_indices_in_repo)
        assert callable(find_and_mount_indices_in_date_range)
