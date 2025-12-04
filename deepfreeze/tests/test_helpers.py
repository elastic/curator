"""
Tests for helpers module (Task Group 4)

These tests verify that:
1. Repository can be serialized to dict and JSON
2. Repository can be deserialized from Elasticsearch response
3. Settings can be initialized with defaults and custom values
4. Repository state transitions work correctly
5. Elasticsearch client interactions work with mocks
"""

import json
import pytest
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch


class TestRepositorySerialization:
    """Tests for Repository serialization/deserialization"""

    def test_repository_to_dict(self):
        """Test Repository.to_dict() converts all fields correctly"""
        from deepfreeze.helpers import Repository

        start_dt = datetime(2023, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        end_dt = datetime(2023, 12, 31, 23, 59, 59, tzinfo=timezone.utc)

        repo = Repository(
            name="test-repo-000001",
            bucket="test-bucket",
            base_path="snapshots/test",
            start=start_dt,
            end=end_dt,
            is_mounted=True,
            thaw_state="active",
        )

        result = repo.to_dict()

        assert result["name"] == "test-repo-000001"
        assert result["bucket"] == "test-bucket"
        assert result["base_path"] == "snapshots/test"
        assert result["start"] == "2023-01-01T00:00:00+00:00"
        assert result["end"] == "2023-12-31T23:59:59+00:00"
        assert result["is_mounted"] is True
        assert result["thaw_state"] == "active"
        assert result["doctype"] == "repository"

    def test_repository_to_json(self):
        """Test Repository.to_json() returns valid JSON"""
        from deepfreeze.helpers import Repository

        repo = Repository(
            name="json-test-repo",
            bucket="json-bucket",
            base_path="path",
        )

        result = repo.to_json()

        # Should be valid JSON
        parsed = json.loads(result)
        assert parsed["name"] == "json-test-repo"
        assert parsed["bucket"] == "json-bucket"

    def test_repository_from_dict_with_string_dates(self):
        """Test Repository __post_init__ converts string dates to datetime"""
        from deepfreeze.helpers import Repository

        repo = Repository(
            name="string-date-repo",
            start="2023-06-15T10:30:00+00:00",
            end="2023-07-15T10:30:00+00:00",
        )

        assert isinstance(repo.start, datetime)
        assert isinstance(repo.end, datetime)
        assert repo.start.year == 2023
        assert repo.start.month == 6

    def test_repository_comparison(self):
        """Test Repository.__lt__() for sorting"""
        from deepfreeze.helpers import Repository

        repo_a = Repository(name="deepfreeze-000001")
        repo_b = Repository(name="deepfreeze-000002")
        repo_c = Repository(name="deepfreeze-000001")

        assert repo_a < repo_b
        assert not repo_b < repo_a
        assert not repo_a < repo_c  # Equal names


class TestRepositoryStateTransitions:
    """Tests for Repository state machine methods"""

    def test_start_thawing_transition(self):
        """Test Repository.start_thawing() sets correct state"""
        from deepfreeze.helpers import Repository
        from deepfreeze.constants import THAW_STATE_THAWING

        repo = Repository(name="thaw-test", thaw_state="frozen")
        expires_at = datetime(2024, 1, 15, tzinfo=timezone.utc)

        repo.start_thawing(expires_at)

        assert repo.thaw_state == THAW_STATE_THAWING
        assert repo.expires_at == expires_at
        assert repo.is_thawed is True  # Backward compat

    def test_mark_thawed_transition(self):
        """Test Repository.mark_thawed() sets correct state"""
        from deepfreeze.helpers import Repository
        from deepfreeze.constants import THAW_STATE_THAWED

        repo = Repository(name="mark-thawed-test", thaw_state="thawing")

        repo.mark_thawed()

        assert repo.thaw_state == THAW_STATE_THAWED
        assert repo.is_thawed is True
        assert repo.is_mounted is True
        assert repo.thawed_at is not None
        assert isinstance(repo.thawed_at, datetime)

    def test_mark_expired_transition(self):
        """Test Repository.mark_expired() sets correct state"""
        from deepfreeze.helpers import Repository
        from deepfreeze.constants import THAW_STATE_EXPIRED

        repo = Repository(name="expire-test", thaw_state="thawed")
        repo.thawed_at = datetime(2023, 12, 1, tzinfo=timezone.utc)

        repo.mark_expired()

        assert repo.thaw_state == THAW_STATE_EXPIRED
        # Historical tracking preserved
        assert repo.thawed_at is not None

    def test_reset_to_frozen_transition(self):
        """Test Repository.reset_to_frozen() clears all thaw state"""
        from deepfreeze.helpers import Repository
        from deepfreeze.constants import THAW_STATE_FROZEN

        repo = Repository(
            name="reset-test",
            thaw_state="expired",
            is_thawed=True,
            is_mounted=True,
            thawed_at=datetime.now(timezone.utc),
            expires_at=datetime.now(timezone.utc),
        )

        repo.reset_to_frozen()

        assert repo.thaw_state == THAW_STATE_FROZEN
        assert repo.is_thawed is False
        assert repo.is_mounted is False
        assert repo.thawed_at is None
        assert repo.expires_at is None

    def test_unmount(self):
        """Test Repository.unmount() sets is_mounted to False"""
        from deepfreeze.helpers import Repository

        repo = Repository(name="unmount-test", is_mounted=True)

        repo.unmount()

        assert repo.is_mounted is False


class TestSettingsInitialization:
    """Tests for Settings class initialization"""

    def test_settings_default_values(self):
        """Test Settings initializes with correct defaults"""
        from deepfreeze.helpers import Settings

        settings = Settings()

        assert settings.doctype == "settings"
        assert settings.repo_name_prefix == "deepfreeze"
        assert settings.bucket_name_prefix == "deepfreeze"
        assert settings.base_path_prefix == "snapshots"
        assert settings.canned_acl == "private"
        assert settings.storage_class == "intelligent_tiering"
        assert settings.provider == "aws"
        assert settings.rotate_by == "path"
        assert settings.style == "oneup"
        assert settings.thaw_request_retention_days_completed == 7
        assert settings.thaw_request_retention_days_failed == 30
        assert settings.thaw_request_retention_days_refrozen == 35

    def test_settings_custom_values(self):
        """Test Settings accepts custom initialization values"""
        from deepfreeze.helpers import Settings

        settings = Settings(
            repo_name_prefix="custom-repo",
            bucket_name_prefix="custom-bucket",
            storage_class="glacier",
            ilm_policy_name="my-ilm-policy",
            index_template_name="my-template",
        )

        assert settings.repo_name_prefix == "custom-repo"
        assert settings.bucket_name_prefix == "custom-bucket"
        assert settings.storage_class == "glacier"
        assert settings.ilm_policy_name == "my-ilm-policy"
        assert settings.index_template_name == "my-template"

    def test_settings_from_hash(self):
        """Test Settings initializes from settings_hash dict"""
        from deepfreeze.helpers import Settings

        settings_hash = {
            "repo_name_prefix": "hash-repo",
            "bucket_name_prefix": "hash-bucket",
            "last_suffix": "000042",
        }

        settings = Settings(settings_hash=settings_hash)

        assert settings.repo_name_prefix == "hash-repo"
        assert settings.bucket_name_prefix == "hash-bucket"
        assert settings.last_suffix == "000042"

    def test_settings_to_dict(self):
        """Test Settings.to_dict() returns all fields"""
        from deepfreeze.helpers import Settings

        settings = Settings(ilm_policy_name="test-policy")
        result = settings.to_dict()

        assert "doctype" in result
        assert "repo_name_prefix" in result
        assert "bucket_name_prefix" in result
        assert "ilm_policy_name" in result
        assert result["ilm_policy_name"] == "test-policy"

    def test_settings_to_json(self):
        """Test Settings.to_json() returns valid JSON"""
        from deepfreeze.helpers import Settings

        settings = Settings()
        result = settings.to_json()

        # Should be valid JSON
        parsed = json.loads(result)
        assert parsed["doctype"] == "settings"


class TestRepositoryElasticsearchInteraction:
    """Tests for Repository interaction with Elasticsearch (mocked)"""

    def test_from_elasticsearch_success(self):
        """Test Repository.from_elasticsearch() fetches and creates instance"""
        from deepfreeze.helpers import Repository

        # Mock ES client
        mock_client = MagicMock()
        mock_client.search.return_value = {
            "hits": {
                "hits": [
                    {
                        "_id": "doc-123",
                        "_source": {
                            "name": "es-repo-test",
                            "bucket": "es-bucket",
                            "base_path": "es-path",
                            "is_mounted": True,
                            "thaw_state": "active",
                            "doctype": "repository",
                        },
                    }
                ]
            }
        }

        result = Repository.from_elasticsearch(mock_client, "es-repo-test")

        assert result is not None
        assert result.name == "es-repo-test"
        assert result.bucket == "es-bucket"
        assert result.docid == "doc-123"
        mock_client.search.assert_called_once()

    def test_from_elasticsearch_not_found(self):
        """Test Repository.from_elasticsearch() returns None when not found"""
        from deepfreeze.helpers import Repository

        mock_client = MagicMock()
        mock_client.search.return_value = {"hits": {"hits": []}}

        result = Repository.from_elasticsearch(mock_client, "nonexistent")

        assert result is None

    def test_persist_calls_es_update(self):
        """Test Repository.persist() calls ES update correctly"""
        from deepfreeze.helpers import Repository

        mock_client = MagicMock()
        repo = Repository(
            name="persist-test",
            bucket="persist-bucket",
            docid="doc-456",
        )

        repo.persist(mock_client)

        mock_client.update.assert_called_once()
        call_kwargs = mock_client.update.call_args[1]
        assert call_kwargs["index"] == "deepfreeze-status"
        assert call_kwargs["id"] == "doc-456"
        assert "doc" in call_kwargs["body"]


class TestDeepfreezeClass:
    """Tests for Deepfreeze class"""

    def test_deepfreeze_class_exists(self):
        """Test Deepfreeze class can be instantiated"""
        from deepfreeze.helpers import Deepfreeze

        df = Deepfreeze()
        assert df is not None
