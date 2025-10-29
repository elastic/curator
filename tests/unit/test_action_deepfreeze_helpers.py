"""Test deepfreeze helpers module"""
# pylint: disable=attribute-defined-outside-init
from unittest import TestCase
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime
import json
import pytest

from curator.actions.deepfreeze.helpers import Deepfreeze, Repository, Settings
from curator.actions.deepfreeze.constants import STATUS_INDEX, SETTINGS_ID


class TestDeepfreeze(TestCase):
    """Test Deepfreeze class"""

    def test_deepfreeze_init(self):
        """Test Deepfreeze class initialization"""
        df = Deepfreeze()
        assert isinstance(df, Deepfreeze)


class TestRepository(TestCase):
    """Test Repository dataclass"""

    def test_repository_init_with_all_params(self):
        """Test Repository initialization with all parameters"""
        start = datetime(2024, 1, 1)
        end = datetime(2024, 12, 31)

        repo = Repository(
            name="test-repo",
            bucket="test-bucket",
            base_path="/path/to/repo",
            start=start,
            end=end,
            is_thawed=True,
            is_mounted=False,
            doctype="repository",
            docid="repo-id-123"
        )

        assert repo.name == "test-repo"
        assert repo.bucket == "test-bucket"
        assert repo.base_path == "/path/to/repo"
        assert repo.start == start
        assert repo.end == end
        assert repo.is_thawed is True
        assert repo.is_mounted is False
        assert repo.doctype == "repository"
        assert repo.docid == "repo-id-123"

    def test_repository_init_with_defaults(self):
        """Test Repository initialization with default values"""
        repo = Repository(name="test-repo")

        assert repo.name == "test-repo"
        assert repo.bucket is None
        assert repo.base_path is None
        assert repo.start is None
        assert repo.end is None
        assert repo.is_thawed is False
        assert repo.is_mounted is True
        assert repo.doctype == "repository"
        assert repo.docid is None

    def test_repository_from_elasticsearch_success(self):
        """Test Repository.from_elasticsearch successful retrieval"""
        mock_client = Mock()
        mock_response = {
            'hits': {
                'hits': [{
                    '_id': 'repo-id-123',
                    '_source': {
                        'name': 'test-repo',
                        'bucket': 'test-bucket',
                        'base_path': '/path/to/repo',
                        'start': '2024-01-01T00:00:00',
                        'end': '2024-12-31T23:59:59',
                        'is_thawed': True,
                        'is_mounted': False,
                        'doctype': 'repository'
                    }
                }]
            }
        }
        mock_client.search.return_value = mock_response

        with patch('curator.actions.deepfreeze.helpers.logging'):
            repo = Repository.from_elasticsearch(mock_client, 'test-repo')

        assert repo is not None
        assert repo.name == 'test-repo'
        assert repo.bucket == 'test-bucket'
        assert repo.base_path == '/path/to/repo'
        assert repo.docid == 'repo-id-123'

        mock_client.search.assert_called_once_with(
            index=STATUS_INDEX,
            query={"match": {"name.keyword": "test-repo"}},
            size=1
        )

    def test_repository_from_elasticsearch_not_found(self):
        """Test Repository.from_elasticsearch when repository not found"""
        mock_client = Mock()
        mock_response = {
            'hits': {
                'hits': []
            }
        }
        mock_client.search.return_value = mock_response

        with patch('curator.actions.deepfreeze.helpers.logging'):
            repo = Repository.from_elasticsearch(mock_client, 'nonexistent-repo')

        assert repo is None

    def test_repository_from_elasticsearch_with_custom_index(self):
        """Test Repository.from_elasticsearch with custom index"""
        mock_client = Mock()
        mock_response = {
            'hits': {
                'hits': [{
                    '_id': 'repo-id',
                    '_source': {
                        'name': 'test-repo',
                        'doctype': 'repository'
                    }
                }]
            }
        }
        mock_client.search.return_value = mock_response

        with patch('curator.actions.deepfreeze.helpers.logging'):
            repo = Repository.from_elasticsearch(
                mock_client,
                'test-repo',
                index='custom-index'
            )

        mock_client.search.assert_called_once_with(
            index='custom-index',
            query={"match": {"name.keyword": "test-repo"}},
            size=1
        )

    def test_repository_to_dict(self):
        """Test Repository.to_dict method"""
        repo = Repository(
            name="test-repo",
            bucket="test-bucket",
            base_path="/path/to/repo",
            start="2024-01-01",
            end="2024-12-31",
            is_thawed=True,
            is_mounted=False,
            doctype="repository"
        )

        result = repo.to_dict()

        assert isinstance(result, dict)
        assert result['name'] == "test-repo"
        assert result['bucket'] == "test-bucket"
        assert result['base_path'] == "/path/to/repo"
        assert result['is_thawed'] is True
        assert result['is_mounted'] is False
        assert result['doctype'] == "repository"
        # Dates are converted to ISO format with time component
        assert result['start'] == "2024-01-01T00:00:00"
        assert result['end'] == "2024-12-31T00:00:00"

    def test_repository_to_dict_with_none_dates(self):
        """Test Repository.to_dict with None dates"""
        repo = Repository(
            name="test-repo",
            start=None,
            end=None
        )

        result = repo.to_dict()

        assert result['start'] is None
        assert result['end'] is None

    def test_repository_to_json(self):
        """Test Repository.to_json method"""
        repo = Repository(
            name="test-repo",
            bucket="test-bucket",
            base_path="/path/to/repo",
            is_thawed=False,
            is_mounted=True
        )

        result = repo.to_json()

        assert isinstance(result, str)
        data = json.loads(result)
        assert data['name'] == "test-repo"
        assert data['bucket'] == "test-bucket"
        assert data['base_path'] == "/path/to/repo"
        assert data['is_thawed'] is False
        assert data['is_mounted'] is True

    def test_repository_lt_comparison(self):
        """Test Repository __lt__ comparison method"""
        repo1 = Repository(name="repo-001")
        repo2 = Repository(name="repo-002")
        repo3 = Repository(name="repo-010")

        assert repo1 < repo2
        assert repo2 < repo3
        assert not repo2 < repo1
        assert not repo3 < repo2

    def test_repository_persist(self):
        """Test Repository.persist method"""
        mock_client = Mock()
        mock_client.update.return_value = {'_id': 'updated-id-123'}

        repo = Repository(
            name="test-repo",
            bucket="test-bucket",
            base_path="/path/to/repo",
            docid="existing-id-123"
        )

        with patch('curator.actions.deepfreeze.helpers.logging'):
            repo.persist(mock_client)

        # Should call update with existing ID
        mock_client.update.assert_called_once()
        call_args = mock_client.update.call_args
        assert call_args[1]['index'] == STATUS_INDEX
        assert call_args[1]['id'] == 'existing-id-123'
        assert call_args[1]['body']['doc']['name'] == 'test-repo'

    def test_repository_unmount(self):
        """Test Repository.unmount method"""
        repo = Repository(
            name="test-repo",
            is_mounted=True
        )

        repo.unmount()

        # Should update is_mounted
        assert repo.is_mounted is False


class TestSettings(TestCase):
    """Test Settings dataclass"""

    def test_settings_init_with_all_params(self):
        """Test Settings initialization with all parameters"""
        settings = Settings(
            repo_name_prefix="deepfreeze",
            bucket_name_prefix="deepfreeze",
            base_path_prefix="snapshots",
            canned_acl="private",
            storage_class="GLACIER",
            provider="aws",
            rotate_by="path",
            style="oneup",
            last_suffix="000001"
        )

        assert settings.repo_name_prefix == "deepfreeze"
        assert settings.bucket_name_prefix == "deepfreeze"
        assert settings.base_path_prefix == "snapshots"
        assert settings.canned_acl == "private"
        assert settings.storage_class == "GLACIER"
        assert settings.provider == "aws"
        assert settings.rotate_by == "path"
        assert settings.style == "oneup"
        assert settings.last_suffix == "000001"

    def test_settings_init_with_defaults(self):
        """Test Settings initialization with default values"""
        settings = Settings()

        assert settings.repo_name_prefix == "deepfreeze"
        assert settings.bucket_name_prefix == "deepfreeze"
        assert settings.base_path_prefix == "snapshots"
        assert settings.canned_acl == "private"
        assert settings.storage_class == "intelligent_tiering"
        assert settings.provider == "aws"
        assert settings.rotate_by == "path"
        assert settings.style == "oneup"
        assert settings.last_suffix is None

    def test_settings_init_with_hash(self):
        """Test Settings initialization with settings hash"""
        settings_hash = {
            'repo_name_prefix': 'custom-prefix',
            'storage_class': 'STANDARD_IA',
            'rotate_by': 'bucket'
        }

        settings = Settings(settings_hash=settings_hash)

        # Settings constructor overrides hash values with defaults if they're passed as parameters
        # Since we're not passing explicit parameters, the hash should be applied first,
        # then defaults override them
        assert settings.repo_name_prefix == "deepfreeze"  # Default overrides hash
        assert settings.storage_class == "intelligent_tiering"  # Default overrides hash
        assert settings.rotate_by == "path"  # Default overrides hash
        # But the hash values should be set via setattr
        # Let's test with no default parameters
        settings2 = Settings(settings_hash=settings_hash, repo_name_prefix=None, storage_class=None, rotate_by=None)
        assert settings2.repo_name_prefix == "custom-prefix"
        assert settings2.storage_class == "STANDARD_IA"
        assert settings2.rotate_by == "bucket"

    def test_settings_dataclass_behavior(self):
        """Test Settings dataclass behavior"""
        settings = Settings(
            repo_name_prefix="test-prefix",
            bucket_name_prefix="test-bucket",
            provider="gcp"
        )

        # Settings is a dataclass, so we can access attributes directly
        assert settings.repo_name_prefix == "test-prefix"
        assert settings.bucket_name_prefix == "test-bucket"
        assert settings.provider == "gcp"
        assert settings.doctype == "settings"

        # Test that we can convert to dict using dataclasses
        import dataclasses
        result = dataclasses.asdict(settings)
        assert isinstance(result, dict)
        assert result['repo_name_prefix'] == "test-prefix"
        assert result['bucket_name_prefix'] == "test-bucket"
        assert result['provider'] == "gcp"