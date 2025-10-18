"""Test deepfreeze utilities module"""
# pylint: disable=attribute-defined-outside-init
from unittest import TestCase
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timezone
import pytest
import botocore.exceptions

from curator.actions.deepfreeze.utilities import (
    push_to_glacier,
    get_all_indices_in_repo,
    get_timestamp_range,
    get_repository,
    get_all_repos,
    get_settings,
    save_settings,
    get_next_suffix,
    get_matching_repo_names,
    get_matching_repos,
    unmount_repo,
    decode_date,
    create_ilm_policy,
    update_repository_date_range,
    get_index_templates,
    get_composable_templates,
    update_template_ilm_policy,
    create_versioned_ilm_policy,
    get_policies_for_repo,
    get_policies_by_suffix,
    is_policy_safe_to_delete,
)
from curator.actions.deepfreeze.helpers import Repository, Settings
from curator.actions.deepfreeze.constants import STATUS_INDEX, SETTINGS_ID
from curator.actions.deepfreeze.exceptions import MissingIndexError
from curator.exceptions import ActionError


class TestPushToGlacier(TestCase):
    """Test push_to_glacier function"""

    def test_push_to_glacier_success(self):
        """Test successful push to Glacier"""
        mock_s3 = Mock()
        mock_s3.list_objects.return_value = [
            {'Key': 'snapshots/file1', 'StorageClass': 'STANDARD'},
            {'Key': 'snapshots/file2', 'StorageClass': 'STANDARD'}
        ]
        mock_s3.copy_object.return_value = None

        repo = Repository(
            name='test-repo',
            bucket='test-bucket',
            base_path='snapshots'
        )

        with patch('curator.actions.deepfreeze.utilities.logging'):
            result = push_to_glacier(mock_s3, repo)

        assert result is True
        assert mock_s3.copy_object.call_count == 2
        mock_s3.copy_object.assert_any_call(
            Bucket='test-bucket',
            Key='snapshots/file1',
            CopySource={'Bucket': 'test-bucket', 'Key': 'snapshots/file1'},
            StorageClass='GLACIER'
        )

    def test_push_to_glacier_with_trailing_slash(self):
        """Test push to Glacier with trailing slash in base_path"""
        mock_s3 = Mock()
        mock_s3.list_objects.return_value = [
            {'Key': 'snapshots/file1', 'StorageClass': 'STANDARD'}
        ]

        repo = Repository(
            name='test-repo',
            bucket='test-bucket',
            base_path='snapshots/'  # With trailing slash
        )

        with patch('curator.actions.deepfreeze.utilities.logging'):
            push_to_glacier(mock_s3, repo)

        # Should normalize the path
        mock_s3.list_objects.assert_called_once_with('test-bucket', 'snapshots/')

    def test_push_to_glacier_partial_failure(self):
        """Test push to Glacier with partial failure"""
        mock_s3 = Mock()
        mock_s3.list_objects.return_value = [
            {'Key': 'snapshots/file1', 'StorageClass': 'STANDARD'},
            {'Key': 'snapshots/file2', 'StorageClass': 'STANDARD'}
        ]

        # First call succeeds, second fails
        mock_s3.copy_object.side_effect = [
            None,
            botocore.exceptions.ClientError({'Error': {'Code': 'AccessDenied'}}, 'copy_object')
        ]

        repo = Repository(
            name='test-repo',
            bucket='test-bucket',
            base_path='snapshots'
        )

        with patch('curator.actions.deepfreeze.utilities.logging'):
            result = push_to_glacier(mock_s3, repo)

        assert result is False  # Should return False due to partial failure
        assert mock_s3.copy_object.call_count == 2

    def test_push_to_glacier_list_error(self):
        """Test push to Glacier with list objects error"""
        mock_s3 = Mock()
        mock_s3.list_objects.side_effect = botocore.exceptions.ClientError(
            {'Error': {'Code': 'NoSuchBucket'}}, 'list_objects'
        )

        repo = Repository(
            name='test-repo',
            bucket='test-bucket',
            base_path='snapshots'
        )

        with patch('curator.actions.deepfreeze.utilities.logging'):
            result = push_to_glacier(mock_s3, repo)

        assert result is False


class TestGetAllIndicesInRepo(TestCase):
    """Test get_all_indices_in_repo function"""

    def test_get_all_indices_success(self):
        """Test successful retrieval of all indices"""
        mock_client = Mock()
        mock_client.snapshot.get.return_value = {
            'snapshots': [
                {'indices': ['index1', 'index2']},
                {'indices': ['index2', 'index3']},
                {'indices': ['index4']}
            ]
        }

        result = get_all_indices_in_repo(mock_client, 'test-repo')

        assert sorted(result) == ['index1', 'index2', 'index3', 'index4']
        mock_client.snapshot.get.assert_called_once_with(
            repository='test-repo',
            snapshot='_all'
        )

    def test_get_all_indices_empty_repo(self):
        """Test get_all_indices with empty repository"""
        mock_client = Mock()
        mock_client.snapshot.get.return_value = {'snapshots': []}

        result = get_all_indices_in_repo(mock_client, 'test-repo')

        assert result == []

    def test_get_all_indices_no_indices(self):
        """Test get_all_indices with snapshots but no indices"""
        mock_client = Mock()
        mock_client.snapshot.get.return_value = {
            'snapshots': [
                {'indices': []},
                {'indices': []}
            ]
        }

        result = get_all_indices_in_repo(mock_client, 'test-repo')

        assert result == []


class TestGetTimestampRange(TestCase):
    """Test get_timestamp_range function"""

    def test_get_timestamp_range_success(self):
        """Test successful timestamp range retrieval"""
        mock_client = Mock()
        mock_client.indices.exists.return_value = True
        mock_client.search.return_value = {
            'aggregations': {
                'earliest': {'value_as_string': '2021-01-01T00:00:00.000Z'},
                'latest': {'value_as_string': '2022-01-01T00:00:00.000Z'}
            }
        }

        with patch('curator.actions.deepfreeze.utilities.logging'):
            earliest, latest = get_timestamp_range(mock_client, ['index1', 'index2'])

        assert earliest == datetime(2021, 1, 1, 0, 0, tzinfo=timezone.utc)
        assert latest == datetime(2022, 1, 1, 0, 0, tzinfo=timezone.utc)

    def test_get_timestamp_range_empty_indices(self):
        """Test timestamp range with empty indices list"""
        mock_client = Mock()

        with patch('curator.actions.deepfreeze.utilities.logging'):
            earliest, latest = get_timestamp_range(mock_client, [])

        assert earliest is None
        assert latest is None

    def test_get_timestamp_range_nonexistent_indices(self):
        """Test timestamp range with non-existent indices"""
        mock_client = Mock()
        mock_client.indices.exists.return_value = False
        # Mock search to raise exception when called with empty index
        mock_client.search.side_effect = Exception("No indices to search")

        with patch('curator.actions.deepfreeze.utilities.logging'):
            earliest, latest = get_timestamp_range(mock_client, ['index1', 'index2'])

        # Should return None, None when no valid indices after filtering (exception caught)
        assert earliest is None
        assert latest is None

    def test_get_timestamp_range_mixed_indices(self):
        """Test timestamp range with mix of existing and non-existing indices"""
        mock_client = Mock()
        mock_client.indices.exists.side_effect = [True, False, True]  # index1 exists, index2 doesn't, index3 exists
        mock_client.search.return_value = {
            'aggregations': {
                'earliest': {'value_as_string': '2021-01-01T00:00:00.000Z'},
                'latest': {'value_as_string': '2022-01-01T00:00:00.000Z'}
            }
        }

        with patch('curator.actions.deepfreeze.utilities.logging'):
            earliest, latest = get_timestamp_range(
                mock_client,
                ['index1', 'index2', 'index3']
            )

        # Should only search on existing indices
        mock_client.search.assert_called_once()
        call_args = mock_client.search.call_args
        assert call_args[1]['index'] == 'index1,index3'


class TestGetRepository(TestCase):
    """Test get_repository function"""

    def test_get_repository_found(self):
        """Test get_repository when repository exists"""
        mock_client = Mock()
        mock_response = {
            'hits': {
                'total': {'value': 1},
                'hits': [{
                    '_id': 'repo-id',
                    '_source': {
                        'name': 'test-repo',
                        'bucket': 'test-bucket'
                    }
                }]
            }
        }
        mock_client.search.return_value = mock_response

        with patch('curator.actions.deepfreeze.utilities.logging'):
            result = get_repository(mock_client, 'test-repo')

        assert result.name == 'test-repo'
        assert result.bucket == 'test-bucket'
        assert result.docid == 'repo-id'

    def test_get_repository_not_found(self):
        """Test get_repository when repository doesn't exist"""
        mock_client = Mock()
        mock_response = {
            'hits': {
                'total': {'value': 0},
                'hits': []
            }
        }
        mock_client.search.return_value = mock_response

        with patch('curator.actions.deepfreeze.utilities.logging'):
            result = get_repository(mock_client, 'test-repo')

        assert result.name == 'test-repo'
        assert result.bucket is None


class TestGetAllRepos(TestCase):
    """Test get_all_repos function"""

    def test_get_all_repos_success(self):
        """Test successful retrieval of all repositories"""
        mock_client = Mock()
        mock_client.search.return_value = {
            'hits': {
                'hits': [
                    {
                        '_id': 'id1',
                        '_source': {
                            'name': 'repo1',
                            'bucket': 'bucket1',
                            'doctype': 'repository'
                        }
                    },
                    {
                        '_id': 'id2',
                        '_source': {
                            'name': 'repo2',
                            'bucket': 'bucket2',
                            'doctype': 'repository'
                        }
                    }
                ]
            }
        }

        with patch('curator.actions.deepfreeze.utilities.logging'):
            result = get_all_repos(mock_client)

        assert len(result) == 2
        assert all(isinstance(repo, Repository) for repo in result)
        assert result[0].name == 'repo1'
        assert result[1].name == 'repo2'

    def test_get_all_repos_empty(self):
        """Test get_all_repos when no repositories exist"""
        mock_client = Mock()
        mock_client.search.return_value = {'hits': {'hits': []}}

        with patch('curator.actions.deepfreeze.utilities.logging'):
            result = get_all_repos(mock_client)

        assert result == []


class TestGetSettings(TestCase):
    """Test get_settings function"""

    def test_get_settings_success(self):
        """Test successful retrieval of settings"""
        mock_client = Mock()
        mock_client.indices.exists.return_value = True
        mock_client.get.return_value = {
            '_source': {
                'repo_name_prefix': 'deepfreeze',
                'bucket_name_prefix': 'deepfreeze',
                'storage_class': 'GLACIER',
                'provider': 'aws',
                'doctype': 'settings'  # Include doctype to test filtering
            }
        }

        with patch('curator.actions.deepfreeze.utilities.logging'):
            result = get_settings(mock_client)

        assert isinstance(result, Settings)
        assert result.repo_name_prefix == 'deepfreeze'
        assert result.storage_class == 'GLACIER'

    def test_get_settings_index_missing(self):
        """Test get_settings when status index doesn't exist"""
        mock_client = Mock()
        mock_client.indices.exists.return_value = False

        with patch('curator.actions.deepfreeze.utilities.logging'):
            with pytest.raises(MissingIndexError):
                get_settings(mock_client)

    def test_get_settings_not_found(self):
        """Test get_settings when settings don't exist"""
        mock_client = Mock()
        mock_client.indices.exists.return_value = True
        from elasticsearch8 import NotFoundError
        mock_client.get.side_effect = NotFoundError(404, 'not_found', {})

        with patch('curator.actions.deepfreeze.utilities.logging'):
            result = get_settings(mock_client)

        assert result is None


class TestSaveSettings(TestCase):
    """Test save_settings function"""

    def test_save_settings_new(self):
        """Test saving new settings"""
        mock_client = Mock()
        from elasticsearch8 import NotFoundError
        mock_client.get.side_effect = NotFoundError(404, 'not_found', {})

        settings = Settings(
            repo_name_prefix='test',
            storage_class='GLACIER'
        )

        with patch('curator.actions.deepfreeze.utilities.logging'):
            save_settings(mock_client, settings)

        mock_client.create.assert_called_once()
        call_args = mock_client.create.call_args
        assert call_args[1]['index'] == STATUS_INDEX
        assert call_args[1]['id'] == SETTINGS_ID

    def test_save_settings_update(self):
        """Test updating existing settings"""
        mock_client = Mock()
        mock_client.get.return_value = {'_source': {}}

        settings = Settings(
            repo_name_prefix='test',
            storage_class='GLACIER'
        )

        with patch('curator.actions.deepfreeze.utilities.logging'):
            save_settings(mock_client, settings)

        mock_client.update.assert_called_once()
        call_args = mock_client.update.call_args
        assert call_args[1]['index'] == STATUS_INDEX
        assert call_args[1]['id'] == SETTINGS_ID


class TestGetNextSuffix(TestCase):
    """Test get_next_suffix function"""

    def test_get_next_suffix_oneup(self):
        """Test get_next_suffix with oneup style"""
        assert get_next_suffix('oneup', '000001', None, None) == '000002'
        assert get_next_suffix('oneup', '000009', None, None) == '000010'
        assert get_next_suffix('oneup', '000099', None, None) == '000100'
        assert get_next_suffix('oneup', '999999', None, None) == '1000000'

    def test_get_next_suffix_date(self):
        """Test get_next_suffix with date style"""
        assert get_next_suffix('date', '2024.01', 2024, 3) == '2024.03'

    def test_get_next_suffix_date_current(self):
        """Test get_next_suffix with date style using current date"""
        with patch('curator.actions.deepfreeze.utilities.datetime') as mock_dt:
            mock_dt.now.return_value = datetime(2024, 3, 15)
            assert get_next_suffix('date', '2024.02', None, None) == '2024.03'

    def test_get_next_suffix_invalid_style(self):
        """Test get_next_suffix with invalid style"""
        with pytest.raises(ValueError, match="Invalid style"):
            get_next_suffix('invalid', '000001', None, None)


class TestGetMatchingRepoNames(TestCase):
    """Test get_matching_repo_names function"""

    def test_get_matching_repo_names_success(self):
        """Test successful retrieval of matching repository names"""
        mock_client = Mock()
        mock_client.snapshot.get_repository.return_value = {
            'deepfreeze-001': {},
            'deepfreeze-002': {},
            'other-repo': {},
            'deepfreeze-003': {}
        }

        with patch('curator.actions.deepfreeze.utilities.logging'):
            result = get_matching_repo_names(mock_client, 'deepfreeze-')

        assert sorted(result) == ['deepfreeze-001', 'deepfreeze-002', 'deepfreeze-003']

    def test_get_matching_repo_names_no_matches(self):
        """Test get_matching_repo_names with no matches"""
        mock_client = Mock()
        mock_client.snapshot.get_repository.return_value = {
            'other-repo-1': {},
            'other-repo-2': {}
        }

        with patch('curator.actions.deepfreeze.utilities.logging'):
            result = get_matching_repo_names(mock_client, 'deepfreeze-')

        assert result == []


class TestGetMatchingRepos(TestCase):
    """Test get_matching_repos function"""

    def test_get_matching_repos_success(self):
        """Test successful retrieval of matching repositories"""
        mock_client = Mock()
        mock_client.search.return_value = {
            'hits': {
                'hits': [
                    {
                        '_id': 'id1',
                        '_source': {
                            'name': 'deepfreeze-001',
                            'bucket': 'bucket1',
                            'is_mounted': True
                        }
                    },
                    {
                        '_id': 'id2',
                        '_source': {
                            'name': 'other-repo',
                            'bucket': 'bucket2',
                            'is_mounted': False
                        }
                    },
                    {
                        '_id': 'id3',
                        '_source': {
                            'name': 'deepfreeze-002',
                            'bucket': 'bucket3',
                            'is_mounted': False
                        }
                    }
                ]
            }
        }

        with patch('curator.actions.deepfreeze.utilities.logging'):
            result = get_matching_repos(mock_client, 'deepfreeze-')

        # Should return only deepfreeze repos
        assert len(result) == 2
        repo_names = [repo.name for repo in result]
        assert 'deepfreeze-001' in repo_names
        assert 'deepfreeze-002' in repo_names

    def test_get_matching_repos_mounted_only(self):
        """Test get_matching_repos with mounted filter"""
        mock_client = Mock()
        mock_client.search.return_value = {
            'hits': {
                'hits': [
                    {
                        '_id': 'id1',
                        '_source': {
                            'name': 'deepfreeze-001',
                            'bucket': 'bucket1',
                            'is_mounted': True
                        }
                    },
                    {
                        '_id': 'id2',
                        '_source': {
                            'name': 'deepfreeze-002',
                            'bucket': 'bucket2',
                            'is_mounted': False
                        }
                    }
                ]
            }
        }

        with patch('curator.actions.deepfreeze.utilities.logging'):
            result = get_matching_repos(mock_client, 'deepfreeze-', mounted=True)

        # Should return only mounted repos
        assert len(result) == 1
        assert result[0].name == 'deepfreeze-001'


class TestUnmountRepo(TestCase):
    """Test unmount_repo function"""

    def test_unmount_repo_success(self):
        """Test successful repository unmounting"""
        mock_client = Mock()
        mock_client.snapshot.get_repository.return_value = {
            'test-repo': {
                'settings': {
                    'bucket': 'test-bucket',
                    'base_path': 'test-path'
                }
            }
        }
        mock_client.search.return_value = {
            'hits': {
                'total': {'value': 1},
                'hits': [{
                    '_id': 'repo-id',
                    '_source': {
                        'name': 'test-repo',
                        'bucket': 'test-bucket'
                    }
                }]
            }
        }

        with patch('curator.actions.deepfreeze.utilities.get_all_indices_in_repo', return_value=['index1']):
            with patch('curator.actions.deepfreeze.utilities.get_timestamp_range', return_value=(None, None)):
                with patch('curator.actions.deepfreeze.utilities.decode_date', return_value=datetime.now()):
                    with patch('curator.actions.deepfreeze.utilities.logging'):
                        result = unmount_repo(mock_client, 'test-repo')

        mock_client.snapshot.delete_repository.assert_called_once_with(name='test-repo')
        mock_client.update.assert_called_once()
        assert result.name == 'test-repo'
        assert result.is_mounted is False


class TestDecodeDate(TestCase):
    """Test decode_date function"""

    def test_decode_date_datetime_utc(self):
        """Test decode_date with datetime object in UTC"""
        dt = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        result = decode_date(dt)
        assert result == dt

    def test_decode_date_datetime_naive(self):
        """Test decode_date with naive datetime object"""
        dt = datetime(2024, 1, 1, 12, 0, 0)
        result = decode_date(dt)
        assert result == dt.replace(tzinfo=timezone.utc)

    def test_decode_date_string(self):
        """Test decode_date with ISO string"""
        date_str = "2024-01-01T12:00:00"
        with patch('curator.actions.deepfreeze.utilities.logging'):
            result = decode_date(date_str)

        expected = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        assert result == expected

    def test_decode_date_invalid(self):
        """Test decode_date with invalid input"""
        with pytest.raises(ValueError):
            decode_date(12345)


class TestCreateIlmPolicy(TestCase):
    """Test create_ilm_policy function"""

    def test_create_ilm_policy_success(self):
        """Test successful ILM policy creation"""
        mock_client = Mock()
        policy_body = {'phases': {'hot': {}}}

        with patch('curator.actions.deepfreeze.utilities.logging'):
            create_ilm_policy(mock_client, 'test-policy', policy_body)

        mock_client.ilm.put_lifecycle.assert_called_once_with(
            name='test-policy',
            body=policy_body
        )

    def test_create_ilm_policy_error(self):
        """Test ILM policy creation error"""
        mock_client = Mock()
        mock_client.ilm.put_lifecycle.side_effect = Exception('Policy creation failed')
        policy_body = {'phases': {'hot': {}}}

        with patch('curator.actions.deepfreeze.utilities.logging'):
            with pytest.raises(ActionError):
                create_ilm_policy(mock_client, 'test-policy', policy_body)

class TestUpdateRepositoryDateRange(TestCase):
    """Test update_repository_date_range function"""

    def test_update_date_range_success(self):
        """Test successful date range update"""
        mock_client = Mock()
        # Mock get_all_indices_in_repo
        mock_client.snapshot.get.return_value = {
            'snapshots': [{'indices': ['index1', 'index2']}]
        }
        # Mock index existence checks - simulating partial- prefix
        mock_client.indices.exists.side_effect = [False, True, False, True]
        # Mock status index search for update
        mock_client.search.return_value = {
            'hits': {'total': {'value': 1}, 'hits': [{'_id': 'repo-doc-id'}]}
        }

        repo = Repository(name='test-repo')

        # Mock the get_timestamp_range function directly
        earliest = datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)
        latest = datetime(2024, 12, 31, 23, 59, 59, tzinfo=timezone.utc)

        with patch('curator.actions.deepfreeze.utilities.get_timestamp_range', return_value=(earliest, latest)):
            with patch('curator.actions.deepfreeze.utilities.logging'):
                result = update_repository_date_range(mock_client, repo)

        assert result is True
        assert repo.start is not None
        assert repo.end is not None
        mock_client.update.assert_called_once()

    def test_update_date_range_no_mounted_indices(self):
        """Test update with no mounted indices"""
        mock_client = Mock()
        mock_client.snapshot.get.return_value = {
            'snapshots': [{'indices': ['index1']}]
        }
        # All index existence checks return False
        mock_client.indices.exists.return_value = False

        repo = Repository(name='test-repo')

        with patch('curator.actions.deepfreeze.utilities.logging'):
            result = update_repository_date_range(mock_client, repo)

        assert result is False
        mock_client.update.assert_not_called()

    def test_update_date_range_handles_original_names(self):
        """Test update with indices mounted using original names"""
        mock_client = Mock()
        mock_client.snapshot.get.return_value = {
            'snapshots': [{'indices': ['index1']}]
        }
        # Original name exists
        mock_client.indices.exists.side_effect = [True]
        # Mock status index search for update
        mock_client.search.return_value = {
            'hits': {'total': {'value': 1}, 'hits': [{'_id': 'repo-doc-id'}]}
        }

        repo = Repository(name='test-repo')

        # Mock the get_timestamp_range function directly
        earliest = datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)
        latest = datetime(2024, 12, 31, 23, 59, 59, tzinfo=timezone.utc)

        with patch('curator.actions.deepfreeze.utilities.get_timestamp_range', return_value=(earliest, latest)):
            with patch('curator.actions.deepfreeze.utilities.logging'):
                result = update_repository_date_range(mock_client, repo)

        assert result is True

    def test_update_date_range_handles_restored_prefix(self):
        """Test update with indices using restored- prefix"""
        mock_client = Mock()
        mock_client.snapshot.get.return_value = {
            'snapshots': [{'indices': ['index1']}]
        }
        # Original and partial- don't exist, restored- does
        mock_client.indices.exists.side_effect = [False, False, True]
        # Mock status index search for update
        mock_client.search.return_value = {
            'hits': {'total': {'value': 1}, 'hits': [{'_id': 'repo-doc-id'}]}
        }

        repo = Repository(name='test-repo')

        # Mock the get_timestamp_range function directly
        earliest = datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)
        latest = datetime(2024, 12, 31, 23, 59, 59, tzinfo=timezone.utc)

        with patch('curator.actions.deepfreeze.utilities.get_timestamp_range', return_value=(earliest, latest)):
            with patch('curator.actions.deepfreeze.utilities.logging'):
                result = update_repository_date_range(mock_client, repo)

        assert result is True

    def test_update_date_range_no_timestamp_data(self):
        """Test update when timestamp query returns None"""
        mock_client = Mock()
        mock_client.snapshot.get.return_value = {
            'snapshots': [{'indices': ['index1']}]
        }
        mock_client.indices.exists.return_value = True

        repo = Repository(name='test-repo')

        with patch('curator.actions.deepfreeze.utilities.get_timestamp_range', return_value=(None, None)):
            with patch('curator.actions.deepfreeze.utilities.logging'):
                result = update_repository_date_range(mock_client, repo)

        assert result is False
        mock_client.update.assert_not_called()

    def test_update_date_range_exception_handling(self):
        """Test update handles exceptions gracefully"""
        mock_client = Mock()
        mock_client.snapshot.get.side_effect = Exception("Repository error")

        repo = Repository(name='test-repo')

        with patch('curator.actions.deepfreeze.utilities.logging'):
            result = update_repository_date_range(mock_client, repo)

        assert result is False

    def test_update_date_range_creates_new_document(self):
        """Test update creates document if it doesn't exist"""
        mock_client = Mock()
        mock_client.snapshot.get.return_value = {
            'snapshots': [{'indices': ['index1']}]
        }
        mock_client.indices.exists.return_value = True
        mock_client.search.side_effect = [
            # First search for timestamp data
            {
                'aggregations': {
                    'earliest': {'value_as_string': '2024-01-01T00:00:00.000Z'},
                    'latest': {'value_as_string': '2024-12-31T23:59:59.000Z'}
                }
            },
            # Second search for existing document - returns nothing
            {'hits': {'total': {'value': 0}, 'hits': []}}
        ]

        repo = Repository(name='test-repo')

        with patch('curator.actions.deepfreeze.utilities.logging'):
            result = update_repository_date_range(mock_client, repo)

        assert result is True
        mock_client.index.assert_called_once()


class TestGetIndexTemplates(TestCase):
    """Test get_index_templates function"""

    def test_get_index_templates_success(self):
        """Test successful retrieval of legacy templates"""
        mock_client = Mock()
        mock_client.indices.get_template.return_value = {
            'template1': {'settings': {}},
            'template2': {'settings': {}}
        }

        with patch('curator.actions.deepfreeze.utilities.logging'):
            result = get_index_templates(mock_client)

        assert len(result) == 2
        assert 'template1' in result
        assert 'template2' in result

    def test_get_index_templates_error(self):
        """Test get_index_templates error handling"""
        mock_client = Mock()
        mock_client.indices.get_template.side_effect = Exception("API error")

        with patch('curator.actions.deepfreeze.utilities.logging'):
            with pytest.raises(ActionError):
                get_index_templates(mock_client)


class TestGetComposableTemplates(TestCase):
    """Test get_composable_templates function"""

    def test_get_composable_templates_success(self):
        """Test successful retrieval of composable templates"""
        mock_client = Mock()
        mock_client.indices.get_index_template.return_value = {
            'index_templates': [
                {'name': 'template1'},
                {'name': 'template2'}
            ]
        }

        with patch('curator.actions.deepfreeze.utilities.logging'):
            result = get_composable_templates(mock_client)

        assert 'index_templates' in result
        assert len(result['index_templates']) == 2

    def test_get_composable_templates_error(self):
        """Test get_composable_templates error handling"""
        mock_client = Mock()
        mock_client.indices.get_index_template.side_effect = Exception("API error")

        with patch('curator.actions.deepfreeze.utilities.logging'):
            with pytest.raises(ActionError):
                get_composable_templates(mock_client)


class TestUpdateTemplateIlmPolicy(TestCase):
    """Test update_template_ilm_policy function"""

    def test_update_composable_template_success(self):
        """Test successful update of composable template"""
        mock_client = Mock()
        mock_client.indices.get_index_template.return_value = {
            'index_templates': [{
                'name': 'test-template',
                'index_template': {
                    'template': {
                        'settings': {
                            'index': {
                                'lifecycle': {'name': 'old-policy'}
                            }
                        }
                    }
                }
            }]
        }

        with patch('curator.actions.deepfreeze.utilities.logging'):
            result = update_template_ilm_policy(
                mock_client, 'test-template', 'old-policy', 'new-policy', is_composable=True
            )

        assert result is True
        mock_client.indices.put_index_template.assert_called_once()

    def test_update_legacy_template_success(self):
        """Test successful update of legacy template"""
        mock_client = Mock()
        mock_client.indices.get_template.return_value = {
            'test-template': {
                'settings': {
                    'index': {
                        'lifecycle': {'name': 'old-policy'}
                    }
                }
            }
        }

        with patch('curator.actions.deepfreeze.utilities.logging'):
            result = update_template_ilm_policy(
                mock_client, 'test-template', 'old-policy', 'new-policy', is_composable=False
            )

        assert result is True
        mock_client.indices.put_template.assert_called_once()

    def test_update_template_no_match(self):
        """Test template update when policy doesn't match"""
        mock_client = Mock()
        mock_client.indices.get_index_template.return_value = {
            'index_templates': [{
                'name': 'test-template',
                'index_template': {
                    'template': {
                        'settings': {
                            'index': {
                                'lifecycle': {'name': 'different-policy'}
                            }
                        }
                    }
                }
            }]
        }

        with patch('curator.actions.deepfreeze.utilities.logging'):
            result = update_template_ilm_policy(
                mock_client, 'test-template', 'old-policy', 'new-policy', is_composable=True
            )

        assert result is False
        mock_client.indices.put_index_template.assert_not_called()


class TestCreateVersionedIlmPolicy(TestCase):
    """Test create_versioned_ilm_policy function"""

    def test_create_versioned_policy_success(self):
        """Test successful creation of versioned policy"""
        mock_client = Mock()
        policy_body = {
            'phases': {
                'cold': {
                    'actions': {
                        'searchable_snapshot': {
                            'snapshot_repository': 'old-repo'
                        }
                    }
                }
            }
        }

        with patch('curator.actions.deepfreeze.utilities.logging'):
            result = create_versioned_ilm_policy(
                mock_client, 'my-policy', policy_body, 'new-repo', '000005'
            )

        assert result == 'my-policy-000005'
        mock_client.ilm.put_lifecycle.assert_called_once()
        call_args = mock_client.ilm.put_lifecycle.call_args
        assert call_args[1]['name'] == 'my-policy-000005'
        # Verify repo was updated in policy
        policy_arg = call_args[1]['policy']
        assert policy_arg['phases']['cold']['actions']['searchable_snapshot']['snapshot_repository'] == 'new-repo'

    def test_create_versioned_policy_multiple_phases(self):
        """Test versioned policy with multiple phases"""
        mock_client = Mock()
        policy_body = {
            'phases': {
                'cold': {
                    'actions': {
                        'searchable_snapshot': {
                            'snapshot_repository': 'old-repo'
                        }
                    }
                },
                'frozen': {
                    'actions': {
                        'searchable_snapshot': {
                            'snapshot_repository': 'old-repo'
                        }
                    }
                }
            }
        }

        with patch('curator.actions.deepfreeze.utilities.logging'):
            result = create_versioned_ilm_policy(
                mock_client, 'my-policy', policy_body, 'new-repo', '000005'
            )

        # Verify all phases were updated
        call_args = mock_client.ilm.put_lifecycle.call_args
        policy_arg = call_args[1]['policy']
        assert policy_arg['phases']['cold']['actions']['searchable_snapshot']['snapshot_repository'] == 'new-repo'
        assert policy_arg['phases']['frozen']['actions']['searchable_snapshot']['snapshot_repository'] == 'new-repo'

    def test_create_versioned_policy_error(self):
        """Test versioned policy creation error"""
        mock_client = Mock()
        mock_client.ilm.put_lifecycle.side_effect = Exception("Policy creation failed")
        policy_body = {'phases': {}}

        with patch('curator.actions.deepfreeze.utilities.logging'):
            with pytest.raises(ActionError):
                create_versioned_ilm_policy(
                    mock_client, 'my-policy', policy_body, 'new-repo', '000005'
                )


class TestGetPoliciesForRepo(TestCase):
    """Test get_policies_for_repo function"""

    def test_get_policies_for_repo_success(self):
        """Test successful retrieval of policies for repository"""
        mock_client = Mock()
        mock_client.ilm.get_lifecycle.return_value = {
            'policy1': {
                'policy': {
                    'phases': {
                        'cold': {
                            'actions': {
                                'searchable_snapshot': {
                                    'snapshot_repository': 'target-repo'
                                }
                            }
                        }
                    }
                }
            },
            'policy2': {
                'policy': {
                    'phases': {
                        'frozen': {
                            'actions': {
                                'searchable_snapshot': {
                                    'snapshot_repository': 'other-repo'
                                }
                            }
                        }
                    }
                }
            },
            'policy3': {
                'policy': {
                    'phases': {
                        'cold': {
                            'actions': {
                                'searchable_snapshot': {
                                    'snapshot_repository': 'target-repo'
                                }
                            }
                        }
                    }
                }
            }
        }

        with patch('curator.actions.deepfreeze.utilities.logging'):
            result = get_policies_for_repo(mock_client, 'target-repo')

        assert len(result) == 2
        assert 'policy1' in result
        assert 'policy3' in result
        assert 'policy2' not in result

    def test_get_policies_for_repo_no_matches(self):
        """Test get_policies_for_repo with no matches"""
        mock_client = Mock()
        mock_client.ilm.get_lifecycle.return_value = {
            'policy1': {
                'policy': {
                    'phases': {
                        'cold': {
                            'actions': {}
                        }
                    }
                }
            }
        }

        with patch('curator.actions.deepfreeze.utilities.logging'):
            result = get_policies_for_repo(mock_client, 'target-repo')

        assert len(result) == 0


class TestGetPoliciesBySuffix(TestCase):
    """Test get_policies_by_suffix function"""

    def test_get_policies_by_suffix_success(self):
        """Test successful retrieval of policies by suffix"""
        mock_client = Mock()
        mock_client.ilm.get_lifecycle.return_value = {
            'my-policy-000003': {'policy': {}},
            'other-policy-000003': {'policy': {}},
            'different-policy-000004': {'policy': {}},
            'my-policy': {'policy': {}}
        }

        with patch('curator.actions.deepfreeze.utilities.logging'):
            result = get_policies_by_suffix(mock_client, '000003')

        assert len(result) == 2
        assert 'my-policy-000003' in result
        assert 'other-policy-000003' in result
        assert 'different-policy-000004' not in result
        assert 'my-policy' not in result

    def test_get_policies_by_suffix_no_matches(self):
        """Test get_policies_by_suffix with no matches"""
        mock_client = Mock()
        mock_client.ilm.get_lifecycle.return_value = {
            'policy1': {'policy': {}},
            'policy2': {'policy': {}}
        }

        with patch('curator.actions.deepfreeze.utilities.logging'):
            result = get_policies_by_suffix(mock_client, '000003')

        assert len(result) == 0


class TestIsPolicySafeToDelete(TestCase):
    """Test is_policy_safe_to_delete function"""

    def test_policy_safe_to_delete(self):
        """Test policy that is safe to delete"""
        mock_client = Mock()
        mock_client.ilm.get_lifecycle.return_value = {
            'test-policy': {
                'policy': {},
                'in_use_by': {
                    'indices': [],
                    'data_streams': [],
                    'composable_templates': []
                }
            }
        }

        with patch('curator.actions.deepfreeze.utilities.logging'):
            result = is_policy_safe_to_delete(mock_client, 'test-policy')

        assert result is True

    def test_policy_in_use_by_indices(self):
        """Test policy that is in use by indices"""
        mock_client = Mock()
        mock_client.ilm.get_lifecycle.return_value = {
            'test-policy': {
                'policy': {},
                'in_use_by': {
                    'indices': ['index1', 'index2'],
                    'data_streams': [],
                    'composable_templates': []
                }
            }
        }

        with patch('curator.actions.deepfreeze.utilities.logging'):
            result = is_policy_safe_to_delete(mock_client, 'test-policy')

        assert result is False

    def test_policy_in_use_by_data_streams(self):
        """Test policy that is in use by data streams"""
        mock_client = Mock()
        mock_client.ilm.get_lifecycle.return_value = {
            'test-policy': {
                'policy': {},
                'in_use_by': {
                    'indices': [],
                    'data_streams': ['logs-stream'],
                    'composable_templates': []
                }
            }
        }

        with patch('curator.actions.deepfreeze.utilities.logging'):
            result = is_policy_safe_to_delete(mock_client, 'test-policy')

        assert result is False

    def test_policy_in_use_by_templates(self):
        """Test policy that is in use by templates"""
        mock_client = Mock()
        mock_client.ilm.get_lifecycle.return_value = {
            'test-policy': {
                'policy': {},
                'in_use_by': {
                    'indices': [],
                    'data_streams': [],
                    'composable_templates': ['template1']
                }
            }
        }

        with patch('curator.actions.deepfreeze.utilities.logging'):
            result = is_policy_safe_to_delete(mock_client, 'test-policy')

        assert result is False

    def test_policy_not_found(self):
        """Test policy that doesn't exist"""
        mock_client = Mock()
        mock_client.ilm.get_lifecycle.return_value = {}

        with patch('curator.actions.deepfreeze.utilities.logging'):
            result = is_policy_safe_to_delete(mock_client, 'test-policy')

        assert result is False

    def test_policy_not_found_exception(self):
        """Test policy check with NotFoundError"""
        mock_client = Mock()
        from elasticsearch8 import NotFoundError
        mock_client.ilm.get_lifecycle.side_effect = NotFoundError(404, 'not_found', {})

        with patch('curator.actions.deepfreeze.utilities.logging'):
            result = is_policy_safe_to_delete(mock_client, 'test-policy')

        assert result is False
