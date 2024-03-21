"""Unit tests for utils"""
from unittest import TestCase
import pytest
from unittest.mock import Mock
from elastic_transport import ApiResponseMeta
from elasticsearch8 import Elasticsearch
from elasticsearch8.exceptions import AuthenticationException, NotFoundError
from curator.exceptions import (
     ConfigurationError, FailedExecution, MissingArgument, RepositoryException,
     SearchableSnapshotException)
from curator.helpers.testers import (
    has_lifecycle_name, is_idx_partial, repository_exists, rollable_alias, snapshot_running,
    validate_filters, verify_client_object, verify_repository)

FAKE_FAIL = Exception('Simulated Failure')

class TestRepositoryExists(TestCase):
    """TestRepositoryExists

    Test helpers.testers.repository_exists functionality.
    """
    def test_missing_arg(self):
        """test_missing_arg

        Should raise an exception if the repository isn't passed as an arg
        """
        client = Mock()
        with pytest.raises(MissingArgument, match=r'No value for "repository" provided'):
            repository_exists(client)
    def test_repository_in_results(self):
        """test_repository_in_results

        Should return ``True`` if the passed repository exists
        """
        client = Mock()
        client.snapshot.get_repository.return_value = {'repo':{'foo':'bar'}}
        assert repository_exists(client, repository="repo")
    def test_repo_not_in_results(self):
        """test_repo_not_in_results

        Should return ``False`` if the passed repository does not exist
        """
        client = Mock()
        client.snapshot.get_repository.return_value = {'not_your_repo':{'foo':'bar'}}
        assert not repository_exists(client, repository="repo")

class TestRollableAlias(TestCase):
    """TestRollableAlias

    Test helpers.testers.rollable_alias functionality.
    """
    def test_return_false_if_no_alias(self):
        """test_return_false_if_no_alias

        Should return ``False`` with a simulated Exception being raised
        """
        err = 'simulated error'
        client = Mock()
        client.info.return_value = {'version': {'number': '8.3.3'} }
        client.indices.get_alias.return_value = {}
        client.indices.get_alias.side_effect = NotFoundError(404, err, err)
        assert not rollable_alias(client, 'foo')
    def test_return_false_too_many_indices(self):
        """test_return_false_too_many_indices

        Should return ``False`` if alias associated with too many indices
        """
        retval = {'index-a': {'aliases': {'foo': {}}}, 'index-b': {'aliases': {'foo': {}}}}
        client = Mock()
        client.info.return_value = {'version': {'number': '8.3.3'} }
        client.indices.get_alias.return_value = retval
        assert not rollable_alias(client, 'foo')
    def test_return_false_non_numeric(self):
        """test_return_false_non_numeric

        Should return ``False`` if index name doesn't end in rollable digits
        """
        retval = {'index-a': {'aliases': {'foo': {}}}}
        client = Mock()
        client.info.return_value = {'version': {'number': '8.3.3'} }
        client.indices.get_alias.return_value = retval
        assert not rollable_alias(client, 'foo')
    def test_return_true_two_digits(self):
        """test_return_true_two_digits

        Should return ``True`` if the index ends in rollable digits
        """
        retval = {'index-00001': {'aliases': {'foo': {}}}}
        client = Mock()
        client.info.return_value = {'version': {'number': '8.3.3'} }
        client.indices.get_alias.return_value = retval
        assert rollable_alias(client, 'foo')
    def test_return_true_hyphenated(self):
        """test_return_true_hyphenated

        Should return ``True`` if the index has a rollable, hyphenated name
        """
        retval = {'index-2017.03.07-1': {'aliases': {'foo': {}}}}
        client = Mock()
        client.info.return_value = {'version': {'number': '8.3.3'} }
        client.indices.get_alias.return_value = retval
        assert rollable_alias(client, 'foo')

class TestSnapshotRunning(TestCase):
    """TestSnapshotRunning

    Test helpers.testers.snapshot_running functionality
    """
    # :pylint disable=line-too-long
    def test_true(self):
        """test_true

        Should return ``True`` when a snapshot is in progress/running.
        """
        client = Mock()
        client.snapshot.status.return_value = {'snapshots': ['running']}
        # self.assertTrue(snapshot_running(client))
        assert snapshot_running(client)
    def test_false(self):
        """test_False

        Should return ``False`` when a snapshot is not in progress/running.
        """
        client = Mock()
        client.snapshot.status.return_value = {'snapshots': []}
        # self.assertFalse(snapshot_running(client))
        assert not snapshot_running(client)
    def test_raises_exception(self):
        """test_raises_exception

        Should raise a ``FailedExecution`` exception when an exception happens upstream
        """
        client = Mock()
        client.snapshot.status.return_value = {'snapshots': []}
        client.snapshot.status.side_effect = FAKE_FAIL
        # self.assertRaises(FailedExecution, snapshot_running, client)
        with pytest.raises(FailedExecution, match=r'Rerun with loglevel DEBUG'):
            snapshot_running(client)

class TestValidateFilters(TestCase):
    """TestValidateFilters

    Test helpers.testers.validate_filters functionality.
    """
    def test_snapshot_with_index_filter(self):
        """test_snapshot_with_index_filter

        Should raise ConfigurationError with improper filter for filtertype
        In this case, an index filtertype (``kibana``) for the ``delete_snapshots`` filter
        """
        with pytest.raises(ConfigurationError, match=r'filtertype is not compatible with action'):
            validate_filters('delete_snapshots', [{'filtertype': 'kibana'}])
    def test_index_with_snapshot_filter(self):
        """test_index_with_snapshot_filter

        Should raise ConfigurationError with improper filter for filtertype
        In this case, a snapshot filtertype (``state``) for the ``delete_indices`` filter
        """
        with pytest.raises(ConfigurationError, match=r'filtertype is not compatible with action'):
            validate_filters('delete_indices', [{'filtertype': 'state', 'state': 'SUCCESS'}])

class TestVerifyClientObject(TestCase):
    """TestVerifyClientObject

    Test helpers.testers.verify_client_object functionality.
    """
    def test_is_client_object(self):
        """test_is_client_object

        Should return a ``None`` value for a valid client object.
        """
        test = Elasticsearch(hosts=["http://127.0.0.1:9200"])
        assert None is verify_client_object(test)

    def test_is_not_client_object(self):
        """test_is_not_client_object

        Should raise a ``TypeError`` exception with an invalid client object.
        """
        test = 'not a client object'
        with pytest.raises(TypeError, match=r'Not a valid client object'):
            verify_client_object(test)

class TestVerifyRepository(TestCase):
    """TestVerifyRepository

    Test helpers.testers.verify_repository functionality
    """
    VERIFIED_NODES = {'nodes': {'nodeid1': {'name': 'node1'}, 'nodeid2': {'name': 'node2'}}}
    REPO_NAME = 'repo_name'
    def test_passing(self):
        """test_passing

        Should return ``None`` and raise no Exception on success
        """
        client = Mock()
        client.snapshot.verify_repository.return_value = self.VERIFIED_NODES
        assert None is verify_repository(client, repository=self.REPO_NAME)
    def test_raises_404(self):
        """test_raises_404

        Should raise ``RepositoryException`` when a 404 ``TransportError`` raises first
        """
        client = Mock()
        client.snapshot.verify_repository.return_value = self.VERIFIED_NODES
        # 5 positional args for meta: status, http_version, headers, duration, node
        meta = ApiResponseMeta(404, '1.1', {}, 0.01, None)
        body = f'[{self.REPO_NAME}] missing'
        msg = 'repository_missing_exception'
        # 3 positional args for NotFoundError: message, meta, body
        effect = NotFoundError(msg, meta, body)
        client.snapshot.verify_repository.side_effect = effect
        with pytest.raises(RepositoryException, match=r'Repository "repo_name" not found'):
            verify_repository(client, repository=self.REPO_NAME)
    def test_raises_401(self):
        """test_raises_401

        Should raise ``RepositoryException`` when a 401 AuthenticationException raises first
        """
        client = Mock()
        client.snapshot.verify_repository.return_value = self.VERIFIED_NODES
        # 5 positional args for meta: status, http_version, headers, duration, node
        meta = ApiResponseMeta(401, '1.1', {}, 0.01, None)
        body = 'No authentication'
        msg = 'authentication error'
        # 3 positional args for NotFoundError: message, meta, body
        effect = AuthenticationException(msg, meta, body)
        client.snapshot.verify_repository.side_effect = effect
        with pytest.raises(RepositoryException, match=r'Got a 401 response from Elasticsearch'):
            verify_repository(client, repository=self.REPO_NAME)
    def test_raises_other(self):
        """test_raises_other

        Should raise ``RepositoryException`` when any other Exception raises first
        """
        client = Mock()
        client.snapshot.verify_repository.return_value = self.VERIFIED_NODES
        client.snapshot.verify_repository.side_effect = FAKE_FAIL
        with pytest.raises(RepositoryException, match=r'Failed to verify'):
            verify_repository(client, repository=self.REPO_NAME)

class TestHasLifecycleName(TestCase):
    """TestHasLifecycleName

    Test helpers.testers.has_lifecycle_name functionality
    """
    def test_has_lifecycle_name(self):
        """test_has_lifecycle_name"""
        testval = {'lifecycle': {'name': 'ilm_policy'}}
        assert has_lifecycle_name(testval)
    def test_has_no_lifecycle_name(self):
        """test_has_no_lifecycle_name"""
        testval = {'lifecycle': {'nothere': 'nope'}}
        assert not has_lifecycle_name(testval)

class TestIsIdxPartial(TestCase):
    """TestIsIdxPartial

    Test helpers.testers.is_idx_partial functionality
    """
    def test_is_idx_partial(self):
        """test_is_idx_partial"""
        testval = {'store': {'snapshot': {'partial': True}}}
        assert is_idx_partial(testval)
    def test_is_idx_partial_false1(self):
        """test_is_idx_partial_false1"""
        testval = {'store': {'snapshot': {'partial': False}}}
        assert not is_idx_partial(testval)
    def test_is_idx_partial_false2(self):
        """test_is_idx_partial_false2"""
        testval = {'store': {'snapshot': {'nothere': 'nope'}}}
        assert not is_idx_partial(testval)
    def test_is_idx_partial_raises1(self):
        """test_is_idx_partial_raises1"""
        testval = {'store': {'nothere': 'nope'}}
        with pytest.raises(SearchableSnapshotException, match='not a mounted searchable snapshot'):
            is_idx_partial(testval)
    def test_is_idx_partial_raises2(self):
        """test_is_idx_partial_raises2"""
        testval = {'nothere': 'nope'}
        with pytest.raises(SearchableSnapshotException, match='not a mounted searchable snapshot'):
            is_idx_partial(testval)
