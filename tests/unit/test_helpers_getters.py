"""Unit testing for helpers.creators functions"""
from unittest import TestCase
from mock import Mock
import pytest
from elastic_transport import ApiResponseMeta
from elasticsearch8 import NotFoundError, TransportError
from curator.exceptions import CuratorException, FailedExecution, MissingArgument
from curator.helpers.getters import (
    byte_size, get_alias_actions, get_frozen_prefix, get_indices, get_repository, get_snapshot,
    get_snapshot_data, get_tier_preference, name_to_node_id, node_id_to_name, node_roles,
    single_data_path
)

FAKE_FAIL = Exception('Simulated Failure')
NAMED_INDICES = [ "index-2015.01.01", "index-2015.02.01" ]
REPO_NAME = 'repo_name'
TEST_REPO = {REPO_NAME: {}}
SNAP_NAME = 'snap_name'
SINGLE = {'snapshot': SNAP_NAME, 'indices': NAMED_INDICES }
SNAPSHOT = {'snapshots': [SINGLE]}
SNAPSHOTS = {'snapshots': [SINGLE, {'snapshot': 'snapshot-2015.03.01', 'indices': NAMED_INDICES}]}

class TestByteSize(TestCase):
    """TestByteSize

    Test helpers.getters.byte_size functionality.
    """
    def test_byte_size(self):
        """test_byte_size

        Output should match expected
        """
        size = 3*1024*1024*1024*1024*1024*1024*1024
        unit = ['Z','E','P','T','G','M','K','']
        for i in range(0,7):
            assert f'3.0{unit[i]}B' == byte_size(size)
            size /= 1024
    def test_byte_size_yotta(self):
        """test_byte_size_yotta

        Output should match expected
        """
        size = 3*1024*1024*1024*1024*1024*1024*1024*1024
        assert '3.0YB' == byte_size(size)
    def test_raise_invalid(self):
        """test_raise_invalid

        Should raise a TypeError exception if an invalid value is passed
        """
        with pytest.raises(TypeError):
            byte_size('invalid')

class TestGetIndices(TestCase):
    """TestGetIndices

    Test helpers.getters.get_indices functionality.
    """
    IDX1 = 'index-2016.03.03'
    IDX2 = 'index-2016.03.04'
    SETTINGS = {
        IDX1: {'state': 'open'},
        IDX2: {'state': 'open'}
    }
    def test_client_exception(self):
        """test_client_exception

        Should raise a FailedExecution exception when an upstream exception occurs
        """
        client = Mock()
        client.indices.get_settings.return_value = self.SETTINGS
        client.indices.get_settings.side_effect = FAKE_FAIL
        with pytest.raises(FailedExecution):
            get_indices(client)
    def test_positive(self):
        """test_positive

        Output should match expected
        """
        client = Mock()
        client.indices.get_settings.return_value = self.SETTINGS
        self.assertEqual([self.IDX1, self.IDX2], sorted(get_indices(client)))
    def test_empty(self):
        """test_empty

        Output should be an empty list
        """
        client = Mock()
        client.indices.get_settings.return_value = {}
        self.assertEqual([], get_indices(client))

class TestGetRepository(TestCase):
    """TestGetRepository

    Test helpers.getters.get_repository functionality.
    """
    MULTI = {'other': {}, REPO_NAME: {}}
    def test_get_repository_missing_arg(self):
        """test_get_repository_missing_arg

        Should return an empty response if no repository name provided
        """
        client = Mock()
        client.snapshot.get_repository.return_value = {}
        assert not get_repository(client)
    def test_get_repository_positive(self):
        """test_get_repository_positive

        Return value should match expected
        """
        client = Mock()
        client.snapshot.get_repository.return_value = TEST_REPO
        assert TEST_REPO == get_repository(client, repository=REPO_NAME)
    def test_get_repository_transporterror_negative(self):
        """test_get_repository_transporterror_negative

        Should raise a CuratorException if a TransportError is raised first
        """
        client = Mock()
        client.snapshot.get_repository.side_effect = TransportError(503, ('exception', 'reason'))
        with pytest.raises(CuratorException, match=r'503 Check Elasticsearch logs'):
            get_repository(client, repository=REPO_NAME)
    def test_get_repository_notfounderror_negative(self):
        """test_get_repository_notfounderror_negative

        Should raise a CuratorException if a NotFoundError is raised first
        """
        client = Mock()
        # 5 positional args for meta: status, http_version, headers, duration, node
        meta = ApiResponseMeta(404, '1.1', {}, 0.01, None)
        body = 'simulated error'
        msg = 'simulated error'
        # 3 positional args for NotFoundError: message, meta, body
        effect = NotFoundError(msg, meta, body)
        client.snapshot.get_repository.side_effect = effect
        with pytest.raises(CuratorException, match=r'Error: NotFoundError'):
            get_repository(client, repository=REPO_NAME)
    def test_get_repository_all_positive(self):
        """test_get_repository_all_positive

        Return value should match expected with multiple repositories
        """
        client = Mock()
        client.snapshot.get_repository.return_value = self.MULTI
        assert self.MULTI == get_repository(client)

class TestGetSnapshot(TestCase):
    """TestGetSnapshot

    Test helpers.getters.get_snapshot functionality.
    """
    def test_get_snapshot_missing_repository_arg(self):
        """test_get_snapshot_missing_repository_arg

        Should raise a MissingArgument exception when repository not passed
        """
        client = Mock()
        with pytest.raises(MissingArgument, match=r'No value for "repository" provided'):
            get_snapshot(client, snapshot=SNAP_NAME)
    def test_get_snapshot_positive(self):
        """test_get_snapshot_positive

        Output should match expected
        """
        client = Mock()
        client.snapshot.get.return_value = SNAPSHOT
        assert SNAPSHOT == get_snapshot(client, repository=REPO_NAME, snapshot=SNAP_NAME)
    def test_get_snapshot_transporterror_negative(self):
        """test_get_snapshot_transporterror_negative

        Should raise a FailedExecution exception if a TransportError is raised first
        """
        client = Mock()
        client.snapshot.get_repository.return_value = TEST_REPO
        client.snapshot.get.side_effect = TransportError(401, "simulated error")
        with pytest.raises(FailedExecution, match=r'Error: 401'):
            get_snapshot(client, repository=REPO_NAME, snapshot=SNAP_NAME)
    def test_get_snapshot_notfounderror_negative(self):
        """test_get_snapshot_notfounderror_negative

        Should raise a FailedExecution exception if a NotFoundError is raised first
        """
        client = Mock()
        client.snapshot.get_repository.return_value = TEST_REPO
        # 5 positional args for meta: status, http_version, headers, duration, node
        meta = ApiResponseMeta(404, '1.1', {}, 1.0, None)
        client.snapshot.get.side_effect = NotFoundError('simulated error', meta, 'simulated error')
        with pytest.raises(FailedExecution, match=r'Error: NotFoundError'):
            get_snapshot(client, repository=REPO_NAME, snapshot=SNAP_NAME)

class TestGetSnapshotData(TestCase):
    """TestGetSnapshotData

    Test helpers.getters.get_snapshot_data functionality.
    """
    def test_missing_repo_arg(self):
        """test_missing_repo_arg

        Should raise a MissingArgument exception if the repository arg is missing
        """
        client = Mock()
        with pytest.raises(MissingArgument, match=r'No value for "repository" provided'):
            get_snapshot_data(client)
    def test_return_data(self):
        """test_return_data

        Output should match expected
        """
        client = Mock()
        client.snapshot.get.return_value = SNAPSHOTS
        client.snapshot.get_repository.return_value = TEST_REPO
        assert SNAPSHOTS['snapshots'] == get_snapshot_data(client, repository=REPO_NAME)
    def test_raises_exception_onfail(self):
        """test_raises_exception_onfail

        Should raise a FailedExecution exception if a TransportError is raised upstream first
        """
        client = Mock()
        client.snapshot.get.return_value = SNAPSHOTS
        client.snapshot.get.side_effect = TransportError(401, "simulated error")
        client.snapshot.get_repository.return_value = TEST_REPO
        with pytest.raises(FailedExecution, match=r'Error: 401'):
            get_snapshot_data(client, repository=REPO_NAME)

class TestNodeRoles(TestCase):
    """TestNodeRoles

    Test helpers.getters.node_roles functionality.
    """
    def test_node_roles(self):
        """test_node_roles

        Output should match expected
        """
        node_id = 'my_node'
        expected = ['data']
        client = Mock()
        client.nodes.info.return_value = {'nodes':{node_id:{'roles': expected}}}
        assert expected == node_roles(client, node_id)

class TestSingleDataPath(TestCase):
    """TestSingleDataPath

    Test helpers.getters.single_data_path functionality.
    """
    def test_single_data_path(self):
        """test_single_data_path

        Return value should be True with only one data path
        """
        node_id = 'my_node'
        client = Mock()
        client.nodes.stats.return_value = {'nodes':{node_id:{'fs':{'data':['one']}}}}
        assert single_data_path(client, node_id)
    def test_two_data_paths(self):
        """test_two_data_paths

        Return value should be False with two data paths
        """
        node_id = 'my_node'
        client = Mock()
        client.nodes.stats.return_value = {'nodes':{node_id:{'fs':{'data':['one','two']}}}}
        assert not single_data_path(client, node_id)

class TestNameToNodeId(TestCase):
    """TestNameToNodeId

    Test helpers.getters.name_to_node_id functionality.
    """
    def test_positive(self):
        """test_positive

        Output should match expected
        """
        node_id = 'node_id'
        node_name = 'node_name'
        client = Mock()
        client.nodes.stats.return_value = {'nodes':{node_id:{'name':node_name}}}
        assert node_id == name_to_node_id(client, node_name)
    def test_negative(self):
        """test_negative

        Output should be None due to mismatch
        """
        node_id = 'node_id'
        node_name = 'node_name'
        client = Mock()
        client.nodes.stats.return_value = {'nodes':{node_id:{'name':node_name}}}
        assert None is name_to_node_id(client, 'wrong_name')

class TestNodeIdToName(TestCase):
    """TestNodeIdToName

    Test helpers.getters.node_id_to_name functionality.
    """
    def test_negative(self):
        """test_negative

        Output should be None due to mismatch
        """
        client = Mock()
        client.nodes.stats.return_value = {'nodes':{'my_node_id':{'name':'my_node_name'}}}
        assert None is node_id_to_name(client, 'not_my_node_id')

class TestGetAliasActions(TestCase):
    """TestGetAliasActions

    Test helpers.getters.get_alias_actions functionality.
    """
    def test_get_alias_actions(self):
        """test_get_alias_actions"""
        name = 'alias1'
        aliases = {name: {}}
        oldidx = 'old'
        newidx = 'new'
        expected = [
            {
                'remove': {'index': oldidx, 'alias': name}
            },
            {
                'add': {'index': newidx, 'alias': name}
            }
        ]
        assert get_alias_actions(oldidx, newidx, aliases) == expected

class TestGetFrozenPrefix(TestCase):
    """TestGetFrozenPrefix

    Test helpers.getters.get_frozen_prefix functionality.
    """
    def test_get_frozen_prefix1(self):
        """test_get_frozen_prefix1"""
        oldidx = 'test-000001'
        curridx = 'restored-test-000001'
        assert get_frozen_prefix(oldidx, curridx) == 'partial-restored-'
    def test_get_frozen_prefix2(self):
        """test_get_frozen_prefix2"""
        oldidx = 'test-000001'
        curridx = 'test-000001'
        assert get_frozen_prefix(oldidx, curridx) == 'partial-'

class TestGetTierPreference(TestCase):
    """TestGetTierPreference

    Test helpers.getters.get_tier_preference functionality.
    """
    def test_get_tier_preference1(self):
        """test_get_tier_preference1"""
        client = Mock()
        roles = ['data_cold', 'data_frozen', 'data_hot', 'data_warm']
        client.nodes.info.return_value = {'nodes': {'nodename': {'roles': roles}}}
        assert get_tier_preference(client) == 'data_frozen'
    def test_get_tier_preference2(self):
        """test_get_tier_preference2"""
        client = Mock()
        roles = ['data_cold', 'data_hot', 'data_warm']
        client.nodes.info.return_value = {'nodes': {'nodename': {'roles': roles}}}
        assert get_tier_preference(client) == 'data_cold,data_warm,data_hot'
    def test_get_tier_preference3(self):
        """test_get_tier_preference3"""
        client = Mock()
        roles = ['data_content']
        client.nodes.info.return_value = {'nodes': {'nodename': {'roles': roles}}}
        assert get_tier_preference(client) == 'data_content'
    def test_get_tier_preference4(self):
        """test_get_tier_preference4"""
        client = Mock()
        roles = ['data_cold', 'data_frozen', 'data_hot', 'data_warm']
        client.nodes.info.return_value = {'nodes': {'nodename': {'roles': roles}}}
        assert get_tier_preference(client, target_tier='data_cold') == 'data_cold,data_warm,data_hot'
    def test_get_tier_preference5(self):
        """test_get_tier_preference5"""
        client = Mock()
        roles = ['data_content']
        client.nodes.info.return_value = {'nodes': {'nodename': {'roles': roles}}}
        assert get_tier_preference(client, target_tier='data_hot') == 'data_content'