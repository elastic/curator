"""test_action_cold2frozen"""
from unittest import TestCase
from mock import Mock
import pytest
from curator.actions import Cold2Frozen
from curator.exceptions import CuratorException, SearchableSnapshotException
from curator import IndexList
# Get test variables and constants from a single source
from . import testvars

class TestActionCold2Frozen(TestCase):
    """TestActionCold2Frozen"""
    def test_init_raise_bad_index_list(self):
        """test_init_raise_bad_index_list"""
        self.assertRaises(TypeError, Cold2Frozen, 'invalid')
        with pytest.raises(TypeError):
            Cold2Frozen('not_an_IndexList')
    def test_init_add_kwargs(self):
        """test_init_add_kwargs"""
        client = Mock()
        testval = {'key': 'value'}
        client.info.return_value = {'version': {'number': '8.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        ilo = IndexList(client)
        c2f = Cold2Frozen(ilo, index_settings=testval)
        assert c2f.index_settings == testval
    def test_get_alias_actions(self):
        """test_get_alias_actions"""
        client = Mock()
        client.info.return_value = {'version': {'number': '8.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        ilo = IndexList(client)
        c2f = Cold2Frozen(ilo)
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
        assert c2f.get_alias_actions(oldidx, newidx, aliases) == expected
    def test_get_frozen_prefix1(self):
        """test_get_frozen_prefix1"""
        client = Mock()
        client.info.return_value = {'version': {'number': '8.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        ilo = IndexList(client)
        c2f = Cold2Frozen(ilo)
        oldidx = 'test-000001'
        curridx = 'restored-test-000001'
        assert c2f.get_frozen_prefix(oldidx, curridx) == 'partial-restored-'
    def test_get_frozen_prefix2(self):
        """test_get_frozen_prefix2"""
        client = Mock()
        client.info.return_value = {'version': {'number': '8.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        ilo = IndexList(client)
        c2f = Cold2Frozen(ilo)
        oldidx = 'test-000001'
        curridx = 'test-000001'
        assert c2f.get_frozen_prefix(oldidx, curridx) == 'partial-'
    def test_get_tier_preference1(self):
        """test_get_tier_preference1"""
        client = Mock()
        client.info.return_value = {'version': {'number': '8.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        roles = ['data_cold', 'data_frozen', 'data_hot', 'data_warm']
        client.nodes.info.return_value = {'nodes': {'nodename': {'roles': roles}}}
        ilo = IndexList(client)
        c2f = Cold2Frozen(ilo)
        assert c2f.get_tier_preference() == 'data_frozen'
    def test_get_tier_preference2(self):
        """test_get_tier_preference2"""
        client = Mock()
        client.info.return_value = {'version': {'number': '8.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        roles = ['data_cold', 'data_hot', 'data_warm']
        client.nodes.info.return_value = {'nodes': {'nodename': {'roles': roles}}}
        ilo = IndexList(client)
        c2f = Cold2Frozen(ilo)
        assert c2f.get_tier_preference() == 'data_cold,data_warm,data_hot'
    def test_get_tier_preference3(self):
        """test_get_tier_preference3"""
        client = Mock()
        client.info.return_value = {'version': {'number': '8.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        roles = ['data_content']
        client.nodes.info.return_value = {'nodes': {'nodename': {'roles': roles}}}
        ilo = IndexList(client)
        c2f = Cold2Frozen(ilo)
        assert c2f.get_tier_preference() == 'data_content'
    def test_has_lifecycle_name(self):
        """test_has_lifecycle_name"""
        client = Mock()
        client.info.return_value = {'version': {'number': '8.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        ilo = IndexList(client)
        c2f = Cold2Frozen(ilo)
        testval = {'lifecycle': {'name': 'ilm_policy'}}
        assert c2f.has_lifecycle_name(testval)
    def test_has_no_lifecycle_name(self):
        """test_has_no_lifecycle_name"""
        client = Mock()
        client.info.return_value = {'version': {'number': '8.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        ilo = IndexList(client)
        c2f = Cold2Frozen(ilo)
        testval = {'lifecycle': {'nothere': 'nope'}}
        assert not c2f.has_lifecycle_name(testval)
    def test_is_idx_partial(self):
        """test_is_idx_partial"""
        client = Mock()
        client.info.return_value = {'version': {'number': '8.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        ilo = IndexList(client)
        c2f = Cold2Frozen(ilo)
        testval = {'store': {'snapshot': {'partial': True}}}
        assert c2f.is_idx_partial(testval)
    def test_is_idx_partial_false1(self):
        """test_is_idx_partial_false1"""
        client = Mock()
        client.info.return_value = {'version': {'number': '8.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        ilo = IndexList(client)
        c2f = Cold2Frozen(ilo)
        testval = {'store': {'snapshot': {'partial': False}}}
        assert not c2f.is_idx_partial(testval)
    def test_is_idx_partial_false2(self):
        """test_is_idx_partial_false2"""
        client = Mock()
        client.info.return_value = {'version': {'number': '8.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        ilo = IndexList(client)
        c2f = Cold2Frozen(ilo)
        testval = {'store': {'snapshot': {'nothere': 'nope'}}}
        assert not c2f.is_idx_partial(testval)
    def test_is_idx_partial_raises1(self):
        """test_is_idx_partial_raises1"""
        client = Mock()
        client.info.return_value = {'version': {'number': '8.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        ilo = IndexList(client)
        c2f = Cold2Frozen(ilo)
        testval = {'store': {'nothere': 'nope'}}
        with pytest.raises(SearchableSnapshotException, match='not a mounted searchable snapshot'):
            c2f.is_idx_partial(testval)
    def test_is_idx_partial_raises2(self):
        """test_is_idx_partial_raises2"""
        client = Mock()
        client.info.return_value = {'version': {'number': '8.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        ilo = IndexList(client)
        c2f = Cold2Frozen(ilo)
        testval = {'nothere': 'nope'}
        with pytest.raises(SearchableSnapshotException, match='not a mounted searchable snapshot'):
            c2f.is_idx_partial(testval)
    def test_action_generator1(self):
        """test_action_generator1"""
        settings_ss   = {
            testvars.named_index: {
                'aliases': {'my_alias': {}},
                'mappings': {},
                'settings': {
                    'index': {
                        'creation_date': '1456963200172',
                        'refresh_interval': '5s',
                        'lifecycle': {
                            'indexing_complete': True
                        },
                        'store': {
                            'type': 'snapshot',
                            'snapshot': {
                                'snapshot_name': 'snapname',
                                'index_name': testvars.named_index,
                                'repository_name': 'reponame',
                            }
                        }
                    }
                }
            }
        }
        client = Mock()
        client.info.return_value = {'version': {'number': '8.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        client.indices.get.return_value = settings_ss
        roles = ['data_content']
        client.nodes.info.return_value = {'nodes': {'nodename': {'roles': roles}}}
        ilo = IndexList(client)
        c2f = Cold2Frozen(ilo)
        snap = 'snapname'
        repo = 'reponame'
        renamed = f'partial-{testvars.named_index}'
        settings = {
            "routing": {
                "allocation": {
                    "include": {
                        "_tier_preference": roles[0]
                    }
                }
            }
        }
        expected = {
                'repository': repo, 'snapshot': snap, 'index': testvars.named_index,
                'renamed_index': renamed, 'index_settings': settings,
                'ignore_index_settings': ['index.refresh_interval'],
                'storage': 'shared_cache', 'wait_for_completion': True,
                'aliases': {'my_alias': {}}, 'current_idx': testvars.named_index
        }
        for result in c2f.action_generator():
            assert result == expected
        c2f.do_dry_run() # Do this here as it uses the same generator output.
    def test_action_generator2(self):
        """test_action_generator2"""
        settings_ss   = {
            testvars.named_index: {
                'settings': {'index': {'lifecycle': {'name': 'guaranteed_fail'}}}
            }
        }
        client = Mock()
        client.info.return_value = {'version': {'number': '8.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        client.indices.get.return_value = settings_ss
        ilo = IndexList(client)
        c2f = Cold2Frozen(ilo)
        with pytest.raises(CuratorException, match='associated with an ILM policy'):
            for result in c2f.action_generator():
                _ = result
    def test_action_generator3(self):
        """test_action_generator3"""
        settings_ss   = {
            testvars.named_index: {
                'settings': {
                    'index': {
                        'lifecycle': {'indexing_complete': True},
                        'store': {'snapshot': {'partial': True}}
                    }
                }
            }
        }
        client = Mock()
        client.info.return_value = {'version': {'number': '8.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        client.indices.get.return_value = settings_ss
        ilo = IndexList(client)
        c2f = Cold2Frozen(ilo)
        with pytest.raises(SearchableSnapshotException, match='Index is already in frozen tier'):
            for result in c2f.action_generator():
                _ = result
