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
