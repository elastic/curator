"""test_action_cold2frozen"""
# pylint: disable=attribute-defined-outside-init
from unittest import TestCase
from unittest.mock import Mock
import pytest
from curator.actions import Cold2Frozen
from curator.exceptions import CuratorException, SearchableSnapshotException
from curator import IndexList
# Get test variables and constants from a single source
from . import testvars

class TestActionCold2Frozen(TestCase):
    """TestActionCold2Frozen"""
    VERSION = {'version': {'number': '8.0.0'} }
    def builder(self):
        """Environment builder"""
        self.client = Mock()
        self.client.info.return_value = self.VERSION
        self.client.cat.indices.return_value = testvars.state_one
        self.client.indices.get_settings.return_value = testvars.settings_one
        self.client.indices.stats.return_value = testvars.stats_one
        self.client.indices.exists_alias.return_value = False
        self.ilo = IndexList(self.client)
    def test_init_raise_bad_index_list(self):
        """test_init_raise_bad_index_list"""
        self.assertRaises(TypeError, Cold2Frozen, 'invalid')
        with pytest.raises(TypeError):
            Cold2Frozen('not_an_IndexList')
    def test_init_add_kwargs(self):
        """test_init_add_kwargs"""
        self.builder()
        testval = {'key': 'value'}
        c2f = Cold2Frozen(self.ilo, index_settings=testval)
        assert c2f.index_settings == testval
    def test_action_generator1(self):
        """test_action_generator1"""
        self.builder()
        settings_ss   = {
            testvars.named_index: {
                'aliases': {'my_alias': {}},
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

        self.client.indices.get_settings.return_value = settings_ss
        self.client.indices.get_alias.return_value = settings_ss
        roles = ['data_content']
        self.client.nodes.info.return_value = {'nodes': {'nodename': {'roles': roles}}}
        c2f = Cold2Frozen(self.ilo)
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
        self.builder()
        settings_ss   = {
            testvars.named_index: {
                'settings': {'index': {'lifecycle': {'name': 'guaranteed_fail'}}}
            }
        }
        self.client.indices.get_settings.return_value = settings_ss
        c2f = Cold2Frozen(self.ilo)
        with pytest.raises(CuratorException, match='associated with an ILM policy'):
            for result in c2f.action_generator():
                _ = result
    def test_action_generator3(self):
        """test_action_generator3"""
        self.builder()
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
        self.client.indices.get_settings.return_value = settings_ss
        c2f = Cold2Frozen(self.ilo)
        with pytest.raises(SearchableSnapshotException, match='Index is already in frozen tier'):
            for result in c2f.action_generator():
                _ = result
