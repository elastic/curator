"""Test shrink action"""
# pylint: disable=missing-function-docstring, missing-class-docstring, line-too-long, attribute-defined-outside-init
import os
import logging
from curator.utils import get_indices
from . import CuratorTestCase
from . import testvars

HOST = os.environ.get('TEST_ES_SERVER', 'http://127.0.0.1:9200')

SHRINK = ('---\n'
'actions:\n'
'  1:\n'
'    description: "Act on indices as filtered"\n'
'    action: shrink\n'
'    options:\n'
'      shrink_node: {0}\n'
'      node_filters:\n'
'          {1}: {2}\n'
'      number_of_shards: {3}\n'
'      number_of_replicas: {4}\n'
'      shrink_prefix: {5}\n'
'      shrink_suffix: {6}\n'
'      delete_after: {7}\n'
'      wait_for_rebalance: {8}\n'
'    filters:\n'
'      - filtertype: pattern\n'
'        kind: prefix\n'
'        value: my\n')

NO_PERMIT_MASTERS = ('---\n'
'actions:\n'
'  1:\n'
'    description: "Act on indices as filtered"\n'
'    action: shrink\n'
'    options:\n'
'      shrink_node: {0}\n'
'      number_of_shards: {1}\n'
'      number_of_replicas: {2}\n'
'      shrink_prefix: {3}\n'
'      shrink_suffix: {4}\n'
'      delete_after: {5}\n'
'    filters:\n'
'      - filtertype: pattern\n'
'        kind: prefix\n'
'        value: my\n')

WITH_EXTRA_SETTINGS = ('---\n'
'actions:\n'
'  1:\n'
'    description: "Act on indices as filtered"\n'
'    action: shrink\n'
'    options:\n'
'      shrink_node: {0}\n'
'      node_filters:\n'
'          {1}: {2}\n'
'      number_of_shards: {3}\n'
'      number_of_replicas: {4}\n'
'      shrink_prefix: {5}\n'
'      shrink_suffix: {6}\n'
'      delete_after: {7}\n'
'      extra_settings:\n'
'        settings:\n'
'          {8}: {9}\n'
'        aliases:\n'
'          my_alias: {10}\n'
'      post_allocation:\n'
'          allocation_type: {11}\n'
'          key: {12}\n'
'          value: {13}\n'
'    filters:\n'
'      - filtertype: pattern\n'
'        kind: prefix\n'
'        value: my\n')

COPY_ALIASES = ('---\n'
'actions:\n'
'  1:\n'
'    description: "Act on indices as filtered"\n'
'    action: shrink\n'
'    options:\n'
'      shrink_node: {0}\n'
'      node_filters:\n'
'          {1}: {2}\n'
'      number_of_shards: {3}\n'
'      number_of_replicas: {4}\n'
'      shrink_prefix: {5}\n'
'      shrink_suffix: {6}\n'
'      copy_aliases: {7}\n'
'      delete_after: {8}\n'
'    filters:\n'
'      - filtertype: pattern\n'
'        kind: prefix\n'
'        value: my\n')

SHRINK_FILTER_BY_SHARDS = ('---\n'
'actions:\n'
'  1:\n'
'    description: "Act on indices as filtered"\n'
'    action: shrink\n'
'    options:\n'
'      shrink_node: {0}\n'
'      node_filters:\n'
'          {1}: {2}\n'
'      number_of_shards: {3}\n'
'      number_of_replicas: {4}\n'
'      shrink_prefix: {5}\n'
'      shrink_suffix: {6}\n'
'      delete_after: {7}\n'
'      wait_for_rebalance: {8}\n'
'    filters:\n'
'      - filtertype: shards\n'
'        number_of_shards: {9}\n'
'        shard_filter_behavior: {10}\n')

class TestActionFileShrink(CuratorTestCase):
    def builder(self, action_args):
        self.idx = 'my_index'
        suffix = '-shrunken'
        self.target = f'{self.idx}{suffix}'
        self.create_index(self.idx, shards=2)
        self.add_docs(self.idx)
        # add alias in the source index
        self.alias = 'my_alias'
        alias_actions = []
        alias_actions.append({'add': {'index': self.idx, 'alias': self.alias}})
        self.client.indices.update_aliases(actions=alias_actions)
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(self.args['actionfile'], action_args)
        self.invoke_runner()
    def test_shrink(self):
        suffix = '-shrunken'
        self.builder(
            SHRINK.format(
                'DETERMINISTIC',
                'permit_masters',
                'True',
                1,
                0,
                '',
                suffix,
                'True',
                'True'
            )
        )
        indices = get_indices(self.client)
        assert 1 == len(indices)
        assert indices[0] == self.target
    def test_permit_masters_false(self):
        suffix = '-shrunken'
        self.builder(
            NO_PERMIT_MASTERS.format(
                'DETERMINISTIC',
                1,
                0,
                '',
                suffix,
                'True'
            )
        )
        indices = get_indices(self.client)
        assert 1 == len(indices)
        assert indices[0] == self.idx
        assert 1 == self.result.exit_code
    def test_shrink_non_green(self):
        suffix = '-shrunken'
        self.client.indices.create(index='just_another_index', settings={'number_of_shards': 1, 'number_of_replicas': 1})
        self.builder(
            SHRINK.format(
                'DETERMINISTIC',
                'permit_masters',
                'True',
                1,
                0,
                '',
                suffix,
                'True',
                'True'
            )
        )
        indices = get_indices(self.client)
        assert 2 == len(indices)
        assert 1 == self.result.exit_code
    def test_shrink_with_extras(self):
        suffix = '-shrunken'
        allocation_type = 'exclude'
        key = '_name'
        value = 'not_this_node'
        self.builder(
            WITH_EXTRA_SETTINGS.format(
                'DETERMINISTIC',
                'permit_masters',
                'True',
                1,
                0,
                '',
                suffix,
                'False',
                'index.codec',
                'best_compression',
                dict(),
                allocation_type,
                key,
                value
            )
        )
        indices = get_indices(self.client)
        assert 2 == len(indices)
        settings = self.client.indices.get_settings()
        assert value == settings[self.target]['settings']['index']['routing']['allocation'][allocation_type][key]
        assert '' == settings[self.idx]['settings']['index']['routing']['allocation']['require']['_name']
        assert 'best_compression' == settings[self.target]['settings']['index']['codec']
        # This was erroneously testing for True in previous releases. But the target index
        # should never have had the alias as `copy_aliases` was never set to True for this test.
    def test_shrink_with_copy_alias(self):
        suffix = '-shrunken'
        self.builder(
            COPY_ALIASES.format(
                'DETERMINISTIC',
                'permit_masters',
                'True',
                1,
                0,
                '',
                suffix,
                'True',
                'True'
            )
        )
        indices = get_indices(self.client)
        assert 1 == len(indices)
        assert indices[0] == self.target
        assert self.client.indices.exists_alias(index=self.target, name=self.alias)
    def test_shrink_without_rebalance(self):
        suffix = '-shrunken'
        self.builder(
            SHRINK.format(
                'DETERMINISTIC',
                'permit_masters',
                'True',
                1,
                0,
                '',
                suffix,
                'True',
                'False'
            )
        )
        indices = get_indices(self.client)
        assert 1 == len(indices)
        assert indices[0] == self.target
    def test_shrink_implicit_shard_filter(self):
        self.create_index('my_invalid_shrink_index', shards=1)
        self.create_index('my_valid_shrink_index', shards=5)
        suffix = '-shrunken'
        self.builder(
            SHRINK.format(
                'DETERMINISTIC',
                'permit_masters',
                'True',
                1,
                0,
                '',
                suffix,
                'True',
                'False'
            )
        )

        indices = get_indices(self.client)
        assert 3 == len(indices)
        assert 'my_invalid_shrink_index-shrunken' not in indices
        assert 'my_valid_shrink_index-shrunken' in indices
    def test_shrink_explicit_shard_filter(self):
        self.create_index('my_invalid_shrink_index', shards=3)
        self.create_index('my_valid_shrink_index', shards=5)
        suffix = '-shrunken'
        self.builder(
            SHRINK_FILTER_BY_SHARDS.format(
                'DETERMINISTIC',
                'permit_masters',
                'True',
                1,
                0,
                '',
                suffix,
                'True',
                'False',
                5,
                'greater_than_or_equal'
            )
        )
        indices = get_indices(self.client)
        assert 3 == len(indices)
        assert 'my_invalid_shrink_index-shrunken' not in indices
        assert 'my_valid_shrink_index-shrunken' in indices
        assert 'my_index-shrunken' not in indices

class TestCLIShrink(CuratorTestCase):
    def builder(self):
        self.loogger = logging.getLogger('TestCLIShrink.builder')
        self.idx = 'my_index'
        self.suffix = '-shrunken'
        self.target = f'{self.idx}{self.suffix}'
        self.create_index(self.idx, shards=2)
        self.add_docs(self.idx)
        # add alias in the source index
        self.alias = 'my_alias'
        alias_actions = []
        alias_actions.append({'add': {'index': self.idx, 'alias': self.alias}})
        self.client.indices.update_aliases(actions=alias_actions)
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.loogger.debug('Test pre-execution build phase complete.')
    def test_shrink(self):
        self.builder()
        args = self.get_runner_args()
        args += [
            '--config', self.args['configfile'],
            'shrink',
            '--shrink_node', 'DETERMINISTIC',
            '--node_filters', '{"permit_masters":"true"}',
            '--number_of_shards', "1",
            '--number_of_replicas', "0",
            '--shrink_suffix', self.suffix,
            '--delete_after',
            '--wait_for_rebalance',
            '--filter_list', '{"filtertype":"none"}',
        ]
        assert 0 == self.run_subprocess(args, logname='TestCLIShrink.test_shrink')
        indices = get_indices(self.client)
        assert 1 == len(indices)
        assert indices[0] == self.target
