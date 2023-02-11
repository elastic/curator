"""Test the Create Index action"""
# pylint: disable=missing-function-docstring, missing-class-docstring, line-too-long
import os
from curator import IndexList
from curator.helpers.date_ops import parse_date_pattern
from curator.helpers.getters import get_indices
from . import CuratorTestCase
from . import testvars

HOST = os.environ.get('TEST_ES_SERVER', 'http://127.0.0.1:9200')

class TestCLICreateIndex(CuratorTestCase):
    def test_plain(self):
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(self.args['actionfile'], testvars.create_index.format('testing'))
        assert not get_indices(self.client)
        self.invoke_runner()
        self.assertEqual(['testing'], get_indices(self.client))
        assert ['testing'] == get_indices(self.client)
    def test_with_extra_settings(self):
        idx = 'testing'
        alias = 'aliasname'
        mapkey1 = 'meep'
        mapval1 = 'integer'
        mapkey2 = 'beep'
        mapval2 = 'keyword'
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(self.args['actionfile'],
            testvars.create_index_with_extra_settings.format(idx, alias, mapkey1, mapval1, mapkey2, mapval2))
        assert not get_indices(self.client)
        self.invoke_runner()
        ilo = IndexList(self.client)
        aliases = self.client.indices.get_alias(name=alias)
        mapping = self.client.indices.get_mapping(index=idx)
        assert [idx] == ilo.indices
        assert '1' == ilo.index_info[idx]['number_of_shards']
        assert '0' == ilo.index_info[idx]['number_of_replicas']
        assert mapping[idx]['mappings']['properties'][mapkey1] == {'type': mapval1}
        assert mapping[idx]['mappings']['properties'][mapkey2] == {'type': mapval2}
        assert aliases[idx]['aliases'] == {alias: {'is_write_index': True}}
    def test_with_strftime(self):
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(self.args['actionfile'], testvars.create_index.format('testing-%Y.%m.%d'))
        assert not get_indices(self.client)
        idx = parse_date_pattern('testing-%Y.%m.%d')
        self.invoke_runner()
        assert [idx] == get_indices(self.client)
    def test_with_date_math(self):
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(self.args['actionfile'], testvars.create_index.format('<testing-{now/d}>'))
        assert not get_indices(self.client)
        idx = parse_date_pattern('testing-%Y.%m.%d')
        self.invoke_runner()
        assert [idx] == get_indices(self.client)
    def test_extra_option(self):
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(self.args['actionfile'], testvars.bad_option_proto_test.format('create_index'))
        self.invoke_runner()
        assert not get_indices(self.client)
        assert 1 == self.result.exit_code
    def test_already_existing_fail(self):
        idx = 'testing'
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(self.args['actionfile'], testvars.create_index.format(idx))
        self.create_index(idx)
        self.invoke_runner()
        assert [idx] == get_indices(self.client)
        assert 1 == self.result.exit_code
    def test_already_existing_pass(self):
        config = (
            '---\n'
            'actions:\n'
            '  1:\n'
            '    description: "Create index as named"\n'
            '    action: create_index\n'
            '    options:\n'
            '      name: {0}\n'
            '      ignore_existing: true\n'
        )
        idx = 'testing'
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(self.args['actionfile'], config.format(idx))
        self.create_index(idx)
        self.invoke_runner()
        assert [idx] == get_indices(self.client)
        assert 0 == self.result.exit_code
