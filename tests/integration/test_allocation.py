"""Test Allocation action"""
# pylint: disable=missing-function-docstring, missing-class-docstring, invalid-name, line-too-long
import os
from . import CuratorTestCase
from . import testvars

HOST = os.environ.get('TEST_ES_SERVER', 'http://127.0.0.1:9200')

TIEREDROUTING = {'allocation': {'include': {'_tier_preference': 'data_content'}}}
KEY = 'tag'
VALUE = 'value'

class TestActionFileAllocation(CuratorTestCase):
    def test_include(self):
        alloc = 'include'
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(self.args['actionfile'], testvars.allocation_test.format(KEY, VALUE, alloc, False))
        idx1, idx2 = ('my_index', 'not_my_index')
        self.create_index(idx1)
        self.create_index(idx2)
        self.invoke_runner()
        assert VALUE == self.client.indices.get_settings(index=idx1)[idx1]['settings']['index']['routing']['allocation'][alloc][KEY]
        assert TIEREDROUTING == self.client.indices.get_settings(index=idx2)[idx2]['settings']['index']['routing']
    def test_require(self):
        alloc = 'require'
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(self.args['actionfile'], testvars.allocation_test.format(KEY, VALUE, alloc, False))
        idx1, idx2 = ('my_index', 'not_my_index')
        self.create_index(idx1)
        self.create_index(idx2)
        self.invoke_runner()
        assert VALUE == self.client.indices.get_settings(index=idx1)[idx1]['settings']['index']['routing']['allocation'][alloc][KEY]
        assert TIEREDROUTING == self.client.indices.get_settings(index=idx2)[idx2]['settings']['index']['routing']
    def test_exclude(self):
        alloc = 'exclude'
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(self.args['actionfile'], testvars.allocation_test.format(KEY, VALUE, alloc, False))
        idx1, idx2 = ('my_index', 'not_my_index')
        self.create_index(idx1)
        self.create_index(idx2)
        self.invoke_runner()
        assert VALUE == self.client.indices.get_settings(index=idx1)[idx1]['settings']['index']['routing']['allocation'][alloc][KEY]
        assert TIEREDROUTING == self.client.indices.get_settings(index=idx2)[idx2]['settings']['index']['routing']
    def test_remove_exclude_with_none_value(self):
        empty = '' ### EMPTYVALUE
        alloc = 'exclude'
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(self.args['actionfile'], testvars.allocation_test.format(KEY, empty, alloc, False))
        idx1, idx2 = ('my_index', 'not_my_index')
        self.create_index(idx1)
        self.create_index(idx2)
        # Put a setting in place before we start the test.
        self.client.indices.put_settings(index=idx1, settings={f'index.routing.allocation.{alloc}.{KEY}': 'bar'})
        # Ensure we _have_ it here first.
        assert 'bar' == self.client.indices.get_settings(index=idx1)[idx1]['settings']['index']['routing']['allocation'][alloc][KEY]
        self.invoke_runner()
        assert TIEREDROUTING == self.client.indices.get_settings(index=idx1)[idx1]['settings']['index']['routing']
        assert TIEREDROUTING == self.client.indices.get_settings(index=idx2)[idx2]['settings']['index']['routing']
    def test_invalid_allocation_type(self):
        alloc = 'invalid'
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(self.args['actionfile'], testvars.allocation_test.format(KEY, VALUE, alloc, False))
        idx1, idx2 = ('my_index', 'not_my_index')
        self.create_index(idx1)
        self.create_index(idx2)
        self.invoke_runner()
        assert 1 == self.result.exit_code
    def test_extra_option(self):
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(self.args['actionfile'], testvars.bad_option_proto_test.format('allocation'))
        idx1, idx2 = ('my_index', 'not_my_index')
        self.create_index(idx1)
        self.create_index(idx2)
        self.invoke_runner()
        assert 1 == self.result.exit_code
    def test_skip_closed(self):
        alloc = 'include'
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(self.args['actionfile'], testvars.allocation_test.format(KEY, VALUE, alloc, False))
        idx1, idx2 = ('my_index', 'not_my_index')
        self.create_index(idx1)
        self.client.indices.close(index=idx1)
        self.create_index(idx2)
        self.invoke_runner()
        assert TIEREDROUTING == self.client.indices.get_settings(index=idx1)[idx1]['settings']['index']['routing']
        assert TIEREDROUTING == self.client.indices.get_settings(index=idx2)[idx2]['settings']['index']['routing']
    def test_wait_for_completion(self):
        alloc = 'require'
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(self.args['actionfile'], testvars.allocation_test.format(KEY, VALUE, alloc, True))
        idx1, idx2 = ('my_index', 'not_my_index')
        self.create_index(idx1)
        self.create_index(idx2)
        self.invoke_runner()
        assert VALUE == self.client.indices.get_settings(index=idx1)[idx1]['settings']['index']['routing']['allocation'][alloc][KEY]
        assert TIEREDROUTING == self.client.indices.get_settings(index=idx2)[idx2]['settings']['index']['routing']
class TestCLIAllocation(CuratorTestCase):
    def test_include(self):
        alloc = 'include'
        idx1, idx2 = ('my_index', 'not_my_index')
        self.create_index(idx1)
        self.create_index(idx2)
        args = self.get_runner_args()
        args += [
            '--config', self.args['configfile'],
            'allocation',
            '--key', KEY,
            '--value', VALUE,
            '--allocation_type', alloc,
            '--wait_for_completion',
            '--filter_list', '{"filtertype":"pattern","kind":"prefix","value":"my"}',
        ]
        assert 0 == self.run_subprocess(args, logname='TestCLIAllocation.test_include')
        assert VALUE == self.client.indices.get_settings(index=idx1)[idx1]['settings']['index']['routing']['allocation'][alloc][KEY]
        assert TIEREDROUTING == self.client.indices.get_settings(index=idx2)[idx2]['settings']['index']['routing']
