"""Test CLI functionality"""
# pylint: disable=missing-function-docstring, missing-class-docstring, line-too-long
import os
from curator.exceptions import ConfigurationError
from curator.helpers.getters import get_indices
from . import CuratorTestCase
from . import testvars

HOST = os.environ.get('TEST_ES_SERVER', 'http://127.0.0.1:9200')

class TestCLIMethods(CuratorTestCase):
    def test_bad_client_config(self):
        self.create_indices(10)
        self.write_config(self.args['configfile'], testvars.bad_client_config.format(HOST))
        self.write_config(self.args['actionfile'], testvars.disabled_proto.format('close', 'delete_indices'))
        self.invoke_runner()
        assert 1 == self.result.exit_code
    def test_no_config(self):
        # This test checks whether localhost:9200 is provided if no hosts or
        # port are in the configuration. But in testing, sometimes
        # TEST_ES_SERVER is set to something other than localhost:9200.  In this
        # case, the test here would fail.  The if statement at the end now
        # compensates. See https://github.com/elastic/curator/issues/843
        localtest = False
        if HOST == 'http://127.0.0.1:9200':
            localtest = True
        self.create_indices(10)
        self.write_config(self.args['configfile'], ' \n') # Empty file.
        self.write_config(self.args['actionfile'], testvars.disabled_proto.format('close', 'delete_indices'))
        self.invoke_runner()
        if localtest:
            assert 0 == self.result.exit_code
        else:
            assert -1 == self.result.exit_code
    def test_no_logging_config(self):
        self.create_indices(10)
        self.write_config(self.args['configfile'], testvars.no_logging_config.format(HOST))
        self.write_config(self.args['actionfile'], testvars.disabled_proto.format('close', 'delete_indices'))
        self.invoke_runner()
        assert 0 == self.result.exit_code
    def test_logging_none(self):
        self.create_indices(10)
        self.write_config(self.args['configfile'], testvars.none_logging_config.format(HOST))
        self.write_config(self.args['actionfile'], testvars.disabled_proto.format('close', 'delete_indices'))
        self.invoke_runner()
        assert 0 == self.result.exit_code
    def test_invalid_action(self):
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(self.args['actionfile'],
            testvars.optionless_proto.format('invalid_action'))
        self.invoke_runner()
        assert 1 == self.result.exit_code
    def test_action_is_none(self):
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(self.args['actionfile'], testvars.optionless_proto.format(' '))
        self.invoke_runner()
        assert isinstance(self.result.exception, ConfigurationError)
    def test_no_action(self):
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(self.args['actionfile'], testvars.actionless_proto)
        self.invoke_runner()
        assert isinstance(self.result.exception, ConfigurationError)
    def test_dry_run(self):
        self.create_indices(10)
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(self.args['actionfile'],
            testvars.delete_proto.format('age', 'name', 'older', '\'%Y.%m.%d\'', 'days', 5, ' ', ' ', ' '))
        self.invoke_runner(dry_run=True)
        assert 10 == len(get_indices(self.client))
    def test_action_disabled(self):
        self.create_indices(10)
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(self.args['actionfile'], testvars.disabled_proto.format('close', 'delete_indices'))
        self.invoke_runner()
        assert 0 == len(get_indices(self.client))
        assert 0 == self.result.exit_code
    # I'll have to think up another way to create an exception.
    # The exception that using "alias" created, a missing argument,
    # is caught too early for this to actually run the test now :/
    def test_continue_if_exception(self):
        name = 'log1'
        self.create_index(name)
        self.create_index('log2')
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(self.args['actionfile'], testvars.continue_proto.format(name, True, 'delete_indices', False))
        self.invoke_runner()
        assert 0 == len(get_indices(self.client))
        assert 0 == self.result.exit_code
    def test_continue_if_exception_false(self):
        name = 'log1'
        self.create_index(name)
        self.create_index('log2')
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(self.args['actionfile'], testvars.continue_proto.format(name, False, 'delete_indices', False))
        self.invoke_runner()
        assert 2 == len(get_indices(self.client))
        assert 1 == self.result.exit_code
    def test_no_options_in_action(self):
        self.create_indices(10)
        self.create_index('my_index') # Added for the ILM filter's sake
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(self.args['actionfile'], testvars.no_options_proto.format('delete_indices'))
        self.invoke_runner(dry_run=True)
        assert 0 == self.result.exit_code
