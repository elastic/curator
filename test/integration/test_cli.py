import elasticsearch
import curator
import os
import json
import string, random, tempfile
import click
from click import testing as clicktest
from mock import patch, Mock

from . import CuratorTestCase
from . import testvars as testvars

import logging
logger = logging.getLogger(__name__)

host, port = os.environ.get('TEST_ES_SERVER', 'localhost:9200').split(':')
port = int(port) if port else 9200

class TestCLIMethods(CuratorTestCase):
    def test_bad_client_config(self):
        self.create_indices(10)
        self.write_config(
            self.args['configfile'],
            testvars.bad_client_config.format(host, port)
        )
        self.write_config(self.args['actionfile'],
            testvars.disabled_proto.format('close', 'delete_indices'))
        test = clicktest.CliRunner()
        result = test.invoke(
                    curator.cli,
                    [
                        '--config', self.args['configfile'],
                        '--dry-run',
                        self.args['actionfile']

                    ],
                    )
        self.assertEqual(-1, result.exit_code)
    def test_no_config(self):
        # This test checks whether localhost:9200 is provided if no hosts or
        # port are in the configuration. But in testing, sometimes
        # TEST_ES_SERVER is set to something other than localhost:9200.  In this
        # case, the test here would fail.  The if statement at the end now
        # compensates. See https://github.com/elastic/curator/issues/843
        localtest = False
        if (host == 'localhost' or host == '127.0.0.1') and \
          port == 9200:
            localtest = True
        self.create_indices(10)
        self.write_config(
            self.args['configfile'],
            ' \n'
        )
        self.write_config(self.args['actionfile'],
            testvars.disabled_proto.format('close', 'delete_indices'))
        test = clicktest.CliRunner()
        result = test.invoke(
                    curator.cli,
                    [
                        '--config', self.args['configfile'],
                        '--dry-run',
                        self.args['actionfile']

                    ],
                    )
        if localtest:
            self.assertEqual(0, result.exit_code)
        else:
            self.assertEqual(-1, result.exit_code)
    def test_no_logging_config(self):
        self.create_indices(10)
        self.write_config(
            self.args['configfile'],
            testvars.no_logging_config.format(host, port)
        )
        self.write_config(self.args['actionfile'],
            testvars.disabled_proto.format('close', 'delete_indices'))
        test = clicktest.CliRunner()
        result = test.invoke(
                    curator.cli,
                    [
                        '--config', self.args['configfile'],
                        '--dry-run',
                        self.args['actionfile']

                    ],
                    )
        self.assertEqual(0, result.exit_code)
    def test_logging_none(self):
        self.create_indices(10)
        self.write_config(
            self.args['configfile'],
            testvars.none_logging_config.format(host, port)
        )
        self.write_config(self.args['actionfile'],
            testvars.disabled_proto.format('close', 'delete_indices'))
        test = clicktest.CliRunner()
        result = test.invoke(
                    curator.cli,
                    [
                        '--config', self.args['configfile'],
                        '--dry-run',
                        self.args['actionfile']

                    ],
                    )
        self.assertEqual(0, result.exit_code)
    def test_invalid_action(self):
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(self.args['actionfile'],
            testvars.optionless_proto.format('invalid_action'))
        test = clicktest.CliRunner()
        result = test.invoke(
                    curator.cli,
                    [
                        '--config', self.args['configfile'],
                        self.args['actionfile']
                    ],
                    )
        self.assertEqual(-1, result.exit_code)
    def test_action_is_None(self):
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(self.args['actionfile'],
            testvars.optionless_proto.format(' '))
        test = clicktest.CliRunner()
        result = test.invoke(
                    curator.cli,
                    [
                        '--config', self.args['configfile'],
                        self.args['actionfile']
                    ],
                    )
        self.assertEqual(
            type(curator.ConfigurationError()), type(result.exception))
    def test_no_action(self):
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(self.args['actionfile'],
            testvars.actionless_proto)
        test = clicktest.CliRunner()
        result = test.invoke(
                    curator.cli,
                    [
                        '--config', self.args['configfile'],
                        self.args['actionfile']
                    ],
                    )
        self.assertEqual(
            type(curator.ConfigurationError()), type(result.exception))
    def test_dry_run(self):
        self.create_indices(10)
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(self.args['actionfile'],
            testvars.delete_proto.format(
                'age', 'name', 'older', '\'%Y.%m.%d\'', 'days', 5, ' ', ' ', ' '
            )
        )
        test = clicktest.CliRunner()
        _ = test.invoke(
                    curator.cli,
                    [
                        '--config', self.args['configfile'],
                        '--dry-run',
                        self.args['actionfile']

                    ],
                    )
        self.assertEquals(10, len(curator.get_indices(self.client)))
    def test_action_disabled(self):
        self.create_indices(10)
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(self.args['actionfile'],
            testvars.disabled_proto.format('close', 'delete_indices'))
        test = clicktest.CliRunner()
        result = test.invoke(
                    curator.cli,
                    [
                        '--config', self.args['configfile'],
                        self.args['actionfile']
                    ],
                    )
        self.assertEquals(0, len(curator.get_indices(self.client)))
        self.assertEqual(0, result.exit_code)
    # I'll have to think up another way to create an exception.
    # The exception that using "alias" created, a missing argument,
    # is caught too early for this to actually run the test now :/
    #
    def test_continue_if_exception(self):
        name = 'log1'
        self.create_index(name)
        self.create_index('log2')
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(self.args['actionfile'],
            testvars.continue_proto.format(
                name, True, 'delete_indices', False
            )
        )
        test = clicktest.CliRunner()
        result = test.invoke(
                    curator.cli,
                    [
                        '--config', self.args['configfile'],
                        self.args['actionfile']
                    ],
                    )
        self.assertEquals(0, len(curator.get_indices(self.client)))
        self.assertEqual(0, result.exit_code)
    def test_continue_if_exception_False(self):
        name = 'log1'
        self.create_index(name)
        self.create_index('log2')
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(self.args['actionfile'],
            testvars.continue_proto.format(
                name, False, 'delete_indices', False
            )
        )
        test = clicktest.CliRunner()
        result = test.invoke(
                    curator.cli,
                    [
                        '--config', self.args['configfile'],
                        self.args['actionfile']
                    ],
                    )
        self.assertEquals(2, len(curator.get_indices(self.client)))
        self.assertEqual(1, result.exit_code)
    def test_no_options_in_action(self):
        self.create_indices(10)
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(self.args['actionfile'],
            testvars.no_options_proto.format('delete_indices'))
        test = clicktest.CliRunner()
        result = test.invoke(
                    curator.cli,
                    [
                        '--config', self.args['configfile'],
                        '--dry-run',
                        self.args['actionfile']

                    ],
                    )
        self.assertEqual(0, result.exit_code)
