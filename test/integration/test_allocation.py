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


class TestActionFileAllocation(CuratorTestCase):
    def test_include(self):
        key = 'tag'
        value = 'value'
        at = 'include'
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(self.args['actionfile'],
            testvars.allocation_test.format(key, value, at, False))
        self.create_index('my_index')
        self.create_index('not_my_index')
        test = clicktest.CliRunner()
        _ = test.invoke(
                    curator.cli,
                    [
                        '--config', self.args['configfile'],
                        self.args['actionfile']
                    ],
                    )
        self.assertEquals(value,
            self.client.indices.get_settings(index='my_index')['my_index']['settings']['index']['routing']['allocation'][at][key])
        self.assertNotIn('routing',
            self.client.indices.get_settings(index='not_my_index')['not_my_index']['settings']['index'])
    def test_require(self):
        key = 'tag'
        value = 'value'
        at = 'require'
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(self.args['actionfile'],
            testvars.allocation_test.format(key, value, at, False))
        self.create_index('my_index')
        self.create_index('not_my_index')
        test = clicktest.CliRunner()
        _ = test.invoke(
                    curator.cli,
                    [
                        '--config', self.args['configfile'],
                        self.args['actionfile']
                    ],
                    )
        self.assertEquals(value,
            self.client.indices.get_settings(index='my_index')['my_index']['settings']['index']['routing']['allocation'][at][key])
        self.assertNotIn('routing',
            self.client.indices.get_settings(index='not_my_index')['not_my_index']['settings']['index'])
    def test_exclude(self):
        key = 'tag'
        value = 'value'
        at = 'exclude'
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(self.args['actionfile'],
            testvars.allocation_test.format(key, value, at, False))
        self.create_index('my_index')
        self.create_index('not_my_index')
        test = clicktest.CliRunner()
        _ = test.invoke(
                    curator.cli,
                    [
                        '--config', self.args['configfile'],
                        self.args['actionfile']
                    ],
                    )
        self.assertEquals(value,
            self.client.indices.get_settings(index='my_index')['my_index']['settings']['index']['routing']['allocation'][at][key])
        self.assertNotIn('routing',
            self.client.indices.get_settings(index='not_my_index')['not_my_index']['settings']['index'])
    def test_remove_exclude_with_none_value(self):
        key = 'tag'
        value = ''
        at = 'exclude'
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(self.args['actionfile'],
            testvars.allocation_test.format(key, value, at, False))
        self.create_index('my_index')
        self.create_index('not_my_index')
        # Put a setting in place before we start the test.
        self.client.indices.put_settings(
            index='my_index',
            body={'index.routing.allocation.{0}.{1}'.format(at, key): 'bar'}
        )
        # Ensure we _have_ it here first.
        self.assertEquals('bar',
            self.client.indices.get_settings(index='my_index')['my_index']['settings']['index']['routing']['allocation'][at][key])
        test = clicktest.CliRunner()
        _ = test.invoke(
                    curator.cli,
                    [
                        '--config', self.args['configfile'],
                        self.args['actionfile']
                    ],
                    )
        self.assertNotIn('routing',
            self.client.indices.get_settings(index='my_index')['my_index']['settings']['index'])
        self.assertNotIn('routing',
            self.client.indices.get_settings(index='not_my_index')['not_my_index']['settings']['index'])
    def test_invalid_allocation_type(self):
        key = 'tag'
        value = 'value'
        at = 'invalid'
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(self.args['actionfile'],
            testvars.allocation_test.format(key, value, at, False))
        self.create_index('my_index')
        self.create_index('not_my_index')
        test = clicktest.CliRunner()
        result = test.invoke(
                    curator.cli,
                    [
                        '--config', self.args['configfile'],
                        self.args['actionfile']
                    ],
                    )
        self.assertEqual(-1, result.exit_code)
    def test_extra_option(self):
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(self.args['actionfile'],
            testvars.bad_option_proto_test.format('allocation'))
        self.create_index('my_index')
        self.create_index('not_my_index')
        test = clicktest.CliRunner()
        result = test.invoke(
                    curator.cli,
                    [
                        '--config', self.args['configfile'],
                        self.args['actionfile']
                    ],
                    )
        self.assertEqual(-1, result.exit_code)
    def test_skip_closed(self):
        key = 'tag'
        value = 'value'
        at = 'include'
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(self.args['actionfile'],
            testvars.allocation_test.format(key, value, at, False))
        self.create_index('my_index')
        self.client.indices.close(index='my_index')
        self.create_index('not_my_index')
        test = clicktest.CliRunner()
        _ = test.invoke(
                    curator.cli,
                    [
                        '--config', self.args['configfile'],
                        self.args['actionfile']
                    ],
                    )
        self.assertNotIn('routing',
            self.client.indices.get_settings(index='my_index')['my_index']['settings']['index'])
        self.assertNotIn('routing',
            self.client.indices.get_settings(index='not_my_index')['not_my_index']['settings']['index'])
    def test_wait_for_completion(self):
        key = 'tag'
        value = 'value'
        at = 'require'
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(self.args['actionfile'],
            testvars.allocation_test.format(key, value, at, True))
        self.create_index('my_index')
        self.create_index('not_my_index')
        test = clicktest.CliRunner()
        _ = test.invoke(
                    curator.cli,
                    [
                        '--config', self.args['configfile'],
                        self.args['actionfile']
                    ],
                    )
        self.assertEquals(value,
            self.client.indices.get_settings(index='my_index')['my_index']['settings']['index']['routing']['allocation'][at][key])
        self.assertNotIn('routing',
            self.client.indices.get_settings(index='not_my_index')['not_my_index']['settings']['index'])

class TestCLIAllocation(CuratorTestCase):
    def test_include(self):
        key = 'tag'
        value = 'value'
        at = 'include'
        self.create_index('my_index')
        self.create_index('not_my_index')
        args = self.get_runner_args()
        args += [
            '--config', self.args['configfile'],
            'allocation',
            '--key', key,
            '--value', value,
            '--allocation_type', at,
            '--wait_for_completion',
            '--filter_list', '{"filtertype":"pattern","kind":"prefix","value":"my"}',
        ]
        self.assertEqual(0, self.run_subprocess(args, logname='TestCLIAllocation.test_include'))
        self.assertEquals(value,
            self.client.indices.get_settings(index='my_index')['my_index']['settings']['index']['routing']['allocation'][at][key])
        self.assertNotIn('routing',
            self.client.indices.get_settings(index='not_my_index')['not_my_index']['settings']['index'])
