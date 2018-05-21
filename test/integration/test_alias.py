import elasticsearch
import curator
import os
import json
import string, random, tempfile
import click
from click import testing as clicktest
from mock import patch, Mock
from datetime import datetime, timedelta

from . import CuratorTestCase
from . import testvars as testvars

import logging
logger = logging.getLogger(__name__)

host, port = os.environ.get('TEST_ES_SERVER', 'localhost:9200').split(':')
port = int(port) if port else 9200

class TestActionFileAlias(CuratorTestCase):
    def test_add_only(self):
        alias = 'testalias'
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(self.args['actionfile'],
            testvars.alias_add_only.format(alias))
        self.create_index('my_index')
        self.create_index('dummy')
        self.client.indices.put_alias(index='dummy', name=alias)
        test = clicktest.CliRunner()
        _ = test.invoke(
                    curator.cli,
                    [
                        '--config', self.args['configfile'],
                        self.args['actionfile']
                    ],
                    )
        self.assertEquals(2, len(self.client.indices.get_alias(name=alias)))
    def test_add_only_with_extra_settings(self):
        alias = 'testalias'
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(self.args['actionfile'],
            testvars.alias_add_only_with_extra_settings.format(alias))
        self.create_index('my_index')
        test = clicktest.CliRunner()
        _ = test.invoke(
                    curator.cli,
                    [
                        '--config', self.args['configfile'],
                        self.args['actionfile']
                    ],
                    )
        self.assertEquals(
            {
                'my_index': {
                    'aliases': {
                        'testalias': {
                            'filter': {
                                'term': {
                                    'user': 'kimchy'
                                }
                            }
                        }
                    }
                }
            },
            self.client.indices.get_alias(name=alias)
        )
    def test_alias_remove_only(self):
        alias = 'testalias'
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(self.args['actionfile'],
            testvars.alias_remove_only.format(alias))
        self.create_index('my_index')
        self.create_index('dummy')
        self.client.indices.put_alias(index='dummy', name=alias)
        test = clicktest.CliRunner()
        _ = test.invoke(
                    curator.cli,
                    [
                        '--config', self.args['configfile'],
                        self.args['actionfile']
                    ],
                    )
        self.assertEqual(
            {'dummy': {'aliases': {}}, 'my_index': {'aliases': {}}},
            self.client.indices.get_alias(index='dummy,my_index')
        )
    def test_add_only_skip_closed(self):
        alias = 'testalias'
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(self.args['actionfile'],
            testvars.alias_add_only.format(alias))
        self.create_index('my_index')
        self.client.indices.close(index='my_index')
        self.create_index('dummy')
        self.client.indices.put_alias(index='dummy', name=alias)
        test = clicktest.CliRunner()
        _ = test.invoke(
                    curator.cli,
                    [
                        '--config', self.args['configfile'],
                        self.args['actionfile']
                    ],
                    )
        version = curator.get_version(self.client)
        if version > (3,0,0):
            self.assertEquals(2, len(self.client.indices.get_alias(name=alias)))
        else:
            self.assertEquals(1, len(self.client.indices.get_alias(name=alias)))
    def test_add_and_remove(self):
        alias = 'testalias'
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(self.args['actionfile'],
            testvars.alias_add_remove.format(alias))
        self.create_index('my_index')
        self.create_index('dummy')
        self.client.indices.put_alias(index='dummy', name=alias)
        test = clicktest.CliRunner()
        _ = test.invoke(
                    curator.cli,
                    [
                        '--config', self.args['configfile'],
                        self.args['actionfile']
                    ],
                    )
        self.assertEqual(
            {u'my_index': {u'aliases': {alias: {}}}},
            self.client.indices.get_alias(name=alias)
        )
    def test_add_and_remove_datemath(self):
        alias = '<testalias-{now-1d/d}>'
        alias_parsed = u'testalias-{0}'.format((datetime.utcnow()-timedelta(days=1)).strftime('%Y.%m.%d'))
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(self.args['actionfile'],
            testvars.alias_add_remove.format(alias))
        self.create_index('my_index')
        self.create_index('dummy')
        self.client.indices.put_alias(index='dummy', name=alias_parsed)
        test = clicktest.CliRunner()
        _ = test.invoke(
                    curator.cli,
                    [
                        '--config', self.args['configfile'],
                        self.args['actionfile']
                    ],
                    )
        self.assertEqual(
            {u'my_index': {u'aliases': {alias_parsed: {}}}},
            self.client.indices.get_alias(name=alias_parsed)
        )
    def test_add_with_empty_remove(self):
        alias = 'testalias'
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(self.args['actionfile'],
            testvars.alias_add_with_empty_remove.format(alias))
        self.create_index('my_index')
        self.create_index('dummy')
        self.client.indices.put_alias(index='dummy', name=alias)
        test = clicktest.CliRunner()
        _ = test.invoke(
                    curator.cli,
                    [
                        '--config', self.args['configfile'],
                        self.args['actionfile']
                    ],
                    )
        self.assertEqual(
            {'dummy': {'aliases': {alias: {}}},'my_index': {'aliases': {alias: {}}}},
            self.client.indices.get_alias()
        )
    def test_remove_with_empty_add(self):
        alias = 'testalias'
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(self.args['actionfile'],
            testvars.alias_remove_with_empty_add.format(alias))
        self.create_index('my_index')
        self.create_index('dummy')
        self.client.indices.put_alias(index='dummy,my_index', name=alias)
        test = clicktest.CliRunner()
        _ = test.invoke(
                    curator.cli,
                    [
                        '--config', self.args['configfile'],
                        self.args['actionfile']
                    ],
                    )
        self.assertEqual(
            {'dummy': {'aliases': {}},'my_index': {'aliases': {alias: {}}}},
            self.client.indices.get_alias()
        )
    def test_add_with_empty_list(self):
        alias = 'testalias'
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(self.args['actionfile'],
            testvars.alias_add_remove_empty.format(alias, 'du', 'rickroll'))
        self.create_index('my_index')
        self.create_index('dummy')
        self.client.indices.put_alias(index='dummy', name=alias)
        test = clicktest.CliRunner()
        _ = test.invoke(
                    curator.cli,
                    [
                        '--config', self.args['configfile'],
                        self.args['actionfile']
                    ],
                    )
        self.assertEqual(
            {'dummy': {'aliases': {alias: {}}}, 'my_index': {'aliases': {}}},
            self.client.indices.get_alias()
        )
    def test_remove_with_empty_list(self):
        alias = 'testalias'
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(self.args['actionfile'],
            testvars.alias_add_remove_empty.format(alias, 'rickroll', 'my'))
        self.create_index('my_index')
        self.create_index('dummy')
        self.client.indices.put_alias(index='dummy', name=alias)
        test = clicktest.CliRunner()
        _ = test.invoke(
                    curator.cli,
                    [
                        '--config', self.args['configfile'],
                        self.args['actionfile']
                    ],
                    )
        self.assertEqual(
            {'dummy': {'aliases': {alias: {}}}, 'my_index': {'aliases': {}}},
            self.client.indices.get_alias()
        )
    def test_remove_index_not_in_alias(self):
        alias = 'testalias'
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(self.args['actionfile'],
            testvars.alias_remove_index_not_there.format(alias,'my'))
        self.create_index('my_index1')
        self.create_index('my_index2')
        self.client.indices.put_alias(index='my_index1', name=alias)
        test = clicktest.CliRunner()
        result = test.invoke(
                    curator.cli,
                    [
                        '--config', self.args['configfile'],
                        self.args['actionfile']
                    ],
                    )
        self.assertEqual(
            {'my_index1': {'aliases': {}}, 'my_index2': {'aliases': {}}},
            self.client.indices.get_alias()
        )
        self.assertEqual(0, result.exit_code)
    def test_no_add_remove(self):
        alias = 'testalias'
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(self.args['actionfile'],
            testvars.alias_no_add_remove.format(alias))
        self.create_index('my_index')
        self.create_index('dummy')
        test = clicktest.CliRunner()
        result = test.invoke(
                    curator.cli,
                    [
                        '--config', self.args['configfile'],
                        self.args['actionfile']
                    ],
                    )
        self.assertEqual(1, result.exit_code)
    def test_no_alias(self):
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(self.args['actionfile'],
            testvars.alias_no_alias)
        self.create_index('my_index')
        self.create_index('dummy')
        test = clicktest.CliRunner()
        result = test.invoke(
                    curator.cli,
                    [
                        '--config', self.args['configfile'],
                        self.args['actionfile']
                    ],
                    )
        self.assertEqual(-1, result.exit_code)
    def test_extra_options(self):
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(self.args['actionfile'],
            testvars.bad_option_proto_test.format('alias'))
        test = clicktest.CliRunner()
        result = test.invoke(
                    curator.cli,
                    [
                        '--config', self.args['configfile'],
                        self.args['actionfile']
                    ],
                    )
        self.assertEqual(-1, result.exit_code)
    def test_add_and_remove_sorted(self):
        alias = 'testalias'
        alias_add_remove = (
            '---\n'
            'actions:\n'
            '  1:\n'
            '    description: "Add/remove specified indices from designated alias"\n'
            '    action: alias\n'
            '    options:\n'
            '      name: {0}\n'
            '      continue_if_exception: False\n'
            '      disable_action: False\n'
            '    add:\n'
            '      filters:\n'
            '        - filtertype: pattern\n'
            '          kind: prefix\n'
            '          value: dum\n'
            '    remove:\n'
            '      filters:\n'
            '        - filtertype: pattern\n'
            '          kind: prefix\n'
            '          value: my\n'
        )
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(self.args['actionfile'], alias_add_remove.format(alias))
        self.create_index('my_index')
        self.create_index('dummy')
        self.client.indices.put_alias(index='my_index', name=alias)
        test = clicktest.CliRunner()
        _ = test.invoke(
                    curator.cli,
                    [
                        '--config', self.args['configfile'],
                        self.args['actionfile']
                    ],
                    )
        self.assertEqual(
            {'dummy':{'aliases':{'testalias':{}}}},
            self.client.indices.get_alias(name=alias)
        )

class TestCLIAlias(CuratorTestCase):
    def test_add_and_remove_alias(self):
        alias = 'testalias'
        self.create_index('my_index')
        self.create_index('dummy')
        self.client.indices.put_alias(index='my_index', name=alias)
        args = self.get_runner_args()
        args += [
            '--config', self.args['configfile'],
            'alias',
            '--name', alias,
            '--add', '{"filtertype":"pattern","kind":"prefix","value":"dum"}',
            '--remove', '{"filtertype":"pattern","kind":"prefix","value":"my"}',
        ]
        self.assertEqual(0, self.run_subprocess(args, logname='TestCLIAlias.test_shrink'))
        self.assertEqual(
            {'dummy': {'aliases': {alias: {}}}},
            self.client.indices.get_alias(name=alias)
        )
    def test_warn_if_no_indices(self):
        alias = 'testalias'
        self.create_index('dummy1')
        self.create_index('dummy2')
        self.client.indices.put_alias(index='dummy1', name=alias)
        args = self.get_runner_args()
        args += [
            '--config', self.args['configfile'],
            'alias',
            '--name', alias,
            '--add', '{"filtertype":"none"}',
            '--remove', '{"filtertype":"pattern","kind":"prefix","value":"my"}',
            '--warn_if_no_indices'
        ]
        self.assertEqual(0, self.run_subprocess(args, logname='TestCLIAlias.test_warn_if_no_indices'))
        self.assertEqual(
            {'dummy1': {'aliases': {alias: {}}}, 'dummy2': {'aliases': {alias: {}}}},
            self.client.indices.get_alias(name=alias)
        )
    def test_exit_1_on_empty_list(self):
        alias = 'testalias'
        self.create_index('dummy')
        args = self.get_runner_args()
        args += [
            '--config', self.args['configfile'],
            'alias',
            '--name', alias,
            '--add', '{"filtertype":"pattern","kind":"prefix","value":"dum","exclude":false}',
            '--remove', '{"filtertype":"pattern","kind":"prefix","value":"my","exclude":false}',
        ]
        self.assertEqual(1, self.run_subprocess(args, logname='TestCLIAlias.test_warn_if_no_indices'))