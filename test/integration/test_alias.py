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

class TestCLIAlias(CuratorTestCase):
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
        result = test.invoke(
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
        result = test.invoke(
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
        result = test.invoke(
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
        result = test.invoke(
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
        result = test.invoke(
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
        result = test.invoke(
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
        result = test.invoke(
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
        result = test.invoke(
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
        result = test.invoke(
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
        somevar = 'testalias'
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
        somevar = 'testalias'
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
