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

class TestFilters(CuratorTestCase):
    def test_filter_by_alias(self):
        alias = 'testalias'
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(self.args['actionfile'],
            testvars.filter_by_alias.format('testalias', False))
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
        self.assertEquals(1, len(curator.get_indices(self.client)))
    def test_filter_by_array_of_aliases(self):
        alias = 'testalias'
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(self.args['actionfile'],
            testvars.filter_by_alias.format(' [ testalias, foo ]', False))
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
        ver = curator.get_version(self.client)
        if ver >= (5,5,0):
            self.assertEquals(2, len(curator.get_indices(self.client)))
        else:
            self.assertEquals(1, len(curator.get_indices(self.client)))
    def test_filter_by_alias_bad_aliases(self):
        alias = 'testalias'
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(self.args['actionfile'],
            testvars.filter_by_alias.format('{"this":"isadict"}', False))
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
        self.assertEquals(
            type(curator.ConfigurationError()), type(result.exception))
        self.assertEquals(2, len(curator.get_indices(self.client)))
