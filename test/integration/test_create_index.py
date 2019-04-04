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

class TestCLICreateIndex(CuratorTestCase):
    def test_plain(self):
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(self.args['actionfile'],
            testvars.create_index.format('testing'))
        self.assertEqual([], curator.get_indices(self.client))
        test = clicktest.CliRunner()
        result = test.invoke(
                    curator.cli,
                    [
                        '--config', self.args['configfile'],
                        self.args['actionfile']
                    ],
                    )
        self.assertEqual(['testing'], curator.get_indices(self.client))
    def test_with_extra_settings(self):
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(self.args['actionfile'],
            testvars.create_index_with_extra_settings.format('testing'))
        self.assertEqual([], curator.get_indices(self.client))
        test = clicktest.CliRunner()
        _ = test.invoke(
            curator.cli,
            ['--config', self.args['configfile'], self.args['actionfile']],
        )
        ilo = curator.IndexList(self.client)
        self.assertEqual(['testing'], ilo.indices)
        self.assertEqual(ilo.index_info['testing']['number_of_shards'], '1')
        self.assertEqual(ilo.index_info['testing']['number_of_replicas'], '0')
    def test_with_strftime(self):
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(self.args['actionfile'],
            testvars.create_index.format('testing-%Y.%m.%d'))
        self.assertEqual([], curator.get_indices(self.client))
        name = curator.parse_date_pattern('testing-%Y.%m.%d')
        test = clicktest.CliRunner()
        result = test.invoke(
                    curator.cli,
                    [
                        '--config', self.args['configfile'],
                        self.args['actionfile']
                    ],
                    )
        self.assertEqual([name], curator.get_indices(self.client))
    def test_with_date_math(self):
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(self.args['actionfile'],
            testvars.create_index.format('<testing-{now/d}>'))
        self.assertEqual([], curator.get_indices(self.client))
        name = curator.parse_date_pattern('testing-%Y.%m.%d')
        test = clicktest.CliRunner()
        result = test.invoke(
                    curator.cli,
                    [
                        '--config', self.args['configfile'],
                        self.args['actionfile']
                    ],
                    )
        self.assertEqual([name], curator.get_indices(self.client))
    def test_extra_option(self):
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(self.args['actionfile'],
            testvars.bad_option_proto_test.format('create_index'))
        test = clicktest.CliRunner()
        result = test.invoke(
                    curator.cli,
                    [
                        '--config', self.args['configfile'],
                        self.args['actionfile']
                    ],
                    )
        self.assertEqual([], curator.get_indices(self.client))
        self.assertEqual(-1, result.exit_code)
