import elasticsearch
import curator
import os
import json
import string, random, tempfile
import time
from click import testing as clicktest
from mock import patch, Mock
import unittest
from . import CuratorTestCase
from . import testvars as testvars

import logging
logger = logging.getLogger(__name__)

host, port = os.environ.get('TEST_ES_SERVER', 'localhost:9200').split(':')
port = int(port) if port else 9200
# '      - filtertype: {0}\n'
# '        source: {1}\n'
# '        direction: {2}\n'
# '        timestring: {3}\n'
# '        unit: {4}\n'
# '        unit_count: {5}\n'
# '        field: {6}\n'
# '        stats_result: {7}\n'
# '        epoch: {8}\n')

global_client = elasticsearch.Elasticsearch(host=host, port=port)

delete_count_pattern = ('---\n'
'actions:\n'
'  1:\n'
'    description: "Delete indices as filtered"\n'
'    action: delete_indices\n'
'    options:\n'
'      continue_if_exception: False\n'
'      disable_action: False\n'
'    filters:\n'
'      - filtertype: count\n'
'        pattern: {0}\n'
'        use_age: {1}\n'
'        source: {2}\n'
'        timestring: {3}\n'
'        reverse: {4}\n'
'        count: {5}\n')

class TestCLICountPattern(CuratorTestCase):
    def test_match_proper_indices(self):
        for i in range(1, 4):
            self.create_index('a-{0}'.format(i))
        for i in range(4, 7):
            self.create_index('b-{0}'.format(i))
        for i in range(5, 9):
            self.create_index('c-{0}'.format(i))
        self.create_index('not_a_match')
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(
            self.args['actionfile'],
            delete_count_pattern.format(
                '\'^(a|b|c)-\d$\'', 'false', 'name', '\'%Y.%m.%d\'', 'true', 1
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
        indices = sorted(list(self.client.indices.get('_all')))
        self.assertEquals(['a-3', 'b-6', 'c-8', 'not_a_match'], indices)
    def test_match_proper_indices_by_age(self):
        self.create_index('a-2017.10.01')
        self.create_index('a-2017.10.02')
        self.create_index('a-2017.10.03')
        self.create_index('b-2017.09.01')
        self.create_index('b-2017.09.02')
        self.create_index('b-2017.09.03')
        self.create_index('not_a_match')
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(
            self.args['actionfile'],
            delete_count_pattern.format(
                '\'^(a|b)-\d{4}\.\d{2}\.\d{2}$\'', 'true', 'name', '\'%Y.%m.%d\'', 'true', 1
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
        indices = sorted(list(self.client.indices.get('_all')))
        self.assertEquals(['a-2017.10.03', 'b-2017.09.03', 'not_a_match'], indices)