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

class TestCLIDeleteIndices(CuratorTestCase):
    def test_name_older_than_now(self):
        self.create_indices(10)
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(self.args['actionfile'],
            testvars.delete_proto.format(
                'age', 'name', 'older', '\'%Y.%m.%d\'', 'days', 5, ' ', ' ', ' '
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
        self.assertEquals(5, len(curator.get_indices(self.client)))
    def test_creation_date_newer_than_epoch(self):
        self.create_indices(10)
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(self.args['actionfile'],
            testvars.delete_proto.format(
                'age', 'creation_date', 'younger', ' ', 'seconds', 0,
                ' ', ' ', int(time.time()) - 60
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
    def test_empty_list(self):
        self.create_indices(10)
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(self.args['actionfile'],
            testvars.delete_proto.format(
                'age', 'creation_date', 'older', ' ', 'days', 90,
                ' ', ' ', int(time.time())
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
        self.assertEquals(10, len(curator.get_indices(self.client)))
        self.assertEqual(1, result.exit_code)
    def test_ignore_empty_list(self):
        self.create_indices(10)
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(self.args['actionfile'],
            testvars.delete_ignore_proto.format(
                'age', 'creation_date', 'older', ' ', 'days', 90,
                ' ', ' ', int(time.time())
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
        self.assertEquals(10, len(curator.get_indices(self.client)))
        self.assertEqual(0, result.exit_code)
    def test_extra_options(self):
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(self.args['actionfile'],
            testvars.bad_option_proto_test.format('delete_indices'))
        test = clicktest.CliRunner()
        result = test.invoke(
                    curator.cli,
                    [
                        '--config', self.args['configfile'],
                        self.args['actionfile']
                    ],
                    )
        self.assertEqual(-1, result.exit_code)

class TestCLIFixFor687(CuratorTestCase):
    @unittest.skipIf(curator.get_version(global_client) >= (3, 0, 0),
                     'not supported for this version of ES')
    def test_fix_for_687(self):
        self.create_repository()
        snap_name = 'test687'
        # 7 june (week 23)
        self.client.indices.create(
            index='logstash-2016.23',
            body={
                'settings': {
                    'creation_date': 1465293737000,
                    'number_of_shards': 1, 'number_of_replicas': 0
                }
            }
        )
        # 14 june (week 24)
        self.client.indices.create(
            index='logstash-2016.24',
            body={
                'settings': {
                    'creation_date': 1465898537000,
                    'number_of_shards': 1, 'number_of_replicas': 0
                }
            }
        )
        # 21 june (week 25)
        self.client.indices.create(
            index='logstash-2016.25',
            body={
                'settings': {
                    'creation_date': 1466503337000,
                    'number_of_shards': 1, 'number_of_replicas': 0
                }
            }
        )
        # 28 july (week 26)
        self.client.indices.create(
            index='logstash-2016.26',
            body={
                'settings': {
                    'creation_date': 1467108137000,
                    'number_of_shards': 1, 'number_of_replicas': 0
                }
            }
        )
        # 5 july (week 27)
        self.client.indices.create(
            index='logstash-2016.27',
            body={
                'settings': {
                    'creation_date': 1467712937000,
                    'number_of_shards': 1, 'number_of_replicas': 0
                }
            }
        )
        # 12 july (week 28)
        self.client.indices.create(
            index='logstash-2016.28',
            body={
                'settings': {
                    'creation_date': 1468317737000,
                    'number_of_shards': 1, 'number_of_replicas': 0
                }
            }
        )
        self.client.cluster.health(wait_for_status='yellow')
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(self.args['actionfile'],
            testvars.test_687.format(self.args['repository'], snap_name))
        test = clicktest.CliRunner()
        result = test.invoke(
                    curator.cli,
                    [
                        '--config', self.args['configfile'],
                        self.args['actionfile']
                    ],
                    )
        snapshot = curator.get_snapshot(
                    self.client, self.args['repository'], '_all'
                   )
        self.assertEquals(6, len(curator.get_indices(self.client)))
