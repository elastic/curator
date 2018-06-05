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

class TestActionFileforceMerge(CuratorTestCase):
    def test_merge(self):
        count = 1
        idx = 'my_index'
        self.create_index(idx)
        self.add_docs(idx)
        ilo1 = curator.IndexList(self.client)
        ilo1._get_segment_counts()
        self.assertEqual(3, ilo1.index_info[idx]['segments'])
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(self.args['actionfile'],
            testvars.forcemerge_test.format(count, 0.20))
        test = clicktest.CliRunner()
        _ = test.invoke(
                    curator.cli,
                    [
                        '--config', self.args['configfile'],
                        self.args['actionfile']
                    ],
                    )
        ilo2 = curator.IndexList(self.client)
        ilo2._get_segment_counts()
        self.assertEqual(count, ilo2.index_info[idx]['segments'])
    def test_extra_option(self):
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(self.args['actionfile'],
            testvars.bad_option_proto_test.format('forcemerge'))
        test = clicktest.CliRunner()
        result = test.invoke(
                    curator.cli,
                    [
                        '--config', self.args['configfile'],
                        self.args['actionfile']
                    ],
                    )
        self.assertEqual(-1, result.exit_code)

class TestCLIforceMerge(CuratorTestCase):
    def test_merge(self):
        count = 1
        idx = 'my_index'
        self.create_index(idx)
        self.add_docs(idx)
        ilo1 = curator.IndexList(self.client)
        ilo1._get_segment_counts()
        self.assertEqual(3, ilo1.index_info[idx]['segments'])
        args = self.get_runner_args()
        args += [
            '--config', self.args['configfile'],
            'forcemerge',
            '--max_num_segments', str(count),
            '--delay', '0.20',
            '--filter_list', '{"filtertype":"pattern","kind":"prefix","value":"my"}',
        ]
        self.assertEqual(0, self.run_subprocess(args, logname='TestCLIforceMerge.test_merge'))
        ilo2 = curator.IndexList(self.client)
        ilo2._get_segment_counts()
        self.assertEqual(count, ilo2.index_info[idx]['segments'])