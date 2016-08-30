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

class TestCLIClose(CuratorTestCase):
    def test_close_opened(self):
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(self.args['actionfile'],
            testvars.optionless_proto.format('close'))
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
        self.assertEquals(
            'close',
            self.client.cluster.state(
                index='my_index',
                metric='metadata',
            )['metadata']['indices']['my_index']['state']
        )
        self.assertNotEqual(
            'close',
            self.client.cluster.state(
                index='dummy',
                metric='metadata',
            )['metadata']['indices']['dummy']['state']
        )
    def test_close_closed(self):
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(self.args['actionfile'],
            testvars.optionless_proto.format('close'))
        self.create_index('my_index')
        self.client.indices.close(
            index='my_index', ignore_unavailable=True)
        self.create_index('dummy')
        test = clicktest.CliRunner()
        result = test.invoke(
                    curator.cli,
                    [
                        '--config', self.args['configfile'],
                        self.args['actionfile']
                    ],
                    )
        self.assertEquals(
            'close',
            self.client.cluster.state(
                index='my_index',
                metric='metadata',
            )['metadata']['indices']['my_index']['state']
        )
        self.assertNotEqual(
            'close',
            self.client.cluster.state(
                index='dummy',
                metric='metadata',
            )['metadata']['indices']['dummy']['state']
        )
    def test_close_delete_aliases(self):
        # Create aliases first
        alias = 'testalias'
        index = 'my_index'
        self.create_index(index)
        self.create_index('dummy')
        self.create_index('my_other')
        self.client.indices.put_alias(index='my_index,dummy', name=alias)
        self.assertEquals(
            {
                "dummy":{"aliases":{"testalias":{}}},
                "my_index":{"aliases":{"testalias":{}}}
            },
            self.client.indices.get_alias(name=alias)
        )
        # Now close `index` with delete_aliases=True (dummy stays open)
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(self.args['actionfile'],
            testvars.close_delete_aliases)
        test = clicktest.CliRunner()
        result = test.invoke(
                    curator.cli,
                    [
                        '--config', self.args['configfile'],
                        self.args['actionfile']
                    ],
                    )
        self.assertEquals(
            'close',
            self.client.cluster.state(
                index=index,
                metric='metadata',
            )['metadata']['indices'][index]['state']
        )
        self.assertEquals(
            'close',
            self.client.cluster.state(
                index='my_other',
                metric='metadata',
            )['metadata']['indices']['my_other']['state']
        )
        # Now open the indices and verify that the alias is still gone.
        self.client.indices.open(index=index)
        self.assertEquals(
            {"dummy":{"aliases":{"testalias":{}}}},
            self.client.indices.get_alias(name=alias)
        )
    def test_extra_option(self):
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(self.args['actionfile'],
            testvars.bad_option_proto_test.format('close'))
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
        self.assertNotEqual(
            'close',
            self.client.cluster.state(
                index='my_index',
                metric='metadata',
            )['metadata']['indices']['my_index']['state']
        )
        self.assertNotEqual(
            'close',
            self.client.cluster.state(
                index='dummy',
                metric='metadata',
            )['metadata']['indices']['dummy']['state']
        )
        self.assertEqual(-1, result.exit_code)
