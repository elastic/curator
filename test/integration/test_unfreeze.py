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

class TestActionFileUnfreezeFrozen(CuratorTestCase):
    def test_unfreeze_frozen(self):
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(self.args['actionfile'],
            testvars.optionless_proto.format('unfreeze'))

        version = curator.utils.get_version(self.client)        
        if version <= (6, 6, 0):
            expected = 1
            # Freeze / Unfreeze not supported before 6.6
            self.assertEqual(expected, 1)
            return
        
        t1, t2 = ('dummy', 'my_index')
        self.create_index(t1)
        self.create_index(t2)
        # Decorators make this pylint exception necessary
        # pylint: disable=E1123
        self.client.xpack.indices.freeze(index=t2)
        cluster_state = self.client.cluster.state(
                metric='metadata',
            )

        self.assertEqual(
            "true",
            cluster_state['metadata']['indices']['my_index']['settings']['index'].get('frozen', "false")
        )
        
        self.assertEqual(
            "false",
            cluster_state['metadata']['indices']['dummy']['settings']['index'].get('frozen', "false")
        )        
        
        test = clicktest.CliRunner()
        _ = test.invoke(
                    curator.cli,
                    [
                        '--config', self.args['configfile'],
                        self.args['actionfile']
                    ],
                    )
        cluster_state = self.client.cluster.state(
                metric='metadata',
            )

        self.assertEqual(
            "false",
            cluster_state['metadata']['indices']['my_index']['settings']['index'].get('frozen', "false")
        )
        self.assertEqual(
            "false",
            cluster_state['metadata']['indices']['dummy']['settings']['index'].get('frozen', "false")
        )
                    
    def test_extra_option(self):
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(self.args['actionfile'],
            testvars.bad_option_proto_test.format('unfreeze'))

        version = curator.utils.get_version(self.client)        
        if version <= (6, 6, 0):
            expected = 1
            # Freeze / Unfreeze not supported before 6.6
            self.assertEqual(expected, 1)
            return
        
        t1, t2 = ('dummy', 'my_index')
        self.create_index(t1)
        self.create_index(t2)
        # Decorators make this pylint exception necessary
        # pylint: disable=E1123
        self.client.xpack.indices.freeze(index=t2, ignore_unavailable=True)
        cluster_state = self.client.cluster.state(
                metric='metadata',
            )
        self.assertEqual(
            "true",
            cluster_state['metadata']['indices']['my_index']['settings']['index'].get('frozen', "false")
        )
        
        self.assertEqual(
            "false",
            cluster_state['metadata']['indices']['dummy']['settings']['index'].get('frozen', "false")
        )
                
        test = clicktest.CliRunner()
        result = test.invoke(
                    curator.cli,
                    [
                        '--config', self.args['configfile'],
                        self.args['actionfile']
                    ],
                    )
        cluster_state = self.client.cluster.state(
                metric='metadata',
            )
        self.assertEqual(
            "true",
            cluster_state['metadata']['indices']['my_index']['settings']['index'].get('frozen', "false")
        )
        
        self.assertEqual(
            "false",
            cluster_state['metadata']['indices']['dummy']['settings']['index'].get('frozen', "false")
        )                    
        self.assertEqual(-1, result.exit_code)

class TestCLIUnfreezeFrozen(CuratorTestCase):
    def test_unfreeze_frozen(self):
        version = curator.utils.get_version(self.client)
        if version <= (6, 6, 0):
            expected = 1
            # Freeze / Unfreeze not supported before 6.6
            self.assertEqual(expected, 1)
            return
        
        t1, t2 = ('dummy', 'my_index')
        self.create_index(t1)
        self.create_index(t2)
        # Decorators make this pylint exception necessary
        # pylint: disable=E1123
        self.client.xpack.indices.freeze(index=t2)
        args = self.get_runner_args()
        args += [
            '--config', self.args['configfile'],
            'unfreeze',
            '--filter_list', '{"filtertype":"pattern","kind":"prefix","value":"my"}',
        ]
        self.assertEqual(0, self.run_subprocess(args, logname='TestCLIUnfreezeFrozen.test_unfreeze_frozen'))
        cluster_state = self.client.cluster.state(
                metric='metadata',
            )
        self.assertEqual(
            "false",
            cluster_state['metadata']['indices']['my_index']['settings']['index'].get('frozen', "false")
        )
        self.assertEqual(
            "false",
            cluster_state['metadata']['indices']['dummy']['settings']['index'].get('frozen', "false")
        )                    

        
