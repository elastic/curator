import elasticsearch
import curator
import os
import click
from click import testing as clicktest

from . import CuratorTestCase
from . import testvars as testvars

import logging
logger = logging.getLogger(__name__)

host, port = os.environ.get('TEST_ES_SERVER', 'localhost:9200').split(':')
port = int(port) if port else 9200

class TestActionFileFreeze(CuratorTestCase):
    def test_freeze_unfrozen(self):
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(self.args['actionfile'],
            testvars.optionless_proto.format('freeze'))

        version = curator.utils.get_version(self.client)
        if version <= (6, 6, 0):
            expected = 1
            # Freeze / Unfreeze not supported before 6.6
            self.assertEqual(expected, 1)
            return
        
        self.create_index('my_index')
        self.create_index('dummy')
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
            "true",
            cluster_state['metadata']['indices']['my_index']['settings']['index'].get('frozen', "false")
        )
        
        self.assertEqual(
            "false",
            cluster_state['metadata']['indices']['dummy']['settings']['index'].get('frozen', "false")
        )
        
    def test_freeze_frozen(self):
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(self.args['actionfile'],
            testvars.optionless_proto.format('freeze'))

        version = curator.utils.get_version(self.client)
        if version <= (6, 6, 0):
            expected = 1
            # Freeze / Unfreeze not supported before 6.6
            self.assertEqual(expected, 1)
            return

        self.create_index('my_index')
        # pylint: disable=E1123
        self.client.xpack.indices.freeze(
            index='my_index')
        self.create_index('dummy')
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
            "true",
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
            testvars.bad_option_proto_test.format('freeze'))
        self.create_index('my_index')
        self.create_index('dummy')

        version = curator.utils.get_version(self.client)
        if version <= (6, 6, 0):
            expected = 1
            # Freeze / Unfreeze not supported before 6.6
            self.assertEqual(expected, 1)
            return

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
            "false",
            cluster_state['metadata']['indices']['my_index']['settings']['index'].get('frozen', "false")
        )
        
        self.assertEqual(
            "false",
            cluster_state['metadata']['indices']['dummy']['settings']['index'].get('frozen', "false")
        )

class TestCLIFreeze(CuratorTestCase):
    def test_freeze_unfrozen(self):
        index = 'my_index'

        version = curator.utils.get_version(self.client)
        if version <= (6, 6, 0):
            expected = 1
            # Freeze / Unfreeze not supported before 6.6
            self.assertEqual(expected, 1)
            return

        self.create_index(index)
        self.create_index('dummy')
        self.create_index('my_other')
        cluster_state = self.client.cluster.state(
                metric='metadata',
            )
        self.assertEqual(
            "false",
            cluster_state['metadata']['indices']['my_index']['settings']['index'].get('frozen', "false")
        )

        self.assertEqual(
            "false",
            cluster_state['metadata']['indices']['my_other']['settings']['index'].get('frozen', "false")
        )
        
        # Now freeze `index` (dummy stays unfrozen)
        
        args = self.get_runner_args()
        args += [
            '--config', self.args['configfile'],
            'freeze',
            '--filter_list', '{"filtertype":"pattern","kind":"prefix","value":"my"}',
        ]
        self.assertEqual(0, self.run_subprocess(args, logname='TestCLIFreeze.test_freeze_unfrozen'))
        cluster_state = self.client.cluster.state(
                metric='metadata',
            )
        self.assertEqual(
            "true",
            cluster_state['metadata']['indices']['my_index']['settings']['index'].get('frozen', "false")
        )

        self.assertEqual(
            "true",
            cluster_state['metadata']['indices']['my_other']['settings']['index'].get('frozen', "false")
        )
        
        self.assertEqual(
            "false",
            cluster_state['metadata']['indices']['dummy']['settings']['index'].get('frozen', "false")
        )

