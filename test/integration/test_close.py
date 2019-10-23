"""Integration tests of the Close action class"""
import os
import logging
import elasticsearch
import click
from click import testing as clicktest
import curator

from . import CuratorTestCase
from . import testvars

HOST, PORT = os.environ.get('TEST_ES_SERVER', 'localhost:9200').split(':')
PORT = int(PORT) if PORT else 9200

class TestActionFileClose(CuratorTestCase):
    """Tests of the Close action class"""
    def test_close_opened(self):
        """Test if it can close opened indices"""
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST, PORT))
        self.write_config(self.args['actionfile'], testvars.optionless_proto.format('close'))
        self.create_index('my_index')
        self.create_index('dummy')
        test = clicktest.CliRunner()
        _ = test.invoke(
            curator.cli,
            [
                '--config',
                self.args['configfile'],
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
        """Test if it will close/ignore already closed indices"""
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST, PORT))
        self.write_config(self.args['actionfile'], testvars.optionless_proto.format('close'))
        self.create_index('my_index')
        # pylint: disable=E1123
        self.client.indices.close(
            index='my_index', ignore_unavailable=True)
        self.create_index('dummy')
        test = clicktest.CliRunner()
        _ = test.invoke(
            curator.cli,
            [
                '--config',
                self.args['configfile'],
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
        """Test if it can delete aliases from an index before closing"""
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
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST, PORT))
        self.write_config(self.args['actionfile'], testvars.close_delete_aliases)
        test = clicktest.CliRunner()
        _ = test.invoke(
            curator.cli,
            [
                '--config',
                self.args['configfile'],
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

    def test_close_skip_flush(self):
        """Test if it will skip the synced flush if so flagged"""
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST, PORT))
        self.write_config(self.args['actionfile'], testvars.close_skip_flush)
        self.create_index('dummy')
        # Disable shard allocation to make my_index go red
        disable_allocation = '{"transient":{"cluster.routing.allocation.enable":"none"}}'
        self.client.cluster.put_settings(body=disable_allocation)
        self.create_index('my_index', wait_for_yellow=False, wait_for_active_shards=0)
        test = clicktest.CliRunner()
        _ = test.invoke(
            curator.cli,
            [
                '--config',
                self.args['configfile'],
                self.args['actionfile']
            ]
        )
        try:
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
        finally:
            # re-enable shard allocation for next tests
            enable_allocation = '{"transient":{"cluster.routing.allocation.enable":null}}'
            self.client.cluster.put_settings(body=enable_allocation)

    def test_close_ignore_sync_failures(self):
        """Test if it will ignore sync failures if so flagged"""
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST, PORT))
        self.write_config(self.args['actionfile'], testvars.close_ignore_sync.format('true'))
        self.create_index('dummy')
        # Disable shard allocation to make my_index go red
        disable_allocation = '{"transient":{"cluster.routing.allocation.enable":"none"}}'
        self.client.cluster.put_settings(body=disable_allocation)
        self.create_index('my_index', wait_for_yellow=False, wait_for_active_shards=0)
        test = clicktest.CliRunner()
        _ = test.invoke(
            curator.cli,
            [
                '--config',
                self.args['configfile'],
                self.args['actionfile']
            ]
        )
        try:
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
        finally:
            # re-enable shard allocation for next tests
            enable_allocation = '{"transient":{"cluster.routing.allocation.enable":null}}'
            self.client.cluster.put_settings(body=enable_allocation)

    def test_close_has_sync_failures(self):
        """Test if it will exit with an error if there are sync failures and not so flagged"""
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST, PORT))
        self.write_config(self.args['actionfile'], testvars.close_ignore_sync.format('false'))
        self.create_index('dummy')
        # Disable shard allocation to make my_index go red
        disable_allocation = '{"transient":{"cluster.routing.allocation.enable":"none"}}'
        self.client.cluster.put_settings(body=disable_allocation)
        self.create_index('my_index', wait_for_yellow=False, wait_for_active_shards=0)
        test = clicktest.CliRunner()
        _ = test.invoke(
            curator.cli,
            [
                '--config',
                self.args['configfile'],
                self.args['actionfile']
            ]
        )
        try:
            self.assertEquals(
                'open',
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
            self.assertEquals(1, _.exit_code)
        finally:
            # re-enable shard allocation for next tests
            enable_allocation = '{"transient":{"cluster.routing.allocation.enable":null}}'
            self.client.cluster.put_settings(body=enable_allocation)

    def test_extra_option(self):
        """Test if extra options cause an exit failure"""
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST, PORT))
        self.write_config(self.args['actionfile'], testvars.bad_option_proto_test.format('close'))
        self.create_index('my_index')
        self.create_index('dummy')
        test = clicktest.CliRunner()
        result = test.invoke(
            curator.cli,
            [
                '--config',
                self.args['configfile'],
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

class TestCLIClose(CuratorTestCase):
    """Test curator_cli Close action functionality"""
    def test_close_delete_aliases(self):
        """Test if curator_cli will delete aliases when closing indices, if so flagged"""
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
        args = self.get_runner_args()
        args += [
            '--config', self.args['configfile'],
            'close',
            '--delete_aliases',
            '--filter_list', '{"filtertype":"pattern","kind":"prefix","value":"my"}',
        ]
        self.assertEqual(
            0,
            self.run_subprocess(args, logname='TestCLIClose.test_close_delete_aliases')
        )
        self.assertEquals(
            'close',
            self.client.cluster.state(
                index=index, metric='metadata')['metadata']['indices'][index]['state']
        )
        self.assertEquals(
            'close',
            self.client.cluster.state(
                index='my_other', metric='metadata')['metadata']['indices']['my_other']['state']
        )
        # Now open the indices and verify that the alias is still gone.
        self.client.indices.open(index=index)
        self.assertEquals(
            {"dummy":{"aliases":{"testalias":{}}}},
            self.client.indices.get_alias(name=alias)
        )
    def test_close_skip_flush(self):
        """Test if curator_cli will skip flush on close if so flagged"""
        args = self.get_runner_args()
        args += [
            '--config', self.args['configfile'],
            'close',
            '--skip_flush',
            '--filter_list', '{"filtertype":"pattern","kind":"prefix","value":"my"}',
        ]
        self.create_index('my_index')
        self.create_index('dummy')
        self.assertEqual(
            0,
            self.run_subprocess(args, logname='TestCLIClose.test_close_skip_flush')
        )
        self.assertEquals(
            'close',
            self.client.cluster.state(
                index='my_index', metric='metadata')['metadata']['indices']['my_index']['state']
        )
