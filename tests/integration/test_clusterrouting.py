"""Test the Cluster Routing Action"""
# pylint: disable=missing-function-docstring, missing-class-docstring, line-too-long
import os
from . import CuratorTestCase
from . import testvars

HOST = os.environ.get('TEST_ES_SERVER', 'http://127.0.0.1:9200')

class TestCLIClusterRouting(CuratorTestCase):
    def test_allocation_all(self):
        routing_type = 'allocation'
        value = 'all'
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(self.args['actionfile'], testvars.cluster_routing_test.format(routing_type, value))
        self.create_index('my_index')
        self.create_index('not_my_index')
        self.invoke_runner()
        assert testvars.CRA_all == self.client.cluster.get_settings()
    def test_extra_option(self):
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(self.args['actionfile'], testvars.bad_option_proto_test.format('cluster_routing'))
        self.create_index('my_index')
        self.create_index('not_my_index')
        self.invoke_runner()
        assert 1 == self.result.exit_code
