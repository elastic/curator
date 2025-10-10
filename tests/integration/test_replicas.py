"""Test replica count changing functionality"""

# pylint: disable=C0115, C0116, invalid-name
import os
from . import CuratorTestCase
from . import testvars

HOST = os.environ.get('TEST_ES_SERVER', 'http://127.0.0.1:9200')


class TestActionFileReplicas(CuratorTestCase):
    def test_increase_count(self):
        count = 2
        idx = 'my_index'
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(self.args['actionfile'], testvars.replicas_test.format(count))
        self.create_index(idx)
        self.invoke_runner()
        assert count == int(
            self.client.indices.get_settings(index=idx)[idx]['settings']['index'][
                'number_of_replicas'
            ]
        )

    def test_no_count(self):
        self.create_index('foo')
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(self.args['actionfile'], testvars.replicas_test.format(' '))
        self.invoke_runner()
        assert 1 == self.result.exit_code

    def test_extra_option(self):
        self.create_index('foo')
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(
            self.args['actionfile'], testvars.bad_option_proto_test.format('replicas')
        )
        self.invoke_runner()
        assert 1 == self.result.exit_code


class TestCLIReplicas(CuratorTestCase):
    def test_increase_count(self):
        count = 2
        idx = 'my_index'
        self.create_index(idx)
        args = self.get_runner_args()
        args += [
            '--config',
            self.args['configfile'],
            'replicas',
            '--count',
            str(count),
            '--filter_list',
            '{"filtertype":"pattern","kind":"prefix","value":"my"}',
        ]
        assert 0 == self.run_subprocess(
            args, logname='TestCLIOpenClosed.test_open_closed'
        )
        assert count == int(
            self.client.indices.get_settings(index=idx)[idx]['settings']['index'][
                'number_of_replicas'
            ]
        )
