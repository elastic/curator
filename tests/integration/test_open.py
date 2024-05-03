"""Test index opening functionality"""
# pylint: disable=missing-function-docstring, missing-class-docstring, line-too-long
import os
from . import CuratorTestCase
from . import testvars

HOST = os.environ.get('TEST_ES_SERVER', 'http://127.0.0.1:9200')
MET = 'metadata'

class TestActionFileOpenClosed(CuratorTestCase):
    def test_open_closed(self):
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(self.args['actionfile'], testvars.optionless_proto.format('open'))
        idx1, idx2 = ('dummy', 'my_index')
        self.create_index(idx1)
        self.create_index(idx2)
        self.client.indices.close(index=idx2, ignore_unavailable=True)
        self.invoke_runner()
        csi = self.client.cluster.state(metric=MET)[MET]['indices']
        for idx in (idx1, idx2):
            assert 'close' !=  csi[idx]['state']
    def test_extra_option(self):
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(self.args['actionfile'], testvars.bad_option_proto_test.format('open'))
        idx1, idx2 = ('dummy', 'my_index')
        self.create_index(idx1)
        self.create_index(idx2)
        self.client.indices.close(index=idx2, ignore_unavailable=True)
        self.invoke_runner()
        csi = self.client.cluster.state(metric=MET)[MET]['indices']
        assert 'close' != csi[idx1]['state']
        assert 'close' == csi[idx2]['state']
        assert 1 == self.result.exit_code

class TestCLIOpenClosed(CuratorTestCase):
    def test_open_closed(self):
        idx1, idx2 = ('dummy', 'my_index')
        self.create_index(idx1)
        self.create_index(idx2)
        self.client.indices.close(index=idx2, ignore_unavailable=True)
        args = self.get_runner_args()
        args += [
            '--config', self.args['configfile'],
            'open',
            '--filter_list', '{"filtertype":"pattern","kind":"prefix","value":"my"}',
        ]
        assert 0 == self.run_subprocess(args, logname='TestCLIOpenClosed.test_open_closed')
        csi = self.client.cluster.state(metric=MET)[MET]['indices']
        for idx in (idx1, idx2):
            assert 'close' != csi[idx]['state']
