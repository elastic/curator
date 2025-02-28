"""Integration tests of the Close action class"""

# pylint: disable=C0115, C0116, invalid-name
import os
from . import CuratorTestCase
from . import testvars

HOST = os.environ.get('TEST_ES_SERVER', 'http://127.0.0.1:9200')

MET = 'metadata'
IDC = 'indices'
STA = 'state'


def get_state(client, data):
    """Return cluster state data"""
    return client.cluster.state(index=data, metric=MET)


class TestActionFileClose(CuratorTestCase):
    def test_close_opened(self):
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(
            self.args['actionfile'], testvars.optionless_proto.format('close')
        )
        idx1, idx2 = ('dummy', 'my_index')
        indices = f'{idx1},{idx2}'
        self.create_index(idx1)
        self.create_index(idx2)
        self.invoke_runner()
        state = get_state(self.client, indices)
        assert 'close' != state[MET][IDC][idx1][STA]
        assert 'close' == state[MET][IDC][idx2][STA]

    def test_close_closed(self):
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(
            self.args['actionfile'], testvars.optionless_proto.format('close')
        )
        idx1, idx2 = ('dummy', 'my_index')
        indices = f'{idx1},{idx2}'
        self.create_index(idx2)
        # pylint: disable=E1123
        self.client.indices.close(
            index=idx2, ignore_unavailable=True, wait_for_active_shards=0
        )
        self.create_index(idx1)
        self.invoke_runner()
        state = get_state(self.client, indices)
        assert 'close' != state[MET][IDC][idx1][STA]
        assert 'close' == state[MET][IDC][idx2][STA]

    def test_close_delete_aliases(self):
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(self.args['actionfile'], testvars.close_delete_aliases)
        # Create aliases first
        alias = 'testalias'
        idxtuple = ('dummy', 'my_index', 'my_other')
        idx1, idx2, idx3 = idxtuple
        indices = None
        for idx in idxtuple:
            self.create_index(idx)
            indices = f'{indices},{idx}' if indices else f'{idx}'
        self.client.indices.put_alias(index=f'{idx1},{idx2}', name=alias)
        precheck = {idx1: {"aliases": {alias: {}}}, idx2: {"aliases": {alias: {}}}}
        assert precheck == self.client.indices.get_alias(name=alias)
        # Now close `index` with delete_aliases=True (dummy stays open)
        self.invoke_runner()
        state = get_state(self.client, indices)
        for idx in (idx2, idx3):
            assert 'close' == state[MET][IDC][idx][STA]
        # Now open the indices and verify that the alias is still gone.
        self.client.indices.open(index=idx2)
        assert {idx1: {"aliases": {alias: {}}}} == self.client.indices.get_alias(
            name=alias
        )

    def test_close_skip_flush(self):
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(self.args['actionfile'], testvars.close_skip_flush)
        idx1, idx2 = ('dummy', 'my_index')
        indices = f'{idx1},{idx2}'
        self.create_index(idx1)
        # Disable shard allocation to make my_index go red
        disable_allocation = {"cluster.routing.allocation.enable": "none"}
        self.client.cluster.put_settings(transient=disable_allocation)
        self.create_index(idx2, wait_for_yellow=False, wait_for_active_shards=0)
        self.invoke_runner()
        state = get_state(self.client, indices)
        assert 'close' != state[MET][IDC][idx1][STA]
        assert 'close' == state[MET][IDC][idx2][STA]

    def test_extra_option(self):
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(
            self.args['actionfile'], testvars.bad_option_proto_test.format('close')
        )
        idx1, idx2 = ('dummy', 'my_index')
        indices = f'{idx1},{idx2}'
        self.create_index(idx1)
        self.create_index(idx2)
        self.invoke_runner()
        state = get_state(self.client, indices)
        for idx in (idx1, idx2):
            assert 'close' != state[MET][IDC][idx][STA]
        assert 1 == self.result.exit_code


class TestCLIClose(CuratorTestCase):
    def test_close_delete_aliases(self):
        # Create aliases first
        alias = 'testalias'
        idx1, idx2, idx3 = ('my_index', 'dummy', 'my_other')
        indices = f'{idx1},{idx2},{idx3}'
        self.create_index(idx1)
        self.create_index(idx2)
        self.create_index(idx3)
        self.client.indices.put_alias(index=f'{idx1},{idx2}', name=alias)
        precheck = {idx1: {"aliases": {alias: {}}}, idx2: {"aliases": {alias: {}}}}
        assert precheck == self.client.indices.get_alias(name=alias)
        # Now close `index` with delete_aliases=True idx2 stays open)
        args = self.get_runner_args()
        args += [
            '--config',
            self.args['configfile'],
            'close',
            '--delete_aliases',
            '--filter_list',
            '{"filtertype":"pattern","kind":"prefix","value":"my"}',
        ]
        assert 0 == self.run_subprocess(
            args, logname='TestCLIClose.test_close_delete_aliases'
        )
        state = get_state(self.client, indices)
        for idx in (idx1, idx3):
            assert 'close' == state[MET][IDC][idx][STA]
        # Now open the indices and verify that the alias is still gone.
        self.client.indices.open(index=idx1)
        assert {idx2: {"aliases": {alias: {}}}} == self.client.indices.get_alias(
            name=alias
        )

    def test_close_skip_flush(self):
        args = self.get_runner_args()
        args += [
            '--config',
            self.args['configfile'],
            'close',
            '--skip_flush',
            '--filter_list',
            '{"filtertype":"pattern","kind":"prefix","value":"my"}',
        ]
        idx1, idx2 = ('my_index', 'dummy')
        indices = f'{idx1},{idx2}'
        self.create_index(idx1)
        self.create_index(idx2)
        assert 0 == self.run_subprocess(
            args, logname='TestCLIClose.test_close_skip_flush'
        )
        state = get_state(self.client, indices)
        assert 'close' == state[MET][IDC][idx1][STA]
