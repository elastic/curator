"""Test Alias action"""

# pylint: disable=C0115, C0116, invalid-name
import os
import logging
from datetime import datetime, timedelta, timezone
import pytest
from elasticsearch8.exceptions import NotFoundError
from . import CuratorTestCase
from . import testvars

LOGGER = logging.getLogger('test_alias')

HOST = os.environ.get('TEST_ES_SERVER', 'http://127.0.0.1:9200')


class TestActionFileAlias(CuratorTestCase):
    def test_add_only(self):
        alias = 'testalias'
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(
            self.args['actionfile'], testvars.alias_add_only.format(alias)
        )
        self.create_index('my_index')
        self.create_index('dummy')
        self.client.indices.put_alias(index='dummy', name=alias)
        self.invoke_runner()
        assert 2 == len(self.client.indices.get_alias(name=alias))

    def test_add_only_with_extra_settings(self):
        alias = 'testalias'
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(
            self.args['actionfile'],
            testvars.alias_add_only_with_extra_settings.format(alias),
        )
        self.create_index('my_index')
        self.invoke_runner()
        expected = {
            'my_index': {
                'aliases': {'testalias': {'filter': {'term': {'user': 'kimchy'}}}}
            }
        }
        assert expected == self.client.indices.get_alias(name=alias)

    def test_alias_remove_only(self):
        alias = 'testalias'
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(
            self.args['actionfile'], testvars.alias_remove_only.format(alias)
        )
        idx1, idx2 = ('my_index', 'dummy')
        self.create_index(idx1)
        self.create_index(idx2)
        self.client.indices.put_alias(index=idx2, name=alias)
        self.invoke_runner()
        expected = {idx2: {'aliases': {}}, idx1: {'aliases': {}}}
        assert expected == self.client.indices.get_alias(index=f'{idx1},{idx2}')

    def test_add_only_skip_closed(self):
        alias = 'testalias'
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(
            self.args['actionfile'], testvars.alias_add_only.format(alias)
        )
        idx1, idx2 = ('dummy', 'my_index')
        self.create_index(idx2)
        self.client.indices.close(index=idx2, wait_for_active_shards=0)
        self.create_index(idx1)
        self.client.indices.put_alias(index=idx1, name=alias)
        self.invoke_runner()
        assert 2 == len(self.client.indices.get_alias(name=alias))

    def test_add_and_remove(self):
        alias = 'testalias'
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(
            self.args['actionfile'], testvars.alias_add_remove.format(alias)
        )
        idx1, idx2 = ('dummy', 'my_index')
        self.create_index(idx1)
        self.create_index(idx2)
        self.client.indices.put_alias(index=idx1, name=alias)
        self.invoke_runner()
        expected = {idx2: {'aliases': {alias: {}}}}
        assert expected == self.client.indices.get_alias(name=alias)

    def test_add_and_remove_datemath(self):
        alias = '<testalias-{now-1d/d}>'
        _ = (datetime.now(timezone.utc) - timedelta(days=1)).strftime('%Y.%m.%d')
        alias_parsed = f"testalias-{_}"
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(
            self.args['actionfile'], testvars.alias_add_remove.format(alias)
        )
        idx1, idx2 = ('dummy', 'my_index')
        self.create_index(idx1)
        self.create_index(idx2)
        self.client.indices.put_alias(index=idx1, name=alias_parsed)
        self.invoke_runner()
        expected = {idx2: {'aliases': {alias_parsed: {}}}}
        assert expected == self.client.indices.get_alias(name=alias_parsed)

    def test_add_with_empty_remove(self):
        alias = 'testalias'
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(
            self.args['actionfile'], testvars.alias_add_with_empty_remove.format(alias)
        )
        idx1, idx2 = ('dummy', 'my_index')
        self.create_index(idx1)
        self.create_index(idx2)
        self.client.indices.put_alias(index=idx1, name=alias)
        self.invoke_runner()
        expected = {idx1: {'aliases': {alias: {}}}, idx2: {'aliases': {alias: {}}}}
        assert expected == self.client.indices.get_alias(name=alias)

    def test_remove_with_empty_add(self):
        alias = 'testalias'
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(
            self.args['actionfile'], testvars.alias_remove_with_empty_add.format(alias)
        )
        idx1, idx2 = ('dummy', 'my_index')
        self.create_index(idx1)
        self.create_index(idx2)
        self.client.indices.put_alias(index=f'{idx1},{idx2}', name=alias)
        self.invoke_runner()
        expected = {idx2: {'aliases': {alias: {}}}}
        assert expected == self.client.indices.get_alias(name=alias)

    def test_add_with_empty_list(self):
        alias = 'testalias'
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(
            self.args['actionfile'],
            testvars.alias_add_remove_empty.format(alias, 'du', 'rickroll'),
        )
        idx1, idx2 = ('dummy', 'my_index')
        self.create_index(idx1)
        self.create_index(idx2)
        self.client.indices.put_alias(index=idx1, name=alias)
        self.invoke_runner()
        expected = {idx1: {'aliases': {alias: {}}}}
        assert expected == self.client.indices.get_alias(name=alias)

    def test_remove_with_empty_list(self):
        alias = 'testalias'
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(
            self.args['actionfile'],
            testvars.alias_add_remove_empty.format(alias, 'rickroll', 'my'),
        )
        idx1, idx2 = ('dummy', 'my_index')
        self.create_index(idx1)
        self.create_index(idx2)
        self.client.indices.put_alias(index=idx1, name=alias)
        self.invoke_runner()
        expected = {idx1: {'aliases': {alias: {}}}}
        assert expected == self.client.indices.get_alias(name=alias)

    def test_remove_index_not_in_alias(self):
        alias = 'testalias'
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(
            self.args['actionfile'],
            testvars.alias_remove_index_not_there.format(alias, 'my'),
        )
        idx1, idx2 = ('my_index1', 'my_index2')
        self.create_index(idx1)
        self.create_index(idx2)
        self.client.indices.put_alias(index=idx1, name=alias)
        self.invoke_runner()
        with pytest.raises(NotFoundError):
            self.client.indices.get_alias(name=alias)
        assert 0 == self.result.exit_code

    def test_no_add_remove(self):
        alias = 'testalias'
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(
            self.args['actionfile'], testvars.alias_no_add_remove.format(alias)
        )
        idx1, idx2 = ('dummy', 'my_index')
        self.create_index(idx1)
        self.create_index(idx2)
        self.invoke_runner()
        assert 1 == self.result.exit_code

    def test_no_alias(self):
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(self.args['actionfile'], testvars.alias_no_alias)
        idx1, idx2 = ('dummy', 'my_index')
        self.create_index(idx1)
        self.create_index(idx2)
        self.invoke_runner()
        assert 1 == self.result.exit_code

    def test_extra_options(self):
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(
            self.args['actionfile'], testvars.bad_option_proto_test.format('alias')
        )
        self.invoke_runner()
        assert 1 == self.result.exit_code

    def test_add_and_remove_sorted(self):
        alias = 'testalias'
        alias_add_remove = (
            '---\n'
            'actions:\n'
            '  1:\n'
            '    description: "Add/remove specified indices from designated alias"\n'
            '    action: alias\n'
            '    options:\n'
            '      name: {0}\n'
            '      continue_if_exception: False\n'
            '      disable_action: False\n'
            '    add:\n'
            '      filters:\n'
            '        - filtertype: pattern\n'
            '          kind: prefix\n'
            '          value: dum\n'
            '    remove:\n'
            '      filters:\n'
            '        - filtertype: pattern\n'
            '          kind: prefix\n'
            '          value: my\n'
        )
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(self.args['actionfile'], alias_add_remove.format(alias))
        idx1, idx2 = ('dummy', 'my_index')
        self.create_index(idx1)
        self.create_index(idx2)
        self.client.indices.put_alias(index=idx2, name=alias)
        self.invoke_runner()
        expected = {idx1: {'aliases': {alias: {}}}}
        assert expected == self.client.indices.get_alias(name=alias)


class TestCLIAlias(CuratorTestCase):
    """Test CLI Singleton Alias action"""

    def test_add_and_remove_alias(self):
        """test_add_and_remove_alias"""
        alias = 'testalias'
        idx1, idx2 = ('dummy', 'my_index')
        self.create_index(idx1)
        self.create_index(idx2)
        self.client.indices.put_alias(index=idx2, name=alias)
        args = self.get_runner_args()
        args += [
            '--config',
            self.args['configfile'],
            'alias',
            '--name',
            alias,
            '--add',
            '{"filtertype":"pattern","kind":"prefix","value":"dum"}',
            '--remove',
            '{"filtertype":"pattern","kind":"prefix","value":"my"}',
        ]
        assert 0 == self.run_subprocess(args)
        expected = {idx1: {'aliases': {alias: {}}}}
        assert expected == self.client.indices.get_alias(name=alias)

    def test_warn_if_no_indices(self):
        """test_warn_if_no_indices"""
        alias = 'testalias'
        idx1, idx2 = ('dummy1', 'dummy2')
        self.create_index(idx1)
        self.create_index(idx2)
        self.client.indices.put_alias(index=idx1, name=alias)
        args = self.get_runner_args()
        args += [
            '--config',
            self.args['configfile'],
            'alias',
            '--name',
            alias,
            '--add',
            '{"filtertype":"none"}',
            '--remove',
            '{"filtertype":"pattern","kind":"prefix","value":"my"}',
            '--warn_if_no_indices',
        ]
        LOGGER.debug('ARGS = %s', args)
        assert 0 == self.run_subprocess(args)
        expected = {idx1: {'aliases': {alias: {}}}, idx2: {'aliases': {alias: {}}}}
        assert expected == dict(self.client.indices.get_alias(name=alias))

    def test_exit_1_on_empty_list(self):
        """test_exit_1_on_empty_list"""
        alias = 'testalias'
        self.create_index('dummy')
        args = self.get_runner_args()
        args += [
            '--config',
            self.args['configfile'],
            'alias',
            '--name',
            alias,
            '--add',
            '{"filtertype":"pattern","kind":"prefix","value":"dum","exclude":false}',
            '--remove',
            '{"filtertype":"pattern","kind":"prefix","value":"my","exclude":false}',
        ]
        assert 1 == self.run_subprocess(
            args, logname='TestCLIAlias.test_exit_1_on_empty_list'
        )
