"""Test rollover action functionality"""
# pylint: disable=missing-function-docstring, missing-class-docstring, line-too-long
import os
import time
from curator.utils import get_indices, parse_date_pattern
from . import CuratorTestCase
from . import testvars

HOST = os.environ.get('TEST_ES_SERVER', 'http://127.0.0.1:9200')
OLDINDEX = 'rolltome-000001'
NEWINDEX = 'rolltome-000002'
ALIAS = 'delamitri'

class TestActionFileRollover(CuratorTestCase):
    def test_max_age_true(self):
        condition = 'max_age'
        value = '1s'
        expected = {NEWINDEX: {'aliases': {ALIAS: {}}}}
        self.client.indices.create(index=OLDINDEX, aliases={ALIAS: {}})
        time.sleep(1)
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(self.args['actionfile'], testvars.rollover_one.format(ALIAS, condition, value))
        self.invoke_runner()
        assert expected == self.client.indices.get_alias(name=ALIAS)
    def test_max_age_false(self):
        condition = 'max_age'
        value = '10s'
        expected = {OLDINDEX: {'aliases': {ALIAS: {}}}}
        self.client.indices.create(index=OLDINDEX, aliases={ALIAS: {}})
        time.sleep(1)
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(self.args['actionfile'], testvars.rollover_one.format(ALIAS, condition, value))
        self.invoke_runner()
        assert expected == self.client.indices.get_alias(name=ALIAS)
    def test_max_docs_true(self):
        condition = 'max_docs'
        value = '2'
        expecto = {'aliases': {ALIAS: {}}}
        self.client.indices.create(index=OLDINDEX, aliases={ALIAS: {}})
        self.add_docs(OLDINDEX)
        self.client.indices.rollover(alias=ALIAS, conditions={condition: value}, dry_run=True)
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(self.args['actionfile'], testvars.rollover_one.format(ALIAS, condition, value))
        self.invoke_runner()
        response = self.client.indices.get_alias(name=ALIAS)
        assert 1 == len(list(response.keys()))
        assert NEWINDEX == list(response.keys())[0]
        assert expecto == response[NEWINDEX]
    def test_max_docs_false(self):
        condition = 'max_docs'
        value = '5'
        expected = {OLDINDEX: {'aliases': {ALIAS: {}}}}
        self.client.indices.create(index=OLDINDEX, aliases={ALIAS: {}})
        self.add_docs(OLDINDEX)
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(self.args['actionfile'], testvars.rollover_one.format(ALIAS, condition, value))
        self.invoke_runner()
        assert expected == self.client.indices.get_alias(name=ALIAS)
    def test_conditions_both_false(self):
        max_age = '10s'
        max_docs = '5'
        expected = {OLDINDEX: {'aliases': {ALIAS: {}}}}
        self.client.indices.create(index=OLDINDEX, aliases={ALIAS: {}})
        self.add_docs(OLDINDEX)
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(self.args['actionfile'], testvars.rollover_both.format(ALIAS, max_age, max_docs))
        self.invoke_runner()
        assert expected == self.client.indices.get_alias(name=ALIAS)
    def test_conditions_both_true(self):
        max_age = '1s'
        max_docs = '2'
        expected = {NEWINDEX: {'aliases': {ALIAS: {}}}}
        self.client.indices.create(index=OLDINDEX, aliases={ALIAS: {}})
        time.sleep(1)
        self.add_docs(OLDINDEX)
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(self.args['actionfile'], testvars.rollover_both.format(ALIAS, max_age, max_docs))
        self.invoke_runner()
        assert expected == self.client.indices.get_alias(name=ALIAS)
    def test_conditions_one_false_one_true(self):
        max_age = '10s'
        max_docs = '2'
        expected = {NEWINDEX: {'aliases': {ALIAS: {}}}}
        self.client.indices.create(index=OLDINDEX, aliases={ALIAS: {}})
        self.add_docs(OLDINDEX)
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(self.args['actionfile'], testvars.rollover_both.format(ALIAS, max_age, max_docs))
        self.invoke_runner()
        assert expected == self.client.indices.get_alias(name=ALIAS)
    def test_conditions_one_empty_one_true(self):
        max_age = ' '
        max_docs = '2'
        expected = {OLDINDEX: {'aliases': {ALIAS: {}}}}
        self.client.indices.create(index=OLDINDEX, aliases={ALIAS: {}})
        self.add_docs(OLDINDEX)
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(self.args['actionfile'], testvars.rollover_both.format(ALIAS, max_age, max_docs))
        self.invoke_runner()
        assert expected == self.client.indices.get_alias(name=ALIAS)
        assert 1 == self.result.exit_code
    def test_bad_settings(self):
        max_age = '10s'
        max_docs = '2'
        expected = {OLDINDEX: {'aliases': {ALIAS: {}}}}
        self.client.indices.create(index=OLDINDEX, aliases={ALIAS: {}})
        self.add_docs(OLDINDEX)
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(self.args['actionfile'], testvars.rollover_bad_settings.format(ALIAS, max_age, max_docs))
        self.invoke_runner()
        assert expected == self.client.indices.get_alias(name=ALIAS)
        assert 1 == self.result.exit_code
    def test_extra_option(self):
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(self.args['actionfile'], testvars.bad_option_rollover_test.format('rollover'))
        before = get_indices(self.client)
        self.invoke_runner()
        assert before == get_indices(self.client)
        assert 1 == self.result.exit_code
    def test_max_age_with_new_name(self):
        newindex = 'crazy_test'
        condition = 'max_age'
        value = '1s'
        expected = {newindex: {'aliases': {ALIAS: {}}}}
        self.client.indices.create(index=OLDINDEX, aliases={ALIAS: {}})
        time.sleep(1)
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(self.args['actionfile'], testvars.rollover_with_name.format(ALIAS, condition, value, newindex))
        self.invoke_runner()
        assert expected == self.client.indices.get_alias(name=ALIAS)
    def test_max_age_with_new_name_with_date(self):
        newindex = 'crazy_test-%Y.%m.%d'
        condition = 'max_age'
        value = '1s'
        expected = {parse_date_pattern(newindex): {'aliases': {ALIAS: {}}}}
        self.client.indices.create(index=OLDINDEX, aliases={ALIAS: {}})
        time.sleep(1)
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(self.args['actionfile'], testvars.rollover_with_name.format(ALIAS, condition, value, newindex))
        self.invoke_runner()
        assert expected == self.client.indices.get_alias(name=ALIAS)
    def test_max_age_old_index_with_date_with_new_index(self):
        oldindex = 'crazy_test-2017.01.01'
        newindex = 'crazy_test-%Y.%m.%d'
        condition = 'max_age'
        value = '1s'
        expected = {f"{parse_date_pattern(newindex)}": {'aliases': {ALIAS: {}}}}
        self.client.indices.create(index=oldindex, aliases={ALIAS: {}})
        time.sleep(1)
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(self.args['actionfile'], testvars.rollover_with_name.format(ALIAS, condition, value, newindex))
        self.invoke_runner()
        assert expected == self.client.indices.get_alias(name=ALIAS)
    def test_is_write_alias(self):
        condition = 'max_age'
        value = '1s'
        request_body = { 'aliases': { ALIAS: {'is_write_index': True} } }
        expected = 2
        self.client.indices.create(index=OLDINDEX, aliases=request_body['aliases'])
        time.sleep(1)
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(self.args['actionfile'], testvars.rollover_one.format(ALIAS, condition, value))
        self.invoke_runner()
        assert expected == len(self.client.indices.get_alias(name=ALIAS))
    def test_no_rollover_ilm_associated(self):
        condition = 'max_age'
        value = '1s'
        expected = 1
        self.client.indices.create(index=OLDINDEX, settings={'index': {'lifecycle': {'name': 'generic'}}}, aliases={ ALIAS:{}})
        time.sleep(1)
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(self.args['actionfile'], testvars.rollover_one.format(ALIAS, condition, value))
        self.invoke_runner()
        assert 0 == self.result.exit_code
        assert expected == len(self.client.indices.get_alias(name=ALIAS))
        assert OLDINDEX == list(self.client.indices.get_alias(name=ALIAS).keys())[0]

class TestCLIRollover(CuratorTestCase):
    def test_max_age_true(self):
        value = '1s'
        expected = {NEWINDEX: {'aliases': {ALIAS: {}}}}
        self.client.indices.create(index=OLDINDEX, aliases={ALIAS:{}})
        time.sleep(1)
        args = self.get_runner_args()
        args += [
            '--config', self.args['configfile'],
            'rollover',
            '--name', ALIAS,
            '--max_age', value
        ]
        assert 0 == self.run_subprocess(args, logname='TestCLIRollover.test_max_age_true')
        assert expected == self.client.indices.get_alias(name=ALIAS)
