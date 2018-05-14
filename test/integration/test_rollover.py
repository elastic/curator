import elasticsearch
import curator
import os
import json
import string, random, tempfile
import click
from click import testing as clicktest
import time

from . import CuratorTestCase
from . import testvars as testvars

import logging
logger = logging.getLogger(__name__)

host, port = os.environ.get('TEST_ES_SERVER', 'localhost:9200').split(':')
port = int(port) if port else 9200

class TestActionFileRollover(CuratorTestCase):
    def test_max_age_true(self):
        oldindex  = 'rolltome-000001'
        newindex  = 'rolltome-000002'
        alias     = 'delamitri'
        condition = 'max_age'
        value     = '1s'
        expected  = {newindex: {u'aliases': {alias: {}}}}
        self.client.indices.create(
            index=oldindex,
            body={ 'aliases': { alias: {} } }
        )
        time.sleep(1)
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(self.args['actionfile'],
            testvars.rollover_one.format(alias, condition, value))
        test = clicktest.CliRunner()
        _ = test.invoke(
                    curator.cli,
                    [
                        '--config', self.args['configfile'],
                        self.args['actionfile']
                    ],
                    )
        self.assertEqual(expected, self.client.indices.get_alias(name=alias))
    def test_max_age_false(self):
        oldindex  = 'rolltome-000001'
        alias     = 'delamitri'
        condition = 'max_age'
        value     = '10s'
        expected  = {oldindex: {u'aliases': {alias: {}}}}
        self.client.indices.create(
            index=oldindex,
            body={ 'aliases': { alias: {} } }
        )
        time.sleep(1)
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(self.args['actionfile'],
            testvars.rollover_one.format(alias, condition, value))
        test = clicktest.CliRunner()
        _ = test.invoke(
                    curator.cli,
                    [
                        '--config', self.args['configfile'],
                        self.args['actionfile']
                    ],
                    )
        self.assertEqual(expected, self.client.indices.get_alias(name=alias))
    def test_max_docs_true(self):
        oldindex  = 'rolltome-000001'
        newindex  = 'rolltome-000002'
        alias     = 'delamitri'
        condition = 'max_docs'
        value     = '2'
        expected  = {newindex: {u'aliases': {alias: {}}}}
        self.client.indices.create(
            index=oldindex,
            body={ 'aliases': { alias: {} } }
        )
        self.add_docs(oldindex)
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(self.args['actionfile'],
            testvars.rollover_one.format(alias, condition, value))
        test = clicktest.CliRunner()
        _ = test.invoke(
                    curator.cli,
                    [
                        '--config', self.args['configfile'],
                        self.args['actionfile']
                    ],
                    )
        self.assertEqual(expected, self.client.indices.get_alias(name=alias))
    def test_max_docs_false(self):
        oldindex  = 'rolltome-000001'
        alias     = 'delamitri'
        condition = 'max_docs'
        value     = '5'
        expected  = {oldindex: {u'aliases': {alias: {}}}}
        self.client.indices.create(
            index=oldindex,
            body={ 'aliases': { alias: {} } }
        )
        self.add_docs(oldindex)
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(self.args['actionfile'],
            testvars.rollover_one.format(alias, condition, value))
        test = clicktest.CliRunner()
        _ = test.invoke(
                    curator.cli,
                    [
                        '--config', self.args['configfile'],
                        self.args['actionfile']
                    ],
                    )
        self.assertEqual(expected, self.client.indices.get_alias(name=alias))
    def test_conditions_both_false(self):
        oldindex  = 'rolltome-000001'
        alias     = 'delamitri'
        max_age   = '10s'
        max_docs  = '5'
        expected  = {oldindex: {u'aliases': {alias: {}}}}
        self.client.indices.create(
            index=oldindex,
            body={ 'aliases': { alias: {} } }
        )
        self.add_docs(oldindex)
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(self.args['actionfile'],
            testvars.rollover_both.format(alias, max_age, max_docs))
        test = clicktest.CliRunner()
        _ = test.invoke(
                    curator.cli,
                    [
                        '--config', self.args['configfile'],
                        self.args['actionfile']
                    ],
                    )
        self.assertEqual(expected, self.client.indices.get_alias(name=alias))
    def test_conditions_both_true(self):
        oldindex  = 'rolltome-000001'
        newindex  = 'rolltome-000002'
        alias     = 'delamitri'
        max_age   = '1s'
        max_docs  = '2'
        expected  = {newindex: {u'aliases': {alias: {}}}}
        self.client.indices.create(
            index=oldindex,
            body={ 'aliases': { alias: {} } }
        )
        time.sleep(1)
        self.add_docs(oldindex)
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(self.args['actionfile'],
            testvars.rollover_both.format(alias, max_age, max_docs))
        test = clicktest.CliRunner()
        _ = test.invoke(
                    curator.cli,
                    [
                        '--config', self.args['configfile'],
                        self.args['actionfile']
                    ],
                    )
        self.assertEqual(expected, self.client.indices.get_alias(name=alias))
    def test_conditions_one_false_one_true(self):
        oldindex  = 'rolltome-000001'
        newindex  = 'rolltome-000002'
        alias     = 'delamitri'
        max_age   = '10s'
        max_docs  = '2'
        expected  = {newindex: {u'aliases': {alias: {}}}}
        self.client.indices.create(
            index=oldindex,
            body={ 'aliases': { alias: {} } }
        )
        self.add_docs(oldindex)
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(self.args['actionfile'],
            testvars.rollover_both.format(alias, max_age, max_docs))
        test = clicktest.CliRunner()
        _ = test.invoke(
                    curator.cli,
                    [
                        '--config', self.args['configfile'],
                        self.args['actionfile']
                    ],
                    )
        self.assertEqual(expected, self.client.indices.get_alias(name=alias))
    def test_conditions_one_empty_one_true(self):
        oldindex  = 'rolltome-000001'
        alias     = 'delamitri'
        max_age   = ' '
        max_docs  = '2'
        expected  = {oldindex: {u'aliases': {alias: {}}}}
        self.client.indices.create(
            index=oldindex,
            body={ 'aliases': { alias: {} } }
        )
        self.add_docs(oldindex)
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(self.args['actionfile'],
            testvars.rollover_both.format(alias, max_age, max_docs))
        test = clicktest.CliRunner()
        _ = test.invoke(
                    curator.cli,
                    [
                        '--config', self.args['configfile'],
                        self.args['actionfile']
                    ],
                    )
        self.assertEqual(expected, self.client.indices.get_alias(name=alias))
        self.assertEqual(-1, _.exit_code)
    def test_bad_settings(self):
        oldindex  = 'rolltome-000001'
        alias     = 'delamitri'
        max_age   = '10s'
        max_docs  = '2'
        expected  = {oldindex: {u'aliases': {alias: {}}}}
        self.client.indices.create(
            index=oldindex,
            body={ 'aliases': { alias: {} } }
        )
        self.add_docs(oldindex)
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(self.args['actionfile'],
            testvars.rollover_bad_settings.format(alias, max_age, max_docs))
        test = clicktest.CliRunner()
        _ = test.invoke(
                    curator.cli,
                    [
                        '--config', self.args['configfile'],
                        self.args['actionfile']
                    ],
                    )
        self.assertEqual(expected, self.client.indices.get_alias(name=alias))
        self.assertEqual(1, _.exit_code)
    def test_extra_option(self):
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(self.args['actionfile'],
            testvars.bad_option_proto_test.format('rollover'))
        test = clicktest.CliRunner()
        _ = test.invoke(
                    curator.cli,
                    [
                        '--config', self.args['configfile'],
                        self.args['actionfile']
                    ],
                    )
        self.assertEqual([], curator.get_indices(self.client))
        self.assertEqual(-1, _.exit_code)
    def test_max_age_with_new_name(self):
        oldindex  = 'rolltome-000001'
        newindex  = 'crazy_test'
        alias     = 'delamitri'
        condition = 'max_age'
        value     = '1s'
        expected  = {newindex: {u'aliases': {alias: {}}}}
        self.client.indices.create(
            index=oldindex,
            body={ 'aliases': { alias: {} } }
        )
        time.sleep(1)
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(self.args['actionfile'],
            testvars.rollover_with_name.format(alias, condition, value, newindex))
        test = clicktest.CliRunner()
        _ = test.invoke(
                    curator.cli,
                    [
                        '--config', self.args['configfile'],
                        self.args['actionfile']
                    ],
                    )
        self.assertEqual(expected, self.client.indices.get_alias(name=alias))
    def test_max_age_with_new_name_with_date(self):
        oldindex  = 'rolltome-000001'
        newindex  = 'crazy_test-%Y.%m.%d'
        alias     = 'delamitri'
        condition = 'max_age'
        value     = '1s'
        expected  = {curator.parse_date_pattern(newindex): {u'aliases': {alias: {}}}}
        self.client.indices.create(
            index=oldindex,
            body={ 'aliases': { alias: {} } }
        )
        time.sleep(1)
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(self.args['actionfile'],
            testvars.rollover_with_name.format(alias, condition, value, newindex))
        test = clicktest.CliRunner()
        _ = test.invoke(
                    curator.cli,
                    [
                        '--config', self.args['configfile'],
                        self.args['actionfile']
                    ],
                    )
        self.assertEqual(expected, self.client.indices.get_alias(name=alias))
    def test_max_age_old_index_with_date_with_new_index(self):
        oldindex  = 'crazy_test-2017.01.01'
        newindex  = 'crazy_test-%Y.%m.%d'
        alias     = 'delamitri'
        condition = 'max_age'
        value     = '1s'
        expected  = {"%s" % curator.parse_date_pattern(newindex): {u'aliases': {alias: {}}}}
        self.client.indices.create(
            index=oldindex,
            body={ 'aliases': { alias: {} } }
        )
        time.sleep(1)
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(self.args['actionfile'],
            testvars.rollover_with_name.format(alias, condition, value, newindex))
        test = clicktest.CliRunner()
        _ = test.invoke(
                    curator.cli,
                    [
                        '--config', self.args['configfile'],
                        self.args['actionfile']
                    ],
                    )
        self.assertEqual(expected, self.client.indices.get_alias(name=alias))

class TestCLIRollover(CuratorTestCase):
    def test_max_age_true(self):
        oldindex  = 'rolltome-000001'
        newindex  = 'rolltome-000002'
        alias     = 'delamitri'
        value     = '1s'
        expected  = {newindex: {u'aliases': {alias: {}}}}
        self.client.indices.create(index=oldindex, body={'aliases':{alias:{}}})
        time.sleep(1)
        args = self.get_runner_args()
        args += [
            '--config', self.args['configfile'],
            'rollover',
            '--name', alias,
            '--max_age', value
        ]
        self.assertEqual(0, self.run_subprocess(args, logname='TestCLIRollover.test_max_age_true'))
        self.assertEqual(expected, self.client.indices.get_alias(name=alias))