import elasticsearch
import curator
import os
import json
import click
import string, random, tempfile
from click import testing as clicktest
from mock import patch, Mock, MagicMock

from . import CuratorTestCase
from . import testvars as testvars

import logging
logger = logging.getLogger(__name__)

host, port = os.environ.get('TEST_ES_SERVER', 'localhost:9200').split(':')
port = int(port) if port else 9200

class TestLoggingModules(CuratorTestCase):
    def test_logger_without_null_handler(self):
        mock = Mock()
        modules = {'logger': mock, 'logger.NullHandler': mock.module}
        self.write_config(
            self.args['configfile'],
            testvars.client_conf_logfile.format(host, port, os.devnull)
        )
        with patch.dict('sys.modules', modules):
            self.create_repository()
            test = clicktest.CliRunner()
            result = test.invoke(
                        curator.repo_mgr_cli,
                        [
                            '--config', self.args['configfile'],
                            'show'
                        ]
            )
        self.assertEqual(self.args['repository'], result.output.rstrip())


class TestCLIRepositoryCreate(CuratorTestCase):
    def test_create_fs_repository_success(self):
        self.write_config(
            self.args['configfile'],
            testvars.client_conf_logfile.format(host, port, os.devnull)
        )
        test = clicktest.CliRunner()
        result = test.invoke(
                    curator.repo_mgr_cli,
                    [
                        '--config', self.args['configfile'],
                        'create',
                        'fs',
                        '--repository', self.args['repository'],
                        '--location', self.args['location']
                    ]
        )
        self.assertTrue(1, len(self.client.snapshot.get_repository(repository=self.args['repository'])))
        self.assertEqual(0, result.exit_code)

    def test_create_fs_repository_fail(self):
        self.write_config(
            self.args['configfile'],
            testvars.client_conf_logfile.format(host, port, os.devnull)
        )
        test = clicktest.CliRunner()
        result = test.invoke(
                    curator.repo_mgr_cli,
                    [
                        '--config', self.args['configfile'],
                        'create',
                        'fs',
                        '--repository', self.args['repository'],
                        '--location', os.devnull
                    ]
        )
        self.assertEqual(1, result.exit_code)

    def test_create_s3_repository_fail(self):
        self.write_config(
            self.args['configfile'],
            testvars.client_conf_logfile.format(host, port, os.devnull)
        )
        test = clicktest.CliRunner()
        result = test.invoke(
                    curator.repo_mgr_cli,
                    [
                        '--config', self.args['configfile'],
                        'create',
                        's3',
                        '--bucket', 'mybucket',
                        '--repository', self.args['repository'],
                    ]
        )
        self.assertEqual(1, result.exit_code)


class TestCLIDeleteRepository(CuratorTestCase):
    def test_delete_repository_success(self):
        self.create_repository()
        self.write_config(
            self.args['configfile'],
            testvars.client_conf_logfile.format(host, port, os.devnull)
        )
        test = clicktest.CliRunner()
        _ = test.invoke(
                    curator.repo_mgr_cli,
                    [
                        '--config', self.args['configfile'],
                        'delete',
                        '--yes', # This ensures no prompting will happen
                        '--repository', self.args['repository']
                    ]
        )
        self.assertFalse(
            curator.repository_exists(self.client, self.args['repository'])
        )
    def test_delete_repository_notfound(self):
        self.write_config(
            self.args['configfile'],
            testvars.client_conf_logfile.format(host, port, os.devnull)
        )
        test = clicktest.CliRunner()
        result = test.invoke(
                    curator.repo_mgr_cli,
                    [
                        '--config', self.args['configfile'],
                        'delete',
                        '--yes', # This ensures no prompting will happen
                        '--repository', self.args['repository']
                    ]
        )
        self.assertEqual(1, result.exit_code)

class TestCLIShowRepositories(CuratorTestCase):
    def test_show_repository(self):
        self.create_repository()
        self.write_config(
            self.args['configfile'],
            testvars.client_conf_logfile.format(host, port, os.devnull)
        )
        test = clicktest.CliRunner()
        result = test.invoke(
                    curator.repo_mgr_cli,
                    [
                        '--config', self.args['configfile'],
                        'show'
                    ]
        )
        self.assertEqual(self.args['repository'], result.output.rstrip())
