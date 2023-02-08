"""Test es_repo_mgr script and functions"""
import logging
import os
from click import testing as clicktest
from curator import repo_mgr_cli
from curator.helpers.testers import repository_exists
from . import CuratorTestCase
from . import testvars

LOGGER = logging.getLogger(__name__)
REPO_PATH = '/media'

HOST = os.environ.get('TEST_ES_SERVER', 'http://127.0.0.1:9200')

# class TestLoggingModules(CuratorTestCase):
#     def test_logger_without_null_handler(self):
#         from mock import patch, Mock
#         mock = Mock()
#         modules = {'logger': mock, 'logger.NullHandler': mock.module}
#         self.write_config(
#             self.args['configfile'],
#             testvars.client_conf_logfile.format(HOST, os.devnull)
#         )
#         with patch.dict('sys.modules', modules):
#             self.create_repository()
#             test = clicktest.CliRunner()
#             result = test.invoke(
#                 repo_mgr_cli,
#                 ['--config', self.args['configfile'], 'show']
#             )
#         self.assertEqual(self.args['repository'], result.output.rstrip())


class TestCLIRepositoryCreate(CuratorTestCase):
    def test_create_fs_repository_success(self):
        self.write_config(self.args['configfile'], testvars.client_conf_logfile.format(HOST, os.devnull))
        test = clicktest.CliRunner()
        result = test.invoke(
            repo_mgr_cli,
            [
                '--config', self.args['configfile'],
                'create',
                'fs',
                '--name', self.args['repository'],
                '--location', REPO_PATH,
                '--verify'
            ]
        )
        assert 1 == len(self.client.snapshot.get_repository(name=self.args['repository']))
        assert 0 == result.exit_code

    def test_create_fs_repository_fail(self):
        self.write_config(self.args['configfile'], testvars.client_conf_logfile.format(HOST, os.devnull))
        test = clicktest.CliRunner()
        result = test.invoke(
            repo_mgr_cli,
            [
                '--config', self.args['configfile'],
                'create',
                'fs',
                '--name', self.args['repository'],
                '--location', os.devnull,
                '--verify'
            ]
        )
        assert 1 == result.exit_code

    def test_create_s3_repository_fail(self):
        self.write_config(self.args['configfile'], testvars.client_conf_logfile.format(HOST, os.devnull))
        test = clicktest.CliRunner()
        result = test.invoke(
            repo_mgr_cli,
            [
                '--config', self.args['configfile'],
                'create',
                's3',
                '--bucket', 'mybucket',
                '--name', self.args['repository'],
                '--verify'
            ]
        )
        assert 1 == result.exit_code

    def test_create_azure_repository_fail(self):
        self.write_config(self.args['configfile'], testvars.client_conf_logfile.format(HOST, os.devnull))
        test = clicktest.CliRunner()
        result = test.invoke(
            repo_mgr_cli,
            [
                '--config', self.args['configfile'],
                'create',
                'azure',
                '--container', 'mybucket',
                '--name', self.args['repository'],
                '--verify'
            ]
        )
        assert 1 == result.exit_code

    def test_create_gcs_repository_fail(self):
        self.write_config(self.args['configfile'], testvars.client_conf_logfile.format(HOST, os.devnull))
        test = clicktest.CliRunner()
        result = test.invoke(
            repo_mgr_cli,
            [
                '--config', self.args['configfile'],
                'create',
                'gcs',
                '--bucket', 'mybucket',
                '--name', self.args['repository'],
                '--verify'
            ]
        )
        assert 1 == result.exit_code


class TestCLIDeleteRepository(CuratorTestCase):
    def test_delete_repository_success(self):
        self.create_repository()
        self.write_config(self.args['configfile'], testvars.client_conf_logfile.format(HOST, os.devnull))
        test = clicktest.CliRunner()
        _ = test.invoke(
            repo_mgr_cli,
            [
                '--config', self.args['configfile'],
                'delete',
                '--yes', # This ensures no prompting will happen
                '--name', self.args['repository']
            ]
        )
        assert not repository_exists(self.client, self.args['repository'])
    def test_delete_repository_notfound(self):
        self.write_config(self.args['configfile'], testvars.client_conf_logfile.format(HOST, os.devnull))
        test = clicktest.CliRunner()
        result = test.invoke(
            repo_mgr_cli,
            [
                '--config', self.args['configfile'],
                'delete',
                '--yes', # This ensures no prompting will happen
                '--name', self.args['repository']
            ]
        )
        assert 1 == result.exit_code

class TestCLIShowRepositories(CuratorTestCase):
    def test_show_repository(self):
        self.create_repository()
        self.write_config(self.args['configfile'], testvars.client_conf_logfile.format(HOST, os.devnull))
        test = clicktest.CliRunner()
        result = test.invoke(
            repo_mgr_cli,
            [
                '--config', self.args['configfile'],
                'show'
            ]
        )
        assert self.args['repository'] == result.output.rstrip()
