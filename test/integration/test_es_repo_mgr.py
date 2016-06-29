import elasticsearch
import curator
import os
import json
import click
import string, random, tempfile
from click import testing as clicktest
from mock import patch, Mock, MagicMock

from . import CuratorTestCase

import logging
logger = logging.getLogger(__name__)

host, port = os.environ.get('TEST_ES_SERVER', 'localhost:9200').split(':')
port = int(port) if port else 9200

class TestLoggingModules(CuratorTestCase):
    def test_logger_without_null_handler(self):
        mock = Mock()
        modules = {'logger': mock, 'logger.NullHandler': mock.module}
        with patch.dict('sys.modules', modules):
           self.create_repository()
           test = clicktest.CliRunner()
           result = test.invoke(
                       curator.repo_mgr_cli,
                       [
                           '--logfile', os.devnull,
                           '--host', host,
                           '--port', str(port),
                           'show'
                       ],
                       obj={"filters":[]})
        self.assertEqual(self.args['repository'], result.output.rstrip())


class TestCLIRepositoryCreate(CuratorTestCase):
    def test_create_fs_repository_success(self):
        test = clicktest.CliRunner()
        result = test.invoke(
                    curator.repo_mgr_cli,
                    [
                        '--logfile', os.devnull,
                        '--host', host,
                        '--port', str(port),
                        'create',
                        'fs',
                        '--repository', self.args['repository'],
                        '--location', self.args['location']
                    ],
                    obj={"filters":[]})
        self.assertTrue(1, len(self.client.snapshot.get_repository(repository=self.args['repository'])))
        self.assertEqual(0, result.exit_code)

    def test_create_fs_repository_fail(self):
        test = clicktest.CliRunner()
        result = test.invoke(
                    curator.repo_mgr_cli,
                    [
                        '--logfile', os.devnull,
                        '--host', host,
                        '--port', str(port),
                        'create',
                        'fs',
                        '--repository', self.args['repository'],
                        '--location', os.devnull
                    ],
                    obj={"filters":[]})
        self.assertEqual(1, result.exit_code)

    def test_create_s3_repository_fail(self):
        test = clicktest.CliRunner()
        result = test.invoke(
                    curator.repo_mgr_cli,
                    [
                        '--logfile', os.devnull,
                        '--host', host,
                        '--port', str(port),
                        'create',
                        's3',
                        '--bucket', 'mybucket',
                        '--repository', self.args['repository'],
                    ],
                    obj={"filters":[]})
        self.assertEqual(1, result.exit_code)


class TestCLIDeleteRepository(CuratorTestCase):
    def test_delete_repository_success(self):
        self.create_repository()
        test = clicktest.CliRunner()
        result = test.invoke(
                    curator.repo_mgr_cli,
                    [
                        '--logfile', os.devnull,
                        '--host', host,
                        '--port', str(port),
                        'delete',
                        '--yes', # This ensures no prompting will happen
                        '--repository', self.args['repository']
                    ],
                    obj={"filters":[]})
        self.assertFalse(curator.get_repository(self.client, self.args['repository']))
    def test_delete_repository_notfound(self):
        test = clicktest.CliRunner()
        result = test.invoke(
                    curator.repo_mgr_cli,
                    [
                        '--logfile', os.devnull,
                        '--debug',
                        '--host', host,
                        '--port', str(port),
                        'delete',
                        '--yes', # This ensures no prompting will happen
                        '--repository', self.args['repository']
                    ],
                    obj={"filters":[]})
        self.assertEqual(1, result.exit_code)

class TestCLIShowRepositories(CuratorTestCase):
    def test_show_repository(self):
        self.create_repository()
        test = clicktest.CliRunner()
        result = test.invoke(
                    curator.repo_mgr_cli,
                    [
                        '--logfile', os.devnull,
                        '--host', host,
                        '--port', str(port),
                        'show'
                    ],
                    obj={"filters":[]})
        self.assertEqual(self.args['repository'], result.output.rstrip())

class TestRepoMGR_CLIOptions(CuratorTestCase):
    def test_debug_logging(self):
        dirname = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(8))
        logfile = tempfile.mkdtemp(suffix=dirname) + 'logfile'
        self.create_repository()
        test = clicktest.CliRunner()
        result = test.invoke(
                    curator.repo_mgr_cli,
                    [
                        '--logfile', logfile,
                        '--debug',
                        '--host', host,
                        '--port', str(port),
                        'show'
                    ],
                    obj={"filters":[]})
        self.assertEqual(0, result.exit_code)

    def test_logstash_formatting(self):
        dirname = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(8))
        logfile = tempfile.mkdtemp(suffix=dirname) + 'logfile'
        self.create_repository()
        test = clicktest.CliRunner()
        result = test.invoke(
                    curator.repo_mgr_cli,
                    [
                        '--logformat', 'logstash',
                        '--debug',
                        '--host', host,
                        '--port', str(port),
                        'show'
                    ],
                    obj={"filters":[]})
        d = json.loads(result.output.splitlines()[:1][0])
        keys = sorted(list(d.keys()))
        self.assertEqual(['@timestamp','function','linenum','loglevel','message','name'], keys)
