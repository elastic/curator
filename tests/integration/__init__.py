"""Test setup"""

# pylint: disable=C0115, C0116
import logging
import os
import random
import shutil
import string
import sys
import tempfile
import time
import json
import warnings
from datetime import timedelta, datetime, date, timezone
from subprocess import Popen, PIPE
from unittest import SkipTest, TestCase
from elasticsearch8 import Elasticsearch
from elasticsearch8.exceptions import ConnectionError as ESConnectionError
from elasticsearch8.exceptions import ElasticsearchWarning, NotFoundError
from click import testing as clicktest
from es_client.helpers.utils import get_version
from curator.cli import cli

from . import testvars

DEBUG_LEVEL = '5'

client = None

DATEMAP = {
    'months': '%Y.%m',
    'weeks': '%Y.%W',
    'days': '%Y.%m.%d',
    'hours': '%Y.%m.%d.%H',
}

HOST = os.environ.get('TEST_ES_SERVER', 'http://127.0.0.1:9200')


def random_directory():
    dirname = ''.join(
        random.choice(string.ascii_uppercase + string.digits) for _ in range(8)
    )
    directory = tempfile.mkdtemp(suffix=dirname)
    if not os.path.exists(directory):
        os.makedirs(directory)
    return directory


def get_client():
    # pylint: disable=global-statement, invalid-name
    global client
    if client is not None:
        return client

    client = Elasticsearch(hosts=HOST, request_timeout=300)

    # wait for yellow status
    for _ in range(100):
        time.sleep(0.1)
        try:
            # pylint: disable=E1123
            client.cluster.health(wait_for_status='yellow')
            return client
        except ESConnectionError:
            continue
    # timeout
    raise SkipTest("Elasticsearch failed to start.")


def setup():
    get_client()


class Args(dict):
    def __getattr__(self, att_name):
        return self.get(att_name, None)


class CuratorTestCase(TestCase):
    def setUp(self):
        super(CuratorTestCase, self).setUp()
        self.logger = logging.getLogger('CuratorTestCase.setUp')
        self.client = get_client()

        args = {}
        args['HOST'] = HOST
        args['time_unit'] = 'days'
        args['prefix'] = 'logstash-'
        self.args = args
        # dirname = ''.join(random.choice(string.ascii_uppercase + string.digits)
        #   for _ in range(8))
        # This will create a psuedo-random temporary directory on the machine
        # which runs the unit tests, but NOT on the machine where elasticsearch
        # is running. This means tests may fail if run against remote instances
        # unless you explicitly set `self.args['location']` to a proper spot
        # on the target machine.
        # self.args['location'] = random_directory()
        nodesinfo = self.client.nodes.info()
        nodename = list(nodesinfo['nodes'].keys())[0]
        if 'repo' in nodesinfo['nodes'][nodename]['settings']['path']:
            if isinstance(
                nodesinfo['nodes'][nodename]['settings']['path']['repo'], list
            ):
                self.args['location'] = nodesinfo['nodes'][nodename]['settings'][
                    'path'
                ]['repo'][0]
            else:
                self.args['location'] = nodesinfo['nodes'][nodename]['settings'][
                    'path'
                ]['repo']
        else:  # Use a random directory if repo is not specified, but log it
            self.logger.warning('path.repo is not configured!')
            self.args['location'] = random_directory()
        self.args['configdir'] = random_directory()
        self.args['configfile'] = os.path.join(self.args['configdir'], 'curator.yml')
        self.args['actionfile'] = os.path.join(self.args['configdir'], 'actions.yml')
        self.args['repository'] = 'test_repository'
        # if not os.path.exists(self.args['location']):
        #     os.makedirs(self.args['location'])
        self.logger.debug('setUp completed...')
        self.runner = clicktest.CliRunner()
        self.runner_args = [
            '--config',
            self.args['configfile'],
            '--debug-level',
            DEBUG_LEVEL,
            self.args['actionfile'],
        ]
        self.result = None

    def get_version(self):
        return get_version(self.client)

    def tearDown(self):
        self.logger = logging.getLogger('CuratorTestCase.tearDown')
        self.logger.debug('tearDown initiated...')
        # re-enable shard allocation for next tests
        enable_allocation = json.loads('{"cluster.routing.allocation.enable":null}')
        self.client.cluster.put_settings(transient=enable_allocation)
        self.delete_repositories()
        # 8.0 removes our ability to purge with wildcards...
        # ElasticsearchWarning: this request accesses system indices: [.tasks],
        # but in a future major version, direct access to system indices will be
        # prevented by default
        warnings.filterwarnings("ignore", category=ElasticsearchWarning)
        indices = list(
            self.client.indices.get(index="*", expand_wildcards='open,closed').keys()
        )
        if len(indices) > 0:
            # ElasticsearchWarning: this request accesses system indices: [.tasks],
            # but in a future major version, direct access to system indices will be
            # prevented by default
            warnings.filterwarnings("ignore", category=ElasticsearchWarning)
            self.client.indices.delete(index=','.join(indices))
        for path_arg in ['location', 'configdir']:
            if os.path.exists(self.args[path_arg]):
                shutil.rmtree(self.args[path_arg])

    def parse_args(self):
        return Args(self.args)

    def create_indices(self, count, unit=None, ilm_policy=None):
        now = datetime.now(timezone.utc)
        unit = unit if unit else self.args['time_unit']
        fmt = DATEMAP[unit]
        if not unit == 'months':
            step = timedelta(**{unit: 1})
            for _ in range(count):
                self.create_index(
                    self.args['prefix'] + now.strftime(fmt),
                    wait_for_yellow=False,
                    ilm_policy=ilm_policy,
                )
                now -= step
        else:  # months
            now = date.today()
            d = date(now.year, now.month, 1)
            self.create_index(
                self.args['prefix'] + now.strftime(fmt),
                wait_for_yellow=False,
                ilm_policy=ilm_policy,
            )

            for _ in range(1, count):
                if d.month == 1:
                    d = date(d.year - 1, 12, 1)
                else:
                    d = date(d.year, d.month - 1, 1)
                self.create_index(
                    self.args['prefix'] + datetime(d.year, d.month, 1).strftime(fmt),
                    wait_for_yellow=False,
                    ilm_policy=ilm_policy,
                )
        # pylint: disable=E1123
        self.client.cluster.health(wait_for_status='yellow')

    def wfy(self):
        # pylint: disable=E1123
        self.client.cluster.health(wait_for_status='yellow')

    def create_index(
        self,
        name,
        shards=1,
        wait_for_yellow=True,
        ilm_policy=None,
        wait_for_active_shards=1,
    ):
        request_body = {'index': {'number_of_shards': shards, 'number_of_replicas': 0}}
        if ilm_policy is not None:
            request_body['index']['lifecycle'] = {'name': ilm_policy}
        # ElasticsearchWarning: index name [.shouldbehidden] starts with a dot '.',
        # in the next major version, index names starting with a dot are reserved
        # for hidden indices and system indices
        warnings.filterwarnings("ignore", category=ElasticsearchWarning)
        self.client.indices.create(
            index=name,
            settings=request_body,
            wait_for_active_shards=wait_for_active_shards,
        )
        if wait_for_yellow:
            self.wfy()

    def add_docs(self, idx):
        for i in ["1", "2", "3"]:
            self.client.create(index=idx, id=i, document={"doc" + i: 'TEST DOCUMENT'})
            # This should force each doc to be in its own segment.
            # pylint: disable=E1123
            self.client.indices.flush(index=idx, force=True)
            self.client.indices.refresh(index=idx)

    def create_snapshot(self, name, csv_indices):
        self.create_repository()
        self.client.snapshot.create(
            repository=self.args['repository'],
            snapshot=name,
            ignore_unavailable=False,
            include_global_state=True,
            partial=False,
            indices=csv_indices,
            wait_for_completion=True,
        )

    def delete_snapshot(self, name):
        try:
            self.client.snapshot.delete(
                repository=self.args['repository'], snapshot=name
            )
        except NotFoundError:
            pass

    def create_repository(self):
        request_body = {'type': 'fs', 'settings': {'location': self.args['location']}}
        self.client.snapshot.create_repository(
            name=self.args['repository'], body=request_body
        )

    def create_named_repository(self, repo_name):
        request_body = {
            'type': 'fs',
            'settings': {'location': self.args['location']}
        }
        self.client.snapshot.create_repository(name=repo_name, body=request_body)

    def delete_repositories(self):
        result = []
        try:
            result = self.client.snapshot.get_repository(name='*')
        except NotFoundError:
            pass
        for repo in result:
            try:
                cleanup = self.client.snapshot.get(repository=repo, snapshot='*')
            # pylint: disable=broad-except
            except Exception:
                cleanup = {'snapshots': []}
            for listitem in cleanup['snapshots']:
                self.delete_snapshot(listitem['snapshot'])
            self.client.snapshot.delete_repository(name=repo)

    def close_index(self, name):
        self.client.indices.close(index=name)

    def write_config(self, fname, data):
        with open(fname, 'w', encoding='utf-8') as fhandle:
            fhandle.write(data)

    def get_runner_args(self):
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        runner = os.path.join(os.getcwd(), 'run_singleton.py')
        return [sys.executable, runner]

    def run_subprocess(self, args, logname='subprocess'):
        local_logger = logging.getLogger(logname)
        p = Popen(args, stderr=PIPE, stdout=PIPE)
        stdout, stderr = p.communicate()
        local_logger.debug('STDOUT = %s', stdout.decode('utf-8'))
        local_logger.debug('STDERR = %s', stderr.decode('utf-8'))
        return p.returncode

    def invoke_runner(self, dry_run=False):
        if dry_run:
            self.result = self.runner.invoke(
                cli,
                [
                    '--config',
                    self.args['configfile'],
                    '--debug-level',
                    DEBUG_LEVEL,
                    '--dry-run',
                    self.args['actionfile'],
                ],
            )
            return
        self.result = self.runner.invoke(cli, self.runner_args)

    def invoke_runner_alt(self, **kwargs):
        myargs = []
        if kwargs:
            for key, value in kwargs.items():
                myargs.append(f'--{key}')
                myargs.append(value)
            myargs.append(self.args['actionfile'])
            self.result = self.runner.invoke(cli, myargs)
