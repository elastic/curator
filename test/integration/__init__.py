import logging
import os
import random
import shutil
import string
import sys
import tempfile
import time
from datetime import timedelta, datetime, date
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ConnectionError
from subprocess import Popen, PIPE
from curator import get_version

from . import testvars as testvars
from unittest import SkipTest, TestCase
from mock import Mock

client = None

DATEMAP = {
    'months': '%Y.%m',
    'weeks': '%Y.%W',
    'days': '%Y.%m.%d',
    'hours': '%Y.%m.%d.%H',
}

host, port = os.environ.get('TEST_ES_SERVER', 'localhost:9200').split(':')
port = int(port) if port else 9200

def random_directory():
    dirname = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(8))
    directory = tempfile.mkdtemp(suffix=dirname)
    if not os.path.exists(directory):
        os.makedirs(directory)
    return directory

def get_client():
    global client
    if client is not None:
        return client

    client = Elasticsearch([os.environ.get('TEST_ES_SERVER', {})], timeout=300)

    # wait for yellow status
    for _ in range(100):
        time.sleep(.1)
        try:
            # pylint: disable=E1123
            client.cluster.health(wait_for_status='yellow')
            return client
        except ConnectionError:
            continue
    else:
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
        args['host'], args['port'] = host, port
        args['time_unit'] = 'days'
        args['prefix'] = 'logstash-'
        self.args = args
        # dirname = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(8))
        # This will create a psuedo-random temporary directory on the machine
        # which runs the unit tests, but NOT on the machine where elasticsearch
        # is running. This means tests may fail if run against remote instances
        # unless you explicitly set `self.args['location']` to a proper spot
        # on the target machine.
        self.args['location'] = random_directory()
        self.args['configdir'] = random_directory()
        self.args['configfile'] = os.path.join(self.args['configdir'], 'curator.yml')
        self.args['actionfile'] = os.path.join(self.args['configdir'], 'actions.yml')
        self.args['repository'] = 'TEST_REPOSITORY'
        # if not os.path.exists(self.args['location']):
        #     os.makedirs(self.args['location'])
        self.logger.debug('setUp completed...')

    def tearDown(self):
        self.logger = logging.getLogger('CuratorTestCase.tearDown')
        self.logger.debug('tearDown initiated...')
        self.delete_repositories()
        self.client.indices.delete(index='*')
        # pylint: disable=E1123
        self.client.indices.delete_template(name='*', ignore=404)
        for path_arg in ['location', 'configdir']:
            if os.path.exists(self.args[path_arg]):
                shutil.rmtree(self.args[path_arg])

    def parse_args(self):
        return Args(self.args)

    def create_indices(self, count, unit=None, ilm_policy=None):
        now = datetime.utcnow()
        unit = unit if unit else self.args['time_unit']
        format = DATEMAP[unit]
        if not unit == 'months':
            step = timedelta(**{unit: 1})
            for _ in range(count):
                self.create_index(self.args['prefix'] + now.strftime(format), wait_for_yellow=False, ilm_policy=ilm_policy)
                now -= step
        else: # months
            now = date.today()
            d = date(now.year, now.month, 1)
            self.create_index(self.args['prefix'] + now.strftime(format), wait_for_yellow=False, ilm_policy=ilm_policy)

            for _ in range(1, count):
                if d.month == 1:
                    d = date(d.year-1, 12, 1)
                else:
                    d = date(d.year, d.month-1, 1)
                self.create_index(self.args['prefix'] + datetime(d.year, d.month, 1).strftime(format), wait_for_yellow=False, ilm_policy=ilm_policy)
        # pylint: disable=E1123
        self.client.cluster.health(wait_for_status='yellow')

    def wfy(self):
        # pylint: disable=E1123
        self.client.cluster.health(wait_for_status='yellow')

    def create_index(self, name, shards=1, wait_for_yellow=True, ilm_policy=None):
        request_body={'settings': {'number_of_shards': shards, 'number_of_replicas': 0}}
        if ilm_policy is not None:
            request_body['settings']['index'] = {'lifecycle': {'name': ilm_policy}}
        self.client.indices.create(index=name, body=request_body)
        if wait_for_yellow:
            self.wfy()

    def add_docs(self, idx):
        for i in ["1", "2", "3"]:
            ver = get_version(self.client)
            if ver >= (7, 0, 0):
                self.client.create(
                    index=idx, doc_type='_doc', id=i, body={"doc" + i :'TEST DOCUMENT'})
            else:
                self.client.create(
                    index=idx, doc_type='doc', id=i, body={"doc" + i :'TEST DOCUMENT'})
            # This should force each doc to be in its own segment.
            # pylint: disable=E1123
            self.client.indices.flush(index=idx, force=True)
            self.client.indices.refresh(index=idx)

    def create_snapshot(self, name, csv_indices):
        body = {
            "indices": csv_indices,
            "ignore_unavailable": False,
            "include_global_state": True,
            "partial": False,
        }
        self.create_repository()
        # pylint: disable=E1123
        self.client.snapshot.create(
            repository=self.args['repository'], snapshot=name, body=body,
            wait_for_completion=True
        )

    def create_repository(self):
        body = {'type':'fs', 'settings':{'location':self.args['location']}}
        self.client.snapshot.create_repository(repository=self.args['repository'], body=body)

    def delete_repositories(self):
        result = self.client.snapshot.get_repository(repository='_all')
        for repo in result:
            self.client.snapshot.delete_repository(repository=repo)

    def close_index(self, name):
        self.client.indices.close(index=name)

    def write_config(self, fname, data):
        with open(fname, 'w') as f:
            f.write(data)

    def get_runner_args(self):
        self.write_config(self.args['configfile'], testvars.client_config.format(host, port))
        runner = os.path.join(os.getcwd(), 'run_singleton.py')
        return [ sys.executable, runner ]
    
    def run_subprocess(self, args, logname='subprocess'):
        logger = logging.getLogger(logname)
        p = Popen(args, stderr=PIPE, stdout=PIPE)
        stdout, stderr = p.communicate()
        logger.debug('STDOUT = {0}'.format(stdout.decode('utf-8')))
        logger.debug('STDERR = {0}'.format(stderr.decode('utf-8')))
        return p.returncode