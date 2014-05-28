import time
import os
from datetime import timedelta, datetime

from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ConnectionError

from curator import curator

from unittest import SkipTest, TestCase
from mock import Mock

client = None

host, port = os.environ.get('TEST_ES_SERVER', 'localhost:9200').split(':')
port = int(port) if port else 9200

def get_client():
    global client
    if client is not None:
        return client

    client = Elasticsearch([os.environ.get('TEST_ES_SERVER', {})])

    # wait for yellow status
    for _ in range(100):
        time.sleep(.1)
        try:
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
        self.client = get_client()

        # make sure we can inject any parameters to the curator; since it takes
        # all it's params from cmdline we have to resort to mocking
        self._old_parse = curator.make_parser
        curator.make_parser = Mock()
        curator.make_parser.return_value = self
        args = curator.DEFAULT_ARGS.copy()
        args['host'], args['port'] = host, port
        self.args = args

    def tearDown(self):
        self.client.indices.delete(index='*')
        self.client.indices.delete_template(name='*', ignore=404)
        curator.make_parser = self._old_parse

    def parse_args(self):
        return Args(self.args)

    def run_curator(self, **kwargs):
        self.args.update(kwargs)
        curator.main()

    def create_indices(self, count, unit=None, alias=''):
        now = datetime.utcnow()
        unit = unit if unit else self.args['time_unit']
        if unit == 'days':
            format = self.args['separator'].join(('%Y', '%m', '%d'))
        else:
            format = self.args['separator'].join(('%Y', '%m', '%d', '%H'))

        step = timedelta(**{unit: 1})
        for x in range(count):
            self.create_index(self.args['prefix'] + now.strftime(format), wait_for_yellow=False, alias=alias)
            now -= step

        self.client.cluster.health(wait_for_status='yellow')

    def create_index(self, name, shards=1, wait_for_yellow=True, alias=''):
        self.client.indices.create(
            index=name,
            body={'settings': {'number_of_shards': shards, 'number_of_replicas': 0}}
        )
        if alias:
            self.client.indices.update_aliases(body={'actions': [ { 'add': { 'index': name, 'alias': alias }} ]})
        if wait_for_yellow:
            self.client.cluster.health(wait_for_status='yellow')

