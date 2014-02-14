import time
import os
from datetime import date, timedelta

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
    for _ in range(10):
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

    def create_day_indices(self, day_count):
        # TODO: hours as well
        format = self.args['separator'].join(('%Y', '%m', '%d'))
        for x in range(day_count):
            day = date.today() - timedelta(days=x)
            self.create_index(self.args['prefix'] + day.strftime(format))

    def create_index(self, name):
        self.client.indices.create(
            index=name,
            body={'settings': {'number_of_shards': 1, 'number_of_replicas': 0}}
        )

