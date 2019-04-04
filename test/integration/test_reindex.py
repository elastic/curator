import elasticsearch
import curator
import os
import json
import string
import random
import tempfile
import click
from click import testing as clicktest
import time

from . import CuratorTestCase
from unittest.case import SkipTest
from . import testvars as testvars

import logging
logger = logging.getLogger(__name__)

host,  port  = os.environ.get('TEST_ES_SERVER',   'localhost:9200').split(':')
rhost, rport = os.environ.get('REMOTE_ES_SERVER', 'localhost:9201').split(':')
port  = int(port)  if port  else 9200
rport = int(rport) if rport else 9201

class TestActionFileReindex(CuratorTestCase):
    def test_reindex_manual(self):
        wait_interval = 1
        max_wait = 3
        source = 'my_source'
        dest = 'my_dest'
        expected = 3

        self.create_index(source)
        self.add_docs(source)
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(self.args['actionfile'],
            testvars.reindex.format(wait_interval, max_wait, source, dest))
        test = clicktest.CliRunner()
        _ = test.invoke(
            curator.cli,
            ['--config', self.args['configfile'], self.args['actionfile']],
        )
        self.assertEqual(expected, self.client.count(index=dest)['count'])
    def test_reindex_selected(self):
        wait_interval = 1
        max_wait = 3
        source = 'my_source'
        dest = 'my_dest'
        expected = 3

        self.create_index(source)
        self.add_docs(source)
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(self.args['actionfile'],
            testvars.reindex.format(wait_interval, max_wait, 'REINDEX_SELECTION', dest))
        test = clicktest.CliRunner()
        _ = test.invoke(
            curator.cli,
            ['--config', self.args['configfile'], self.args['actionfile']],
        )
        self.assertEqual(expected, self.client.count(index=dest)['count'])
    def test_reindex_empty_list(self):
        wait_interval = 1
        max_wait = 3
        source = 'my_source'
        dest = 'my_dest'
        expected = '.tasks'

        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(self.args['actionfile'],
            testvars.reindex.format(wait_interval, max_wait, source, dest))
        test = clicktest.CliRunner()
        _ = test.invoke(
            curator.cli,
            ['--config', self.args['configfile'], self.args['actionfile']],
        )
        self.assertEqual(expected, curator.get_indices(self.client)[0])
    def test_reindex_selected_many_to_one(self):
        wait_interval = 1
        max_wait = 3
        source1 = 'my_source1'
        source2 = 'my_source2'
        dest = 'my_dest'
        expected = 6

        self.create_index(source1)
        self.add_docs(source1)
        self.create_index(source2)
        for i in ["4", "5", "6"]:
            ver = curator.get_version(self.client)
            if ver >= (7, 0, 0):
                self.client.create(
                    index=source2, doc_type='doc', id=i, body={"doc" + i :'TEST DOCUMENT'})
            else:
                self.client.create(
                    index=source2, doc_type='doc', id=i, body={"doc" + i :'TEST DOCUMENT'})
            # Decorators make this pylint exception necessary
            # pylint: disable=E1123
            self.client.indices.flush(index=source2, force=True)
            self.client.indices.refresh(index=source2)
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(
            self.args['actionfile'],
            testvars.reindex.format(wait_interval, max_wait, 'REINDEX_SELECTION', dest)
        )
        test = clicktest.CliRunner()
        _ = test.invoke(
            curator.cli,
            ['--config', self.args['configfile'], self.args['actionfile']],
        )
        self.client.indices.refresh(index=dest)
        self.assertEqual(expected, self.client.count(index=dest)['count'])
    def test_reindex_selected_empty_list_fail(self):
        wait_interval = 1
        max_wait = 3
        source1 = 'my_source1'
        source2 = 'my_source2'
        dest = 'my_dest'
        expected = 6

        self.create_index(source1)
        self.add_docs(source1)
        self.create_index(source2)
        for i in ["4", "5", "6"]:
            self.client.create(
                index=source2, doc_type='log', id=i,
                body={"doc" + i :'TEST DOCUMENT'},
            )
            # Decorators make this pylint exception necessary
            # pylint: disable=E1123
            self.client.indices.flush(index=source2, force=True)
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(self.args['actionfile'],
            testvars.reindex_empty_list.format('false', wait_interval, max_wait, dest))
        test = clicktest.CliRunner()
        _ = test.invoke(
            curator.cli,
            ['--config', self.args['configfile'], self.args['actionfile']],
        )
        self.assertEqual(_.exit_code, 1)
    def test_reindex_selected_empty_list_pass(self):
        wait_interval = 1
        max_wait = 3
        source1 = 'my_source1'
        source2 = 'my_source2'
        dest = 'my_dest'
        expected = 6

        self.create_index(source1)
        self.add_docs(source1)
        self.create_index(source2)
        for i in ["4", "5", "6"]:
            self.client.create(
                index=source2, doc_type='log', id=i,
                body={"doc" + i :'TEST DOCUMENT'},
            )
            # Decorators make this pylint exception necessary
            # pylint: disable=E1123
            self.client.indices.flush(index=source2, force=True)
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(self.args['actionfile'],
            testvars.reindex_empty_list.format('true', wait_interval, max_wait, dest))
        test = clicktest.CliRunner()
        _ = test.invoke(
            curator.cli,
            ['--config', self.args['configfile'], self.args['actionfile']],
        )
        self.assertEqual(_.exit_code, 0)
    def test_reindex_from_remote(self):
        wait_interval = 1
        max_wait = 3
        source1 = 'my_source1'
        source2 = 'my_source2'
        prefix = 'my_'
        dest = 'my_dest'
        expected = 6

        # Build remote client
        try:
            rclient = curator.get_client(
                host=rhost, port=rport, skip_version_test=True)
            rclient.info()
        except:
            raise SkipTest(
                'Unable to connect to host at {0}:{1}'.format(rhost, rport))
        # Build indices remotely.
        counter = 0
        for rindex in [source1, source2]:
            rclient.indices.create(index=rindex)
            for i in range(0, 3):
                rclient.create(
                    index=rindex, doc_type='log', id=str(counter+1),
                    body={"doc" + str(counter+i) :'TEST DOCUMENT'},
                )
                counter += 1
                # Decorators make this pylint exception necessary
                # pylint: disable=E1123
                rclient.indices.flush(index=rindex, force=True)
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(self.args['actionfile'],
            testvars.remote_reindex.format(
                wait_interval, 
                max_wait, 
                'http://{0}:{1}'.format(rhost, rport),
                'REINDEX_SELECTION', 
                dest,
                prefix
            )
        )
        test = clicktest.CliRunner()
        _ = test.invoke(
            curator.cli,
            ['--config', self.args['configfile'], self.args['actionfile']],
        )
        # Do our own cleanup here.
        rclient.indices.delete(index='{0},{1}'.format(source1, source2))
        self.assertEqual(expected, self.client.count(index=dest)['count'])
    def test_reindex_migrate_from_remote(self):
        wait_interval = 1
        max_wait = 3
        source1 = 'my_source1'
        source2 = 'my_source2'
        prefix = 'my_'
        dest = 'MIGRATION'
        expected = 3


        # Build remote client
        try:
            rclient = curator.get_client(
                host=rhost, port=rport, skip_version_test=True)
            rclient.info()
        except:
            raise SkipTest(
                'Unable to connect to host at {0}:{1}'.format(rhost, rport))
        # Build indices remotely.
        counter = 0
        for rindex in [source1, source2]:
            rclient.indices.create(index=rindex)
            for i in range(0, 3):
                rclient.create(
                    index=rindex, doc_type='log', id=str(counter+1),
                    body={"doc" + str(counter+i) :'TEST DOCUMENT'},
                )
                counter += 1
                # Decorators make this pylint exception necessary
                # pylint: disable=E1123
                rclient.indices.flush(index=rindex, force=True)
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(self.args['actionfile'],
            testvars.remote_reindex.format(
                wait_interval, 
                max_wait, 
                'http://{0}:{1}'.format(rhost, rport),
                'REINDEX_SELECTION', 
                dest,
                prefix
            )
        )
        test = clicktest.CliRunner()
        _ = test.invoke(
            curator.cli,
            ['--config', self.args['configfile'], self.args['actionfile']],
        )
        # Do our own cleanup here.
        rclient.indices.delete(index='{0},{1}'.format(source1, source2))
        # And now the neat trick of verifying that the reindex worked to both 
        # indices, and they preserved their names
        self.assertEqual(expected, self.client.count(index=source1)['count'])
        self.assertEqual(expected, self.client.count(index=source2)['count'])

    def test_reindex_migrate_from_remote_with_pre_suf_fixes(self):
        wait_interval = 1
        max_wait = 3
        source1 = 'my_source1'
        source2 = 'my_source2'
        prefix = 'my_'
        dest = 'MIGRATION'
        expected = 3
        mpfx = 'pre-'
        msfx = '-fix'


        # Build remote client
        try:
            rclient = curator.get_client(
                host=rhost, port=rport, skip_version_test=True)
            rclient.info()
        except:
            raise SkipTest(
                'Unable to connect to host at {0}:{1}'.format(rhost, rport))
        # Build indices remotely.
        counter = 0
        for rindex in [source1, source2]:
            rclient.indices.create(index=rindex)
            for i in range(0, 3):
                rclient.create(
                    index=rindex, doc_type='log', id=str(counter+1),
                    body={"doc" + str(counter+i) :'TEST DOCUMENT'},
                )
                counter += 1
                # Decorators make this pylint exception necessary
                # pylint: disable=E1123
                rclient.indices.flush(index=rindex, force=True)
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(self.args['actionfile'],
            testvars.migration_reindex.format(
                wait_interval, 
                max_wait,
                mpfx,
                msfx,
                'http://{0}:{1}'.format(rhost, rport),
                'REINDEX_SELECTION', 
                dest,
                prefix
            )
        )
        test = clicktest.CliRunner()
        _ = test.invoke(
            curator.cli,
            ['--config', self.args['configfile'], self.args['actionfile']],
        )
        # Do our own cleanup here.
        rclient.indices.delete(index='{0},{1}'.format(source1, source2))
        # And now the neat trick of verifying that the reindex worked to both 
        # indices, and they preserved their names
        self.assertEqual(expected, self.client.count(index='{0}{1}{2}'.format(mpfx,source1,msfx))['count'])
        self.assertEqual(expected, self.client.count(index='{0}{1}{2}'.format(mpfx,source1,msfx))['count'])

    def test_reindex_from_remote_no_connection(self):
        wait_interval = 1
        max_wait = 3
        bad_port = 70000
        dest = 'my_dest'
        expected = 1
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(self.args['actionfile'],
            testvars.remote_reindex.format(
                wait_interval, 
                max_wait, 
                'http://{0}:{1}'.format(rhost, bad_port),
                'REINDEX_SELECTION', 
                dest,
                'my_'
            )
        )
        test = clicktest.CliRunner()
        _ = test.invoke(
            curator.cli,
            ['--config', self.args['configfile'], self.args['actionfile']],
        )
        self.assertEqual(expected, _.exit_code)
    def test_reindex_from_remote_no_indices(self):
        wait_interval = 1
        max_wait = 3
        source1 = 'wrong1'
        source2 = 'wrong2'
        prefix = 'my_'
        dest = 'my_dest'
        expected = 1

        # Build remote client
        try:
            rclient = curator.get_client(
                host=rhost, port=rport, skip_version_test=True)
            rclient.info()
        except:
            raise SkipTest(
                'Unable to connect to host at {0}:{1}'.format(rhost, rport))
        # Build indices remotely.
        counter = 0
        for rindex in [source1, source2]:
            rclient.indices.create(index=rindex)
            for i in range(0, 3):
                rclient.create(
                    index=rindex, doc_type='log', id=str(counter+1),
                    body={"doc" + str(counter+i) :'TEST DOCUMENT'},
                )
                counter += 1
                # Decorators make this pylint exception necessary
                # pylint: disable=E1123
                rclient.indices.flush(index=rindex, force=True)        
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(self.args['actionfile'],
            testvars.remote_reindex.format(
                wait_interval, 
                max_wait, 
                'http://{0}:{1}'.format(rhost, rport),
                'REINDEX_SELECTION', 
                dest,
                prefix
            )
        )
        test = clicktest.CliRunner()
        _ = test.invoke(
            curator.cli,
            ['--config', self.args['configfile'], self.args['actionfile']],
        )
        # Do our own cleanup here.
        rclient.indices.delete(index='{0},{1}'.format(source1, source2))
        self.assertEqual(expected, _.exit_code)
    def test_reindex_into_alias(self):
        wait_interval = 1
        max_wait = 3
        source = 'my_source'
        dest = 'my_dest'
        expected = 3
        alias_body = {'aliases' : {dest : {}}}
        self.client.indices.create(index='dummy', body=alias_body)
        self.add_docs(source)
        self.write_config(self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(
            self.args['actionfile'], testvars.reindex.format(wait_interval, max_wait, source, dest)
        )
        test = clicktest.CliRunner()
        _ = test.invoke(
            curator.cli,
            ['--config', self.args['configfile'], self.args['actionfile']],
        )
        self.assertEqual(expected, self.client.count(index=dest)['count'])
    def test_reindex_manual_date_math(self):
        wait_interval = 1
        max_wait = 3
        source = '<source-{now/d}>'
        dest = '<target-{now/d}>'
        expected = 3

        self.create_index(source)
        self.add_docs(source)
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(self.args['actionfile'],
            testvars.reindex.format(wait_interval, max_wait, source, dest))
        test = clicktest.CliRunner()
        _ = test.invoke(
            curator.cli,
            ['--config', self.args['configfile'], self.args['actionfile']],
        )
        self.assertEqual(expected, self.client.count(index=dest)['count'])
    def test_reindex_bad_mapping(self):
        # This test addresses GitHub issue #1260 
        wait_interval = 1
        max_wait = 3
        source = 'my_source'
        dest = 'my_dest'
        expected = 1
        ver = curator.get_version(self.client)
        if ver < (7, 0, 0):
            request_body = {
                "settings": { "number_of_shards": 1, "number_of_replicas": 0},
                "mappings": { "doc": { "properties": { "doc1": { "type": "keyword" }}}}
            }
        else:
            request_body = {
                "settings": { "number_of_shards": 1, "number_of_replicas": 0},
                "mappings": { "properties": { "doc1": { "type": "keyword" }}}
            }

        self.client.indices.create(index=source, body=request_body)
        self.add_docs(source)
        # Create the dest index with a different mapping.
        if ver < (7, 0, 0):
            request_body['mappings']['doc']['properties']['doc1']['type'] = 'integer'
        else:
            request_body['mappings']['properties']['doc1']['type'] = 'integer'
        self.client.indices.create(index=dest, body=request_body)
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(self.args['actionfile'],
            testvars.reindex.format(wait_interval, max_wait, source, dest))
        test = clicktest.CliRunner()
        _ = test.invoke(
            curator.cli,
            ['--config', self.args['configfile'], self.args['actionfile']],
        )
        self.assertEqual(expected, _.exit_code)
