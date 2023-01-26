"""Test reindex action functionality"""
# pylint: disable=missing-function-docstring, missing-class-docstring, line-too-long
import os
from unittest.case import SkipTest
import pytest
from es_client.builder import ClientArgs, Builder
from curator.utils import get_indices
from . import CuratorTestCase
from . import testvars

UNDEF = 'undefined'
HOST = os.environ.get('TEST_ES_SERVER', 'http://127.0.0.1:9200')
RHOST = os.environ.get('REMOTE_ES_SERVER', UNDEF)
WAIT_INTERVAL = 1
MAX_WAIT = 3

class TestActionFileReindex(CuratorTestCase):
    """Test file-based reindex operations"""
    def test_reindex_manual(self):
        """Test that manual reindex results in proper count of documents"""
        source = 'my_source'
        dest = 'my_dest'
        expected = 3
        self.create_index(source)
        self.add_docs(source)
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(self.args['actionfile'], testvars.reindex.format(WAIT_INTERVAL, MAX_WAIT, source, dest))
        self.invoke_runner()
        assert expected == self.client.count(index=dest)['count']
    def test_reindex_selected(self):
        """Reindex selected indices"""
        source = 'my_source'
        dest = 'my_dest'
        expected = 3
        self.create_index(source)
        self.add_docs(source)
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(self.args['actionfile'], testvars.reindex.format(WAIT_INTERVAL, MAX_WAIT, 'REINDEX_SELECTION', dest))
        self.invoke_runner()
        assert expected == self.client.count(index=dest)['count']
    def test_reindex_empty_list(self):
        """
        This test raises <class 'curator.exceptions.FailedExecution'>:
        Reindex failed. The index or alias identified by "my_dest" was not found.
        The source is never created, so the dest is also not able to be created
        This means that we expect an empty list.
        """
        source = 'my_source'
        dest = 'my_dest'
        expected = []
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(self.args['actionfile'], testvars.reindex.format(WAIT_INTERVAL, MAX_WAIT, source, dest))
        self.invoke_runner()
        assert expected == get_indices(self.client)
    def test_reindex_selected_many_to_one(self):
        """Test reindexing many indices to one destination"""
        source1 = 'my_source1'
        source2 = 'my_source2'
        dest = 'my_dest'
        expected = 6
        self.create_index(source1)
        self.add_docs(source1)
        self.create_index(source2)
        for i in ["4", "5", "6"]:
            self.client.create(index=source2, id=i, document={"doc" + i :'TEST DOCUMENT'})
            # Decorators make this pylint exception necessary
            # pylint: disable=E1123
            self.client.indices.flush(index=source2, force=True)
            self.client.indices.refresh(index=source2)
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(self.args['actionfile'], testvars.reindex.format(WAIT_INTERVAL, MAX_WAIT, 'REINDEX_SELECTION', dest))
        self.invoke_runner()
        self.client.indices.refresh(index=dest)
        assert expected == self.client.count(index=dest)['count']
    def test_reindex_selected_empty_list_fail(self):
        """Ensure an empty list results in an exit code 1"""
        source1 = 'my_source1'
        source2 = 'my_source2'
        dest = 'my_dest'
        self.create_index(source1)
        self.add_docs(source1)
        self.create_index(source2)
        for i in ["4", "5", "6"]:
            self.client.create(index=source2, id=i, document={"doc" + i :'TEST DOCUMENT'})
            # Decorators make this pylint exception necessary
            # pylint: disable=E1123
            self.client.indices.flush(index=source2, force=True)
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(self.args['actionfile'], testvars.reindex_empty_list.format('false', WAIT_INTERVAL, MAX_WAIT, dest))
        self.invoke_runner()
        assert 1 == self.result.exit_code
    def test_reindex_selected_empty_list_pass(self):
        """Ensure an empty list results in an exit code 0"""
        source1 = 'my_source1'
        source2 = 'my_source2'
        dest = 'my_dest'
        self.create_index(source1)
        self.add_docs(source1)
        self.create_index(source2)
        for i in ["4", "5", "6"]:
            self.client.create(index=source2, id=i, document={"doc" + i :'TEST DOCUMENT'})
            # Decorators make this pylint exception necessary
            # pylint: disable=E1123
            self.client.indices.flush(index=source2, force=True)
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(self.args['actionfile'], testvars.reindex_empty_list.format('true', WAIT_INTERVAL, MAX_WAIT, dest))
        self.invoke_runner()
        assert 0 == self.result.exit_code

    @pytest.mark.skipif((RHOST == UNDEF), reason='REMOTE_ES_SERVER is not defined')
    def test_reindex_from_remote(self):
        """Test remote reindex functionality"""
        diff_wait = 6
        source1 = 'my_source1'
        source2 = 'my_source2'
        prefix = 'my_'
        dest = 'my_dest'
        expected = 6
        # Build remote client
        try:
            remote_args = ClientArgs()
            remote_args.hosts = RHOST
            remote_config = {'elasticsearch': {'client': remote_args.asdict()}}
            builder = Builder(configdict=remote_config, version_min=(5,0,0))
            builder.connect()
            rclient = builder.client
            rclient.info()
        except Exception as exc:
            raise SkipTest(f'Unable to connect to host at {RHOST}') from exc
        # Build indices remotely.
        counter = 0
        rclient.indices.delete(index=f'{source1},{source2}', ignore_unavailable=True)
        for rindex in [source1, source2]:
            rclient.indices.create(index=rindex)
            for i in range(0, 3):
                rclient.create(index=rindex, id=str(counter+1), document={"doc" + str(i) :'TEST DOCUMENT'})
                counter += 1
                # Decorators make this pylint exception necessary
                # pylint: disable=E1123
                rclient.indices.flush(index=rindex, force=True)
                rclient.indices.refresh(index=rindex)
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(self.args['actionfile'], testvars.remote_reindex.format(
                WAIT_INTERVAL,
                diff_wait,
                RHOST,
                'REINDEX_SELECTION',
                dest,
                prefix
            )
        )
        self.invoke_runner()
        # Do our own cleanup here.
        rclient.indices.delete(index=f'{source1},{source2}', ignore_unavailable=True)
        assert expected == self.client.count(index=dest)['count']

    @pytest.mark.skipif((RHOST == UNDEF), reason='REMOTE_ES_SERVER is not defined')
    def test_reindex_migrate_from_remote(self):
        """Test remote reindex migration"""
        source1 = 'my_source1'
        source2 = 'my_source2'
        prefix = 'my_'
        dest = 'MIGRATION'
        expected = 3
        # Build remote client
        try:
            remote_args = ClientArgs()
            remote_args.hosts = RHOST
            remote_config = {'elasticsearch': {'client': remote_args.asdict()}}
            builder = Builder(configdict=remote_config, version_min=(5,0,0))
            builder.connect()
            rclient = builder.client
            rclient.info()
        except Exception as exc:
            raise SkipTest(f'Unable to connect to host at {RHOST}') from exc
        # Build indices remotely.
        counter = 0
        rclient.indices.delete(index=f'{source1},{source2}', ignore_unavailable=True)
        for rindex in [source1, source2]:
            rclient.indices.create(index=rindex)
            for i in range(0, 3):
                rclient.create(index=rindex, id=str(counter+1), document={"doc" + str(i) :'TEST DOCUMENT'})
                counter += 1
                # Decorators make this pylint exception necessary
                # pylint: disable=E1123
                rclient.indices.flush(index=rindex, force=True)
                rclient.indices.refresh(index=rindex)
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(self.args['actionfile'], testvars.remote_reindex.format(
                WAIT_INTERVAL,
                MAX_WAIT,
                RHOST,
                'REINDEX_SELECTION',
                dest,
                prefix
            )
        )
        self.invoke_runner()
        # Do our own cleanup here.
        rclient.indices.delete(index=f'{source1},{source2}', ignore_unavailable=True)
        # And now the neat trick of verifying that the reindex worked to both
        # indices, and they preserved their names
        assert expected == self.client.count(index=source1)['count']
        assert expected == self.client.count(index=source2)['count']

    @pytest.mark.skipif((RHOST == UNDEF), reason='REMOTE_ES_SERVER is not defined')
    def test_reindex_migrate_from_remote_with_pre_suf_fixes(self):
        """Ensure migrate from remote with prefixes and suffixes works"""
        source1 = 'my_source1'
        source2 = 'my_source2'
        prefix = 'my_'
        dest = 'MIGRATION'
        expected = 3
        mpfx = 'pre-'
        msfx = '-fix'
        # Build remote client
        try:
            remote_args = ClientArgs()
            remote_args.hosts = RHOST
            remote_config = {'elasticsearch': {'client': remote_args.asdict()}}
            builder = Builder(configdict=remote_config, version_min=(5,0,0))
            builder.connect()
            rclient = builder.client
            rclient.info()
        except Exception as exc:
            raise SkipTest(f'Unable to connect to host at {RHOST}') from exc
        # Build indices remotely.
        counter = 0
        rclient.indices.delete(index=f'{source1},{source2}', ignore_unavailable=True)
        for rindex in [source1, source2]:
            rclient.indices.create(index=rindex)
            for i in range(0, 3):
                rclient.create(index=rindex, id=str(counter+1), document={"doc" + str(i) :'TEST DOCUMENT'})
                counter += 1
                # Decorators make this pylint exception necessary
                # pylint: disable=E1123
                rclient.indices.flush(index=rindex, force=True)
                rclient.indices.refresh(index=rindex)
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(self.args['actionfile'], testvars.migration_reindex.format(
                WAIT_INTERVAL,
                MAX_WAIT,
                mpfx,
                msfx,
                RHOST,
                'REINDEX_SELECTION',
                dest,
                prefix
            )
        )
        self.invoke_runner()
        # Do our own cleanup here.
        rclient.indices.delete(index=f'{source1},{source2}')
        # And now the neat trick of verifying that the reindex worked to both
        # indices, and they preserved their names
        assert expected == self.client.count(index=f'{mpfx}{source1}{msfx}')['count']
    def test_reindex_from_remote_no_connection(self):
        """Ensure that the inability to connect to the remote cluster fails"""
        dest = 'my_dest'
        bad_remote = 'http://127.0.0.1:9601'
        expected = 1
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(self.args['actionfile'], testvars.remote_reindex.format(
                WAIT_INTERVAL,
                MAX_WAIT,
                bad_remote,
                'REINDEX_SELECTION',
                dest,
                'my_'
            )
        )
        self.invoke_runner()
        assert expected == self.result.exit_code

    @pytest.mark.skipif((RHOST == UNDEF), reason='REMOTE_ES_SERVER is not defined')
    def test_reindex_from_remote_no_indices(self):
        """Test that attempting to reindex remotely with an empty list exits with a fail"""
        source1 = 'wrong1'
        source2 = 'wrong2'
        prefix = 'my_'
        dest = 'my_dest'
        expected = 1
        # Build remote client
        try:
            remote_args = ClientArgs()
            remote_args.hosts = RHOST
            remote_config = {'elasticsearch': {'client': remote_args.asdict()}}
            builder = Builder(configdict=remote_config, version_min=(5,0,0))
            builder.connect()
            rclient = builder.client
            rclient.info()
        except Exception as exc:
            raise SkipTest(f'Unable to connect to host at {RHOST}') from exc
        # Build indices remotely.
        counter = 0
        # Force remove my_source1 and my_source2 to prevent false positives
        rclient.indices.delete(index=f"{'my_source1'},{'my_source2'}", ignore_unavailable=True)
        rclient.indices.delete(index=f'{source1},{source2}', ignore_unavailable=True)
        for rindex in [source1, source2]:
            rclient.indices.create(index=rindex)
            for i in range(0, 3):
                rclient.create(index=rindex, id=str(counter+1), document={"doc" + str(i) :'TEST DOCUMENT'})
                counter += 1
                rclient.indices.flush(index=rindex, force=True)
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(self.args['actionfile'], testvars.remote_reindex.format(
                WAIT_INTERVAL,
                MAX_WAIT,
                f'{RHOST}',
                'REINDEX_SELECTION',
                dest,
                prefix
            )
        )
        self.invoke_runner()
        # Do our own cleanup here.
        rclient.indices.delete(index=f'{source1},{source2}')
        assert expected == self.result.exit_code
    def test_reindex_into_alias(self):
        """Ensure that reindexing into an alias works as expected"""
        source = 'my_source'
        dest = 'my_dest'
        expected = 3
        alias_dict = {dest : {}}
        self.client.indices.create(index='dummy', aliases=alias_dict)
        self.add_docs(source)
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(self.args['actionfile'], testvars.reindex.format(WAIT_INTERVAL, MAX_WAIT, source, dest))
        self.invoke_runner()
        assert expected == self.client.count(index=dest)['count']
    def test_reindex_manual_date_math(self):
        """Ensure date math is functional with reindex calls"""
        source = '<source-{now/d}>'
        dest = '<target-{now/d}>'
        expected = 3

        self.create_index(source)
        self.add_docs(source)
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(self.args['actionfile'], testvars.reindex.format(WAIT_INTERVAL, MAX_WAIT, source, dest))
        self.invoke_runner()
        assert expected == self.client.count(index=dest)['count']
    def test_reindex_bad_mapping(self):
        """This test addresses GitHub issue #1260"""
        source = 'my_source'
        dest = 'my_dest'
        expected = 1
        settings = { "number_of_shards": 1, "number_of_replicas": 0}
        mappings1 = { "properties": { "doc1": { "type": "keyword" }}}
        mappings2 = { "properties": { "doc1": { "type": "integer" }}}
        self.client.indices.create(index=source, settings=settings, mappings=mappings1)
        self.add_docs(source)
        # Create the dest index with a different mapping.
        self.client.indices.create(index=dest, settings=settings, mappings=mappings2)
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(self.args['actionfile'], testvars.reindex.format(WAIT_INTERVAL, MAX_WAIT, source, dest))
        self.invoke_runner()
        assert expected == self.result.exit_code
