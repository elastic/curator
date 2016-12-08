from datetime import datetime, timedelta
from unittest import TestCase
from mock import Mock
import elasticsearch
import yaml
from . import testvars as testvars

import curator

class TestEnsureList(TestCase):
    def test_ensure_list_returns_lists(self):
        l = ["a", "b", "c", "d"]
        e = ["a", "b", "c", "d"]
        self.assertEqual(e, curator.ensure_list(l))
        l = "abcd"
        e = ["abcd"]
        self.assertEqual(e, curator.ensure_list(l))
        l = [["abcd","defg"], 1, 2, 3]
        e = [["abcd","defg"], 1, 2, 3]
        self.assertEqual(e, curator.ensure_list(l))
        l = {"a":"b", "c":"d"}
        e = [{"a":"b", "c":"d"}]
        self.assertEqual(e, curator.ensure_list(l))

class TestTo_CSV(TestCase):
    def test_to_csv_will_return_csv(self):
        l = ["a", "b", "c", "d"]
        c = "a,b,c,d"
        self.assertEqual(c, curator.to_csv(l))
    def test_to_csv_will_return_single(self):
        l = ["a"]
        c = "a"
        self.assertEqual(c, curator.to_csv(l))
    def test_to_csv_will_return_None(self):
        l = []
        self.assertIsNone(curator.to_csv(l))

class TestCheckCSV(TestCase):
    def test_check_csv_positive(self):
        c = "1,2,3"
        self.assertTrue(curator.check_csv(c))
    def test_check_csv_negative(self):
        c = "12345"
        self.assertFalse(curator.check_csv(c))
    def test_check_csv_list(self):
        l = ["1", "2", "3"]
        self.assertTrue(curator.check_csv(l))
    def test_check_csv_unicode(self):
        u = u'test'
        self.assertFalse(curator.check_csv(u))
    def test_check_csv_wrong_value(self):
        v = 123
        self.assertRaises(TypeError,
            curator.check_csv, v
        )

class TestGetVersion(TestCase):
    def test_positive(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '9.9.9'} }
        version = curator.get_version(client)
        self.assertEqual(version, (9,9,9))
    def test_negative(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '9.9.9'} }
        version = curator.get_version(client)
        self.assertNotEqual(version, (8,8,8))
    def test_dev_version_4_dots(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '9.9.9.dev'} }
        version = curator.get_version(client)
        self.assertEqual(version, (9,9,9))
    def test_dev_version_with_dash(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '9.9.9-dev'} }
        version = curator.get_version(client)
        self.assertEqual(version, (9,9,9))

class TestIsMasterNode(TestCase):
    def test_positive(self):
        client = Mock()
        client.nodes.info.return_value = {
            'nodes': { "foo" : "bar"}
        }
        client.cluster.state.return_value = {
            "master_node" : "foo"
        }
        self.assertTrue(curator.is_master_node(client))
    def test_negative(self):
        client = Mock()
        client.nodes.info.return_value = {
            'nodes': { "bad" : "mojo"}
        }
        client.cluster.state.return_value = {
            "master_node" : "foo"
        }
        self.assertFalse(curator.is_master_node(client))

class TestGetIndexTime(TestCase):
    def test_get_datetime(self):
        for text, datestring, dt in [
            ('2014.01.19', '%Y.%m.%d', datetime(2014, 1, 19)),
            ('14.01.19', '%y.%m.%d', datetime(2014, 1, 19)),
            ('2014-01-19', '%Y-%m-%d', datetime(2014, 1, 19)),
            ('2010-12-29', '%Y-%m-%d', datetime(2010, 12, 29)),
            ('2012-12', '%Y-%m', datetime(2012, 12, 1)),
            ('2011.01', '%Y.%m', datetime(2011, 1, 1)),
            ('2014-28', '%Y-%W', datetime(2014, 7, 14)),
            ('2014-28', '%Y-%U', datetime(2014, 7, 14)),
            ('2010.12.29.12', '%Y.%m.%d.%H', datetime(2010, 12, 29, 12)),
            ('2009101112136', '%Y%m%d%H%M%S', datetime(2009, 10, 11, 12, 13, 6)),
            ('2016-03-30t16', '%Y-%m-%dt%H', datetime(2016, 3, 30, 16, 0)),
                ]:
            self.assertEqual(dt, curator.get_datetime(text, datestring))

class TestGetDateRegex(TestCase):
    def test_non_escaped(self):
        self.assertEqual(
            '\\d{4}\\-\\d{2}\\-\\d{2}t\\d{2}',
            curator.get_date_regex('%Y-%m-%dt%H')
        )
class TestFixEpoch(TestCase):
    def test_fix_epoch(self):
        for long_epoch, epoch in [
            (1459287636999, 1459287636),
            (1459287636000000, 1459287636),
            (145928763600000000, 1459287636),
            (145928763600000001, 1459287636),
            (1459287636123456789, 1459287636),
            (1459287636999, 1459287636),
                ]:
            self.assertEqual(epoch, curator.fix_epoch(long_epoch))
    def test_fix_epoch_raise(self):
        self.assertRaises(ValueError, curator.fix_epoch, 12345678901)

class TestGetPointOfReference(TestCase):
    def test_get_point_of_reference(self):
        epoch = 1459288037
        for unit, result in [
            ('seconds', epoch-1),
            ('minutes', epoch-60),
            ('hours', epoch-3600),
            ('days', epoch-86400),
            ('weeks', epoch-(86400*7)),
            ('months', epoch-(86400*30)),
            ('years', epoch-(86400*365)),
                ]:
            self.assertEqual(result, curator.get_point_of_reference(unit, 1, epoch))
    def test_get_por_raise(self):
        self.assertRaises(ValueError, curator.get_point_of_reference, 'invalid', 1)

class TestByteSize(TestCase):
    def test_byte_size(self):
        size = 3*1024*1024*1024*1024*1024*1024*1024
        unit = ['Z','E','P','T','G','M','K','']
        for i in range(0,7):
            self.assertEqual('3.0{0}B'.format(unit[i]), curator.byte_size(size))
            size /= 1024
    def test_byte_size_yotta(self):
        size = 3*1024*1024*1024*1024*1024*1024*1024*1024
        self.assertEqual('3.0YB', curator.byte_size(size))
    def test_raise_invalid(self):
        self.assertRaises(TypeError, curator.byte_size, 'invalid')

class TestChunkIndexList(TestCase):
    def test_big_list(self):
        indices = []
        for i in range(100,150):
            indices.append("superlongindexnamebyanystandardyouchoosethisissillyhowbigcanthisgetbeforeitbreaks" + str(i))
        self.assertEqual(2, len(curator.chunk_index_list(indices)))
    def test_small_list(self):
        self.assertEqual(1, len(curator.chunk_index_list(['short','list','of','indices'])))

class TestGetIndices(TestCase):
    def test_client_exception(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '2.4.1'} }
        client.indices.get_settings.return_value = testvars.settings_two
        client.indices.get_settings.side_effect = testvars.fake_fail
        self.assertRaises(
            curator.FailedExecution, curator.get_indices, client)
    def test_positive(self):
        client = Mock()
        client.indices.get_settings.return_value = testvars.settings_two
        client.info.return_value = {'version': {'number': '2.4.1'} }
        self.assertEqual(
            ['index-2016.03.03', 'index-2016.03.04'],
            sorted(curator.get_indices(client))
        )
    def test_empty(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '2.4.1'} }
        client.indices.get_settings.return_value = {}
        self.assertEqual([], curator.get_indices(client))
    def test_issue_826(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '2.4.2'} }
        client.indices.get_settings.return_value = testvars.settings_two
        client.indices.exists.return_value = True
        self.assertEqual(
            ['.security', 'index-2016.03.03', 'index-2016.03.04'],
            sorted(curator.get_indices(client))
        )

class TestCheckVersion(TestCase):
    def test_check_version_(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '2.2.0'} }
        self.assertIsNone(curator.check_version(client))
    def test_check_version_less_than(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '0.90.3'} }
        self.assertRaises(curator.CuratorException, curator.check_version, client)
    def test_check_version_greater_than(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '6.0.1'} }
        self.assertRaises(curator.CuratorException, curator.check_version, client)

class TestCheckMaster(TestCase):
    def test_check_master_positive(self):
        client = Mock()
        client.nodes.info.return_value = {
            'nodes': { "foo" : "bar"}
        }
        client.cluster.state.return_value = {
            "master_node" : "foo"
        }
        self.assertIsNone(curator.check_master(client, master_only=True))
    def test_check_master_negative(self):
        client = Mock()
        client.nodes.info.return_value = {
            'nodes': { "bad" : "mojo"}
        }
        client.cluster.state.return_value = {
            "master_node" : "foo"
        }
        with self.assertRaises(SystemExit) as cm:
            curator.check_master(client, master_only=True)
        self.assertEqual(cm.exception.code, 0)

class TestGetClient(TestCase):
    # These unit test cases can't really get a client object, so it's more for
    # code coverage than anything
    def test_url_prefix_none(self):
        kwargs = {
            'url_prefix': None, 'use_ssl' : True, 'ssl_no_validate' : True
        }
        self.assertRaises(
            elasticsearch.ElasticsearchException,
            curator.get_client, **kwargs
        )
    def test_url_prefix_none_str(self):
        kwargs = {
            'url_prefix': 'None', 'use_ssl' : True, 'ssl_no_validate' : True
        }
        self.assertRaises(
            elasticsearch.ElasticsearchException,
            curator.get_client, **kwargs
        )
    def test_master_only_multiple_hosts(self):
        kwargs = {
            'url_prefix': '', 'master_only' : True,
            'hosts' : ['127.0.0.1', '127.0.0.1']
        }
        self.assertRaises(
            curator.ConfigurationError,
            curator.get_client, **kwargs
        )
    def test_host_with_hosts(self):
        kwargs = {
            'url_prefix': '',
            'host' : '127.0.0.1',
            'hosts' : ['127.0.0.2'],
        }
        self.assertRaises(
            curator.ConfigurationError,
            curator.get_client, **kwargs
        )
    def test_certificate_logic(self):
        kwargs = { 'use_ssl' : True, 'certificate' : 'mycert.pem' }
        self.assertRaises(
            elasticsearch.ElasticsearchException,
            curator.get_client, **kwargs
        )
    def test_client_cert_logic(self):
        kwargs = { 'use_ssl' : True, 'client_cert' : 'myclientcert.pem' }
        self.assertRaises(
            elasticsearch.ElasticsearchException,
            curator.get_client, **kwargs
        )
    def test_client_key_logic(self):
        kwargs = { 'use_ssl' : True, 'client_key' : 'myclientkey.pem' }
        self.assertRaises(
            elasticsearch.ElasticsearchException,
            curator.get_client, **kwargs
        )
    def test_certificate_no_verify_logic(self):
        kwargs = { 'use_ssl' : True, 'ssl_no_validate' : True }
        self.assertRaises(
            elasticsearch.ElasticsearchException,
            curator.get_client, **kwargs
        )

class TestOverrideTimeout(TestCase):
    def test_no_change(self):
        self.assertEqual(30, curator.override_timeout(30, 'delete'))
    def test_forcemerge_snapshot(self):
        self.assertEqual(21600, curator.override_timeout(30, 'forcemerge'))
        self.assertEqual(21600, curator.override_timeout(30, 'snapshot'))
    def test_sync_flush(self):
        self.assertEqual(180, curator.override_timeout(30, 'sync_flush'))
    def test_invalid_action(self):
        self.assertEqual(30, curator.override_timeout(30, 'invalid'))
    def test_invalid_timeout(self):
        self.assertRaises(TypeError, curator.override_timeout('invalid', 'delete'))

class TestShowDryRun(TestCase):
    # For now, since it's a pain to capture logging output, this is just a
    # simple code coverage run
    def test_index_list(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '2.4.1'} }
        client.indices.get_settings.return_value = testvars.settings_two
        client.cluster.state.return_value = testvars.clu_state_two
        client.indices.stats.return_value = testvars.stats_two
        client.field_stats.return_value = testvars.fieldstats_two
        il = curator.IndexList(client)
        self.assertIsNone(curator.show_dry_run(il, 'test_action'))

class TestGetRepository(TestCase):
    def test_get_repository_missing_arg(self):
        client = Mock()
        client.snapshot.get_repository.return_value = {}
        self.assertEqual({}, curator.get_repository(client))
    def test_get_repository_positive(self):
        client = Mock()
        client.snapshot.get_repository.return_value = testvars.test_repo
        self.assertEqual(testvars.test_repo, curator.get_repository(client, repository=testvars.repo_name))
    def test_get_repository_transporterror_negative(self):
        client = Mock()
        client.snapshot.get_repository.side_effect = elasticsearch.TransportError
        self.assertFalse(curator.get_repository(client, repository=testvars.repo_name))
    def test_get_repository_notfounderror_negative(self):
        client = Mock()
        client.snapshot.get_repository.side_effect = elasticsearch.NotFoundError
        self.assertFalse(curator.get_repository(client, repository=testvars.repo_name))
    def test_get_repository__all_positive(self):
        client = Mock()
        client.snapshot.get_repository.return_value = testvars.test_repos
        self.assertEqual(testvars.test_repos, curator.get_repository(client))

class TestGetSnapshot(TestCase):
    def test_get_snapshot_missing_repository_arg(self):
        client = Mock()
        self.assertRaises(
            curator.MissingArgument,
            curator.get_snapshot, client, snapshot=testvars.snap_name
        )
    def test_get_snapshot_positive(self):
        client = Mock()
        client.snapshot.get.return_value = testvars.snapshot
        self.assertEqual(testvars.snapshot, curator.get_snapshot(client, repository=testvars.repo_name, snapshot=testvars.snap_name))
    def test_get_snapshot_transporterror_negative(self):
        client = Mock()
        client.snapshot.get_repository.return_value = testvars.test_repo
        client.snapshot.get.side_effect = testvars.four_oh_one
        self.assertRaises(
            curator.FailedExecution,
            curator.get_snapshot, client,
            repository=testvars.repo_name, snapshot=testvars.snap_name
        )
    def test_get_snapshot_notfounderror_negative(self):
        client = Mock()
        client.snapshot.get_repository.return_value = testvars.test_repo
        client.snapshot.get.side_effect = elasticsearch.NotFoundError(404, 'Snapshot not found')
        self.assertRaises(
            curator.FailedExecution,
            curator.get_snapshot, client,
            repository=testvars.repo_name, snapshot=testvars.snap_name
        )

class TestGetSnapshotData(TestCase):
    def test_missing_repo_arg(self):
        client = Mock()
        self.assertRaises(curator.MissingArgument, curator.get_snapshot_data, client)
    def test_return_data(self):
        client = Mock()
        client.snapshot.get.return_value = testvars.snapshots
        client.snapshot.get_repository.return_value = testvars.test_repo
        self.assertEqual(
            testvars.snapshots['snapshots'],
            curator.get_snapshot_data(client, repository=testvars.repo_name)
        )
    def test_raises_exception_onfail(self):
        client = Mock()
        client.snapshot.get.return_value = testvars.snapshots
        client.snapshot.get.side_effect = testvars.four_oh_one
        client.snapshot.get_repository.return_value = testvars.test_repo
        self.assertRaises(
            curator.FailedExecution,
            curator.get_snapshot_data, client, repository=testvars.repo_name
        )

class TestSnapshotInProgress(TestCase):
    def test_all_snapshots_for_in_progress(self):
        client = Mock()
        client.snapshot.get.return_value = testvars.inprogress
        client.snapshot.get_repository.return_value = testvars.test_repo
        self.assertEqual(
            'snapshot-2015.03.01',
            curator.snapshot_in_progress(client, repository=testvars.repo_name)
        )
    def test_specified_snapshot_in_progress(self):
        client = Mock()
        client.snapshot.get.return_value = testvars.inprogress
        client.snapshot.get_repository.return_value = testvars.test_repo
        self.assertEqual(
            'snapshot-2015.03.01',
            curator.snapshot_in_progress(
                client, repository=testvars.repo_name,
                snapshot='snapshot-2015.03.01'
            )
        )
    def test_specified_snapshot_in_progress_negative(self):
        client = Mock()
        client.snapshot.get.return_value = testvars.inprogress
        client.snapshot.get_repository.return_value = testvars.test_repo
        self.assertFalse(
            curator.snapshot_in_progress(
                client, repository=testvars.repo_name,
                snapshot=testvars.snap_name
            )
        )
    def test_all_snapshots_for_in_progress_negative(self):
        client = Mock()
        client.snapshot.get.return_value = testvars.snapshots
        client.snapshot.get_repository.return_value = testvars.test_repo
        self.assertFalse(
            curator.snapshot_in_progress(client, repository=testvars.repo_name)
        )
    def test_for_multiple_in_progress(self):
        client = Mock()
        client.snapshot.get.return_value = testvars.highly_unlikely
        client.snapshot.get_repository.return_value = testvars.test_repo
        self.assertRaises(
            curator.CuratorException,
            curator.snapshot_in_progress, client, repository=testvars.repo_name
        )

class TestCreateSnapshotBody(TestCase):
    def test_create_snapshot_body_empty_arg(self):
        self.assertFalse(curator.create_snapshot_body([]))
    def test_create_snapshot_body__all_positive(self):
        self.assertEqual(testvars.snap_body_all, curator.create_snapshot_body('_all'))
    def test_create_snapshot_body_positive(self):
        self.assertEqual(testvars.snap_body, curator.create_snapshot_body(testvars.named_indices))

class TestCreateRepoBody(TestCase):
    def test_missing_repo_type(self):
        self.assertRaises(curator.MissingArgument,
            curator.create_repo_body
        )

    def test_s3(self):
        body = curator.create_repo_body(repo_type='s3')
        self.assertEqual(body['type'], 's3')

class TestCreateRepository(TestCase):
    def test_missing_arg(self):
        client = Mock()
        self.assertRaises(curator.MissingArgument,
            curator.create_repository, client
        )

    def test_empty_result_call(self):
        client = Mock()
        client.snapshot.get_repository.return_value = None
        self.assertTrue(curator.create_repository(client, repository="repo", repo_type="fs"))

    def test_repo_not_in_results(self):
        client = Mock()
        client.snapshot.get_repository.return_value = {'not_your_repo':{'foo':'bar'}}
        self.assertTrue(curator.create_repository(client, repository="repo", repo_type="fs"))

    def test_repo_already_in_results(self):
        client = Mock()
        client.snapshot.get_repository.return_value = {'repo':{'foo':'bar'}}
        self.assertRaises(curator.FailedExecution,
            curator.create_repository, client, repository="repo", repo_type="fs"
        )

    def test_raises_exception(self):
        client = Mock()
        client.snapshot.get_repository.return_value = {'not_your_repo':{'foo':'bar'}}
        client.snapshot.create_repository.side_effect = elasticsearch.TransportError(500, "Error message", {"message":"Error"})
        self.assertRaises(curator.FailedExecution, curator.create_repository, client, repository="repo", repo_type="fs")

class TestRepositoryExists(TestCase):
    def test_missing_arg(self):
        client = Mock()
        self.assertRaises(curator.MissingArgument,
            curator.repository_exists, client
        )

    def test_repository_in_results(self):
        client = Mock()
        client.snapshot.get_repository.return_value = {'repo':{'foo':'bar'}}
        self.assertTrue(curator.repository_exists(client, repository="repo"))

    def test_repo_not_in_results(self):
        client = Mock()
        client.snapshot.get_repository.return_value = {'not_your_repo':{'foo':'bar'}}
        self.assertFalse(curator.repository_exists(client, repository="repo"))

class TestRepositoryFs(TestCase):
    def test_passing(self):
        client = Mock()
        client.snapshot.verify_repository.return_value = testvars.verified_nodes
        self.assertIsNone(
            curator.test_repo_fs(client, repository=testvars.repo_name))
    def test_raises_404(self):
        client = Mock()
        client.snapshot.verify_repository.return_value = testvars.verified_nodes
        client.snapshot.verify_repository.side_effect = testvars.four_oh_four
        self.assertRaises(curator.ActionError, curator.test_repo_fs, client,
            repository=testvars.repo_name)
    def test_raises_401(self):
        client = Mock()
        client.snapshot.verify_repository.return_value = testvars.verified_nodes
        client.snapshot.verify_repository.side_effect = testvars.four_oh_one
        self.assertRaises(curator.ActionError, curator.test_repo_fs, client,
            repository=testvars.repo_name)
    def test_raises_other(self):
        client = Mock()
        client.snapshot.verify_repository.return_value = testvars.verified_nodes
        client.snapshot.verify_repository.side_effect = testvars.fake_fail
        self.assertRaises(curator.ActionError, curator.test_repo_fs, client,
            repository=testvars.repo_name)

class TestSafeToSnap(TestCase):
    def test_missing_arg(self):
        client = Mock()
        self.assertRaises(curator.MissingArgument,
            curator.safe_to_snap, client
        )
    def test_in_progress_fail(self):
        client = Mock()
        client.snapshot.get.return_value = testvars.inprogress
        client.snapshot.get_repository.return_value = testvars.test_repo
        self.assertFalse(
            curator.safe_to_snap(
                client, repository=testvars.repo_name,
                retry_interval=0, retry_count=1
            )
        )
    def test_in_progress_pass(self):
        client = Mock()
        client.snapshot.get.return_value = testvars.snapshots
        client.snapshot.get_repository.return_value = testvars.test_repo
        self.assertTrue(
            curator.safe_to_snap(
                client, repository=testvars.repo_name,
                retry_interval=0, retry_count=1
            )
        )

class TestSnapshotRunning(TestCase):
    def test_true(self):
        client = Mock()
        client.snapshot.status.return_value = testvars.snap_running
        self.assertTrue(curator.snapshot_running(client))
    def test_false(self):
        client = Mock()
        client.snapshot.status.return_value = testvars.nosnap_running
        self.assertFalse(curator.snapshot_running(client))
    def test_raises_exception(self):
        client = Mock()
        client.snapshot.status.return_value = testvars.nosnap_running
        client.snapshot.status.side_effect = testvars.fake_fail
        self.assertRaises(
            curator.FailedExecution, curator.snapshot_running, client)

class TestPruneNones(TestCase):
    def test_prune_nones_with(self):
        self.assertEqual({}, curator.prune_nones({'a':None}))
    def test_prune_nones_without(self):
        a = {'foo':'bar'}
        self.assertEqual(a, curator.prune_nones(a))

class TestValidateFilters(TestCase):
    def test_snapshot_with_index_filter(self):
        self.assertRaises(
            curator.ConfigurationError,
            curator.validate_filters,
            'delete_snapshots',
            [{'filtertype': 'kibana'}]
        )
    def test_index_with_snapshot_filter(self):
        self.assertRaises(
            curator.ConfigurationError,
            curator.validate_filters,
            'delete_indices',
            [{'filtertype': 'state', 'state': 'SUCCESS'}]
        )
