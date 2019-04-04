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
            # ISO weeks
            # In 2014 ISO weeks were one week more than Greg weeks
            ('2014-42', '%Y-%W', datetime(2014, 10, 20)),
            ('2014-42', '%G-%V', datetime(2014, 10, 13)),
            ('2014-43', '%G-%V', datetime(2014, 10, 20)),
            # 
            ('2008-52', '%G-%V', datetime(2008, 12, 22)),
            ('2008-52', '%Y-%W', datetime(2008, 12, 29)),
            ('2009-01', '%Y-%W', datetime(2009, 1, 5)),
            ('2009-01', '%G-%V', datetime(2008, 12, 29)),
            # The case when both ISO and Greg are same week number
            ('2017-16', '%Y-%W', datetime(2017, 4, 17)),
            ('2017-16', '%G-%V', datetime(2017, 4, 17)),
            # Weeks were leading 0 is needed for week number
            ('2017-02', '%Y-%W', datetime(2017, 1, 9)),
            ('2017-02', '%G-%V', datetime(2017, 1, 9)),
            ('2010-01', '%G-%V', datetime(2010, 1, 4)),
            ('2010-01', '%Y-%W', datetime(2010, 1, 4)),
            # In Greg week 53 for year 2009 doesn't exist, it converts to week 1 of next year.
            ('2009-53', '%Y-%W', datetime(2010, 1, 4)),
            ('2009-53', '%G-%V', datetime(2009, 12, 28)),
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
            (1459287636, 1459287636),
            (14592876369, 14592876),
            (145928763699, 145928763),
            (1459287636999, 1459287636),
            (1459287636000000, 1459287636),
            (145928763600000000, 1459287636),
            (145928763600000001, 1459287636),
            (1459287636123456789, 1459287636),
                ]:
            self.assertEqual(epoch, curator.fix_epoch(long_epoch))
    def test_fix_epoch_raise(self):
            self.assertRaises(ValueError, curator.fix_epoch, None)

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
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_two
        client.indices.get_settings.side_effect = testvars.fake_fail
        self.assertRaises(
            curator.FailedExecution, curator.get_indices, client)
    def test_positive(self):
        client = Mock()
        client.indices.get_settings.return_value = testvars.settings_two
        client.info.return_value = {'version': {'number': '5.0.0'} }
        self.assertEqual(
            ['index-2016.03.03', 'index-2016.03.04'],
            sorted(curator.get_indices(client))
        )
    def test_empty(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_settings.return_value = {}
        self.assertEqual([], curator.get_indices(client))

class TestCheckVersion(TestCase):
    def test_check_version_(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.2'} }
        self.assertIsNone(curator.check_version(client))
    def test_check_version_less_than(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '2.4.3'} }
        self.assertRaises(curator.CuratorException, curator.check_version, client)
    def test_check_version_greater_than(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '8.0.1'} }
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

class TestShowDryRun(TestCase):
    # For now, since it's a pain to capture logging output, this is just a
    # simple code coverage run
    def test_index_list(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
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
        self.assertEqual(testvars.test_repo, 
            curator.get_repository(client, repository=testvars.repo_name))
    def test_get_repository_transporterror_negative(self):
        client = Mock()
        client.snapshot.get_repository.side_effect = elasticsearch.TransportError(503,'foo','bar')
        self.assertRaises(
            curator.CuratorException,
            curator.get_repository, client, repository=testvars.repo_name
        )
    def test_get_repository_notfounderror_negative(self):
        client = Mock()
        client.snapshot.get_repository.side_effect = elasticsearch.NotFoundError(404,'foo','bar')
        self.assertRaises(
            curator.CuratorException,
            curator.get_repository, client, repository=testvars.repo_name
        )
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
        client.tasks.get.return_value = testvars.no_snap_tasks
        self.assertFalse(
            curator.safe_to_snap(
                client, repository=testvars.repo_name,
                retry_interval=0, retry_count=1
            )
        )
    def test_ongoing_tasks_fail(self):
        client = Mock()
        client.snapshot.get.return_value = testvars.snapshots
        client.snapshot.get_repository.return_value = testvars.test_repo
        client.tasks.get.return_value = testvars.snap_task
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
        client.tasks.get.return_value = testvars.no_snap_tasks
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


class TestVerifyClientObject(TestCase):

    def test_is_client_object(self):
        test = elasticsearch.Elasticsearch()
        self.assertIsNone(curator.verify_client_object(test))

    def test_is_not_client_object(self):
        test = 'not a client object'
        self.assertRaises(TypeError, curator.verify_client_object, test)

    def test_is_a_subclass_client_object(self):
        class ElasticsearchSubClass(elasticsearch.Elasticsearch):
            pass
        test = ElasticsearchSubClass()
        self.assertIsNone(curator.verify_client_object(test))

class TestRollableAlias(TestCase):
    def test_return_false_if_no_alias(self):
        client = Mock()
        client.indices.get_alias.return_value = {}
        client.indices.get_alias.side_effect = elasticsearch.NotFoundError
        self.assertFalse(curator.rollable_alias(client, 'foo'))
    def test_return_false_too_many_indices(self):
        client = Mock()
        client.indices.get_alias.return_value = testvars.not_rollable_multiple
        self.assertFalse(curator.rollable_alias(client, 'foo'))
    def test_return_false_non_numeric(self):
        client = Mock()
        client.indices.get_alias.return_value = testvars.not_rollable_non_numeric
        self.assertFalse(curator.rollable_alias(client, 'foo'))
    def test_return_true_two_digits(self):
        client = Mock()
        client.indices.get_alias.return_value = testvars.is_rollable_2digits
        self.assertTrue(curator.rollable_alias(client, 'foo'))
    def test_return_true_hypenated(self):
        client = Mock()
        client.indices.get_alias.return_value = testvars.is_rollable_hypenated
        self.assertTrue(curator.rollable_alias(client, 'foo'))

class TestHealthCheck(TestCase):
    def test_no_kwargs(self):
        client = Mock()
        self.assertRaises(
            curator.MissingArgument, curator.health_check, client
        )
    def test_key_value_match(self):
        client = Mock()
        client.cluster.health.return_value = testvars.cluster_health
        self.assertTrue(
            curator.health_check(client, status='green')
        )
    def test_key_value_no_match(self):
        client = Mock()
        client.cluster.health.return_value = testvars.cluster_health
        self.assertFalse(
            curator.health_check(client, status='red')
        )
    def test_key_not_found(self):
        client = Mock()
        client.cluster.health.return_value = testvars.cluster_health
        self.assertRaises(
            curator.ConfigurationError,
            curator.health_check, client, foo='bar'
        )

class TestSnapshotCheck(TestCase):
    def test_fail_to_get_snapshot(self):
        client = Mock()
        client.snapshot.get.side_effect = testvars.fake_fail
        self.assertRaises(
            curator.CuratorException, curator.snapshot_check, client
        )
    def test_in_progress(self):
        client = Mock()
        client.snapshot.get.return_value = testvars.oneinprogress
        self.assertFalse(
            curator.snapshot_check(client, 
                repository='foo', snapshot=testvars.snap_name)
        )
    def test_success(self):
        client = Mock()
        client.snapshot.get.return_value = testvars.snapshot
        self.assertTrue(
            curator.snapshot_check(client, 
                repository='foo', snapshot=testvars.snap_name)
        )
    def test_partial(self):
        client = Mock()
        client.snapshot.get.return_value = testvars.partial
        self.assertTrue(
            curator.snapshot_check(client, 
                repository='foo', snapshot=testvars.snap_name)
        )
    def test_failed(self):
        client = Mock()
        client.snapshot.get.return_value = testvars.failed
        self.assertTrue(
            curator.snapshot_check(client, 
                repository='foo', snapshot=testvars.snap_name)
        )
    def test_other(self):
        client = Mock()
        client.snapshot.get.return_value = testvars.othersnap
        self.assertTrue(
            curator.snapshot_check(client, 
                repository='foo', snapshot=testvars.snap_name)
        )

class TestRestoreCheck(TestCase):
    def test_fail_to_get_recovery(self):
        client = Mock()
        client.indices.recovery.side_effect = testvars.fake_fail
        self.assertRaises(
            curator.CuratorException, curator.restore_check, client, []
        )
    def test_incomplete_recovery(self):
        client = Mock()
        client.indices.recovery.return_value = testvars.unrecovered_output
        self.assertFalse(
            curator.restore_check(client, testvars.named_indices)
        )
    def test_completed_recovery(self):
        client = Mock()
        client.indices.recovery.return_value = testvars.recovery_output
        self.assertTrue(
            curator.restore_check(client, testvars.named_indices)
        )
    def test_empty_recovery(self):
        client = Mock()
        client.indices.recovery.return_value = {}
        self.assertFalse(
            curator.restore_check(client, testvars.named_indices)
        )
    def test_fix_966(self):
        client = Mock()
        client.indices.recovery.return_value = testvars.recovery_966
        self.assertTrue(
            curator.restore_check(client, testvars.index_list_966)
        )

class TestTaskCheck(TestCase):
    def test_bad_task_id(self):
        client = Mock()
        client.tasks.get.side_effect = testvars.fake_fail
        self.assertRaises(
            curator.CuratorException, curator.task_check, client, 'foo'
        )
    def test_incomplete_task(self):
        client = Mock()
        client.tasks.get.return_value = testvars.incomplete_task
        self.assertFalse(
            curator.task_check(client, task_id=testvars.generic_task['task'])
        )
    def test_complete_task(self):
        client = Mock()
        client.tasks.get.return_value = testvars.completed_task
        self.assertTrue(
            curator.task_check(client, task_id=testvars.generic_task['task'])
        )

class TestWaitForIt(TestCase):
    def test_bad_action(self):
        client = Mock()
        self.assertRaises(
            curator.ConfigurationError, curator.wait_for_it, client, 'foo')
    def test_reindex_action_no_task_id(self):
        client = Mock()
        self.assertRaises(
            curator.MissingArgument, curator.wait_for_it, 
            client, 'reindex')
    def test_snapshot_action_no_snapshot(self):
        client = Mock()
        self.assertRaises(
            curator.MissingArgument, curator.wait_for_it, 
            client, 'snapshot', repository='foo')
    def test_snapshot_action_no_repository(self):
        client = Mock()
        self.assertRaises(
            curator.MissingArgument, curator.wait_for_it, 
            client, 'snapshot', snapshot='foo')
    def test_restore_action_no_indexlist(self):
        client = Mock()
        self.assertRaises(
            curator.MissingArgument, curator.wait_for_it, 
            client, 'restore')
    def test_reindex_action_bad_task_id(self):
        client = Mock()
        client.tasks.get.return_value = {'a':'b'}
        client.tasks.get.side_effect = testvars.fake_fail
        self.assertRaises(
            curator.CuratorException, curator.wait_for_it, 
            client, 'reindex', task_id='foo')
    def test_reached_max_wait(self):
        client = Mock()
        client.cluster.health.return_value = {'status':'red'}
        self.assertRaises(curator.ActionTimeout,
            curator.wait_for_it, client, 'replicas', 
                wait_interval=1, max_wait=1
        )

class TestDateRange(TestCase):
    def test_bad_unit(self):
        self.assertRaises(curator.ConfigurationError,
            curator.date_range, 'invalid', 1, 1
        )
    def test_bad_range(self):
        self.assertRaises(curator.ConfigurationError,
            curator.date_range, 'hours', 1, -1
        )
    def test_hours_single(self):
        unit = 'hours'
        range_from = -1
        range_to = -1
        epoch = curator.datetime_to_epoch(datetime(2017,  4,  3, 22, 50, 17))
        start = curator.datetime_to_epoch(datetime(2017,  4,  3, 21,  0,  0))
        end   = curator.datetime_to_epoch(datetime(2017,  4,  3, 21, 59, 59))
        self.assertEqual((start,end), 
            curator.date_range(unit, range_from, range_to, epoch=epoch))
    def test_hours_past_range(self):
        unit = 'hours'
        range_from = -3
        range_to = -1
        epoch = curator.datetime_to_epoch(datetime(2017,  4,  3, 22, 50, 17))
        start = curator.datetime_to_epoch(datetime(2017,  4,  3, 19,  0,  0))
        end   = curator.datetime_to_epoch(datetime(2017,  4,  3, 21, 59, 59))
        self.assertEqual((start,end), 
            curator.date_range(unit, range_from, range_to, epoch=epoch))
    def test_hours_future_range(self):
        unit = 'hours'
        range_from = 0
        range_to = 2
        epoch = curator.datetime_to_epoch(datetime(2017,  4,  3, 22, 50, 17))
        start = curator.datetime_to_epoch(datetime(2017,  4,  3, 22,  0,  0))
        end   = curator.datetime_to_epoch(datetime(2017,  4,  4, 00, 59, 59))
        self.assertEqual((start,end), 
            curator.date_range(unit, range_from, range_to, epoch=epoch))
    def test_hours_span_range(self):
        unit = 'hours'
        range_from = -1
        range_to = 2
        epoch = curator.datetime_to_epoch(datetime(2017,  4,  3, 22, 50, 17))
        start = curator.datetime_to_epoch(datetime(2017,  4,  3, 21,  0,  0))
        end   = curator.datetime_to_epoch(datetime(2017,  4,  4, 00, 59, 59))
        self.assertEqual((start,end), 
            curator.date_range(unit, range_from, range_to, epoch=epoch))
    def test_days_single(self):
        unit = 'days'
        range_from = -1
        range_to = -1
        epoch = curator.datetime_to_epoch(datetime(2017,  4,  3, 22, 50, 17))
        start = curator.datetime_to_epoch(datetime(2017,  4,  2,  0,  0,  0))
        end   = curator.datetime_to_epoch(datetime(2017,  4,  2, 23, 59, 59))
        self.assertEqual((start,end), 
            curator.date_range(unit, range_from, range_to, epoch=epoch))
    def test_days_past_range(self):
        unit = 'days'
        range_from = -3
        range_to = -1
        epoch = curator.datetime_to_epoch(datetime(2017,  4,  3, 22, 50, 17))
        start = curator.datetime_to_epoch(datetime(2017,  3, 31,  0,  0,  0))
        end   = curator.datetime_to_epoch(datetime(2017,  4,  2, 23, 59, 59))
        self.assertEqual((start,end), 
            curator.date_range(unit, range_from, range_to, epoch=epoch))
    def test_days_future_range(self):
        unit = 'days'
        range_from = 0
        range_to = 2
        epoch = curator.datetime_to_epoch(datetime(2017,  4,  3, 22, 50, 17))
        start = curator.datetime_to_epoch(datetime(2017,  4,  3,  0,  0,  0))
        end   = curator.datetime_to_epoch(datetime(2017,  4,  5, 23, 59, 59))
        self.assertEqual((start,end), 
            curator.date_range(unit, range_from, range_to, epoch=epoch))
    def test_days_span_range(self):
        unit = 'days'
        range_from = -1
        range_to = 2
        epoch = curator.datetime_to_epoch(datetime(2017,  4,  3, 22, 50, 17))
        start = curator.datetime_to_epoch(datetime(2017,  4,  2,  0,  0,  0))
        end   = curator.datetime_to_epoch(datetime(2017,  4,  5, 23, 59, 59))
        self.assertEqual((start,end), 
            curator.date_range(unit, range_from, range_to, epoch=epoch))
    def test_weeks_single(self):
        unit = 'weeks'
        range_from = -1
        range_to = -1
        epoch = curator.datetime_to_epoch(datetime(2017,  4,  3, 22, 50, 17))
        start = curator.datetime_to_epoch(datetime(2017,  3, 26,  0,  0,  0))
        end   = curator.datetime_to_epoch(datetime(2017,  4,  1, 23, 59, 59))
        self.assertEqual((start,end), 
            curator.date_range(unit, range_from, range_to, epoch=epoch))
    def test_weeks_past_range(self):
        unit = 'weeks'
        range_from = -3
        range_to = -1
        epoch = curator.datetime_to_epoch(datetime(2017,  4,  3, 22, 50, 17))
        start = curator.datetime_to_epoch(datetime(2017,  3, 12,  0,  0,  0))
        end   = curator.datetime_to_epoch(datetime(2017,  4,  1, 23, 59, 59))
        self.assertEqual((start,end), 
            curator.date_range(unit, range_from, range_to, epoch=epoch))
    def test_weeks_future_range(self):
        unit = 'weeks'
        range_from = 0
        range_to = 2
        epoch = curator.datetime_to_epoch(datetime(2017,  4,  3, 22, 50, 17))
        start = curator.datetime_to_epoch(datetime(2017,  4,  2, 00,  0,  0))
        end   = curator.datetime_to_epoch(datetime(2017,  4, 22, 23, 59, 59))
        self.assertEqual((start,end), 
            curator.date_range(unit, range_from, range_to, epoch=epoch))
    def test_weeks_span_range(self):
        unit = 'weeks'
        range_from = -1
        range_to = 2
        epoch = curator.datetime_to_epoch(datetime(2017,  4,  3, 22, 50, 17))
        start = curator.datetime_to_epoch(datetime(2017,  3, 26,  0,  0,  0))
        end   = curator.datetime_to_epoch(datetime(2017,  4, 22, 23, 59, 59))
        self.assertEqual((start,end), 
            curator.date_range(unit, range_from, range_to, epoch=epoch))
    def test_weeks_single_iso(self):
        unit = 'weeks'
        range_from = -1
        range_to = -1
        epoch = curator.datetime_to_epoch(datetime(2017,  4,  3, 22, 50, 17))
        start = curator.datetime_to_epoch(datetime(2017,  3, 27,  0,  0,  0))
        end   = curator.datetime_to_epoch(datetime(2017,  4,  2, 23, 59, 59))
        self.assertEqual((start,end), 
            curator.date_range(unit, range_from, range_to, epoch=epoch, 
                week_starts_on='monday')
        )
    def test_weeks_past_range_iso(self):
        unit = 'weeks'
        range_from = -3
        range_to = -1
        epoch = curator.datetime_to_epoch(datetime(2017,  4,  3, 22, 50, 17))
        start = curator.datetime_to_epoch(datetime(2017,  3, 13,  0,  0,  0))
        end   = curator.datetime_to_epoch(datetime(2017,  4,  2, 23, 59, 59))
        self.assertEqual((start,end), 
            curator.date_range(unit, range_from, range_to, epoch=epoch, 
                week_starts_on='monday')
        )
    def test_weeks_future_range_iso(self):
        unit = 'weeks'
        range_from = 0
        range_to = 2
        epoch = curator.datetime_to_epoch(datetime(2017,  4,  3, 22, 50, 17))
        start = curator.datetime_to_epoch(datetime(2017,  4,  3,  0,  0,  0))
        end   = curator.datetime_to_epoch(datetime(2017,  4, 23, 23, 59, 59))
        self.assertEqual((start,end), 
            curator.date_range(unit, range_from, range_to, epoch=epoch, 
                week_starts_on='monday')
        )
    def test_weeks_span_range_iso(self):
        unit = 'weeks'
        range_from = -1
        range_to = 2
        epoch = curator.datetime_to_epoch(datetime(2017,  4,  3, 22, 50, 17))
        start = curator.datetime_to_epoch(datetime(2017,  3, 27,  0,  0,  0))
        end   = curator.datetime_to_epoch(datetime(2017,  4, 23, 23, 59, 59))
        self.assertEqual((start,end), 
            curator.date_range(unit, range_from, range_to, epoch=epoch, 
                week_starts_on='monday')
        )
    def test_months_single(self):
        unit = 'months'
        range_from = -1
        range_to = -1
        epoch = curator.datetime_to_epoch(datetime(2017,  4,  3, 22, 50, 17))
        start = curator.datetime_to_epoch(datetime(2017,  3,  1,  0,  0,  0))
        end   = curator.datetime_to_epoch(datetime(2017,  3, 31, 23, 59, 59))
        self.assertEqual((start,end), 
            curator.date_range(unit, range_from, range_to, epoch=epoch))
    def test_months_past_range(self):
        unit = 'months'
        range_from = -4
        range_to = -1
        epoch = curator.datetime_to_epoch(datetime(2017,  4,  3, 22, 50, 17))
        start = curator.datetime_to_epoch(datetime(2016, 12,  1,  0,  0,  0))
        end   = curator.datetime_to_epoch(datetime(2017,  3, 31, 23, 59, 59))
        self.assertEqual((start,end), 
            curator.date_range(unit, range_from, range_to, epoch=epoch))
    def test_months_future_range(self):
        unit = 'months'
        range_from = 7
        range_to = 10
        epoch = curator.datetime_to_epoch(datetime(2017,  4,  3, 22, 50, 17))
        start = curator.datetime_to_epoch(datetime(2017, 11,  1,  0,  0,  0))
        end   = curator.datetime_to_epoch(datetime(2018,  2, 28, 23, 59, 59))
        self.assertEqual((start,end), 
            curator.date_range(unit, range_from, range_to, epoch=epoch))
    def test_months_super_future_range(self):
        unit = 'months'
        range_from = 9
        range_to = 10
        epoch = curator.datetime_to_epoch(datetime(2017,  4,  3, 22, 50, 17))
        start = curator.datetime_to_epoch(datetime(2018,  1,  1,  0,  0,  0))
        end   = curator.datetime_to_epoch(datetime(2018,  2, 28, 23, 59, 59))
        self.assertEqual((start,end), 
            curator.date_range(unit, range_from, range_to, epoch=epoch))
    def test_months_span_range(self):
        unit = 'months'
        range_from = -1
        range_to = 2
        epoch = curator.datetime_to_epoch(datetime(2017,  4,  3, 22, 50, 17))
        start = curator.datetime_to_epoch(datetime(2017,  3,  1,  0,  0,  0))
        end   = curator.datetime_to_epoch(datetime(2017,  6, 30, 23, 59, 59))
        self.assertEqual((start,end), 
            curator.date_range(unit, range_from, range_to, epoch=epoch))
    def test_years_single(self):
        unit = 'years'
        range_from = -1
        range_to = -1
        epoch = curator.datetime_to_epoch(datetime(2017,  4,  3, 22, 50, 17))
        start = curator.datetime_to_epoch(datetime(2016,  1,  1,  0,  0,  0))
        end   = curator.datetime_to_epoch(datetime(2016, 12, 31, 23, 59, 59))
        self.assertEqual((start,end), 
            curator.date_range(unit, range_from, range_to, epoch=epoch))
    def test_years_past_range(self):
        unit = 'years'
        range_from = -3
        range_to = -1
        epoch = curator.datetime_to_epoch(datetime(2017,  4,  3, 22, 50, 17))
        start = curator.datetime_to_epoch(datetime(2014,  1,  1,  0,  0,  0))
        end   = curator.datetime_to_epoch(datetime(2016, 12, 31, 23, 59, 59))
        self.assertEqual((start,end), 
            curator.date_range(unit, range_from, range_to, epoch=epoch))
    def test_years_future_range(self):
        unit = 'years'
        range_from = 0
        range_to = 2
        epoch = curator.datetime_to_epoch(datetime(2017,  4,  3, 22, 50, 17))
        start = curator.datetime_to_epoch(datetime(2017,  1,  1,  0,  0,  0))
        end   = curator.datetime_to_epoch(datetime(2019, 12, 31, 23, 59, 59))
        self.assertEqual((start,end), 
            curator.date_range(unit, range_from, range_to, epoch=epoch))
    def test_years_span_range(self):
        unit = 'years'
        range_from = -1
        range_to = 2
        epoch = curator.datetime_to_epoch(datetime(2017,  4,  3, 22, 50, 17))
        start = curator.datetime_to_epoch(datetime(2016,  1,  1,  0,  0,  0))
        end   = curator.datetime_to_epoch(datetime(2019, 12, 31, 23, 59, 59))
        self.assertEqual((start,end), 
            curator.date_range(unit, range_from, range_to, epoch=epoch))

class TestAbsoluteDateRange(TestCase):
    def test_bad_unit(self):
        unit = 'invalid'
        date_from = '2017.01'
        date_from_format = '%Y.%m'
        date_to = '2017.01'
        date_to_format = '%Y.%m'
        self.assertRaises(
            curator.ConfigurationError,
            curator.absolute_date_range, unit, date_from, date_to, date_from_format, date_to_format
        )
    def test_bad_formats(self):
        unit = 'days'
        self.assertRaises(
            curator.ConfigurationError,
            curator.absolute_date_range, unit, 'meh', 'meh', None, 'meh'
        )
        self.assertRaises(
            curator.ConfigurationError,
            curator.absolute_date_range, unit, 'meh', 'meh', 'meh', None
        )
    def test_bad_dates(self):
        unit = 'weeks'
        date_from_format = '%Y.%m'
        date_to_format = '%Y.%m'
        self.assertRaises(
            curator.ConfigurationError,
            curator.absolute_date_range, unit, 'meh', '2017.01', date_from_format, date_to_format
        )
        self.assertRaises(
            curator.ConfigurationError,
            curator.absolute_date_range, unit, '2017.01', 'meh', date_from_format, date_to_format
        )
    def test_single_month(self):
        unit = 'months'
        date_from = '2017.01'
        date_from_format = '%Y.%m'
        date_to = '2017.01'
        date_to_format = '%Y.%m'
        start = curator.datetime_to_epoch(datetime(2017,  1,  1,  0,  0,  0))
        end   = curator.datetime_to_epoch(datetime(2017,  1, 31, 23, 59, 59))
        self.assertEqual((start,end),
            curator.absolute_date_range(unit, date_from, date_to, date_from_format, date_to_format))
    def test_multiple_month(self):
        unit = 'months'
        date_from = '2016.11'
        date_from_format = '%Y.%m'
        date_to = '2016.12'
        date_to_format = '%Y.%m'
        start = curator.datetime_to_epoch(datetime(2016, 11,  1,  0,  0,  0))
        end   = curator.datetime_to_epoch(datetime(2016, 12, 31, 23, 59, 59))
        self.assertEqual((start,end),
            curator.absolute_date_range(unit, date_from, date_to, date_from_format, date_to_format))
    def test_single_year(self):
        unit = 'years'
        date_from = '2017'
        date_from_format = '%Y'
        date_to = '2017'
        date_to_format = '%Y'
        start = curator.datetime_to_epoch(datetime(2017,  1,  1,  0,  0,  0))
        end   = curator.datetime_to_epoch(datetime(2017, 12, 31, 23, 59, 59))
        self.assertEqual((start,end),
            curator.absolute_date_range(unit, date_from, date_to, date_from_format, date_to_format))
    def test_multiple_year(self):
        unit = 'years'
        date_from = '2016'
        date_from_format = '%Y'
        date_to = '2017'
        date_to_format = '%Y'
        start = curator.datetime_to_epoch(datetime(2016,  1,  1,  0,  0,  0))
        end   = curator.datetime_to_epoch(datetime(2017, 12, 31, 23, 59, 59))
        self.assertEqual((start,end),
            curator.absolute_date_range(unit, date_from, date_to, date_from_format, date_to_format))
    def test_single_week_UW(self):
        unit = 'weeks'
        date_from = '2017-01'
        date_from_format = '%Y-%U'
        date_to = '2017-01'
        date_to_format = '%Y-%U'
        start = curator.datetime_to_epoch(datetime(2017,  1,  2,  0,  0,  0))
        end   = curator.datetime_to_epoch(datetime(2017,  1,  8, 23, 59, 59))
        self.assertEqual((start,end),
            curator.absolute_date_range(unit, date_from, date_to, date_from_format, date_to_format))
    def test_multiple_weeks_UW(self):
        unit = 'weeks'
        date_from = '2017-01'
        date_from_format = '%Y-%U'
        date_to = '2017-04'
        date_to_format = '%Y-%U'
        start = curator.datetime_to_epoch(datetime(2017,  1,   2,  0,  0,  0))
        end   = curator.datetime_to_epoch(datetime(2017,  1,  29, 23, 59, 59))
        self.assertEqual((start,end),
            curator.absolute_date_range(unit, date_from, date_to, date_from_format, date_to_format))
    def test_single_week_ISO(self):
        unit = 'weeks'
        date_from = '2014-01'
        date_from_format = '%G-%V'
        date_to = '2014-01'
        date_to_format = '%G-%V'
        start = curator.datetime_to_epoch(datetime(2013, 12, 30,  0,  0,  0))
        end   = curator.datetime_to_epoch(datetime(2014,  1,  5, 23, 59, 59))
        self.assertEqual((start,end),
            curator.absolute_date_range(unit, date_from, date_to, date_from_format, date_to_format))
    def test_multiple_weeks_ISO(self):
        unit = 'weeks'
        date_from = '2014-01'
        date_from_format = '%G-%V'
        date_to = '2014-04'
        date_to_format = '%G-%V'
        start = curator.datetime_to_epoch(datetime(2013, 12, 30,  0,  0,  0))
        end   = curator.datetime_to_epoch(datetime(2014,  1, 26, 23, 59, 59))
        self.assertEqual((start,end),
            curator.absolute_date_range(unit, date_from, date_to, date_from_format, date_to_format))
    def test_single_day(self):
        unit = 'days'
        date_from = '2017.01.01'
        date_from_format = '%Y.%m.%d'
        date_to = '2017.01.01'
        date_to_format = '%Y.%m.%d'
        start = curator.datetime_to_epoch(datetime(2017,  1,  1,  0,  0,  0))
        end   = curator.datetime_to_epoch(datetime(2017,  1,  1, 23, 59, 59))
        self.assertEqual((start,end),
            curator.absolute_date_range(unit, date_from, date_to, date_from_format, date_to_format))
    def test_multiple_days(self):
        unit = 'days'
        date_from = '2016.12.31'
        date_from_format = '%Y.%m.%d'
        date_to = '2017.01.01'
        date_to_format = '%Y.%m.%d'
        start = curator.datetime_to_epoch(datetime(2016, 12, 31,  0,  0,  0))
        end   = curator.datetime_to_epoch(datetime(2017,  1,  1, 23, 59, 59))
        self.assertEqual((start,end),
            curator.absolute_date_range(unit, date_from, date_to, date_from_format, date_to_format))
    def test_ISO8601(self):
        unit = 'seconds'
        date_from = '2017-01-01T00:00:00'
        date_from_format = '%Y-%m-%dT%H:%M:%S'
        date_to = '2017-01-01T12:34:56'
        date_to_format = '%Y-%m-%dT%H:%M:%S'
        start = curator.datetime_to_epoch(datetime(2017,  1,  1,  0,  0,  0))
        end   = curator.datetime_to_epoch(datetime(2017,  1,  1, 12, 34, 56))
        self.assertEqual((start,end),
            curator.absolute_date_range(unit, date_from, date_to, date_from_format, date_to_format))

class TestNodeRoles(TestCase):
    def test_node_roles(self):
        node_id = u'my_node'
        expected = ['data']
        client = Mock()
        client.nodes.info.return_value = {u'nodes':{node_id:{u'roles':testvars.data_only_node_role}}}
        self.assertEqual(expected, curator.node_roles(client, node_id))

class TestSingleDataPath(TestCase):
    def test_single_data_path(self):
        node_id = 'my_node'
        client = Mock()
        client.nodes.stats.return_value = {u'nodes':{node_id:{u'fs':{u'data':[u'one']}}}}
        self.assertTrue(curator.single_data_path(client, node_id))
    def test_two_data_paths(self):
        node_id = 'my_node'
        client = Mock()
        client.nodes.stats.return_value = {u'nodes':{node_id:{u'fs':{u'data':[u'one',u'two']}}}}
        self.assertFalse(curator.single_data_path(client, node_id))

class TestNameToNodeId(TestCase):
    def test_positive(self):
        node_id = 'node_id'
        node_name = 'node_name'
        client = Mock()
        client.nodes.stats.return_value = {u'nodes':{node_id:{u'name':node_name}}}
        self.assertEqual(node_id, curator.name_to_node_id(client, node_name))
    def test_negative(self):
        node_id = 'node_id'
        node_name = 'node_name'
        client = Mock()
        client.nodes.stats.return_value = {u'nodes':{node_id:{u'name':node_name}}}
        self.assertIsNone(curator.name_to_node_id(client, 'wrong_name'))

class TestNodeIdToName(TestCase):
    def test_negative(self):
        client = Mock()
        client.nodes.stats.return_value = {u'nodes':{'my_node_id':{u'name':'my_node_name'}}}
        self.assertIsNone(curator.node_id_to_name(client, 'not_my_node_id'))

class TestIsDateMath(TestCase):
    def test_positive(self):
        data = '<encapsulated>'
        self.assertTrue(curator.isdatemath(data))
    def test_negative(self):
        data = 'not_encapsulated'
        self.assertFalse(curator.isdatemath(data))
    def test_raises(self):
        data = '<badly_encapsulated'
        self.assertRaises(curator.ConfigurationError, curator.isdatemath, data)

class TestGetDateMath(TestCase):
    def test_success(self):
        client = Mock()
        datemath = u'{hasthemath}'
        psuedo_random = u'not_random_at_all'
        expected = u'curator_get_datemath_function_' + psuedo_random + u'-hasthemath'
        client.indices.get.side_effect = (
            elasticsearch.NotFoundError(
                404, "simulated error", {u'error':{u'index':expected}})
        )
        self.assertEqual('hasthemath', curator.get_datemath(client, datemath, psuedo_random))
    def test_failure(self):
        client = Mock()
        datemath = u'{hasthemath}'
        client.indices.get.side_effect = (
            elasticsearch.NotFoundError(
                404, "simulated error", {u'error':{u'index':'failure'}})
        )
        self.assertRaises(curator.ConfigurationError, curator.get_datemath, client, datemath)
