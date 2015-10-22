from datetime import datetime, timedelta
from unittest import TestCase
from mock import Mock
import elasticsearch

from curator import api as curator

named_index    = 'index_name'
closed_index   = {'metadata': {'indices' : { named_index : {'state' : 'close'}}}}
named_indices  = [ "index1", "index2" ]
named_alias    = 'alias_name'
alias_retval   = { "pre_aliased_index": { "aliases" : { named_alias : { }}}}
aliases_retval = {
    "index1": { "aliases" : { named_alias : { } } },
    "index2": { "aliases" : { named_alias : { } } },
    }
fake_fail      = Exception('Simulated Failure')
repo_name      = 'repo_name'
test_repo      = {repo_name: {'type': 'fs', 'settings': {'compress': 'true', 'location': '/tmp/repos/repo_name'}}}
test_repos     = {'TESTING': {'type': 'fs', 'settings': {'compress': 'true', 'location': '/tmp/repos/TESTING'}},
                  repo_name: {'type': 'fs', 'settings': {'compress': 'true', 'location': '/rmp/repos/repo_name'}}}
snap_name      = 'snap_name'
snapshot       = { 'snapshots': [
                    {
                        'duration_in_millis': 60000, 'start_time': '2015-01-01T00:00:00.000Z',
                        'shards': {'successful': 4, 'failed': 0, 'total': 4},
                        'end_time_in_millis': 0, 'state': 'SUCCESS',
                        'snapshot': snap_name, 'end_time': '2015-01-01T00:00:01.000Z',
                        'indices': named_indices,
                        'failures': [], 'start_time_in_millis': 0
                    }]}
snapshots       = { 'snapshots': [
                    {
                        'duration_in_millis': 60000, 'start_time': '2015-01-01T00:00:00.000Z',
                        'shards': {'successful': 4, 'failed': 0, 'total': 4},
                        'end_time_in_millis': 0, 'state': 'SUCCESS',
                        'snapshot': snap_name, 'end_time': '2015-01-01T00:00:01.000Z',
                        'indices': named_indices,
                        'failures': [], 'start_time_in_millis': 0
                    },
                    {
                        'duration_in_millis': 60000, 'start_time': '2015-01-01T00:00:02.000Z',
                        'shards': {'successful': 4, 'failed': 0, 'total': 4},
                        'end_time_in_millis': 0, 'state': 'SUCCESS',
                        'snapshot': 'snapshot2', 'end_time': '2015-01-01T00:00:03.000Z',
                        'indices': named_indices,
                        'failures': [], 'start_time_in_millis': 0
                    }]}
snap_body_all   = {
                    "ignore_unavailable": False,
                    "include_global_state": True,
                    "partial": False,
                    "indices" : "_all"
                  }
snap_body       = {
                    "ignore_unavailable": False,
                    "include_global_state": True,
                    "partial": False,
                    "indices" : "index1,index2"
                  }

class TestGetAlias(TestCase):
    def test_get_alias_positive(self):
        client = Mock()
        client.indices.exists_alias.return_value = True
        client.indices.get_alias.return_value = aliases_retval
        retval = sorted(curator.get_alias(client, named_alias))
        self.assertEqual(named_indices, retval)
    def test_get_alias_negative(self):
        client = Mock()
        client.indices.exists_alias.return_value = False
        self.assertFalse(curator.get_alias(client, named_alias))

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
        with self.assertRaises(SystemExit) as cm:
            curator.check_csv(v)
        self.assertEqual(cm.exception.code, 1)

class TestPruneKibana(TestCase):
    def test_prune_kibana_positive(self):
        l = [
            "logstash-2015.02.25", "logstash-2015.02.24", ".kibana",
            ".marvel-kibana", "kibana-int", ".marvel-2015.02.25",
            ".marvel-2015.02.24",
            ]
        r = [
            "logstash-2015.02.25", "logstash-2015.02.24", ".marvel-2015.02.25",
            ".marvel-2015.02.24",
            ]
        self.assertEqual(r, curator.prune_kibana(l))
    def test_prune_kibana_negative(self):
        l = [
            "logstash-2015.02.25", "logstash-2015.02.24", ".marvel-2015.02.25",
            ".marvel-2015.02.24",
            ]
        r = [
            "logstash-2015.02.25", "logstash-2015.02.24", ".marvel-2015.02.25",
            ".marvel-2015.02.24",
            ]
        self.assertEqual(r, curator.prune_kibana(l))
    def test_prune_kibana_empty(self):
        l = [ ".kibana", ".marvel-kibana", "kibana-int", ]
        r = []
        self.assertEqual(r, curator.prune_kibana(l))

class TestIndexClosed(TestCase):
    def test_cat_indices_json(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '1.7.2'} }
        client.cat.indices.return_value = [{'status': 'close'}]
        closed = curator.index_closed(client, named_index)
        self.assertEqual(closed, True)

    def test_cat_indices_text_plain(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '1.5.0'} }
        client.cat.indices.return_value = u'[{"status":"close"}]'
        closed = curator.index_closed(client, named_index)
        self.assertEqual(closed, True)

    def test_cluster_state(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '1.4.4'} }
        client.cluster.state.return_value = closed_index
        closed = curator.index_closed(client, named_index)
        self.assertEqual(closed, True)

    def test_open(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '1.7.2'} }
        client.cat.indices.return_value = [{'status': 'open'}]
        closed = curator.index_closed(client, named_index)
        self.assertEqual(closed, False)

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

class TestOptimized(TestCase):
    def test_optimized_index_bad_csv(self):
        client = Mock()
        self.assertRaises(ValueError, curator.optimized, client, "a,b,c,d", max_num_segments=2)
    def test_optimized_index_missing_arg(self):
        client = Mock()
        self.assertRaises(ValueError, curator.optimized, client, named_index)
    def test_optimized_index_closed(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '1.4.0'} }
        client.cluster.state.return_value = closed_index
        self.assertTrue(curator.optimized(client, named_index, max_num_segments=2))

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
            ('2010.12.29.12', '%Y.%m.%d.%H', datetime(2010, 12, 29, 12)),
            ('2009101112136', '%Y%m%d%H%M%S', datetime(2009, 10, 11, 12, 13, 6)),
                ]:
            self.assertEqual(dt, curator.get_datetime(text, datestring))

class TestGetRepository(TestCase):
    def test_get_repository_missing_arg(self):
        client = Mock()
        client.snapshot.get_repository.return_value = {}
        self.assertEqual({}, curator.get_repository(client))
    def test_get_repository_positive(self):
        client = Mock()
        client.snapshot.get_repository.return_value = test_repo
        self.assertEqual(test_repo, curator.get_repository(client, repository=repo_name))
    def test_get_repository_transporterror_negative(self):
        client = Mock()
        client.snapshot.get_repository.side_effect = elasticsearch.TransportError
        self.assertFalse(curator.get_repository(client, repository=repo_name))
    def test_get_repository_notfounderror_negative(self):
        client = Mock()
        client.snapshot.get_repository.side_effect = elasticsearch.NotFoundError
        self.assertFalse(curator.get_repository(client, repository=repo_name))
    def test_get_repository__all_positive(self):
        client = Mock()
        client.snapshot.get_repository.return_value = test_repos
        self.assertEqual(test_repos, curator.get_repository(client))

class TestGetSnapshot(TestCase):
    def test_get_snapshot_missing_repository_arg(self):
        client = Mock()
        self.assertFalse(curator.get_snapshot(client, snapshot=snap_name))
    def test_get_snapshot_positive(self):
        client = Mock()
        client.snapshot.get.return_value = snapshot
        self.assertEqual(snapshot, curator.get_snapshot(client, repository=repo_name, snapshot=snap_name))
    def test_get_snapshot_transporterror_negative(self):
        client = Mock()
        client.snapshot.get_repository.return_value = test_repo
        client.snapshot.get.side_effect = elasticsearch.TransportError
        self.assertFalse(curator.get_snapshot(client, repository=repo_name, snapshot=snap_name))
    def test_get_snapshot_notfounderror_negative(self):
        client = Mock()
        client.snapshot.get_repository.return_value = test_repo
        client.snapshot.get.side_effect = elasticsearch.NotFoundError
        self.assertFalse(curator.get_snapshot(client, repository=repo_name, snapshot=snap_name))

class TestGetSnapshots(TestCase):
    def test_get_snapshots_missing_repository_arg(self):
        client = Mock()
        self.assertFalse(curator.get_snapshots(client))
    def test_get_snapshots_positive(self):
        client = Mock()
        client.snapshot.get.return_value = snapshot
        self.assertEqual(['snap_name'], curator.get_snapshots(client, repository=repo_name))
    def test_get_snapshots_multiple_positive(self):
        client = Mock()
        client.snapshot.get.return_value = snapshots
        self.assertEqual(['snap_name', 'snapshot2'], curator.get_snapshots(client, repository=repo_name))
    def test_get_snapshots_transporterror_negative(self):
        client = Mock()
        client.snapshot.get_repository.return_value = test_repo
        client.snapshot.get.side_effect = elasticsearch.TransportError
        self.assertFalse(curator.get_snapshots(client, repository=repo_name))
    def test_get_snapshots_notfounderror_negative(self):
        client = Mock()
        client.snapshot.get_repository.return_value = test_repo
        client.snapshot.get.side_effect = elasticsearch.NotFoundError
        self.assertFalse(curator.get_snapshots(client, repository=repo_name))

class TestCreateSnapshotBody(TestCase):
    def test_create_snapshot_body_empty_arg(self):
        self.assertFalse(curator.create_snapshot_body([]))
    def test_create_snapshot_body__all_positive(self):
        self.assertEqual(snap_body_all, curator.create_snapshot_body('_all'))
    def test_create_snapshot_body_positive(self):
        self.assertEqual(snap_body, curator.create_snapshot_body(named_indices))
