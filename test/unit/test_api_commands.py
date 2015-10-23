from datetime import datetime, timedelta
from unittest import TestCase
from mock import Mock
import sys
try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO

import elasticsearch
from curator import api as curator

named_index    = 'index_name'
named_indices  = [ "index1", "index2" ]
open_index     = {'metadata': {'indices' : { named_index : {'state' : 'open'}}}}
closed_index   = {'metadata': {'indices' : { named_index : {'state' : 'close'}}}}
cat_open_index = [{'status': 'open'}]
cat_closed_index = [{'status': 'close'}]
open_indices   = { 'metadata': { 'indices' : { 'index1' : { 'state' : 'open' },
                                               'index2' : { 'state' : 'open' }}}}
closed_indices = { 'metadata': { 'indices' : { 'index1' : { 'state' : 'close' },
                                               'index2' : { 'state' : 'close' }}}}
fake_fail      = Exception('Simulated Failure')
named_alias    = 'alias_name'
allocation_in  = {named_index: {'settings': {'index': {'routing': {'allocation': {'require': {'foo': 'bar'}}}}}}}
allocation_out = {named_index: {'settings': {'index': {'routing': {'allocation': {'require': {'not': 'foo'}}}}}}}
alias_retval   = { "pre_aliased_index": { "aliases" : { named_alias : { }}}}
aliases_retval = {
    "index1": { "aliases" : { named_alias : { } } },
    "index2": { "aliases" : { named_alias : { } } },
    }
indices_space  = { 'indices' : {
        'index1' : { 'index' : { 'primary_size_in_bytes': 1083741824 }},
        'index2' : { 'index' : { 'primary_size_in_bytes': 1083741824 }}}}
shards         = { 'indices': { named_index: { 'shards': {
        '0': [ { 'num_search_segments' : 15 }, { 'num_search_segments' : 21 } ],
        '1': [ { 'num_search_segments' : 19 }, { 'num_search_segments' : 16 } ] }}}}
optimize_tuple = (4, 71)
snap_name      = 'snap_name'
repo_name      = 'repo_name'
snap_running   = { 'snapshots': ['running'] }
nosnap_running = { 'snapshots': [] }
snapshot       = { 'snapshots': [
                    {
                        'duration_in_millis': 60000, 'start_time': '2015-01-01T00:00:00.000Z',
                        'shards': {'successful': 4, 'failed': 0, 'total': 4},
                        'end_time_in_millis': 0, 'state': 'SUCCESS',
                        'snapshot': snap_name, 'end_time': '2015-01-01T00:00:01.000Z',
                        'indices': named_indices,
                        'failures': [], 'start_time_in_millis': 0
                    }]}
partial        = { 'snapshots': [
                    {
                        'duration_in_millis': 60000, 'start_time': '2015-01-01T00:00:00.000Z',
                        'shards': {'successful': 4, 'failed': 0, 'total': 4},
                        'end_time_in_millis': 0, 'state': 'PARTIAL',
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
verified_nodes  = {'nodes': {'nodeid1': {'name': 'node1'}, 'nodeid2': {'name': 'node2'}}}
synced_pass     = {
                    "_shards":{"total":1,"successful":1,"failed":0},
                    "index_name":{
                        "total":1,"successful":1,"failed":0,
                        "failures":[],
                    }
                  }
synced_fail     = {
                    "_shards":{"total":1,"successful":0,"failed":1},
                    "index_name":{
                        "total":1,"successful":0,"failed":1,
                        "failures":[
                            {"shard":0,"reason":"pending operations","routing":{"state":"STARTED","primary":True,"node":"nodeid1","relocating_node":None,"shard":0,"index":"index_name"}},
                        ]
                    }
                  }
sync_conflict   = elasticsearch.ConflictError(409, u'{"_shards":{"total":1,"successful":0,"failed":1},"index_name":{"total":1,"successful":0,"failed":1,"failures":[{"shard":0,"reason":"pending operations","routing":{"state":"STARTED","primary":true,"node":"nodeid1","relocating_node":null,"shard":0,"index":"index_name"}}]}})', synced_fail)
synced_fails    = {
                    "_shards":{"total":2,"successful":1,"failed":1},
                    "index1":{
                        "total":1,"successful":0,"failed":1,
                        "failures":[
                            {"shard":0,"reason":"pending operations","routing":{"state":"STARTED","primary":True,"node":"nodeid1","relocating_node":None,"shard":0,"index":"index_name"}},
                        ]
                    },
                    "index2":{
                        "total":1,"successful":1,"failed":0,
                        "failures":[]
                    },
                  }

class TestAlias(TestCase):
    def test_add_to_alias_bad_csv(self):
        client = Mock()
        c = "a,b,c,d"
        self.assertFalse(curator.add_to_alias(client, c))
    def test_add_to_alias_no_alias_arg(self):
        client = Mock()
        self.assertFalse(curator.add_to_alias(client, named_index))
    def test_add_to_alias_alias_not_found(self):
        client = Mock()
        client.indices.exists_alias.return_value = False
        client.cluster.state.return_value = open_index
        client.info.return_value = {'version': {'number': '1.4.4'} }
        self.assertTrue(curator.add_to_alias(client, named_index, alias=named_alias))
    def test_add_to_alias_exception_test(self):
        client = Mock()
        client.indices.get_alias.return_value = alias_retval
        client.indices.exists_alias.return_value = True
        client.cluster.state.return_value = open_index
        client.indices.update_aliases.side_effect = fake_fail
        client.info.return_value = {'version': {'number': '1.4.4'} }
        self.assertFalse(curator.add_to_alias(client, named_index, alias=named_alias))
    def test_remove_from_alias_bad_csv(self):
        client = Mock()
        self.assertFalse(curator.remove_from_alias(client, "a,b,c,d"))
    def test_remove_from_alias_no_alias_arg(self):
        client = Mock()
        self.assertFalse(curator.remove_from_alias(client, named_index))
    def test_remove_from_alias_alias_not_found(self):
        client = Mock()
        client.indices.exists_alias.return_value = False
        self.assertFalse(curator.remove_from_alias(client, named_index, alias=named_alias))
    def test_remove_from_alias_exception_raised(self):
        client = Mock()
        client.indices.exists_alias.return_value = True
        client.indices.get_alias.return_value = aliases_retval
        client.indices.update_aliases.side_effect = fake_fail
        self.assertRaises(Exception, curator.remove_from_alias(client, "index1", alias=named_alias))
    def test_remove_from_alias_exception_return_false(self):
        client = Mock()
        client.indices.exists_alias.return_value = True
        client.indices.get_alias.return_value = aliases_retval
        client.indices.update_aliases.side_effect = fake_fail
        self.assertFalse(curator.remove_from_alias(client, "index1", alias=named_alias))
    def test_remove_from_alias_index_not_found_in_alias(self):
        client = Mock()
        client.indices.exists_alias.return_value = True
        client.indices.get_alias.return_value = aliases_retval
        self.assertFalse(curator.remove_from_alias(client, "foo", alias=named_alias))

class TestAllocate(TestCase):
    def test_apply_allocation_rule_param_check(self):
        client = Mock()
        # Testing for the omission of the rule param
        self.assertFalse(curator.apply_allocation_rule(client, named_indices))
    def test_allocation_rule_positive(self):
        client = Mock()
        client.indices.get_settings.return_value = allocation_out
        client.cluster.state.return_value = open_index
        client.indices.put_settings.return_value = None
        self.assertTrue(curator.apply_allocation_rule(client, named_index, rule="foo=bar"))
    def test_apply_allocation_rule_negative(self):
        client = Mock()
        client.cluster.state.return_value = open_index
        client.indices.get_settings.return_value = allocation_in
        client.indices.put_settings.return_value = None
        self.assertFalse(curator.apply_allocation_rule(client, named_index, rule="foo=bar"))
    def test_apply_allocation_rule_empty_list(self):
        client = Mock()
        self.assertFalse(curator.apply_allocation_rule(client, [], rule="foo=bar"))
    def test_allocation_positive(self):
        client = Mock()
        client.cluster.state.return_value = open_index
        client.indices.get_settings.return_value = allocation_out
        client.indices.put_settings.return_value = None
        self.assertTrue(curator.allocation(client, named_index, rule="foo=bar"))
    def test_allocation_negative_exception(self):
        client = Mock()
        client.cluster.state.return_value = open_index
        client.indices.get_settings.return_value = allocation_out
        client.indices.put_settings.side_effect = fake_fail
        self.assertFalse(curator.allocation(client, named_index, rule="foo=bar"))
    def test_allocation_wrong_type_param(self):
        client = Mock()
        client.cluster.state.return_value = open_index
        client.indices.get_settings.return_value = allocation_out
        client.indices.put_settings.return_value = None
        self.assertFalse(curator.allocation(client, named_index, rule="foo=bar", allocation_type="wrong_type"))
    def test_allocation_good_type_param(self):
        client = Mock()
        client.cluster.state.return_value = open_index
        client.indices.get_settings.return_value = allocation_out
        client.indices.put_settings.return_value = None
        self.assertTrue(curator.allocation(client, named_index, rule="foo=bar", allocation_type="require"))
        self.assertTrue(curator.allocation(client, named_index, rule="foo=bar", allocation_type="include"))
        self.assertTrue(curator.allocation(client, named_index, rule="foo=bar", allocation_type="exclude"))

class TestBloom(TestCase):
    def test_disable_bloom_no_more_bloom_positive(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '1.4.4'} }
        self.assertTrue(curator.disable_bloom_filter(client, named_index))
    def test_disable_bloom_no_more_bloom_positive(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '1.3.4'} }
        client.cluster.state.return_value = open_index
        client.indices.put_settings.return_value = None
        self.assertTrue(curator.disable_bloom_filter(client, named_index))
    def test_disable_bloom_exception_test(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '1.3.4'} }
        client.cluster.state.return_value = open_index
        client.indices.put_settings.side_effect = fake_fail
        self.assertRaises(Exception, curator.disable_bloom_filter(client, named_index))
    def test_disable_bloom_with_delay_positive(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '1.3.4'} }
        client.cluster.state.return_value = open_indices
        self.assertTrue(curator.disable_bloom_filter(
            client, named_indices, delay=1
            ))
    def test_disable_bloom_with_delay_negative(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '1.3.4'} }
        client.cluster.state.return_value = open_indices
        client.indices.put_settings.side_effect = fake_fail
        self.assertFalse(curator.disable_bloom_filter(
            client, named_indices, delay=1
            ))
    def test_bloom_full_positive(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '1.3.4'} }
        client.cluster.state.return_value = open_index
        client.indices.put_settings.return_value = None
        self.assertTrue(curator.bloom(client, named_index))
    def test_bloom_full_negative(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '1.3.4'} }
        client.cluster.state.return_value = open_index
        client.indices.put_settings.side_effect = fake_fail
        self.assertFalse(curator.bloom(client, named_index))

class TestClose(TestCase):
    def test_close_indices_positive_presyncflush(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '1.3.4'} }
        client.indices.flush.return_value = None
        client.indices.close.return_value = None
        self.assertTrue(curator.close_indices(client, named_index))
    def test_close_indices_negative_presyncflush(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '1.3.4'} }
        client.indices.flush.return_value = None
        client.indices.close.side_effect = fake_fail
        client.indices.close.return_value = None
        self.assertFalse(curator.close_indices(client, named_index))
    def test_full_close_positive_presyncflush(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '1.3.4'} }
        client.indices.flush.return_value = None
        client.indices.close.return_value = None
        self.assertTrue(curator.close(client, named_index))
    def test_full_close_negative_presyncflush(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '1.3.4'} }
        client.indices.flush.return_value = None
        client.indices.close.side_effect = fake_fail
        client.indices.close.return_value = None
        self.assertFalse(curator.close(client, named_index))
    def test_close_indices_positive(self):
        client = Mock()
        client.cat.indices.return_value = cat_open_index
        client.indices.flush_synced.return_value = synced_pass
        client.info.return_value = {'version': {'number': '1.6.0'} }
        client.indices.close.return_value = None
        self.assertTrue(curator.close_indices(client, named_index))
    def test_close_indices_negative(self):
        client = Mock()
        client.cat.indices.return_value = cat_open_index
        client.indices.flush_synced.return_value = synced_fail
        client.info.return_value = {'version': {'number': '1.6.0'} }
        client.indices.close.side_effect = fake_fail
        client.indices.close.return_value = None
        self.assertFalse(curator.close_indices(client, named_index))
    def test_full_close_positive(self):
        client = Mock()
        client.cat.indices.return_value = cat_open_index
        client.indices.flush_synced.return_value = synced_pass
        client.info.return_value = {'version': {'number': '1.6.0'} }
        client.indices.close.return_value = None
        self.assertTrue(curator.close(client, named_index))
    def test_full_close_negative(self):
        client = Mock()
        client.cat.indices.return_value = cat_open_index
        client.indices.flush_synced.return_value = synced_fail
        client.info.return_value = {'version': {'number': '1.6.0'} }
        client.indices.close.side_effect = fake_fail
        client.indices.close.return_value = None
        self.assertFalse(curator.close(client, named_index))

class TestDelete(TestCase):
    def test_delete_indices_positive(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '2.0.0'} }
        client.indices.delete.return_value = None
        self.assertTrue(curator.delete_indices(client, named_indices))
    def test_delete_indices_negative(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '2.0.0'} }
        client.indices.delete.side_effect = fake_fail
        self.assertFalse(curator.delete_indices(client, named_indices))

    # This test needs to be able to have get_settings return two different
    # values on subsequent calls.  I don't know how to do that, if it can
    # be done.  Integration testing can cover testing this method, though.
    #
    # def test_full_delete_positive(self):
    #     client = Mock()
    #     client.indices.delete.return_value = None
    #     client.indices.get_settings.return_value = named_indices
    #     self.assertTrue(curator.delete(client, named_indices))
    def test_full_delete_negative(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '1.7.2'} }
        client.indices.delete.return_value = None
        client.indices.get_settings.return_value = named_indices
        self.assertFalse(curator.delete(client, named_indices))
    def test_full_delete_exception(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '1.7.2'} }
        client.indices.delete.side_effect = fake_fail
        client.indices.get_settings.return_value = named_indices
        self.assertFalse(curator.delete(client, named_indices))

class TestOpen(TestCase):
    def test_opener_positive(self):
        client = Mock()
        client.indices.open.return_value = None
        self.assertTrue(curator.open_indices(client, named_indices))
    def test_opener_negative(self):
        client = Mock()
        client.indices.open.side_effect = fake_fail
        self.assertFalse(curator.open_indices(client, named_indices))
    def test_full_opener_positive(self):
        client = Mock()
        client.indices.open.return_value = None
        self.assertTrue(curator.opener(client, named_indices))
    def test_full_opener_negative(self):
        client = Mock()
        client.indices.open.side_effect = fake_fail
        self.assertFalse(curator.opener(client, named_indices))

class TestOptimize(TestCase):
    def test_optimize_index_positive(self):
        client = Mock()
        client.indices.segments.return_value = shards
        client.cluster.state.return_value = open_index
        client.indices.optimize.return_value = None
        client.info.return_value = {'version': {'number': '1.4.4'} }
        self.assertTrue(curator.optimize_index(client, named_index, max_num_segments=2))
    def test_optimize_index_negative(self):
        client = Mock()
        client.indices.segments.return_value = shards
        client.cluster.state.return_value = open_index
        client.indices.optimize.side_effect = fake_fail
        client.info.return_value = {'version': {'number': '1.4.4'} }
        self.assertFalse(curator.optimize_index(client, named_index, max_num_segments=2))
    def test_optimize_positive(self):
        client = Mock()
        client.indices.segments.return_value = shards
        client.cluster.state.return_value = open_index
        client.indices.optimize.return_value = None
        client.info.return_value = {'version': {'number': '1.4.4'} }
        self.assertTrue(curator.optimize(client, named_index, max_num_segments=2))
    def test_optimize_negative(self):
        client = Mock()
        client.indices.segments.return_value = shards
        client.cluster.state.return_value = open_index
        client.indices.optimize.side_effect = fake_fail
        client.info.return_value = {'version': {'number': '1.4.4'} }
        self.assertFalse(curator.optimize(client, named_index, max_num_segments=2))

class TestReplicas(TestCase):
    def test_change_replicas_param_check(self):
        client = Mock()
        # Testing for the omission of the replicas param
        self.assertFalse(curator.change_replicas(client, named_indices))
    def test_change_replicas_positive(self):
        client = Mock()
        client.cluster.state.return_value = open_indices
        client.indices.put_settings.return_value = None
        client.info.return_value = {'version': {'number': '1.4.4'} }
        self.assertTrue(curator.change_replicas(client, named_indices, replicas=0))
    def test_change_replicas_negative(self):
        client = Mock()
        client.cluster.state.return_value = open_indices
        client.indices.put_settings.side_effect = fake_fail
        client.info.return_value = {'version': {'number': '1.4.4'} }
        self.assertFalse(curator.change_replicas(client, named_indices, replicas=0))
    def test_replicas_positive(self):
        client = Mock()
        client.cluster.state.return_value = open_indices
        client.indices.put_settings.return_value = None
        client.info.return_value = {'version': {'number': '1.4.4'} }
        self.assertTrue(curator.replicas(client, named_indices, replicas=0))
    def test_replicas_negative(self):
        client = Mock()
        client.cluster.state.return_value = open_indices
        client.indices.put_settings.side_effect = fake_fail
        client.info.return_value = {'version': {'number': '1.4.4'} }
        self.assertFalse(curator.replicas(client, named_indices, replicas=0))

class TestSeal(TestCase):
    # The seal_indices method pretty much always returns True, requiring log
    # viewing to ascertain if one or more indices failed to seal.
    def test_seal_indices_good_version(self):
        client = Mock()
        client.cat.indices.return_value = cat_open_index
        client.indices.flush_synced.return_value = synced_pass
        client.info.return_value = {'version': {'number': '1.6.0'} }
        self.assertTrue(curator.seal_indices(client, named_index))
    def test_seal_indices_bad_version(self):
        client = Mock()
        client.cluster.state.return_value = open_index
        client.indices.flush_synced.return_value = synced_pass
        client.info.return_value = {'version': {'number': '1.3.4'} }
        self.assertTrue(curator.seal_indices(client, named_index))
    def test_seal_indices_conflicterror(self):
        client = Mock()
        client.cat.indices.return_value = cat_open_index
        client.indices.flush_synced.return_value = synced_fail
        client.indices.flush_synced.side_effect = sync_conflict
        client.info.return_value = {'version': {'number': '1.6.0'} }
        self.assertTrue(curator.seal_indices(client, named_index))
    def test_seal_indices_onepass_onefail(self):
        client = Mock()
        client.cat.indices.return_value = cat_open_index
        client.indices.flush_synced.return_value = synced_fails
        client.info.return_value = {'version': {'number': '1.6.0'} }
        self.assertTrue(curator.seal_indices(client, named_index))
    def test_seal_indices_attribute_exception(self):
        client = Mock()
        client.cat.indices.return_value = cat_open_index
        client.indices.flush_synced.return_value = synced_fail
        client.indices.flush_synced.side_effect = fake_fail
        client.info.return_value = {'version': {'number': '1.6.0'} }
        self.assertTrue(curator.seal_indices(client, named_index))

class TestShow(TestCase):
    def setUp(self):
        self.held, sys.stdout = sys.stdout, StringIO()
    def test_show_positive(self):
        client = Mock()
        client.cluster.state.return_value = open_index
        curator.show(client, named_index)
        self.assertEqual(sys.stdout.getvalue(),'index_name\n')
    def test_show_positive_list(self):
        client = Mock()
        client.cluster.state.return_value = open_indices
        curator.show(client, named_indices)
        self.assertEqual(sys.stdout.getvalue(),'index1\nindex2\n')

class TestSnapshot(TestCase):
    def test_create_snapshot_missing_arg_repository(self):
        client = Mock()
        self.assertFalse(curator.create_snapshot(client, name=snap_name))
    def test_create_snapshot_in_progress(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '1.4.4'} }
        client.snapshot.status.return_value = snap_running
        self.assertFalse(curator.create_snapshot(client, indices=[], repository=repo_name, name=snap_name))
    def test_create_snapshot_in_progress_old_version(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '1.0.3'} }
        client.snapshot.status.return_value = snap_running
        self.assertFalse(curator.create_snapshot(client, indices=[], repository=repo_name, name=snap_name))
    def test_create_snapshot_empty_arg_indices(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '1.4.4'} }
        client.snapshot.status.return_value = nosnap_running
        self.assertFalse(curator.create_snapshot(client, indices=[], repository=repo_name, name=snap_name))
    def test_create_snapshot_verify_nodes_positive(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '1.4.4'} }
        client.cluster.state.return_value = open_indices
        client.snapshot.get.return_value = snapshots
        client.snapshot.verify_repository.return_value = verified_nodes
        client.snapshot.status.return_value = nosnap_running
        self.assertTrue(
            curator.create_snapshot(
                client,
                indices=named_indices,
                repository=repo_name,
                name='not_snap_name'
            )
        )
    def test_create_snapshot_verify_nodes_negative(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '1.4.4'} }
        client.cluster.state.return_value = open_indices
        client.snapshot.get.return_value = snapshots
        client.snapshot.verify_repository.return_value = verified_nodes
        client.snapshot.verify_repository.side_effect = fake_fail
        client.snapshot.status.return_value = nosnap_running
        self.assertFalse(
            curator.create_snapshot(
                client,
                indices=named_indices,
                repository=repo_name,
                name='not_snap_name'
            )
        )
    def test_create_snapshot_name_collision(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '1.4.4'} }
        client.cluster.state.return_value = open_indices
        client.snapshot.get.return_value = snapshots
        client.snapshot.verify_repository.return_value = verified_nodes
        client.snapshot.status.return_value = nosnap_running
        self.assertFalse(
            self.assertFalse(
                curator.create_snapshot(
                    client,
                    indices=named_indices,
                    repository=repo_name,
                    name=snap_name
                )
            )
        )
    def test_create_snapshot_exception(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '1.4.4'} }
        client.cluster.state.return_value = open_indices
        client.snapshot.get.return_value = snapshots
        client.snapshot.verify_repository.return_value = verified_nodes
        client.snapshot.create.side_effect = elasticsearch.TransportError
        client.snapshot.status.return_value = nosnap_running
        self.assertFalse(
            curator.create_snapshot(
                client,
                indices=named_indices,
                repository=repo_name,
                name='not_snap_name'
            )
        )
    def test_create_snapshot_no_wait_for_completion(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '1.4.4'} }
        client.cluster.state.return_value = open_indices
        client.snapshot.get.return_value = snapshot
        client.snapshot.verify_repository.return_value = verified_nodes
        client.snapshot.status.return_value = nosnap_running
        self.assertTrue(
            curator.create_snapshot(
                client,
                indices=named_indices,
                repository=repo_name,
                wait_for_completion=False,
                name='not_snap_name'
            )
        )
    def test_create_snapshot_incomplete(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '1.4.4'} }
        client.cluster.state.return_value = open_indices
        client.snapshot.get.return_value = partial
        client.snapshot.verify_repository.return_value = verified_nodes
        client.snapshot.status.return_value = nosnap_running
        self.assertFalse(
            curator.create_snapshot(
                client,
                indices=named_indices,
                repository=repo_name,
                name='not_snap_name'
            )
        )

class TestDeleteSnapshot(TestCase):
    def test_delete_snapshot_missing_arg_repository(self):
        client = Mock()
        self.assertFalse(curator.delete_snapshot(client, snapshot=snap_name))
    def test_delete_snapshot_missing_arg_name(self):
        client = Mock()
        self.assertFalse(curator.delete_snapshot(client, repository=repo_name))
    def test_delete_snapshot_snapshot_is_list(self):
        client = Mock()
        self.assertFalse(curator.delete_snapshot(client, repository=repo_name, snapshot=['snap1', 'snap2']))
    def test_delete_snapshot_positive(self):
        client = Mock()
        client.snapshot.delete.return_value = None
        self.assertTrue(curator.delete_snapshot(client, repository=repo_name, snapshot=snap_name))
    def test_delete_snapshot_negative(self):
        client = Mock()
        client.snapshot.delete.side_effect = elasticsearch.RequestError
        self.assertFalse(curator.delete_snapshot(client, repository=repo_name, snapshot=snap_name))
