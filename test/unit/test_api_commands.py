from datetime import datetime, timedelta
from unittest import TestCase
from mock import Mock
import sys
from StringIO import StringIO

from curator import api as curator

named_index    = 'index_name'
named_indices  = [ "index1", "index2" ]
open_index     = {'metadata': {'indices' : { named_index : {'state' : 'open'}}}}
closed_index   = {'metadata': {'indices' : { named_index : {'state' : 'close'}}}}
open_indices   = { 'metadata': { 'indices' : { 'index1' : { 'state' : 'open' },
                                               'index2' : { 'state' : 'open' }}}}
closed_indices = { 'metadata': { 'indices' : { 'index1' : { 'state' : 'close' },
                                               'index2' : { 'state' : 'close' }}}}
fake_fail      = Exception('Simulated Failure')
named_alias    = 'alias_name'
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
        self.assertFalse(curator.add_to_alias(client, named_index, alias=named_alias))
    def test_add_to_alias_exception_test(self):
        client = Mock()
        client.indices.get_alias.return_value = alias_retval
        client.indices.exists_alias.return_value = True
        client.cluster.state.return_value = open_index
        client.indices.update_aliases.side_effect = fake_fail
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
    # The next two should cover the lines NOSE says are not covered.
    # They pass but I am confused, because NOSE says no coverage. :(
    def test_remove_from_alias_exception_raised(self):
        client = Mock()
        client.indices.exists_alias.return_value = True
        client.indices.get_alias.return_value = alias_retval
        client.indices.update_aliases.side_effect = fake_fail
        self.assertRaises(Exception, curator.remove_from_alias(client, "index1", alias=named_alias))
    def test_remove_from_alias_exception_return_false(self):
        client = Mock()
        client.indices.exists_alias.return_value = True
        client.indices.get_alias.return_value = alias_retval
        client.indices.update_aliases.side_effect = fake_fail
        self.assertFalse(curator.remove_from_alias(client, "index1", alias=named_alias))
    def test_remove_from_alias_index_not_found_in_alias(self):
        client = Mock()
        client.indices.exists_alias.return_value = False
        client.indices.get_alias.return_value = aliases_retval
        self.assertFalse(curator.remove_from_alias(client, "foo", alias=named_alias))

class TestAllocate(TestCase):
    def test_apply_allocation_rule_param_check(self):
        client = Mock()
        # Testing for the omission of the rule param
        self.assertFalse(curator.apply_allocation_rule(client, named_indices))
    def test_allocation_rule_positive(self):
        client = Mock()
        client.cluster.state.return_value = open_index
        client.indices.put_settings.return_value = None
        self.assertTrue(curator.apply_allocation_rule(client, named_index, rule="foo=bar"))
    def test_apply_allocation_rule_negative(self):
        client = Mock()
        client.cluster.state.return_value = open_index
        client.indices.put_settings.side_effect = fake_fail
        self.assertFalse(curator.apply_allocation_rule(client, named_index, rule="foo=bar"))
    def test_apply_allocation_rule_empty_list(self):
        client = Mock()
        self.assertFalse(curator.apply_allocation_rule(client, [], rule="foo=bar"))
    def test_allocation_positive(self):
        client = Mock()
        client.cluster.state.return_value = open_index
        client.indices.put_settings.return_value = None
        self.assertTrue(curator.allocation(client, named_index, rule="foo=bar"))
    def test_allocation_negative(self):
        client = Mock()
        client.cluster.state.return_value = open_index
        client.indices.put_settings.side_effect = fake_fail
        self.assertFalse(curator.allocation(client, named_index, rule="foo=bar"))

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
    def test_close_indices_positive(self):
        client = Mock()
        client.indices.flush.return_value = None
        client.indices.close.return_value = None
        self.assertTrue(curator.close_indices(client, named_index))
    def test_close_indices_negative(self):
        client = Mock()
        client.indices.flush.side_effect = fake_fail
        client.indices.close.return_value = None
        self.assertFalse(curator.close_indices(client, named_index))
    def test_full_close_positive(self):
        client = Mock()
        client.indices.flush.return_value = None
        client.indices.close.return_value = None
        self.assertTrue(curator.close(client, named_index))
    def test_full_close_negative(self):
        client = Mock()
        client.indices.flush.side_effect = fake_fail
        client.indices.close.return_value = None
        self.assertFalse(curator.close(client, named_index))

class TestDelete(TestCase):
    def test_delete_indices_positive(self):
        client = Mock()
        client.indices.delete.return_value = None
        self.assertTrue(curator.delete_indices(client, named_indices))
    def test_delete_indices_negative(self):
        client = Mock()
        client.indices.delete.side_effect = fake_fail
        self.assertFalse(curator.delete_indices(client, named_indices))
    def test_full_delete_positive(self):
        client = Mock()
        client.indices.delete.return_value = None
        self.assertTrue(curator.delete(client, named_indices))
    def test_full_delete_negative(self):
        client = Mock()
        client.indices.delete.side_effect = fake_fail
        self.assertFalse(curator.delete(client, named_indices))
    def test_full_delete_with_disk_space(self):
        client = Mock()
        ds = 2.0
        client.cluster.state.return_value = open_indices
        client.indices.status.return_value = indices_space
        self.assertTrue(curator.delete(client, named_indices, disk_space=ds))

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
    def test_optimize_index_bad_csv(self):
        client = Mock()
        self.assertFalse(curator.optimize_index(client, "a,b,c,d", max_num_segments=2))
    def test_optimize_index_missing_arg(self):
        client = Mock()
        self.assertFalse(curator.optimize_index(client, named_index))
    def test_optimize_index_closed(self):
        client = Mock()
        client.cluster.state.return_value = closed_index
        self.assertTrue(curator.optimize_index(client, named_index, max_num_segments=2))
    def test_optimize_index_positive(self):
        client = Mock()
        client.indices.segments.return_value = shards
        client.cluster.state.return_value = open_index
        client.indices.optimize.return_value = None
        self.assertTrue(curator.optimize_index(client, named_index, max_num_segments=2))
    def test_optimize_index_negative(self):
        client = Mock()
        client.indices.segments.return_value = shards
        client.cluster.state.return_value = open_index
        client.indices.optimize.side_effect = fake_fail
        self.assertFalse(curator.optimize_index(client, named_index, max_num_segments=2))
    def test_optimize_positive(self):
        client = Mock()
        client.indices.segments.return_value = shards
        client.cluster.state.return_value = open_index
        client.indices.optimize.return_value = None
        self.assertTrue(curator.optimize(client, named_index, max_num_segments=2))
    def test_optimize_negative(self):
        client = Mock()
        client.indices.segments.return_value = shards
        client.cluster.state.return_value = open_index
        client.indices.optimize.side_effect = fake_fail
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
        self.assertTrue(curator.change_replicas(client, named_indices, replicas=0))
    def test_change_replicas_negative(self):
        client = Mock()
        client.cluster.state.return_value = open_indices
        client.indices.put_settings.side_effect = fake_fail
        self.assertFalse(curator.change_replicas(client, named_indices, replicas=0))
    def test_replicas_positive(self):
        client = Mock()
        client.cluster.state.return_value = open_indices
        client.indices.put_settings.return_value = None
        self.assertTrue(curator.replicas(client, named_indices, replicas=0))
    def test_replicas_negative(self):
        client = Mock()
        client.cluster.state.return_value = open_indices
        client.indices.put_settings.side_effect = fake_fail
        self.assertFalse(curator.replicas(client, named_indices, replicas=0))

class TestShow(TestCase):
    def setUp(self):
        self.held, sys.stdout = sys.stdout, StringIO()
    def test_show_positive(self):
        curator.show(named_index)
        self.assertEqual(sys.stdout.getvalue(),'index_name\n')
    def test_show_positive_list(self):
        curator.show(named_indices)
        self.assertEqual(sys.stdout.getvalue(),'index1\nindex2\n')
