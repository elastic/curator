from datetime import datetime, timedelta
from unittest import TestCase
from mock import Mock

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
indices_space  = { 'indices' : {
        'index1' : { 'index' : { 'primary_size_in_bytes': 1083741824 }},
        'index2' : { 'index' : { 'primary_size_in_bytes': 1083741824 }}}}

class FilterBySpace(TestCase):
    def test_filter_by_space_param_check(self):
        client = Mock()
        # Testing for the omission of the disk_space param
        self.assertFalse(curator.filter_by_space(client, named_indices))
    def test_filter_by_space_all_indices_closed(self):
        client = Mock()
        ds = 100.0
        client.cluster.state.return_value = closed_indices
        self.assertEqual([], curator.filter_by_space(client, named_indices, disk_space=ds))
    def test_filter_by_space_no_deletions_positive(self):
        client = Mock()
        ds = 10.0
        client.cluster.state.return_value = open_indices
        # Build return value of over 1G in size for each index
        client.indices.status.return_value = indices_space
        self.assertEqual([], curator.filter_by_space(client, named_indices, disk_space=ds))
    def test_filter_by_space_one_deletion(self):
        client = Mock()
        ds = 2.0
        client.cluster.state.return_value = open_indices
        # Build return value of over 1G in size for each index
        client.indices.status.return_value = indices_space
        self.assertEqual(["index1"], curator.filter_by_space(client, named_indices, disk_space=ds))
    def test_filter_by_space_one_deletion_no_reverse(self):
        client = Mock()
        ds = 2.0
        client.cluster.state.return_value = open_indices
        # Build return value of over 1G in size for each index
        client.indices.status.return_value = indices_space
        self.assertEqual(["index2"], curator.filter_by_space(client, named_indices, disk_space=ds, reverse=False))
