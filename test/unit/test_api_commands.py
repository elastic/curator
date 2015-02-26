from datetime import datetime, timedelta
from unittest import TestCase
from mock import Mock

from curator import api as curator

class TestAlias(TestCase):
    def test_get_alias_positive(self):
        client = Mock()
        client.indices.exists_alias.return_value = True
        client.indices.get_alias.return_value = {
            "index1": { "aliases" : { "alias_name" : { } } },
            "index2": { "aliases" : { "alias_name" : { } } },
            }
        retval = sorted(curator.get_alias(client, "alias_name"))
        l = [ "index1", "index2" ]
        self.assertEqual(l, retval)
    def test_get_alias_negative(self):
        client = Mock()
        client.indices.exists_alias.return_value = False
        self.assertFalse(curator.get_alias(client, "alias_name"))
    def test_add_to_alias_bad_csv(self):
        client = Mock()
        index_name = "a,b,c,d"
        self.assertFalse(curator.add_to_alias(client, index_name))
    def test_add_to_alias_no_alias_arg(self):
        client = Mock()
        index_name = "foo"
        self.assertFalse(curator.add_to_alias(client, index_name))
    def test_add_to_alias_alias_not_found(self):
        client = Mock()
        client.indices.exists_alias.return_value = False
        index_name = "foo"
        self.assertFalse(curator.add_to_alias(client, index_name, alias="abc"))
    def test_add_to_alias_exception_test(self):
        client = Mock()
        client.indices.get_alias.return_value = {
            "foo": { "aliases" : { "abc" : { } } },
            }
        client.indices.exists_alias.return_value = True
        client.cluster.state.return_value = {
            'metadata': {
                'indices' : {
                    'bar' : {
                        'state' : 'open'
                    }
                }
            }
        }
        client.indices.update_aliases.side_effect = Exception('Simulated Failure')
        self.assertRaises(Exception, curator.add_to_alias(client, "bar", alias="abc"))
    def test_remove_from_alias_bad_csv(self):
        client = Mock()
        index_name = "a,b,c,d"
        self.assertFalse(curator.remove_from_alias(client, index_name))
    def test_remove_from_alias_no_alias_arg(self):
        client = Mock()
        index_name = "foo"
        self.assertFalse(curator.remove_from_alias(client, index_name))
    def test_remove_from_alias_alias_not_found(self):
        client = Mock()
        client.indices.exists_alias.return_value = False
        index_name = "foo"
        self.assertFalse(curator.remove_from_alias(client, index_name, alias="abc"))
    def test_remove_from_alias_exception_test(self):
        client = Mock()
        client.indices.exists_alias.return_value = True
        client.indices.get_alias.return_value = {
            "foo": { "aliases" : { "abc" : { } } },
            }
        client.indices.update_aliases.side_effect = Exception('Simulated Failure')
        self.assertRaises(Exception, curator.remove_from_alias(client, "bar", alias="abc"))
    def test_remove_from_alias_exception_return_false(self):
        client = Mock()
        client.indices.exists_alias.return_value = True
        client.indices.get_alias.return_value = {
            "foo": { "aliases" : { "abc" : { } } },
            }
        client.indices.update_aliases.side_effect = Exception('Simulated Failure')
        self.assertFalse(curator.remove_from_alias(client, "bar", alias="abc"))
    def test_remove_from_alias_index_not_found_in_alias(self):
        client = Mock()
        client.indices.exists_alias.return_value = False
        client.indices.get_alias.return_value = {
            "index1": { "aliases" : { "alias_name" : { } } },
            "index2": { "aliases" : { "alias_name" : { } } },
            }
        index_name = "foo"
        self.assertFalse(curator.remove_from_alias(client, index_name, alias="abc"))

class TestBloom(TestCase):
    def test_disable_bloom_no_more_bloom_positive(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '1.4.4'} }
        self.assertTrue(curator.disable_bloom_filter(client, "index_name"))
    def test_disable_bloom_no_more_bloom_positive(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '1.3.4'} }
        client.cluster.state.return_value = {
            'metadata': {
                'indices' : {
                    'index_name' : {
                        'state' : 'open'
                    }
                }
            }
        }
        client.indices.put_settings.return_value = None
        self.assertTrue(curator.disable_bloom_filter(client, "index_name"))
    def test_disable_bloom_exception_test(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '1.3.4'} }
        client.cluster.state.return_value = {
            'metadata': {
                'indices' : {
                    'index_name' : {
                        'state' : 'open'
                    }
                }
            }
        }
        client.indices.put_settings.side_effect = Exception('Simulated Failure')
        self.assertRaises(Exception, curator.disable_bloom_filter(client, "index_name"))
    def test_disable_bloom_with_delay_positive(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '1.3.4'} }
        client.cluster.state.return_value = {
            'metadata': {
                'indices' : {
                    'index1' : {
                        'state' : 'open'
                    },
                    'index2' : {
                        'state' : 'open'
                    },
                }
            }
        }
        self.assertTrue(curator.disable_bloom_filter(
            client, ["index1", "index2"], delay=1
            ))
    def test_disable_bloom_with_delay_negative(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '1.3.4'} }
        client.cluster.state.return_value = {
            'metadata': {
                'indices' : {
                    'index1' : {
                        'state' : 'open'
                    },
                    'index2' : {
                        'state' : 'open'
                    },
                }
            }
        }
        client.indices.put_settings.side_effect = Exception('Simulated Failure')
        self.assertFalse(curator.disable_bloom_filter(
            client, ["index1", "index2"], delay=1
            ))
    def test_bloom_full_positive(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '1.3.4'} }
        client.cluster.state.return_value = {
            'metadata': {
                'indices' : {
                    'index_name' : {
                        'state' : 'open'
                    }
                }
            }
        }
        client.indices.put_settings.return_value = None
        self.assertTrue(curator.bloom(client, "index_name"))
    def test_bloom_full_negative(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '1.3.4'} }
        client.cluster.state.return_value = {
            'metadata': {
                'indices' : {
                    'index_name' : {
                        'state' : 'open'
                    }
                }
            }
        }
        client.indices.put_settings.side_effect = Exception('Simulated Failure')
        self.assertFalse(curator.bloom(client, "index_name"))

class TestClose(TestCase):
    def test_close_indices_positive(self):
        client = Mock()
        client.indices.flush.return_value = None
        client.indices.close.return_value = None
        self.assertTrue(curator.close_indices(client, "index"))
    def test_close_indices_negative(self):
        client = Mock()
        client.indices.flush.side_effect = Exception('Simulated Failure')
        client.indices.close.return_value = None
        self.assertFalse(curator.close_indices(client, "index"))
    def test_full_close_positive(self):
        client = Mock()
        client.indices.flush.return_value = None
        client.indices.close.return_value = None
        self.assertTrue(curator.close(client, "index"))
    def test_full_close_negative(self):
        client = Mock()
        client.indices.flush.side_effect = Exception('Simulated Failure')
        client.indices.close.return_value = None
        self.assertFalse(curator.close(client, "index"))

class TestDelete(TestCase):
    def test_delete_indices_positive(self):
        client = Mock()
        client.indices.delete.return_value = None
        self.assertTrue(curator.delete_indices(client, ["index1", "index2"]))
    def test_delete_indices_negative(self):
        client = Mock()
        client.indices.delete.side_effect = Exception('Simulated Failure')
        self.assertFalse(curator.delete_indices(client, ["index1", "index2"]))
    def test_full_delete_positive(self):
        client = Mock()
        client.indices.delete.return_value = None
        self.assertTrue(curator.delete(client, ["index1", "index2"]))
    def test_full_delete_negative(self):
        client = Mock()
        client.indices.delete.side_effect = Exception('Simulated Failure')
        self.assertFalse(curator.delete(client, ["index1", "index2"]))
    def test_full_delete_with_disk_space(self):
        client = Mock()
        ds = 2.0
        indices = ["logstash-2015.02.25", "logstash-2015.02.26"]
        client.cluster.state.return_value = {
            'metadata': {
                'indices' : {
                    'logstash-2015.02.25' : {
                        'state' : 'open'
                    },
                    'logstash-2015.02.26' : {
                        'state' : 'open'
                    },
                }
            }
        }
        # Build return value of over 1G in size for each index
        client.indices.status.return_value = {
            'indices' : {
                'logstash-2015.02.25' : {
                    'index' : { 'primary_size_in_bytes': 1083741824 }
                },
                'logstash-2015.02.26' : {
                    'index' : { 'primary_size_in_bytes': 1083741824 }
                },
            }
        }
        self.assertTrue(curator.delete(client, indices, disk_space=ds))
