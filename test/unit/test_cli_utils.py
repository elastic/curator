from datetime import datetime, timedelta
from unittest import TestCase
from mock import Mock
import sys
import click
from click import testing as clicktest

import logging
logger = logging.getLogger(__name__)

import curator

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

class TestExitMsg(TestCase):
    def test_exit_msg_positive(self):
        with self.assertRaises(SystemExit) as cm:
            curator.exit_msg(True)
        self.assertEqual(cm.exception.code, 0)
    def test_exit_msg_negative(self):
        with self.assertRaises(SystemExit) as cm:
            curator.exit_msg(False)
        self.assertEqual(cm.exception.code, 1)

class TestCheckVersion(TestCase):
    def test_check_version_positive(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '1.1.1'} }
        self.assertIsNone(curator.check_version(client))
    def test_check_version_less_than(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '0.90.3'} }
        with self.assertRaises(SystemExit) as cm:
            curator.check_version(client)
        self.assertEqual(cm.exception.code, 1)
    def test_check_version_greater_than(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '3.0.1'} }
        with self.assertRaises(SystemExit) as cm:
            curator.check_version(client)
        self.assertEqual(cm.exception.code, 1)

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
        self.assertEqual(cm.exception.code, 9)

class TestInList(TestCase):
    def test_in_list_positive(self):
        v = ['a', 'b']
        s = ['a', 'b', 'c', 'd']
        self.assertEqual(v, curator.in_list(v, s))
    def test_in_list_negative(self):
        v = ['a', 'b', 'q']
        s = ['a', 'b', 'c', 'd']
        self.assertEqual(['a', 'b'], curator.in_list(v, s))

class TestGetClient(TestCase):
    def test_certificate_logic(self):
        client = Mock()
        kwargs = { 'use_ssl' : True, 'certificate' : 'mycert.pem' }
        with self.assertRaises(SystemExit) as cm:
            curator.get_client(**kwargs)
            self.assertEqual(sys.stdout.getvalue(),'ERROR: Connection failure.\n')
        self.assertEqual(cm.exception.code, 1)
