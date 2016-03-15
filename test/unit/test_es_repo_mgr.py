from datetime import datetime, timedelta
from unittest import TestCase
from mock import patch, Mock
import click
import sys
try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO

import elasticsearch
from curator import api as curator

fake_fail      = Exception('Simulated Failure')
repo_name      = 'repo_name'

class TestCreateRepoBody(TestCase):
    def test_create_repo_body_missing_repo_type(self):
        with self.assertRaises(SystemExit) as cm:
            curator.create_repo_body()
        self.assertEqual(cm.exception.code, 1)

    def test_create_repo_body_s3(self):
        body = curator.create_repo_body(repo_type='s3')
        self.assertEqual(body['type'], 's3')

class TestCreateRepository(TestCase):
    def test_create_repository_missing_arg(self):
        client = Mock()
        with self.assertRaises(SystemExit) as cm:
            curator.create_repository(client)
        self.assertEqual(cm.exception.code, 1)

    def test_create_repository_empty_result_call(self):
        client = Mock()
        client.snapshot.get_repository.return_value = None
        self.assertTrue(curator.create_repository(client, repository="repo", repo_type="fs"))

    def test_create_repository_repo_not_in_results(self):
        client = Mock()
        client.snapshot.get_repository.return_value = {'not_your_repo':{'foo':'bar'}}
        self.assertTrue(curator.create_repository(client, repository="repo", repo_type="fs"))

    def test_create_repository_repo_already_in_results(self):
        client = Mock()
        client.snapshot.get_repository.return_value = {'repo':{'foo':'bar'}}
        with self.assertRaises(SystemExit) as cm:
            curator.create_repository(client, repository="repo", repo_type="fs")
        self.assertEqual(cm.exception.code, 1)

    def test_create_repository_exception(self):
        client = Mock()
        client.snapshot.get_repository.return_value = {'not_your_repo':{'foo':'bar'}}
        client.snapshot.create_repository.side_effect = elasticsearch.TransportError(500, "Error message", {"message":"Error"})
        self.assertRaises(Exception, curator.create_repository(client, repository="repo", repo_type="fs"))

class TestVerifyRepository(TestCase):
    def test_verify_repository_missing_arg(self):
        client = Mock()
        with self.assertRaises(SystemExit) as cm:
            curator.verify_repository(client)
        self.assertEqual(cm.exception.code, 1)

    def test_verify_repository_in_results(self):
        client = Mock()
        client.snapshot.get_repository.return_value = {'repo':{'foo':'bar'}}
        self.assertTrue(curator.verify_repository(client, repository="repo"))

    def test_verify_repository_repo_not_in_results(self):
        client = Mock()
        client.snapshot.get_repository.return_value = {'not_your_repo':{'foo':'bar'}}
        self.assertFalse(curator.verify_repository(client, repository="repo"))

class TestDeleteCallback(TestCase):
    def test_delete_callback_no_value(self):
        ctx = Mock()
        param = None
        value = None
        self.assertEqual(curator.delete_callback(ctx, param, value), None)

    def test_delete_callback_with_value(self):
        ctx = Mock()
        param = None
        value = True
        ctx.abort.return_value = None
        self.assertEqual(curator.delete_callback(ctx, param, value), None)
