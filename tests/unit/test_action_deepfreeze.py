"""test_action_deepfreeze"""

# pylint: disable=missing-function-docstring, missing-class-docstring, protected-access, attribute-defined-outside-init
import logging
import sys
from datetime import datetime
from unittest import TestCase
from unittest.mock import Mock

import boto3

from curator.actions import Deepfreeze
from curator.exceptions import RepositoryException

# Get test variables and constants from a single source
from . import testvars


class TestActionDeepfreeze(TestCase):
    VERSION = {"version": {"number": "8.0.0"}}

    def builder(self):
        self.client = Mock()
        self.client.info.return_value = self.VERSION
        self.client.snapshot.get_repository.return_value = testvars.repositories
        self.client.snapshot.create_repository.return_value = {}
        self.client.ilm.put_lifecycle.return_value = {}
        self.client.ilm.get_lifecycle.return_value = testvars.ilm_policy_to_update
        self.client.snapshot.delete_repository.return_value = {}

    def test_init_raise_request_error(self):
        self.builder()
        self.client.snapshot.get_repository.return_value = [
            "foo",
            "bar",
        ]
        with self.assertRaises(RepositoryException):
            Deepfreeze(client=self.client)

    def test_init_raise_repo_exists_error(self):
        self.builder()
        with self.assertRaises(RepositoryException):
            Deepfreeze(self.client, year=testvars.year, month=testvars.month_exists)

    def test_get_repos(self):
        self.builder()
        freezer = Deepfreeze(self.client)
        self.assertEqual(
            testvars.repositories_filtered,
            freezer.get_repos(),
        )

    def test_get_next_suffix_today(self):
        self.builder()
        year = datetime.now().year
        month = datetime.now().month
        freezer = Deepfreeze(self.client)
        self.assertEqual(freezer.get_next_suffix(), f"{year:04}.{month:02}")

    def test_get_next_suffix_for_date(self):
        self.builder()
        freezer = Deepfreeze(self.client, year=testvars.year, month=testvars.month)
        self.assertEqual(
            freezer.get_next_suffix(), f"{testvars.year:04}.{testvars.month:02}"
        )

    def test_create_new_bucket(self):
        self.builder()
        freezer = Deepfreeze(self.client)
        s3 = boto3.client("s3")
        freezer.create_new_bucket()
        response = s3.head_bucket(Bucket=freezer.new_bucket_name)
        self.assertEqual(response["ResponseMetadata"]["HTTPStatusCode"], 200)

    def test_create_new_repo(self):
        self.builder()
        freezer = Deepfreeze(self.client)
        freezer.create_new_repo()
        self.client.snapshot.create_repository.assert_called_with(
            name=freezer.new_repo_name,
            type="s3",
            settings={
                "bucket": freezer.new_bucket_name,
                "base_path": freezer.base_path,
                "canned_acl": freezer.canned_acl,
                "storage_class": freezer.storage_class,
            },
        )

    def test_update_ilm_policies(self):
        self.builder()
        freezer = Deepfreeze(self.client, year=testvars.year, month=testvars.month)
        freezer.update_ilm_policies()
        self.client.ilm.put_lifecycle.assert_called_with(
            policy_id="deepfreeze-ilm-policy",
            body=testvars.ilm_policy_updated,
        )

    def test_unmount_oldest_repos(self):
        self.builder()
        self.client.snapshot.get_repository.return_value = [
            "deepfreeze-2024.01",
            "deepfreeze-2024.02",
            "deepfreeze-2024.03",
            "deepfreeze-2024.04",
            "deepfreeze-2024.05",
            "deepfreeze-2024.06",
            "deepfreeze-2024.07",
        ]
        freezer = Deepfreeze(self.client)
        freezer.unmount_oldest_repos()
        self.client.snapshot.delete_repository.assert_called_with(
            name=freezer.repo_list[0]
        )
