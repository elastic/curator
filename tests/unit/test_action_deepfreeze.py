"""test_action_reindex"""

# pylint: disable=missing-function-docstring, missing-class-docstring, protected-access, attribute-defined-outside-init
from datetime import datetime
from unittest import TestCase
from unittest.mock import Mock
from curator.actions import Deepfreeze

from curator.exceptions import RepositoryException

# Get test variables and constants from a single source
from . import testvars


class TestActionDeepfreeze(TestCase):
    VERSION = {"version": {"number": "8.0.0"}}

    def builder(self):
        self.client = Mock()
        self.client.info.return_value = self.VERSION
        self.client.snapshot.get_repository.return_value = [
            "foo",
            "bar",
            "deepfreeze-foo",
            f"deepfreeze-{testvars.year:04}.{testvars.month_exists:02}",
        ]
        self.client.snapshot.create_repository.return_value = ""

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
            [
                "deepfreeze-foo",
                f"deepfreeze-{testvars.year:04}.{testvars.month_exists:02}",
            ],
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
        # Not sure how to test this since it gets this itself, not
        # from a client I could pass in.

    def test_creat_new_repo(self):
        self.builder()
        freezer = Deepfreeze(self.client)
        freezer.create_new_repo()

    def test_update_ilm_policies(self):
        self.builder()
        freezer = Deepfreeze(self.client)

    def test_unmount_oldest_repos(self):
        self.builder()
        freezer = Deepfreeze(self.client)
