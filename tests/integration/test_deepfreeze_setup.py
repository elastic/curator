"""
Test deepfreeze setup functionality
"""

# pylint: disable=missing-function-docstring, missing-class-docstring, line-too-long
import os

from . import CuratorTestCase, testvars

HOST = os.environ.get("TEST_ES_SERVER", "http://127.0.0.1:9200")


class TestCLISetup(CuratorTestCase):
    def test_setup(self):
        pass


class TestCLISetup_bucket_exists(CuratorTestCase):
    """
    Test deepfreeze setup functionality when the target bucket exists
    """

    def test_setup_bucket_exists(self):
        pass


class TestCLISetup_path_exists(CuratorTestCase):
    """
    Test deepfreeze setup functionality when the target path exists
    """

    def test_setup_path_exists(self):
        pass


class TestCLISetup_repo_exists(CuratorTestCase):
    """
    Test deepfreeze setup functionality when the target repository exists
    """

    def test_setup_repo_exists(self):
        pass


class TestCLISetup_bucket_path_repo_exist(CuratorTestCase):
    """
    Test deepfreeze setup functionality when the target bucket, path, and repository exist
    """

    def test_setup_bucket_path_repo_exist(self):
        pass


class TestCLISetup_status_index_exists(CuratorTestCase):
    """
    Test deepfreeze setup functionality when the target status index exists
    """

    def test_setup_status_index_exists(self):
        pass
