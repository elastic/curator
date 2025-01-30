"""Test the deepfreee Repository class"""

# pylint: disable=missing-function-docstring, pointless-statement, missing-class-docstring, protected-access, attribute-defined-outside-init
from unittest import TestCase

from curator.actions.deepfreeze import ThawedRepo


class TestClassDeepfreezeThawedRepo(TestCase):

    def test_default_values(self):
        tr = ThawedRepo()
        with self.assertRaises(AttributeError):
            tr.repo_name
        with self.assertRaises(AttributeError):
            tr.bucket_name
        with self.assertRaises(AttributeError):
            tr.base_path
        with self.assertRaises(AttributeError):
            tr.provider
        self.assertEqual(tr.indices, None)

    def test_set_from_hash(self):
        tr = ThawedRepo(
            {
                "repo_name": "my_repo",
                "bucket_name": "my_bucket",
                "base_path": "my_path",
                "provider": "aws",
                "indices": ["index1", "index2"],
            }
        )
        self.assertEqual(tr.repo_name, "my_repo")
        self.assertEqual(tr.bucket_name, "my_bucket")
        self.assertEqual(tr.base_path, "my_path")
        self.assertEqual(tr.provider, "aws")
        self.assertEqual(tr.indices, ["index1", "index2"])
