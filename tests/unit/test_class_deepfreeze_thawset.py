"""Test the deepfreee Repository class"""

# pylint: disable=missing-function-docstring, pointless-statement, missing-class-docstring, protected-access, attribute-defined-outside-init
from unittest import TestCase

from curator.actions.deepfreeze import ThawSet


class TestClassDeepfreezeThawSet(TestCase):

    def test_default_values(self):
        ts = ThawSet()
        with self.assertRaises(AttributeError):
            ts.repo_name
        with self.assertRaises(AttributeError):
            ts.bucket_name
        with self.assertRaises(AttributeError):
            ts.base_path
        with self.assertRaises(AttributeError):
            ts.provider
        self.assertEqual(ts.indices, None)

    def test_set_from_hash(self):
        ts = ThawSet(
            {
                "repo_name": "my_repo",
                "bucket_name": "my_bucket",
                "base_path": "my_path",
                "provider": "aws",
                "indices": ["index1", "index2"],
            }
        )
        self.assertEqual(ts.repo_name, "my_repo")
        self.assertEqual(ts.bucket_name, "my_bucket")
        self.assertEqual(ts.base_path, "my_path")
        self.assertEqual(ts.provider, "aws")
        self.assertEqual(ts.indices, ["index1", "index2"])
