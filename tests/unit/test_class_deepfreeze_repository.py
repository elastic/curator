"""Test the deepfreee Repository class"""

# pylint: disable=missing-function-docstring, pointless-statement, missing-class-docstring, protected-access, attribute-defined-outside-init
from unittest import TestCase

from curator.actions.deepfreeze import Repository


class TestClassDeepfreezeRepository(TestCase):

    def test_default_values(self):
        r = Repository()
        with self.assertRaises(AttributeError):
            r.name
        with self.assertRaises(AttributeError):
            r.bucket
        with self.assertRaises(AttributeError):
            r.base_path
        with self.assertRaises(AttributeError):
            r.start
        with self.assertRaises(AttributeError):
            r.end
        self.assertEqual(r.is_thawed, False)
        self.assertEqual(r.is_mounted, True)

    def test_set_from_hash(self):
        r = Repository(
            {
                "name": "my_repo",
                "bucket": "my_bucket",
                "base_path": "my_path",
                "start": "2020-01-01",
                "end": "2020-01-02",
                "is_thawed": True,
                "is_mounted": False,
            }
        )
        self.assertEqual(r.name, "my_repo")
        self.assertEqual(r.bucket, "my_bucket")
        self.assertEqual(r.base_path, "my_path")
        self.assertEqual(r.start, "2020-01-01")
        self.assertEqual(r.end, "2020-01-02")
        self.assertEqual(r.is_thawed, True)
        self.assertEqual(r.is_mounted, False)
