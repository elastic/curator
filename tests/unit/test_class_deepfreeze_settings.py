"""test_action_deepfreeze"""

# pylint: disable=missing-function-docstring, missing-class-docstring, protected-access, attribute-defined-outside-init
from unittest import TestCase

from curator.actions.deepfreeze import Settings

# Get test variables and constants from a single source
# from . import testvars

# from curator.exceptions import RepositoryException


class TestClassDeepfreezeSettings(TestCase):
    """
    Test Deepfreeze Settings class
    """

    def test_default_values(self):
        s = Settings()
        self.assertEqual(s.bucket_name_prefix, "deepfreeze")
        self.assertEqual(s.repo_name_prefix, "deepfreeze")
        self.assertEqual(s.base_path_prefix, "snapshots")
        self.assertEqual(s.canned_acl, "private")
        self.assertEqual(s.storage_class, "intelligent_tiering")
        self.assertEqual(s.provider, "aws")
        self.assertEqual(s.rotate_by, "path")
        self.assertEqual(s.style, "oneup")
        self.assertEqual(s.last_suffix, None)

    def test_setting_bucket_name_prefix(self):
        s = Settings({"bucket_name_prefix": "test_bucket_name_prefix"})
        self.assertEqual(s.bucket_name_prefix, "test_bucket_name_prefix")

    def test_setting_repo_name_prefix(self):
        s = Settings({"repo_name_prefix": "test_repo_name_prefix"})
        self.assertEqual(s.repo_name_prefix, "test_repo_name_prefix")

    def test_setting_base_path_prefix(self):
        s = Settings({"base_path_prefix": "test_base_path_prefix"})
        self.assertEqual(s.base_path_prefix, "test_base_path_prefix")

    def test_setting_canned_acl(self):
        s = Settings({"canned_acl": "test_canned_acl"})
        self.assertEqual(s.canned_acl, "test_canned_acl")

    def test_setting_storage_class(self):
        s = Settings({"storage_class": "test_storage_class"})
        self.assertEqual(s.storage_class, "test_storage_class")

    def test_setting_provider(self):
        s = Settings({"provider": "test_provider"})
        self.assertEqual(s.provider, "test_provider")

    def test_setting_rotate_by(self):
        s = Settings({"rotate_by": "test_rotate_by"})
        self.assertEqual(s.rotate_by, "test_rotate_by")

    def test_setting_style(self):
        s = Settings({"style": "test_style"})
        self.assertEqual(s.style, "test_style")

    def test_setting_last_suffix(self):
        s = Settings({"last_suffix": "test_last_suffix"})
        self.assertEqual(s.last_suffix, "test_last_suffix")

    def test_setting_nmultiple(self):
        s = Settings({"provider": "azure", "style": "date"})
        self.assertEqual(s.provider, "azure")
        self.assertEqual(s.style, "date")
