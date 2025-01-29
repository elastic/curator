"""Test the deepfreee utility function ensure_settings_index"""

# pylint: disable=missing-function-docstring, pointless-statement, missing-class-docstring, protected-access, attribute-defined-outside-init
from unittest import TestCase
from unittest.mock import Mock

from curator.actions.deepfreeze import ensure_settings_index


class TestUtilDeepfreezeEnsureSettingsIndex(TestCase):
    VERSION = {'version': {'number': '8.0.0'}}

    def builder(self):
        self.client = Mock()
        self.client.info.return_value = self.VERSION

    def test_no_existing_index(self):
        self.builder()
        self.client.indices.exists.return_value = False
        self.assertIsNone(ensure_settings_index(self.client))

    def test_existing_index(self):
        self.builder()
        self.client.indices.exists.return_value = True
        self.assertIsNone(ensure_settings_index(self.client))
