"""
Integration tests for the Remount action
"""

from curator.actions.deepfreeze.constants import PROVIDERS
from tests.integration import DeepfreezeTestCase


class TestDeepfreezeRemount(DeepfreezeTestCase):
    def test_remount(self):
        for provider in PROVIDERS:
            self.provider = provider
