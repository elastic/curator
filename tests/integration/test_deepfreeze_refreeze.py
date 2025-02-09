"""
Integration tests for the Refreeze action
"""

from curator.actions.deepfreeze.constants import PROVIDERS
from tests.integration import DeepfreezeTestCase


class TestDeepfreezeRefreeze(DeepfreezeTestCase):
    def test_refreeze(self):
        for provider in PROVIDERS:
            self.provider = provider
