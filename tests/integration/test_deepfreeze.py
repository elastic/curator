"""Deepfreeze integration tests"""

# pylint: disable=missing-function-docstring, missing-class-docstring, line-too-long

from . import testvars
from . import CuratorTestCase

class TestActionDeepfreeze(CuratorTestCase):
    """Test deepthroat operations"""

    def test_deepfreeze(self):
        """
        Testing what deepfreeze does when there is no repo which matches the
        pattern.
        """
        self.create_named_repository(testvars.existing_repo_name)
