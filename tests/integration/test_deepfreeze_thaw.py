from curator.actions.deepfreeze.constants import PROVIDERS
from curator.actions.deepfreeze.thaw import Thaw
from curator.actions.deepfreeze.utilities import (
    get_matching_repo_names,
    get_unmounted_repos,
)
from tests.integration import CuratorTestCase


class TestDeepfreezeThaw(CuratorTestCase):
    def test_deepfreeze_thaw(self):
        for provider in PROVIDERS:
            self.provider = provider
            self.do_setup()
            # Rotate 7 times to create 7 repositories, one of which will be unmounted
            rotate = self.do_rotate(7)
            # We should now have 6 mounted repos
            assert len(rotate.repo_list) == 6
            # ...and one unmounted repo
            assert len(get_unmounted_repos(self.client) == 1)
            # Thaw the unmounted repository
            thaw = Thaw(self.client)
            # We should now have 7 mounted repos, not 6.
            assert len(rotate.repo_list) == 7
            # The extra one should have been updated to reflect its status

            # The new repo should be available as 'thawed-'
            assert len(get_matching_repo_names(self.client, 'thawed-')) > 0
            # The remounted indices should also be mounted as 'thawed-'
