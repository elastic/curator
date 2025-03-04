import os
import warnings

from curator.actions.deepfreeze.constants import PROVIDERS, STATUS_INDEX
from curator.actions.deepfreeze.thaw import Thaw
from curator.actions.deepfreeze.utilities import (
    get_matching_repo_names,
    get_unmounted_repos,
)
from tests.integration import DeepfreezeTestCase, random_suffix, testvars

HOST = os.environ.get("TEST_ES_SERVER", "http://127.0.0.1:9200")
MET = "metadata"


class TestDeepfreezeThaw(DeepfreezeTestCase):
    def test_deepfreeze_thaw_happy_path(self):
        warnings.filterwarnings(
            "ignore", category=DeprecationWarning, module="botocore.auth"
        )
        if self.bucket_name == "":
            self.bucket_name = f"{testvars.df_bucket_name}-{random_suffix()}"

        for provider in PROVIDERS:
            self.provider = provider
            setup = self.do_setup()
            prefix = setup.settings.repo_name_prefix
            csi = self.client.cluster.state(metric=MET)[MET]["indices"]

            # Specific assertions
            # Settings index should exist
            assert csi[STATUS_INDEX]

            # Assert that there is only one document in the STATUS_INDEX
            status_index_docs = self.client.search(index=STATUS_INDEX, size=0)
            assert status_index_docs["hits"]["total"]["value"] == 1

            # Rotate 7 times to create 7 repositories, one of which will be unmounted
            rotate = self.do_rotate(7, populate_index=True)

            # We should now have 6 mounted repos
            assert len(rotate.repo_list) == 7
            # ...and one unmounted repo
            assert len(get_unmounted_repos(self.client)) == 1
            # Thaw the unmounted repository
            # Find a date contained in the unmounted repo
            unmounted_repo = get_unmounted_repos(self.client)[0]
            selected_start = (
                unmounted_repo.start + (unmounted_repo.end - unmounted_repo.start) / 3
            )
            selected_end = (
                unmounted_repo.start
                + 2 * (unmounted_repo.end - unmounted_repo.start) / 3
            )

            thaw = Thaw(
                self.client,
                start=selected_start,
                end=selected_end,
                provider=self.provider,
            )
            thaw.do_action()
            # The new repo should be available as 'thawed-'
            assert len(get_matching_repo_names(self.client, 'thawed-')) > 0
            # The remounted indices should also be mounted as 'thawed-'
