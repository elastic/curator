"""
Test deepfreeze setup functionality
"""

# pylint: disable=missing-function-docstring, missing-class-docstring, line-too-long
import os
import time
import warnings

from curator.actions.deepfreeze import PROVIDERS
from curator.actions.deepfreeze.constants import STATUS_INDEX
from curator.actions.deepfreeze.exceptions import MissingIndexError
from curator.actions.deepfreeze.rotate import Rotate
from curator.actions.deepfreeze.utilities import (
    get_matching_repo_names,
    get_unmounted_repos,
)
from curator.s3client import s3_client_factory
from tests.integration import testvars

from . import DeepfreezeTestCase, random_suffix

HOST = os.environ.get("TEST_ES_SERVER", "http://127.0.0.1:9200")
MET = "metadata"


class TestDeepfreezeRotate(DeepfreezeTestCase):
    def test_rotate_happy_path(self):
        warnings.filterwarnings(
            "ignore", category=DeprecationWarning, module="botocore.auth"
        )

        for provider in PROVIDERS:
            self.provider = provider
            if self.bucket_name == "":
                self.bucket_name = f"{testvars.df_bucket_name}-{random_suffix()}"

            setup = self.do_setup(create_ilm_policy=True)
            prefix = setup.settings.repo_name_prefix
            csi = self.client.cluster.state(metric=MET)[MET]["indices"]

            # Specific assertions
            # Settings index should exist
            assert csi[STATUS_INDEX]

            # Assert that there is only one document in the STATUS_INDEX
            status_index_docs = self.client.search(index=STATUS_INDEX, size=0)
            assert status_index_docs["hits"]["total"]["value"] == 1
            rotate = Rotate(
                self.client,
            )
            assert len(rotate.repo_list) == 1
            assert rotate.repo_list == [f"{prefix}-000001"]
            # Perform the first rotation
            rotate.do_action()
            # There should now be one repositories.
            assert (
                len(
                    get_matching_repo_names(
                        self.client, setup.settings.repo_name_prefix
                    )
                )
                == 2
            )

            # Save off the current repo list
            orig_list = rotate.repo_list
            # Do another rotation with keep=1
            rotate = Rotate(
                self.client,
                keep=1,
            )
            rotate.do_action()
            # There should now be two (one kept and one new)
            assert len(rotate.repo_list) == 2
            assert rotate.repo_list == [f"{prefix}-000002", f"{prefix}-000001"]
            assert (
                len(
                    get_matching_repo_names(
                        self.client, setup.settings.repo_name_prefix
                    )
                )
                == 2
            )
            # They should not be the same two as before
            assert rotate.repo_list != orig_list

            # Save off the current repo list
            orig_list = rotate.repo_list
            # Do another rotation with keep=1
            rotate = Rotate(
                self.client,
                keep=1,
            )
            rotate.do_action()
            # There should now be two (one kept and one new)
            assert len(rotate.repo_list) == 2
            assert rotate.repo_list == [f"{prefix}-000003", f"{prefix}-000002"]
            assert (
                len(
                    get_matching_repo_names(
                        self.client, setup.settings.repo_name_prefix
                    )
                )
                == 2
            )
            # They should not be the same two as before
            assert rotate.repo_list != orig_list
            # Query the settings index to get the unmountd repos
            unmounted = get_unmounted_repos(self.client)
            assert len(unmounted) == 1
            assert unmounted[0].name == f"{prefix}-000001"

    def test_rotate_with_data(self):
        warnings.filterwarnings(
            "ignore", category=DeprecationWarning, module="botocore.auth"
        )

        for provider in PROVIDERS:
            self.provider = provider
            if self.bucket_name == "":
                self.bucket_name = f"{testvars.df_bucket_name}-{random_suffix()}"

            setup = self.do_setup(create_ilm_policy=True)
            prefix = setup.settings.repo_name_prefix
            csi = self.client.cluster.state(metric=MET)[MET]["indices"]

            # Specific assertions
            # Settings index should exist
            assert csi[STATUS_INDEX]

            # Assert that there is only one document in the STATUS_INDEX
            status_index_docs = self.client.search(index=STATUS_INDEX, size=0)
            assert status_index_docs["hits"]["total"]["value"] == 1
            rotate = self.do_rotate(populate_index=True)
            # There should now be one repositories.
            assert (
                len(
                    get_matching_repo_names(
                        self.client, setup.settings.repo_name_prefix
                    )
                )
                == 2
            )

            # Save off the current repo list
            orig_list = rotate.repo_list
            # Do another rotation with keep=1
            rotate = self.do_rotate(populate_index=True)
            # There should now be two (one kept and one new)
            assert len(rotate.repo_list) == 2
            assert rotate.repo_list == [f"{prefix}-000002", f"{prefix}-000001"]
            assert (
                len(
                    get_matching_repo_names(
                        self.client, setup.settings.repo_name_prefix
                    )
                )
                == 2
            )
            # They should not be the same two as before
            assert rotate.repo_list != orig_list

            # Save off the current repo list
            orig_list = rotate.repo_list
            # Do another rotation with keep=1
            rotate = self.do_rotate(populate_index=True)
            # There should now be two (one kept and one new)
            assert len(rotate.repo_list) == 2
            assert rotate.repo_list == [f"{prefix}-000003", f"{prefix}-000002"]
            assert (
                len(
                    get_matching_repo_names(
                        self.client, setup.settings.repo_name_prefix
                    )
                )
                == 2
            )
            # They should not be the same two as before
            assert rotate.repo_list != orig_list
            # Query the settings index to get the unmountd repos
            unmounted = get_unmounted_repos(self.client)
            assert len(unmounted) == 1
            assert unmounted[0].name == f"{prefix}-000001"

    # What can go wrong with repo rotation?
    #
    # 1. Repo deleted outside of our awareness
    # 2. Bucket deleted so no repos at all
    # 3. Missing status index - no historical data available
    # 4. Repo has no indices - what do we do about its time range?
    # 5. ??

    def testMissingStatusIndex(self):
        warnings.filterwarnings(
            "ignore", category=DeprecationWarning, module="botocore.auth"
        )

        for provider in PROVIDERS:
            self.provider = provider
            if self.bucket_name == "":
                self.bucket_name = f"{testvars.df_bucket_name}-{random_suffix()}"

            setup = self.do_setup(create_ilm_policy=True)
            prefix = setup.settings.repo_name_prefix
            csi = self.client.cluster.state(metric=MET)[MET]["indices"]

            # Specific assertions
            # Settings index should exist
            assert csi[STATUS_INDEX]

            # Assert that there is only one document in the STATUS_INDEX
            status_index_docs = self.client.search(index=STATUS_INDEX, size=0)
            assert status_index_docs["hits"]["total"]["value"] == 1

            # Now, delete the status index completely
            self.client.delete(index=STATUS_INDEX)
            csi = self.client.cluster.state(metric=MET)[MET]["indices"]
            assert not csi[STATUS_INDEX]

            with self.assertRaises(MissingIndexError):
                rotate = self.do_rotate(populate_index=True)
