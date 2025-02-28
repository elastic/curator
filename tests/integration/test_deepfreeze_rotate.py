"""
Test deepfreeze setup functionality
"""

# pylint: disable=missing-function-docstring, missing-class-docstring, line-too-long
import os
import random
import warnings

from curator.actions.deepfreeze import PROVIDERS
from curator.actions.deepfreeze.constants import STATUS_INDEX
from curator.actions.deepfreeze.exceptions import MissingIndexError
from curator.actions.deepfreeze.rotate import Rotate
from curator.actions.deepfreeze.utilities import get_repository, get_unmounted_repos
from curator.exceptions import ActionError
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
            assert len(rotate.repo_list) == 1

            # Save off the current repo list
            orig_list = rotate.repo_list
            # Do another rotation with keep=1
            rotate = self.do_rotate(populate_index=True)
            # There should now be two (one kept and one new)
            assert len(rotate.repo_list) == 2
            assert rotate.repo_list == [f"{prefix}-000002", f"{prefix}-000001"]
            # They should not be the same two as before
            assert rotate.repo_list != orig_list

            # Save off the current repo list
            orig_list = rotate.repo_list
            # Do another rotation with keep=1
            rotate = self.do_rotate(populate_index=True, keep=1)
            # There should now be two (one kept and one new)
            assert len(rotate.repo_list) == 3
            assert rotate.repo_list == [
                f"{prefix}-000003",
                f"{prefix}-000002",
                f"{prefix}-000001",
            ]
            # Query the settings index to get the unmounted repos
            unmounted = get_unmounted_repos(self.client)
            assert len(unmounted) == 2
            assert f"{prefix}-000001" in [x.name for x in unmounted]
            assert f"{prefix}-000002" in [x.name for x in unmounted]
            repos = [get_repository(self.client, name=r) for r in rotate.repo_list]
            assert len(repos) == 3
            for repo in repos:
                if repo:
                    assert repo.earliest is not None
                    assert repo.latest is not None
                    assert repo.earliest < repo.latest
                    assert len(repo.indices) > 1
                else:
                    print(f"{repo} is None")

    def test_missing_status_index(self):
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
            self.client.indices.delete(index=STATUS_INDEX)
            csi = self.client.cluster.state(metric=MET)[MET]["indices"]
            assert STATUS_INDEX not in csi

            with self.assertRaises(MissingIndexError):
                rotate = self.do_rotate(populate_index=True)

    def test_missing_repo(self):
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

            rotate = self.do_rotate(6)
            # There should now be one repositories.
            assert len(rotate.repo_list) == 6

            # Delete a random repo
            repo_to_delete = rotate.repo_list[random.randint(0, 5)]
            self.client.snapshot.delete_repository(
                name=repo_to_delete,
            )

            # Do another rotation with keep=1
            rotate = self.do_rotate(populate_index=True)
            # There should now be two (one kept and one new)
            assert len(rotate.repo_list) == 6
            assert repo_to_delete not in rotate.repo_list

    def test_missing_bucket(self):
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

            rotate = self.do_rotate(6, populate_index=True)
            # There should now be one repositories.
            assert len(rotate.repo_list) == 6

            # Delete the bucket
            s3 = s3_client_factory(self.provider)
            s3.delete_bucket(setup.settings.bucket_name_prefix)

            # Do another rotation with keep=1
            with self.assertRaises(ActionError):
                rotate = self.do_rotate(populate_index=True)

            # This indicates a Bad Thing, but I'm not sure what the correct response
            # should be from a DF standpoint.
