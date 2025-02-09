"""Thaw action for deepfreeae"""

# pylint: disable=too-many-arguments,too-many-instance-attributes, raise-missing-from

import logging
from datetime import datetime

from elasticsearch8 import Elasticsearch

from curator.actions.deepfreeze import Remount
from curator.actions.deepfreeze.constants import STATUS_INDEX
from curator.actions.deepfreeze.helpers import Repository, ThawedRepo, ThawSet
from curator.actions.deepfreeze.utilities import (
    decode_date,
    get_settings,
    get_unmounted_repos,
    thaw_repo,
    wait_for_s3_restore,
)
from curator.s3client import s3_client_factory


class Thaw:
    """
    Thaw a deepfreeze repository and make it ready to be remounted. If
    wait_for_completion is True, wait for the thawed repository to be ready and then
    proceed to remount it. This is the default.

    :param client: A client connection object
    :param start: The start of the time range
    :param end: The end of the time range
    :param retain: The number of days to retain the thawed repository
    :param storage_class: The storage class to use for the thawed repository
    :param wait_for_completion: If True, wait for the thawed repository to be ready
    :param wait_interval: The interval to wait between checks
    :param max_wait: The maximum time to wait (-1 for no limit)
    :param enable_multiple_buckets: If True, enable multiple buckets

    :raises Exception: If the repository does not exist
    :raises Exception: If the repository is not empty
    :raises Exception: If the repository is not mounted

    :methods:
        get_repos_to_thaw: Get the list of repos that were active during the given time range.
        do_dry_run: Perform a dry-run of the thawing process.
        do_action: Perform high-level repo thawing steps in sequence.
    """

    def __init__(
        self,
        client: Elasticsearch,
        start: datetime,
        end: datetime,
        retain: int,
        storage_class: str,
        wait_for_completion: bool = True,
        wait_interval: int = 60,
        max_wait: int = -1,
        enable_multiple_buckets: bool = False,
    ) -> None:
        self.loggit = logging.getLogger("curator.actions.deepfreeze")
        self.loggit.debug("Initializing Deepfreeze Rotate")

        self.settings = get_settings(client)
        self.loggit.debug("Settings: %s", str(self.settings))

        self.client = client
        self.start = decode_date(start)
        self.end = decode_date(end)
        self.retain = retain
        self.storage_class = storage_class
        self.wfc = wait_for_completion
        self.wait_interval = wait_interval
        self.max_wait = max_wait
        self.enable_multiple_buckets = enable_multiple_buckets
        self.s3 = s3_client_factory(self.settings.provider)

    def get_repos_to_thaw(self, start: datetime, end: datetime) -> list[Repository]:
        """
        Get the list of repos that were active during the given time range.

        :param start: The start of the time range
        :type start: datetime
        :param end: The end of the time range
        :type start: datetime

        :returns: The repos
        :rtype: list[Repository] A list of repository names

        :raises Exception: If the repository does not exist
        :raises Exception: If the repository is not empty
        """
        loggit = logging.getLogger("curator.actions.deepfreeze")
        repos = get_unmounted_repos(self.client)
        overlapping_repos = []
        for repo in repos:
            if repo.start <= end and repo.end >= start:
                overlapping_repos.append(repo)
        loggit.info("Found overlapping repos: %s", overlapping_repos)
        return overlapping_repos

    def do_dry_run(self) -> None:
        """
        Perform a dry-run of the thawing process.

        :return: None
        :rtype: None
        """
        thawset = ThawSet()

        for repo in self.get_repos_to_thaw(self.start, self.end):
            self.loggit.info("Thawing %s", repo)
            repo_info = self.client.get_repository(repo)
            thawset.add(ThawedRepo(repo_info))
        print(f"Dry Run ThawSet: {thawset}")

    def do_action(self) -> None:
        """
        Perform high-level repo thawing steps in sequence.

        :return: None
        :rtype: None
        """
        # We don't save the settings here because nothing should change our settings.
        # What we _will_ do though, is save a ThawSet showing what indices and repos
        # were thawed out.

        thawset = ThawSet()

        for repo in self.get_repos_to_thaw(self.start, self.end):
            self.loggit.info("Thawing %s", repo)
            if self.provider == "aws":
                if self.setttings.rotate_by == "bucket":
                    bucket = f"{self.settings.bucket_name_prefix}-{self.settings.last_suffix}"
                    path = self.settings.base_path_prefix
                else:
                    bucket = f"{self.settings.bucket_name_prefix}"
                    path = (
                        f"{self.settings.base_path_prefix}-{self.settings.last_suffix}"
                    )
            else:
                raise ValueError("Invalid provider")
            thaw_repo(self.s3, bucket, path, self.retain, self.storage_class)
            repo_info = self.client.get_repository(repo)
            thawset.add(ThawedRepo(repo_info))
        response = self.client.index(index=STATUS_INDEX, document=thawset)
        if not self.wfc:
            thawset_id = response["_id"]
            print(
                f"ThawSet {thawset_id} created. Plase use this ID to remount the thawed repositories."
            )
        else:
            wait_for_s3_restore(self.s3, thawset_id, self.wait_interval, self.max_wait)
            remount = Remount(
                self.client, thawset_id, self.wfc, self.wait_interval, self.max_wait
            )
            remount.do_action()
