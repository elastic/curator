"""Remount action for deepfreeae"""

# pylint: disable=too-many-arguments,too-many-instance-attributes, raise-missing-from

import logging

from elasticsearch import Elasticsearch

from curator.actions.deepfreeze.utilities import (
    check_is_s3_thawed,
    create_repo,
    get_settings,
    get_thawset,
)


class Remount:
    """
    Remount a thawed deepfreeze repository. Remount indices as "thawed-<repo>".

    :param client: A client connection object
    :type client: Elasticsearch
    :param thawset: The thawset to remount
    :type thawset: str
    :param wait_for_completion: If True, wait for the remounted repository to be ready
    :type wait_for_completion: bool
    :param wait_interval: The interval to wait between checks
    :type wait_interval: int
    :param max_wait: The maximum time to wait (-1 for no limit)
    :type max_wait: int

    :methods:
        do_dry_run: Perform a dry-run of the remounting process.
        do_action: Perform high-level repo remounting steps in sequence.
    """

    def __init__(
        self,
        client: Elasticsearch,
        thawset: str,
        wait_for_completion: bool = True,
        wait_interval: int = 9,
        max_wait: int = -1,
    ) -> None:
        self.loggit = logging.getLogger("curator.actions.deepfreeze")
        self.loggit.debug("Initializing Deepfreeze Rotate")

        self.settings = get_settings(client)
        self.loggit.debug("Settings: %s", str(self.settings))

        self.client = client
        self.thawset = get_thawset(thawset)
        self.wfc = wait_for_completion
        self.wait_interval = wait_interval
        self.max_wait = max_wait

    def do_dry_run(self) -> None:
        """
        Perform a dry-run of the remounting process.

        :return: None
        :rtype: None
        """
        if not check_is_s3_thawed(self.s3, self.thawset):
            print("Dry Run Remount: Not all repos thawed")

        for repo in self.thawset_id.repos:
            self.loggit.info("Remounting %s", repo)

    def do_action(self) -> None:
        """
        Perform high-level repo remounting steps in sequence.

        :return: None
        :rtype: None
        """
        if not check_is_s3_thawed(self.s3, self.thawset):
            print("Remount: Not all repos thawed")
            return

        for repo in self.thawset_id.repos:
            self.loggit.info("Remounting %s", repo)
            create_repo(
                self.client,
                f"thawed-{repo.name}",
                repo.bucket,
                repo.base_path,
                self.settings.canned_acl,
                self.settings.storage_class,
            )
