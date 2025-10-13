"""Cleanup action for deepfreeze"""

# pylint: disable=too-many-arguments,too-many-instance-attributes, raise-missing-from

import logging

from elasticsearch import Elasticsearch

from curator.actions.deepfreeze.utilities import (
    check_restore_status,
    get_matching_repos,
    get_settings,
)
from curator.s3client import s3_client_factory


class Cleanup:
    """
    The Cleanup action checks thawed repositories and unmounts them if their S3 objects
    have reverted to Glacier storage.

    When objects are restored from Glacier, they're temporarily available in Standard tier
    for a specified duration. After that duration expires, they revert to Glacier storage.
    This action detects when thawed repositories have expired and unmounts them.

    :param client: A client connection object
    :type client: Elasticsearch

    :methods:
        do_action: Perform the cleanup operation.
        do_dry_run: Perform a dry-run of the cleanup operation.
        do_singleton_action: Entry point for singleton CLI execution.
    """

    def __init__(self, client: Elasticsearch) -> None:
        self.loggit = logging.getLogger("curator.actions.deepfreeze")
        self.loggit.debug("Initializing Deepfreeze Cleanup")

        self.client = client
        self.settings = get_settings(client)
        self.s3 = s3_client_factory(self.settings.provider)

        self.loggit.info("Deepfreeze Cleanup initialized")

    def do_action(self) -> None:
        """
        Check thawed repositories and unmount them if their S3 objects have reverted to Glacier.

        :return: None
        :rtype: None
        """
        self.loggit.debug("Checking for expired thawed repositories")

        # Get all thawed repositories
        all_repos = get_matching_repos(self.client, self.settings.repo_name_prefix)
        thawed_repos = [repo for repo in all_repos if repo.is_thawed and repo.is_mounted]

        if not thawed_repos:
            self.loggit.info("No thawed repositories found")
            return

        self.loggit.info("Found %d thawed repositories to check", len(thawed_repos))

        for repo in thawed_repos:
            self.loggit.debug("Checking thaw status for repository %s", repo.name)

            try:
                # Check restoration status
                status = check_restore_status(self.s3, repo.bucket, repo.base_path)

                # If not all objects are restored, unmount the repository
                if not status["complete"]:
                    self.loggit.info(
                        "Repository %s has expired thaw: %d/%d objects in Glacier, unmounting",
                        repo.name,
                        status["not_restored"],
                        status["total"]
                    )

                    # Mark as not thawed and unmounted
                    repo.is_thawed = False
                    repo.is_mounted = False

                    # Remove from Elasticsearch
                    try:
                        self.client.snapshot.delete_repository(name=repo.name)
                        self.loggit.info("Repository %s unmounted successfully", repo.name)
                    except Exception as e:
                        self.loggit.warning(
                            "Failed to unmount repository %s: %s", repo.name, e
                        )

                    # Persist updated status to status index
                    repo.persist(self.client)
                    self.loggit.info("Repository %s status updated", repo.name)
                else:
                    self.loggit.debug(
                        "Repository %s still has active restoration (%d/%d objects)",
                        repo.name,
                        status["restored"],
                        status["total"]
                    )
            except Exception as e:
                self.loggit.error(
                    "Error checking thaw status for repository %s: %s", repo.name, e
                )

    def do_dry_run(self) -> None:
        """
        Perform a dry-run of the cleanup operation.

        :return: None
        :rtype: None
        """
        self.loggit.info("DRY-RUN MODE. No changes will be made.")

        # Get all thawed repositories
        all_repos = get_matching_repos(self.client, self.settings.repo_name_prefix)
        thawed_repos = [repo for repo in all_repos if repo.is_thawed and repo.is_mounted]

        if not thawed_repos:
            self.loggit.info("DRY-RUN: No thawed repositories found")
            return

        self.loggit.info("DRY-RUN: Found %d thawed repositories to check", len(thawed_repos))

        for repo in thawed_repos:
            self.loggit.debug("DRY-RUN: Checking thaw status for repository %s", repo.name)

            try:
                # Check restoration status
                status = check_restore_status(self.s3, repo.bucket, repo.base_path)

                # If not all objects are restored, report what would be done
                if not status["complete"]:
                    self.loggit.info(
                        "DRY-RUN: Would unmount repository %s (expired thaw: %d/%d objects in Glacier)",
                        repo.name,
                        status["not_restored"],
                        status["total"]
                    )
                else:
                    self.loggit.debug(
                        "DRY-RUN: Repository %s still has active restoration (%d/%d objects)",
                        repo.name,
                        status["restored"],
                        status["total"]
                    )
            except Exception as e:
                self.loggit.error(
                    "DRY-RUN: Error checking thaw status for repository %s: %s", repo.name, e
                )

    def do_singleton_action(self) -> None:
        """
        Entry point for singleton CLI execution.

        :return: None
        :rtype: None
        """
        self.do_action()
