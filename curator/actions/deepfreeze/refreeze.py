"""Refreeze action for deepfreeze"""

# pylint: disable=too-many-arguments,too-many-instance-attributes, raise-missing-from

import logging

from elasticsearch import Elasticsearch

from curator.actions.deepfreeze.utilities import (
    get_all_indices_in_repo,
    get_matching_repos,
    get_settings,
    push_to_glacier,
    unmount_repo,
)
from curator.s3client import s3_client_factory


class Refreeze:
    """
    The Refreeze action forces thawed repositories back to Glacier storage ahead of schedule.
    It deletes indices that have snapshots in the thawed repositories, unmounts the repositories,
    and pushes the S3 objects back to Glacier storage.

    When repositories are thawed, their S3 objects are restored to Standard tier temporarily.
    This action allows you to refreeze them before their automatic expiration, which is useful
    for cost optimization when the thawed data is no longer needed.

    IMPORTANT: This action deletes live indices from the cluster but preserves all snapshots
    in S3. The snapshots remain intact and the S3 data is pushed back to Glacier storage.

    :param client: A client connection object
    :type client: Elasticsearch

    :methods:
        do_action: Perform the refreeze operation (delete indices, unmount repos, push to Glacier).
        do_dry_run: Perform a dry-run of the refreeze operation.
        do_singleton_action: Entry point for singleton CLI execution.
    """

    def __init__(self, client: Elasticsearch) -> None:
        self.loggit = logging.getLogger("curator.actions.deepfreeze")
        self.loggit.debug("Initializing Deepfreeze Refreeze")

        self.client = client
        self.settings = get_settings(client)
        self.s3 = s3_client_factory(self.settings.provider)

        self.loggit.info("Deepfreeze Refreeze initialized")

    def _get_indices_to_delete(self, repo) -> list[str]:
        """
        Get all indices that have snapshots in this repository.

        :param repo: The Repository object being refrozen
        :type repo: Repository

        :return: List of index names to delete
        :rtype: list[str]
        """
        self.loggit.debug("Finding indices to delete from repository %s", repo.name)

        try:
            indices = get_all_indices_in_repo(self.client, repo.name)
            self.loggit.debug(
                "Repository %s contains %d indices in its snapshots",
                repo.name,
                len(indices)
            )
        except Exception as e:
            self.loggit.warning(
                "Could not get indices from repository %s: %s", repo.name, e
            )
            return []

        # Filter to only indices that actually exist in the cluster
        indices_to_delete = []
        for index in indices:
            if self.client.indices.exists(index=index):
                indices_to_delete.append(index)
                self.loggit.debug("Index %s exists and will be deleted", index)
            else:
                self.loggit.debug("Index %s does not exist in cluster, skipping", index)

        self.loggit.info("Found %d indices to delete from repository %s",
                        len(indices_to_delete), repo.name)
        return indices_to_delete

    def do_action(self) -> None:
        """
        Force thawed repositories back to Glacier by deleting their indices,
        unmounting them, and pushing S3 objects back to Glacier storage.

        :return: None
        :rtype: None
        """
        self.loggit.debug("Checking for thawed repositories to refreeze")

        # Get all thawed repositories
        all_repos = get_matching_repos(self.client, self.settings.repo_name_prefix)
        thawed_repos = [repo for repo in all_repos if repo.is_thawed and repo.is_mounted]

        if not thawed_repos:
            self.loggit.info("No thawed repositories found")
            return

        self.loggit.info("Found %d thawed repositories to refreeze", len(thawed_repos))

        for repo in thawed_repos:
            self.loggit.info("Processing repository %s for refreeze", repo.name)

            try:
                # Step 1: Get indices to delete
                indices_to_delete = self._get_indices_to_delete(repo)

                # Step 2: Delete indices
                if indices_to_delete:
                    self.loggit.info(
                        "Deleting %d indices from repository %s",
                        len(indices_to_delete),
                        repo.name
                    )
                    for index in indices_to_delete:
                        try:
                            self.client.indices.delete(index=index)
                            self.loggit.info("Deleted index %s", index)
                        except Exception as e:
                            self.loggit.error("Failed to delete index %s: %s", index, e)
                else:
                    self.loggit.info("No indices to delete for repository %s", repo.name)

                # Step 3: Unmount the repository
                self.loggit.info("Unmounting repository %s", repo.name)
                unmounted_repo = unmount_repo(self.client, repo.name)

                # Step 4: Push to Glacier
                self.loggit.info("Pushing repository %s back to Glacier", repo.name)
                push_to_glacier(self.s3, unmounted_repo)

                # Step 5: Update repository status
                repo.is_thawed = False
                repo.is_mounted = False
                repo.persist(self.client)
                self.loggit.info("Repository %s successfully refrozen", repo.name)

            except Exception as e:
                self.loggit.error(
                    "Error refreezing repository %s: %s", repo.name, e
                )
                continue

        self.loggit.info("Refreeze operation completed")

    def do_dry_run(self) -> None:
        """
        Perform a dry-run of the refreeze operation.
        Shows which repositories would be refrozen and which indices would be deleted.

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

        self.loggit.info("DRY-RUN: Found %d thawed repositories to refreeze", len(thawed_repos))

        for repo in thawed_repos:
            self.loggit.info("DRY-RUN: Would refreeze repository %s", repo.name)

            try:
                # Show indices that would be deleted
                indices_to_delete = self._get_indices_to_delete(repo)

                if indices_to_delete:
                    self.loggit.info(
                        "DRY-RUN: Would delete %d indices from repository %s:",
                        len(indices_to_delete),
                        repo.name
                    )
                    for index in indices_to_delete:
                        self.loggit.info("DRY-RUN:   - %s", index)
                else:
                    self.loggit.info("DRY-RUN: No indices to delete for repository %s", repo.name)

                # Show what would happen
                self.loggit.info("DRY-RUN: Would unmount repository %s", repo.name)
                self.loggit.info("DRY-RUN: Would push repository %s to Glacier", repo.name)
                self.loggit.info("DRY-RUN: Would update status to thawed=False, mounted=False")

            except Exception as e:
                self.loggit.error(
                    "DRY-RUN: Error processing repository %s: %s", repo.name, e
                )

    def do_singleton_action(self) -> None:
        """
        Entry point for singleton CLI execution.

        :return: None
        :rtype: None
        """
        self.do_action()
