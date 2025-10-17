"""Cleanup action for deepfreeze"""

# pylint: disable=too-many-arguments,too-many-instance-attributes, raise-missing-from

import logging
from datetime import datetime, timedelta, timezone

from elasticsearch import Elasticsearch

from curator.actions.deepfreeze.utilities import (
    check_restore_status,
    get_all_indices_in_repo,
    get_matching_repos,
    get_repositories_by_names,
    get_settings,
    list_thaw_requests,
)
from curator.s3client import s3_client_factory


class Cleanup:
    """
    The Cleanup action checks thawed repositories and unmounts them if their S3 objects
    have reverted to Glacier storage. It also deletes indices whose snapshots are only
    in the repositories being cleaned up.

    When objects are restored from Glacier, they're temporarily available in Standard tier
    for a specified duration. After that duration expires, they revert to Glacier storage.
    This action detects when thawed repositories have expired, unmounts them, and removes
    any indices that were only backed up to those repositories.

    :param client: A client connection object
    :type client: Elasticsearch

    :methods:
        do_action: Perform the cleanup operation (unmount repos and delete indices).
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

    def _get_indices_to_delete(self, repos_to_cleanup: list) -> list[str]:
        """
        Find indices that should be deleted because they only have snapshots
        in repositories being cleaned up.

        :param repos_to_cleanup: List of Repository objects being cleaned up
        :type repos_to_cleanup: list[Repository]

        :return: List of index names to delete
        :rtype: list[str]
        """
        self.loggit.debug("Finding indices to delete from repositories being cleaned up")

        # Get all repository names being cleaned up
        cleanup_repo_names = {repo.name for repo in repos_to_cleanup}
        self.loggit.debug("Repositories being cleaned up: %s", cleanup_repo_names)

        # Collect all indices from snapshots in repositories being cleaned up
        indices_in_cleanup_repos = set()
        for repo in repos_to_cleanup:
            try:
                indices = get_all_indices_in_repo(self.client, repo.name)
                indices_in_cleanup_repos.update(indices)
                self.loggit.debug(
                    "Repository %s contains %d indices in its snapshots",
                    repo.name,
                    len(indices)
                )
            except Exception as e:
                self.loggit.warning(
                    "Could not get indices from repository %s: %s", repo.name, e
                )
                continue

        if not indices_in_cleanup_repos:
            self.loggit.debug("No indices found in repositories being cleaned up")
            return []

        self.loggit.debug(
            "Found %d total indices in repositories being cleaned up",
            len(indices_in_cleanup_repos)
        )

        # Get all repositories in the cluster
        try:
            all_repos = self.client.snapshot.get_repository()
            all_repo_names = set(all_repos.keys())
        except Exception as e:
            self.loggit.error("Failed to get repository list: %s", e)
            return []

        # Repositories NOT being cleaned up
        other_repos = all_repo_names - cleanup_repo_names
        self.loggit.debug("Other repositories in cluster: %s", other_repos)

        # Check which indices exist only in repositories being cleaned up
        indices_to_delete = []
        for index in indices_in_cleanup_repos:
            # Check if this index exists in Elasticsearch
            if not self.client.indices.exists(index=index):
                self.loggit.debug(
                    "Index %s does not exist in cluster, skipping", index
                )
                continue

            # Check if this index has snapshots in other repositories
            has_snapshots_elsewhere = False
            for repo_name in other_repos:
                try:
                    indices_in_repo = get_all_indices_in_repo(self.client, repo_name)
                    if index in indices_in_repo:
                        self.loggit.debug(
                            "Index %s has snapshots in repository %s, will not delete",
                            index,
                            repo_name
                        )
                        has_snapshots_elsewhere = True
                        break
                except Exception as e:
                    self.loggit.warning(
                        "Could not check repository %s for index %s: %s",
                        repo_name,
                        index,
                        e
                    )
                    continue

            # Only delete if index has no snapshots in other repositories
            if not has_snapshots_elsewhere:
                indices_to_delete.append(index)
                self.loggit.debug(
                    "Index %s will be deleted (only exists in repositories being cleaned up)",
                    index
                )

        self.loggit.info("Found %d indices to delete", len(indices_to_delete))
        return indices_to_delete

    def _cleanup_old_thaw_requests(self) -> tuple[list[str], list[str]]:
        """
        Clean up old thaw requests based on status and age.

        Deletes:
        - Completed requests older than retention period
        - Failed requests older than retention period
        - Stale in-progress requests where all referenced repos are no longer thawed

        :return: Tuple of (deleted_request_ids, skipped_request_ids)
        :rtype: tuple[list[str], list[str]]
        """
        self.loggit.debug("Cleaning up old thaw requests")

        # Get all thaw requests
        try:
            requests = list_thaw_requests(self.client)
        except Exception as e:
            self.loggit.error("Failed to list thaw requests: %s", e)
            return [], []

        if not requests:
            self.loggit.debug("No thaw requests found")
            return [], []

        self.loggit.info("Found %d thaw requests to evaluate for cleanup", len(requests))

        now = datetime.now(timezone.utc)
        deleted = []
        skipped = []

        # Get retention settings
        retention_completed = self.settings.thaw_request_retention_days_completed
        retention_failed = self.settings.thaw_request_retention_days_failed

        for request in requests:
            request_id = request.get("id")
            status = request.get("status", "unknown")
            created_at_str = request.get("created_at")
            repos = request.get("repos", [])

            try:
                created_at = datetime.fromisoformat(created_at_str)
                if created_at.tzinfo is None:
                    created_at = created_at.replace(tzinfo=timezone.utc)
                age_days = (now - created_at).days

                should_delete = False
                reason = ""

                if status == "completed" and age_days > retention_completed:
                    should_delete = True
                    reason = f"completed request older than {retention_completed} days (age: {age_days} days)"

                elif status == "failed" and age_days > retention_failed:
                    should_delete = True
                    reason = f"failed request older than {retention_failed} days (age: {age_days} days)"

                elif status == "in_progress":
                    # Check if all referenced repos are no longer thawed
                    if repos:
                        try:
                            repo_objects = get_repositories_by_names(self.client, repos)
                            # Check if any repos are still thawed
                            any_thawed = any(repo.is_thawed for repo in repo_objects)

                            if not any_thawed:
                                should_delete = True
                                reason = f"in-progress request with no thawed repos (all repos have expired)"
                        except Exception as e:
                            self.loggit.warning(
                                "Could not check repos for request %s: %s", request_id, e
                            )
                            skipped.append(request_id)
                            continue

                if should_delete:
                    try:
                        from curator.actions.deepfreeze.constants import STATUS_INDEX
                        self.client.delete(index=STATUS_INDEX, id=request_id)
                        self.loggit.info(
                            "Deleted thaw request %s (%s)", request_id, reason
                        )
                        deleted.append(request_id)
                    except Exception as e:
                        self.loggit.error(
                            "Failed to delete thaw request %s: %s", request_id, e
                        )
                        skipped.append(request_id)
                else:
                    self.loggit.debug(
                        "Keeping thaw request %s (status: %s, age: %d days)",
                        request_id,
                        status,
                        age_days
                    )

            except Exception as e:
                self.loggit.error(
                    "Error processing thaw request %s: %s", request_id, e
                )
                skipped.append(request_id)

        self.loggit.info(
            "Thaw request cleanup complete: %d deleted, %d skipped",
            len(deleted),
            len(skipped)
        )
        return deleted, skipped

    def do_action(self) -> None:
        """
        Check thawed repositories and unmount them if their S3 objects have reverted to Glacier.
        Also delete indices whose snapshots are only in the repositories being cleaned up.

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

        # Track repositories that will be cleaned up
        repos_to_cleanup = []

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

                    # Add to cleanup list
                    repos_to_cleanup.append(repo)

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

        # Delete indices whose snapshots are only in repositories being cleaned up
        if repos_to_cleanup:
            self.loggit.info("Checking for indices to delete from cleaned up repositories")
            try:
                indices_to_delete = self._get_indices_to_delete(repos_to_cleanup)

                if indices_to_delete:
                    self.loggit.info(
                        "Deleting %d indices whose snapshots are only in cleaned up repositories",
                        len(indices_to_delete)
                    )
                    for index in indices_to_delete:
                        try:
                            self.client.indices.delete(index=index)
                            self.loggit.info("Deleted index %s", index)
                        except Exception as e:
                            self.loggit.error("Failed to delete index %s: %s", index, e)
                else:
                    self.loggit.info("No indices need to be deleted")
            except Exception as e:
                self.loggit.error("Error deleting indices: %s", e)

        # Clean up old thaw requests
        self.loggit.info("Cleaning up old thaw requests")
        try:
            deleted, skipped = self._cleanup_old_thaw_requests()
            if deleted:
                self.loggit.info("Deleted %d old thaw requests", len(deleted))
        except Exception as e:
            self.loggit.error("Error cleaning up thaw requests: %s", e)

    def do_dry_run(self) -> None:
        """
        Perform a dry-run of the cleanup operation.
        Shows which repositories would be unmounted and which indices would be deleted.

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

        # Track repositories that would be cleaned up
        repos_to_cleanup = []

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
                    repos_to_cleanup.append(repo)
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

        # Show which indices would be deleted
        if repos_to_cleanup:
            self.loggit.info(
                "DRY-RUN: Checking for indices that would be deleted from cleaned up repositories"
            )
            try:
                indices_to_delete = self._get_indices_to_delete(repos_to_cleanup)

                if indices_to_delete:
                    self.loggit.info(
                        "DRY-RUN: Would delete %d indices whose snapshots are only in cleaned up repositories:",
                        len(indices_to_delete)
                    )
                    for index in indices_to_delete:
                        self.loggit.info("DRY-RUN:   - %s", index)
                else:
                    self.loggit.info("DRY-RUN: No indices would be deleted")
            except Exception as e:
                self.loggit.error("DRY-RUN: Error finding indices to delete: %s", e)

        # Show which thaw requests would be cleaned up
        self.loggit.info("DRY-RUN: Checking for old thaw requests that would be deleted")
        try:
            requests = list_thaw_requests(self.client)

            if not requests:
                self.loggit.info("DRY-RUN: No thaw requests found")
            else:
                now = datetime.now(timezone.utc)
                retention_completed = self.settings.thaw_request_retention_days_completed
                retention_failed = self.settings.thaw_request_retention_days_failed

                would_delete = []

                for request in requests:
                    request_id = request.get("id")
                    status = request.get("status", "unknown")
                    created_at_str = request.get("created_at")
                    repos = request.get("repos", [])

                    try:
                        created_at = datetime.fromisoformat(created_at_str)
                        if created_at.tzinfo is None:
                            created_at = created_at.replace(tzinfo=timezone.utc)
                        age_days = (now - created_at).days

                        should_delete = False
                        reason = ""

                        if status == "completed" and age_days > retention_completed:
                            should_delete = True
                            reason = f"completed request older than {retention_completed} days (age: {age_days} days)"

                        elif status == "failed" and age_days > retention_failed:
                            should_delete = True
                            reason = f"failed request older than {retention_failed} days (age: {age_days} days)"

                        elif status == "in_progress" and repos:
                            try:
                                repo_objects = get_repositories_by_names(self.client, repos)
                                any_thawed = any(repo.is_thawed for repo in repo_objects)

                                if not any_thawed:
                                    should_delete = True
                                    reason = "in-progress request with no thawed repos (all repos have expired)"
                            except Exception as e:
                                self.loggit.warning(
                                    "DRY-RUN: Could not check repos for request %s: %s", request_id, e
                                )

                        if should_delete:
                            would_delete.append((request_id, reason))

                    except Exception as e:
                        self.loggit.error(
                            "DRY-RUN: Error processing thaw request %s: %s", request_id, e
                        )

                if would_delete:
                    self.loggit.info(
                        "DRY-RUN: Would delete %d old thaw requests:",
                        len(would_delete)
                    )
                    for request_id, reason in would_delete:
                        self.loggit.info("DRY-RUN:   - %s (%s)", request_id, reason)
                else:
                    self.loggit.info("DRY-RUN: No thaw requests would be deleted")

        except Exception as e:
            self.loggit.error("DRY-RUN: Error checking thaw requests: %s", e)

    def do_singleton_action(self) -> None:
        """
        Entry point for singleton CLI execution.

        :return: None
        :rtype: None
        """
        self.do_action()
