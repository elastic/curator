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
    This action:
    1. Detects thawed repositories that have passed their expires_at timestamp and marks them as expired
    2. Unmounts expired repositories and resets them to frozen state
    3. Deletes indices whose snapshots are only in expired repositories
    4. Cleans up old thaw requests based on status and retention settings
    5. Cleans up orphaned thawed ILM policies

    :param client: A client connection object
    :type client: Elasticsearch

    :methods:
        do_action: Perform the cleanup operation (detect expired repos, unmount, delete indices).
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

    def _detect_and_mark_expired_repos(self) -> int:
        """
        Detect repositories whose S3 restore has expired and mark them as expired.

        Checks repositories in two ways:
        1. Thawed repos with expires_at timestamp that has passed
        2. Mounted repos (regardless of state) by checking S3 restore status directly

        :return: Count of repositories marked as expired
        :rtype: int
        """
        self.loggit.debug("Detecting expired repositories")

        from curator.actions.deepfreeze.constants import THAW_STATE_THAWED
        all_repos = get_matching_repos(self.client, self.settings.repo_name_prefix)

        # Get thawed repos for timestamp-based checking
        thawed_repos = [repo for repo in all_repos if repo.thaw_state == THAW_STATE_THAWED]

        # Get mounted repos for S3-based checking (may overlap with thawed_repos)
        mounted_repos = [repo for repo in all_repos if repo.is_mounted]

        self.loggit.debug(
            "Found %d thawed repositories and %d mounted repositories to check",
            len(thawed_repos),
            len(mounted_repos)
        )

        now = datetime.now(timezone.utc)
        expired_count = 0
        checked_repos = set()  # Track repos we've already processed

        # METHOD 1: Check thawed repos with expires_at timestamp
        for repo in thawed_repos:
            if repo.name in checked_repos:
                continue

            if repo.expires_at:
                expires_at = repo.expires_at
                if expires_at.tzinfo is None:
                    expires_at = expires_at.replace(tzinfo=timezone.utc)

                if now >= expires_at:
                    self.loggit.info(
                        "Repository %s has expired based on timestamp (expired at %s)",
                        repo.name,
                        expires_at.isoformat()
                    )
                    repo.mark_expired()
                    try:
                        repo.persist(self.client)
                        self.loggit.info("Marked repository %s as expired", repo.name)
                        expired_count += 1
                        checked_repos.add(repo.name)
                    except Exception as e:
                        self.loggit.error(
                            "Failed to mark repository %s as expired: %s",
                            repo.name,
                            e
                        )
                else:
                    checked_repos.add(repo.name)
            else:
                self.loggit.warning(
                    "Repository %s is in thawed state but has no expires_at timestamp",
                    repo.name
                )

        # METHOD 2: Check mounted repos by querying S3 restore status
        self.loggit.debug("Checking S3 restore status for mounted repositories")
        for repo in mounted_repos:
            if repo.name in checked_repos:
                continue

            try:
                # Check actual S3 restore status
                self.loggit.debug(
                    "Checking S3 restore status for repository %s (bucket: %s, path: %s)",
                    repo.name,
                    repo.bucket,
                    repo.base_path
                )

                status = check_restore_status(self.s3, repo.bucket, repo.base_path)

                # If all objects are back in Glacier (not restored), mark as expired
                if status["not_restored"] > 0 and status["restored"] == 0 and status["in_progress"] == 0:
                    self.loggit.info(
                        "Repository %s has expired based on S3 status: %d/%d objects not restored",
                        repo.name,
                        status["not_restored"],
                        status["total"]
                    )
                    repo.mark_expired()
                    try:
                        repo.persist(self.client)
                        self.loggit.info("Marked repository %s as expired", repo.name)
                        expired_count += 1
                        checked_repos.add(repo.name)
                    except Exception as e:
                        self.loggit.error(
                            "Failed to mark repository %s as expired: %s",
                            repo.name,
                            e
                        )
                elif status["restored"] > 0 or status["in_progress"] > 0:
                    self.loggit.debug(
                        "Repository %s still has restored objects: %d restored, %d in progress",
                        repo.name,
                        status["restored"],
                        status["in_progress"]
                    )
                    checked_repos.add(repo.name)

            except Exception as e:
                self.loggit.error(
                    "Failed to check S3 restore status for repository %s: %s",
                    repo.name,
                    e
                )
                continue

        if expired_count > 0:
            self.loggit.info("Marked %d repositories as expired", expired_count)

        return expired_count

    def _cleanup_old_thaw_requests(self) -> tuple[list[str], list[str]]:
        """
        Clean up old thaw requests based on status and age.

        Deletes:
        - Completed requests older than retention period
        - Failed requests older than retention period
        - Refrozen requests older than retention period (35 days by default)
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
        retention_refrozen = self.settings.thaw_request_retention_days_refrozen

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

                elif status == "refrozen" and age_days > retention_refrozen:
                    should_delete = True
                    reason = f"refrozen request older than {retention_refrozen} days (age: {age_days} days)"

                elif status == "in_progress":
                    # Check if all referenced repos are no longer in thawing/thawed state
                    if repos:
                        try:
                            from curator.actions.deepfreeze.constants import THAW_STATE_THAWING, THAW_STATE_THAWED
                            repo_objects = get_repositories_by_names(self.client, repos)
                            # Check if any repos are still in thawing or thawed state
                            any_active = any(
                                repo.thaw_state in [THAW_STATE_THAWING, THAW_STATE_THAWED]
                                for repo in repo_objects
                            )

                            if not any_active:
                                should_delete = True
                                reason = f"in-progress request with no active repos (all repos have been cleaned up)"
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

        # First, detect and mark any thawed repositories that have passed their expiration time
        self.loggit.info("Detecting expired thawed repositories based on expires_at timestamp")
        try:
            newly_expired = self._detect_and_mark_expired_repos()
            if newly_expired > 0:
                self.loggit.info("Detected and marked %d newly expired repositories", newly_expired)
        except Exception as e:
            self.loggit.error("Error detecting expired repositories: %s", e)

        # Get all repositories and filter for expired ones
        from curator.actions.deepfreeze.constants import THAW_STATE_EXPIRED
        all_repos = get_matching_repos(self.client, self.settings.repo_name_prefix)
        expired_repos = [repo for repo in all_repos if repo.thaw_state == THAW_STATE_EXPIRED]

        if not expired_repos:
            self.loggit.info("No expired repositories found to clean up")
            return

        self.loggit.info("Found %d expired repositories to clean up", len(expired_repos))

        # Track repositories that were successfully cleaned up
        repos_to_cleanup = []

        for repo in expired_repos:
            self.loggit.info("Cleaning up expired repository %s", repo.name)

            try:
                # CRITICAL FIX: Verify repository mount status from Elasticsearch
                # The in-memory flag may be out of sync with actual cluster state
                is_actually_mounted = False
                try:
                    existing_repos = self.client.snapshot.get_repository(name=repo.name)
                    is_actually_mounted = repo.name in existing_repos
                    if is_actually_mounted:
                        self.loggit.debug("Repository %s is mounted in Elasticsearch", repo.name)
                    else:
                        self.loggit.debug("Repository %s is not mounted in Elasticsearch", repo.name)
                except Exception as e:
                    self.loggit.warning(
                        "Could not verify mount status for repository %s: %s",
                        repo.name,
                        e
                    )
                    is_actually_mounted = False

                # Unmount if actually mounted
                if is_actually_mounted:
                    try:
                        self.loggit.info(
                            "Unmounting repository %s (state: %s, expires_at: %s)",
                            repo.name,
                            repo.thaw_state,
                            repo.expires_at
                        )
                        self.client.snapshot.delete_repository(name=repo.name)
                        self.loggit.info("Repository %s unmounted successfully", repo.name)
                    except Exception as e:
                        self.loggit.error(
                            "Failed to unmount repository %s: %s (type: %s)",
                            repo.name,
                            str(e),
                            type(e).__name__
                        )
                        # Don't add to cleanup list if unmount failed
                        continue
                elif repo.is_mounted:
                    # In-memory flag says mounted, but ES says not mounted
                    self.loggit.info(
                        "Repository %s marked as mounted but not found in Elasticsearch (likely already unmounted)",
                        repo.name
                    )
                else:
                    self.loggit.debug("Repository %s was not mounted", repo.name)

                # Reset repository to frozen state
                repo.reset_to_frozen()
                repo.persist(self.client)
                self.loggit.info("Repository %s reset to frozen state", repo.name)

                # Add to cleanup list for index deletion
                repos_to_cleanup.append(repo)

            except Exception as e:
                self.loggit.error(
                    "Error cleaning up repository %s: %s", repo.name, e
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
                            # CRITICAL FIX: Validate index exists and get its status before deletion
                            if not self.client.indices.exists(index=index):
                                self.loggit.warning("Index %s no longer exists, skipping deletion", index)
                                continue

                            # Get index health before deletion for audit trail
                            try:
                                health = self.client.cluster.health(index=index, level='indices')
                                index_health = health.get('indices', {}).get(index, {})
                                status = index_health.get('status', 'unknown')
                                active_shards = index_health.get('active_shards', 'unknown')
                                active_primary_shards = index_health.get('active_primary_shards', 'unknown')

                                self.loggit.info(
                                    "Preparing to delete index %s (health: %s, primary_shards: %s, total_shards: %s)",
                                    index,
                                    status,
                                    active_primary_shards,
                                    active_shards
                                )
                            except Exception as health_error:
                                # Log but don't fail deletion if health check fails
                                self.loggit.debug("Could not get health for index %s: %s", index, health_error)

                            # Perform deletion
                            self.client.indices.delete(index=index)
                            self.loggit.info("Successfully deleted index %s", index)

                        except Exception as e:
                            self.loggit.error(
                                "Failed to delete index %s: %s (type: %s)",
                                index,
                                str(e),
                                type(e).__name__
                            )
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

        # Clean up orphaned thawed ILM policies
        self.loggit.info("Cleaning up orphaned thawed ILM policies")
        try:
            deleted_policies = self._cleanup_orphaned_thawed_policies()
            if deleted_policies:
                self.loggit.info("Deleted %d orphaned thawed ILM policies", len(deleted_policies))
        except Exception as e:
            self.loggit.error("Error cleaning up orphaned ILM policies: %s", e)

    def _cleanup_orphaned_thawed_policies(self) -> list[str]:
        """
        Delete thawed ILM policies that no longer have any indices assigned to them.

        Thawed ILM policies are named {repo_name}-thawed (e.g., deepfreeze-000010-thawed).
        When all indices using a thawed policy have been deleted, the policy should be
        removed to prevent accumulation.

        :return: List of deleted policy names
        :rtype: list[str]
        """
        self.loggit.debug("Searching for orphaned thawed ILM policies")

        deleted_policies = []

        try:
            # Get all ILM policies
            all_policies = self.client.ilm.get_lifecycle()

            # Filter for thawed policies (ending with -thawed)
            thawed_policies = {
                name: data for name, data in all_policies.items()
                if name.endswith("-thawed") and name.startswith(self.settings.repo_name_prefix)
            }

            if not thawed_policies:
                self.loggit.debug("No thawed ILM policies found")
                return deleted_policies

            self.loggit.debug("Found %d thawed ILM policies to check", len(thawed_policies))

            for policy_name, policy_data in thawed_policies.items():
                try:
                    # Check if policy has any indices assigned
                    in_use_by = policy_data.get("in_use_by", {})
                    indices = in_use_by.get("indices", [])
                    datastreams = in_use_by.get("data_streams", [])

                    if not indices and not datastreams:
                        # Policy has no indices or datastreams, safe to delete
                        self.loggit.info(
                            "Deleting orphaned thawed ILM policy %s (no indices assigned)",
                            policy_name
                        )
                        self.client.ilm.delete_lifecycle(name=policy_name)
                        deleted_policies.append(policy_name)
                        self.loggit.info("Successfully deleted ILM policy %s", policy_name)
                    else:
                        self.loggit.debug(
                            "Keeping ILM policy %s (%d indices, %d datastreams)",
                            policy_name,
                            len(indices),
                            len(datastreams)
                        )

                except Exception as e:
                    self.loggit.error(
                        "Failed to check/delete ILM policy %s: %s", policy_name, e
                    )

        except Exception as e:
            self.loggit.error("Error listing ILM policies: %s", e)

        return deleted_policies

    def do_dry_run(self) -> None:
        """
        Perform a dry-run of the cleanup operation.
        Shows which repositories would be unmounted and which indices would be deleted.

        :return: None
        :rtype: None
        """
        self.loggit.info("DRY-RUN MODE. No changes will be made.")

        # First, show which thawed repositories would be detected as expired
        self.loggit.info("DRY-RUN: Checking for thawed repositories that have passed expiration time")
        from curator.actions.deepfreeze.constants import THAW_STATE_THAWED
        all_repos = get_matching_repos(self.client, self.settings.repo_name_prefix)
        thawed_repos = [repo for repo in all_repos if repo.thaw_state == THAW_STATE_THAWED]

        if thawed_repos:
            now = datetime.now(timezone.utc)
            would_expire = []

            for repo in thawed_repos:
                if repo.expires_at:
                    expires_at = repo.expires_at
                    if expires_at.tzinfo is None:
                        expires_at = expires_at.replace(tzinfo=timezone.utc)

                    if now >= expires_at:
                        time_expired = now - expires_at
                        would_expire.append((repo.name, expires_at, time_expired))
                    else:
                        time_remaining = expires_at - now
                        self.loggit.debug(
                            "DRY-RUN: Repository %s not yet expired (expires in %s)",
                            repo.name,
                            time_remaining
                        )
                else:
                    self.loggit.warning(
                        "DRY-RUN: Repository %s is thawed but has no expires_at timestamp",
                        repo.name
                    )

            if would_expire:
                self.loggit.info(
                    "DRY-RUN: Would mark %d repositories as expired:",
                    len(would_expire)
                )
                for name, expired_at, time_ago in would_expire:
                    self.loggit.info(
                        "DRY-RUN:   - %s (expired %s ago at %s)",
                        name,
                        time_ago,
                        expired_at.isoformat()
                    )
            else:
                self.loggit.info("DRY-RUN: No thawed repositories have passed expiration time")
        else:
            self.loggit.info("DRY-RUN: No thawed repositories found to check")

        # Get all repositories and filter for expired ones
        from curator.actions.deepfreeze.constants import THAW_STATE_EXPIRED
        all_repos = get_matching_repos(self.client, self.settings.repo_name_prefix)
        expired_repos = [repo for repo in all_repos if repo.thaw_state == THAW_STATE_EXPIRED]

        if not expired_repos:
            self.loggit.info("DRY-RUN: No expired repositories found to clean up")
            return

        self.loggit.info("DRY-RUN: Found %d expired repositories to clean up", len(expired_repos))

        # Track repositories that would be cleaned up
        repos_to_cleanup = []

        for repo in expired_repos:
            action = "unmount and reset to frozen" if repo.is_mounted else "reset to frozen"
            self.loggit.info(
                "DRY-RUN: Would %s repository %s (state: %s)",
                action,
                repo.name,
                repo.thaw_state
            )
            repos_to_cleanup.append(repo)

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
                retention_refrozen = self.settings.thaw_request_retention_days_refrozen

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

                        elif status == "refrozen" and age_days > retention_refrozen:
                            should_delete = True
                            reason = f"refrozen request older than {retention_refrozen} days (age: {age_days} days)"

                        elif status == "in_progress" and repos:
                            try:
                                from curator.actions.deepfreeze.constants import THAW_STATE_THAWING, THAW_STATE_THAWED
                                repo_objects = get_repositories_by_names(self.client, repos)
                                any_active = any(
                                    repo.thaw_state in [THAW_STATE_THAWING, THAW_STATE_THAWED]
                                    for repo in repo_objects
                                )

                                if not any_active:
                                    should_delete = True
                                    reason = "in-progress request with no active repos (all repos have been cleaned up)"
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

    def do_singleton_action(self, dry_run: bool = False) -> None:
        """
        Entry point for singleton CLI execution.

        :param dry_run: If True, perform a dry-run without making changes
        :type dry_run: bool

        :return: None
        :rtype: None
        """
        if dry_run:
            self.do_dry_run()
        else:
            self.do_action()
