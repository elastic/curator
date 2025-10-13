"""Thaw action for deepfreeze"""

# pylint: disable=too-many-arguments,too-many-instance-attributes, raise-missing-from

import logging
import time
import uuid
from datetime import datetime

from elasticsearch import Elasticsearch

from curator.actions.deepfreeze.utilities import (
    check_restore_status,
    decode_date,
    find_repos_by_date_range,
    get_settings,
    mount_repo,
    save_thaw_request,
)
from curator.s3client import s3_client_factory


class Thaw:
    """
    The Thaw action restores repositories from Glacier storage to instant-access tiers
    for a specified date range.

    :param client: A client connection object
    :type client: Elasticsearch
    :param start_date: Start of date range (ISO 8601 format)
    :type start_date: str
    :param end_date: End of date range (ISO 8601 format)
    :type end_date: str
    :param sync: Wait for restore and mount (True) or return immediately (False)
    :type sync: bool
    :param restore_days: Number of days to keep objects restored
    :type restore_days: int
    :param retrieval_tier: AWS retrieval tier (Standard/Expedited/Bulk)
    :type retrieval_tier: str

    :methods:
        do_action: Perform the thaw operation.
        do_dry_run: Perform a dry-run of the thaw operation.
        _parse_dates: Parse and validate date inputs.
        _thaw_repository: Thaw a single repository.
        _wait_for_restore: Wait for restoration to complete.
    """

    def __init__(
        self,
        client: Elasticsearch,
        start_date: str,
        end_date: str,
        sync: bool = False,
        restore_days: int = 7,
        retrieval_tier: str = "Standard",
    ) -> None:
        self.loggit = logging.getLogger("curator.actions.deepfreeze")
        self.loggit.debug("Initializing Deepfreeze Thaw")

        self.client = client
        self.sync = sync
        self.restore_days = restore_days
        self.retrieval_tier = retrieval_tier

        # Parse and validate dates
        self.start_date = self._parse_date(start_date, "start_date")
        self.end_date = self._parse_date(end_date, "end_date")

        if self.start_date > self.end_date:
            raise ValueError("start_date must be before or equal to end_date")

        # Get settings and initialize S3 client
        self.settings = get_settings(client)
        self.s3 = s3_client_factory(self.settings.provider)

        # Generate request ID for async operations
        self.request_id = str(uuid.uuid4())

        self.loggit.info("Deepfreeze Thaw initialized")

    def _parse_date(self, date_str: str, field_name: str) -> datetime:
        """
        Parse a date string in ISO 8601 format.

        :param date_str: The date string to parse
        :type date_str: str
        :param field_name: The name of the field (for error messages)
        :type field_name: str

        :returns: The parsed datetime object
        :rtype: datetime

        :raises ValueError: If the date string is invalid
        """
        try:
            dt = decode_date(date_str)
            self.loggit.debug("Parsed %s: %s", field_name, dt.isoformat())
            return dt
        except Exception as e:
            raise ValueError(
                f"Invalid {field_name}: {date_str}. "
                f"Expected ISO 8601 format (e.g., '2025-01-15T00:00:00Z'). Error: {e}"
            )

    def _thaw_repository(self, repo) -> bool:
        """
        Thaw a single repository by restoring its objects from Glacier.

        :param repo: The repository to thaw
        :type repo: Repository

        :returns: True if successful, False otherwise
        :rtype: bool
        """
        self.loggit.info("Thawing repository %s", repo.name)

        # Check if repository is already thawed
        if repo.is_thawed and repo.is_mounted:
            self.loggit.info("Repository %s is already thawed and mounted", repo.name)
            return True

        # Get the list of object keys to restore
        self.loggit.debug(
            "Listing objects in s3://%s/%s", repo.bucket, repo.base_path
        )
        objects = self.s3.list_objects(repo.bucket, repo.base_path)
        object_keys = [obj["Key"] for obj in objects]

        self.loggit.info(
            "Found %d objects to restore in repository %s", len(object_keys), repo.name
        )

        # Restore objects from Glacier
        try:
            self.s3.thaw(
                bucket_name=repo.bucket,
                base_path=repo.base_path,
                object_keys=object_keys,
                restore_days=self.restore_days,
                retrieval_tier=self.retrieval_tier,
            )
            self.loggit.info(
                "Successfully initiated restore for repository %s", repo.name
            )
            return True
        except Exception as e:
            self.loggit.error("Failed to thaw repository %s: %s", repo.name, e)
            return False

    def _wait_for_restore(self, repo, poll_interval: int = 30) -> bool:
        """
        Wait for restoration to complete by polling S3.

        :param repo: The repository to check
        :type repo: Repository
        :param poll_interval: Seconds between status checks
        :type poll_interval: int

        :returns: True if restoration completed, False if timeout or error
        :rtype: bool
        """
        self.loggit.info("Waiting for restoration of repository %s", repo.name)

        max_attempts = 1200  # 10 hours with 30-second polls
        attempt = 0

        while attempt < max_attempts:
            status = check_restore_status(self.s3, repo.bucket, repo.base_path)

            self.loggit.debug(
                "Restore status for %s: %d/%d objects restored, %d in progress",
                repo.name,
                status["restored"],
                status["total"],
                status["in_progress"],
            )

            if status["complete"]:
                self.loggit.info("Restoration complete for repository %s", repo.name)
                return True

            attempt += 1
            if attempt < max_attempts:
                self.loggit.debug(
                    "Waiting %d seconds before next status check...", poll_interval
                )
                time.sleep(poll_interval)

        self.loggit.warning(
            "Restoration timed out for repository %s after %d checks",
            repo.name,
            max_attempts,
        )
        return False

    def do_dry_run(self) -> None:
        """
        Perform a dry-run of the thaw operation.

        :return: None
        :rtype: None
        """
        self.loggit.info("DRY-RUN MODE. No changes will be made.")

        msg = (
            f"DRY-RUN: Thawing repositories with data between "
            f"{self.start_date.isoformat()} and {self.end_date.isoformat()}"
        )
        self.loggit.info(msg)

        # Find matching repositories
        repos = find_repos_by_date_range(self.client, self.start_date, self.end_date)

        if not repos:
            self.loggit.warning("DRY-RUN: No repositories found for date range")
            return

        self.loggit.info("DRY-RUN: Found %d repositories to thaw:", len(repos))
        for repo in repos:
            self.loggit.info(
                "  - %s (bucket: %s, path: %s, dates: %s to %s)",
                repo.name,
                repo.bucket,
                repo.base_path,
                repo.start,
                repo.end,
            )

        if self.sync:
            self.loggit.info("DRY-RUN: Would wait for restoration and mount repositories")
        else:
            self.loggit.info(
                "DRY-RUN: Would return request ID: %s", self.request_id
            )

    def do_action(self) -> None:
        """
        Perform the thaw operation.

        :return: None
        :rtype: None
        """
        self.loggit.info(
            "Thawing repositories with data between %s and %s",
            self.start_date.isoformat(),
            self.end_date.isoformat(),
        )

        # Find matching repositories
        repos = find_repos_by_date_range(self.client, self.start_date, self.end_date)

        if not repos:
            self.loggit.warning("No repositories found for date range")
            return

        self.loggit.info("Found %d repositories to thaw", len(repos))

        # Thaw each repository
        thawed_repos = []
        for repo in repos:
            if self._thaw_repository(repo):
                thawed_repos.append(repo)

        if not thawed_repos:
            self.loggit.error("Failed to thaw any repositories")
            return

        self.loggit.info("Successfully initiated thaw for %d repositories", len(thawed_repos))

        # Handle sync vs async modes
        if self.sync:
            self.loggit.info("Sync mode: Waiting for restoration to complete...")

            # Wait for each repository to be restored
            for repo in thawed_repos:
                if self._wait_for_restore(repo):
                    # Mount the repository
                    mount_repo(self.client, repo)
                else:
                    self.loggit.warning(
                        "Skipping mount for %s due to restoration timeout", repo.name
                    )

            self.loggit.info("Thaw operation completed")

        else:
            self.loggit.info("Async mode: Saving thaw request...")

            # Save thaw request for later querying
            save_thaw_request(
                self.client, self.request_id, thawed_repos, "in_progress"
            )

            self.loggit.info(
                "Thaw request saved with ID: %s. "
                "Use this ID to check status and mount when ready.",
                self.request_id,
            )

    def do_singleton_action(self) -> None:
        """
        Entry point for singleton CLI execution.

        :return: None
        :rtype: None
        """
        self.do_action()
