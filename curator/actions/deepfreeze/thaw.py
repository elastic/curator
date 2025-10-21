"""Thaw action for deepfreeze"""

# pylint: disable=too-many-arguments,too-many-instance-attributes, raise-missing-from

import logging
import time
import uuid
from datetime import datetime

from elasticsearch import Elasticsearch
from rich import print as rprint
from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TimeElapsedColumn
from rich.table import Table

from curator.actions.deepfreeze.utilities import (
    check_restore_status,
    decode_date,
    find_and_mount_indices_in_date_range,
    find_repos_by_date_range,
    get_repositories_by_names,
    get_settings,
    get_thaw_request,
    list_thaw_requests,
    mount_repo,
    save_thaw_request,
    update_repository_date_range,
    update_thaw_request,
)
from curator.s3client import s3_client_factory


class Thaw:
    """
    The Thaw action restores repositories from Glacier storage to instant-access tiers
    for a specified date range, or checks status of existing thaw requests.

    :param client: A client connection object
    :type client: Elasticsearch
    :param start_date: Start of date range (ISO 8601 format) - required for new thaw
    :type start_date: str
    :param end_date: End of date range (ISO 8601 format) - required for new thaw
    :type end_date: str
    :param sync: Wait for restore and mount (True) or return immediately (False)
    :type sync: bool
    :param duration: Number of days to keep objects restored from Glacier
    :type duration: int
    :param retrieval_tier: AWS retrieval tier (Standard/Expedited/Bulk)
    :type retrieval_tier: str
    :param check_status: Thaw request ID to check status and mount if ready
    :type check_status: str
    :param list_requests: List all thaw requests
    :type list_requests: bool
    :param porcelain: Output plain text without rich formatting
    :type porcelain: bool

    :methods:
        do_action: Perform the thaw operation or route to appropriate mode.
        do_dry_run: Perform a dry-run of the thaw operation.
        do_check_status: Check status of a thaw request and mount if ready.
        do_list_requests: Display all thaw requests in a table.
        _display_thaw_status: Display detailed status of a thaw request.
        _parse_date: Parse and validate date inputs.
        _thaw_repository: Thaw a single repository.
        _wait_for_restore: Wait for restoration to complete.
        _update_repo_dates: Update repository date ranges after mounting.
    """

    def __init__(
        self,
        client: Elasticsearch,
        start_date: str = None,
        end_date: str = None,
        sync: bool = False,
        duration: int = 7,
        retrieval_tier: str = "Standard",
        check_status: str = None,
        list_requests: bool = False,
        porcelain: bool = False,
    ) -> None:
        self.loggit = logging.getLogger("curator.actions.deepfreeze")
        self.loggit.debug("Initializing Deepfreeze Thaw")

        self.client = client
        self.sync = sync
        self.duration = duration
        self.retrieval_tier = retrieval_tier
        self.check_status = check_status
        self.list_requests = list_requests
        self.porcelain = porcelain
        self.console = Console()

        # Determine operation mode
        if list_requests:
            self.mode = "list"
        elif check_status is not None:
            # check_status can be "" (check all) or a specific request ID
            if check_status == "":
                self.mode = "check_all_status"
            else:
                self.mode = "check_status"
        else:
            self.mode = "create"
            # Parse and validate dates for create mode
            if not start_date or not end_date:
                raise ValueError(
                    "start_date and end_date are required when creating a new thaw request"
                )
            self.start_date = self._parse_date(start_date, "start_date")
            self.end_date = self._parse_date(end_date, "end_date")

            if self.start_date > self.end_date:
                raise ValueError("start_date must be before or equal to end_date")

        # Get settings and initialize S3 client (not needed for list mode)
        if self.mode not in ["list"]:
            self.settings = get_settings(client)
            self.s3 = s3_client_factory(self.settings.provider)

        # Generate request ID for async create operations
        if self.mode == "create":
            self.request_id = str(uuid.uuid4())

        self.loggit.info("Deepfreeze Thaw initialized in %s mode", self.mode)

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
                restore_days=self.duration,
                retrieval_tier=self.retrieval_tier,
            )
            self.loggit.info(
                "Successfully initiated restore for repository %s", repo.name
            )

            # Update repository state to 'thawing'
            from datetime import timedelta, timezone
            expires_at = datetime.now(timezone.utc) + timedelta(days=self.duration)
            repo.start_thawing(expires_at)
            repo.persist(self.client)
            self.loggit.debug(
                "Repository %s marked as 'thawing', expires at %s",
                repo.name,
                expires_at.isoformat()
            )

            return True
        except Exception as e:
            self.loggit.error("Failed to thaw repository %s: %s", repo.name, e)
            return False

    def _wait_for_restore(self, repo, poll_interval: int = 30, show_progress: bool = False) -> bool:
        """
        Wait for restoration to complete by polling S3.

        :param repo: The repository to check
        :type repo: Repository
        :param poll_interval: Seconds between status checks
        :type poll_interval: int
        :param show_progress: Whether to show rich progress bar (for sync mode)
        :type show_progress: bool

        :returns: True if restoration completed, False if timeout or error
        :rtype: bool
        """
        self.loggit.info("Waiting for restoration of repository %s", repo.name)

        max_attempts = 1200  # 10 hours with 30-second polls
        attempt = 0

        # Initial status check to get total objects
        initial_status = check_restore_status(self.s3, repo.bucket, repo.base_path)
        total_objects = initial_status["total"]

        if show_progress and total_objects > 0:
            # Use rich progress bar for sync mode
            with Progress(
                SpinnerColumn(),
                TextColumn("[bold blue]{task.description}"),
                BarColumn(),
                TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                TextColumn("({task.completed}/{task.total} objects)"),
                TimeElapsedColumn(),
                console=self.console,
            ) as progress:
                task = progress.add_task(
                    f"Restoring {repo.name}",
                    total=total_objects,
                    completed=initial_status["restored"]
                )

                while attempt < max_attempts:
                    status = check_restore_status(self.s3, repo.bucket, repo.base_path)

                    # Update progress bar
                    progress.update(task, completed=status["restored"])

                    if status["complete"]:
                        progress.update(task, completed=total_objects)
                        self.loggit.info("Restoration complete for repository %s", repo.name)
                        return True

                    attempt += 1
                    if attempt < max_attempts:
                        time.sleep(poll_interval)

                self.loggit.warning(
                    "Restoration timed out for repository %s after %d checks",
                    repo.name,
                    max_attempts,
                )
                return False
        else:
            # Non-progress mode (async or no objects)
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

    def _update_repo_dates(self, repo) -> None:
        """
        Update repository date ranges after mounting.

        :param repo: The repository to update
        :type repo: Repository

        :return: None
        :rtype: None
        """
        self.loggit.debug("Updating date range for repository %s", repo.name)

        try:
            updated = update_repository_date_range(self.client, repo)
            if updated:
                self.loggit.info(
                    "Updated date range for %s: %s to %s",
                    repo.name,
                    repo.start.isoformat() if repo.start else "None",
                    repo.end.isoformat() if repo.end else "None"
                )
            else:
                self.loggit.debug(
                    "No date range update needed for %s", repo.name
                )
        except Exception as e:
            self.loggit.warning(
                "Failed to update date range for %s: %s", repo.name, e
            )

    def do_check_status(self) -> None:
        """
        Check the status of a thaw request and mount repositories if restoration is complete.
        Also mounts indices in the date range if all repositories are ready.

        :return: None
        :rtype: None
        """
        self.loggit.info("Checking status of thaw request %s", self.check_status)

        # Retrieve the thaw request
        request = get_thaw_request(self.client, self.check_status)

        # Get the repository objects
        repos = get_repositories_by_names(self.client, request["repos"])

        if not repos:
            self.loggit.warning("No repositories found for thaw request")
            return

        # Display current status
        self._display_thaw_status(request, repos)

        # Check restoration status and mount if ready
        all_complete = True
        mounted_count = 0
        newly_mounted_repos = []

        for repo in repos:
            if repo.is_mounted:
                self.loggit.info("Repository %s is already mounted", repo.name)
                continue

            status = check_restore_status(self.s3, repo.bucket, repo.base_path)

            if status["complete"]:
                self.loggit.info("Restoration complete for %s, mounting...", repo.name)
                mount_repo(self.client, repo)
                self._update_repo_dates(repo)
                mounted_count += 1
                newly_mounted_repos.append(repo)
            else:
                self.loggit.info(
                    "Restoration in progress for %s: %d/%d objects restored",
                    repo.name,
                    status["restored"],
                    status["total"],
                )
                all_complete = False

        # Mount indices if all repositories are complete and at least one is mounted
        # Parse date range from the thaw request
        start_date_str = request.get("start_date")
        end_date_str = request.get("end_date")

        # Check if we should mount indices:
        # - All repos are complete (restoration finished)
        # - At least one repo is mounted
        # - We have date range info
        should_mount_indices = (
            all_complete
            and start_date_str
            and end_date_str
            and any(repo.is_mounted for repo in repos)
        )

        if should_mount_indices:
            try:
                start_date = decode_date(start_date_str)
                end_date = decode_date(end_date_str)

                self.loggit.info(
                    "Mounting indices for date range %s to %s",
                    start_date.isoformat(),
                    end_date.isoformat(),
                )

                # Use all mounted repos, not just newly mounted ones
                # This handles the case where repos were already mounted
                mounted_repos = [repo for repo in repos if repo.is_mounted]

                mount_result = find_and_mount_indices_in_date_range(
                    self.client, mounted_repos, start_date, end_date
                )

                self.loggit.info(
                    "Mounted %d indices (%d skipped outside date range, %d failed, %d added to data streams)",
                    mount_result["mounted"],
                    mount_result["skipped"],
                    mount_result["failed"],
                    mount_result["datastream_successful"],
                )

                if not self.porcelain:
                    rprint(
                        f"[green]Mounted {mount_result['mounted']} indices "
                        f"({mount_result['skipped']} skipped outside date range, "
                        f"{mount_result['failed']} failed, "
                        f"{mount_result['datastream_successful']} added to data streams)[/green]"
                    )

            except Exception as e:
                self.loggit.warning("Failed to mount indices: %s", e)
                if not self.porcelain:
                    rprint(f"[yellow]Warning: Failed to mount indices: {e}[/yellow]")

        # Update thaw request status if all repositories are ready
        if all_complete:
            update_thaw_request(self.client, self.check_status, status="completed")
            self.loggit.info("All repositories restored and mounted. Thaw request completed.")
        else:
            self.loggit.info(
                "Mounted %d repositories. Some restorations still in progress.",
                mounted_count,
            )

    def do_check_all_status(self) -> None:
        """
        Check the status of all thaw requests, mount repositories when ready,
        and display grouped by request ID.

        :return: None
        :rtype: None
        """
        self.loggit.info("Checking status of all thaw requests")

        # Get all thaw requests
        requests = list_thaw_requests(self.client)

        if not requests:
            if not self.porcelain:
                rprint("\n[yellow]No thaw requests found.[/yellow]\n")
            return

        # Process each request
        for req in requests:
            request_id = req["id"]

            # Get the full request data
            try:
                request = get_thaw_request(self.client, request_id)
            except Exception as e:
                self.loggit.warning("Failed to get thaw request %s: %s", request_id, e)
                continue

            # Get the repository objects
            repos = get_repositories_by_names(self.client, request.get("repos", []))

            if not repos:
                self.loggit.warning("No repositories found for thaw request %s", request_id)
                continue

            # Get date range for display/output
            start_date_str = request.get("start_date", "")
            end_date_str = request.get("end_date", "")

            # Track mounting for this request
            all_complete = True
            mounted_count = 0
            newly_mounted_repos = []
            repo_data = []  # Store repo info for output

            # Check each repository's status and mount if ready
            for repo in repos:
                # Check restore status if not mounted
                if not repo.is_mounted:
                    try:
                        status = check_restore_status(self.s3, repo.bucket, repo.base_path)
                        if status["complete"]:
                            # Mount the repository
                            self.loggit.info("Restoration complete for %s, mounting...", repo.name)
                            mount_repo(self.client, repo)
                            self._update_repo_dates(repo)
                            mounted_count += 1
                            newly_mounted_repos.append(repo)
                            progress = "Complete"
                        else:
                            progress = f"{status['restored']}/{status['total']}"
                            all_complete = False
                    except Exception as e:
                        self.loggit.warning("Failed to check status for %s: %s", repo.name, e)
                        progress = "Error"
                        all_complete = False
                else:
                    progress = "Complete"

                # Store repo data for output
                repo_data.append({
                    "name": repo.name,
                    "bucket": repo.bucket if repo.bucket else "",
                    "path": repo.base_path if repo.base_path else "",
                    "state": repo.thaw_state,
                    "mounted": "yes" if repo.is_mounted else "no",
                    "progress": progress,
                })

            # Output based on mode
            if self.porcelain:
                # Machine-readable output: tab-separated values
                # Format: REQUEST\t{request_id}\t{status}\t{created_at}\t{start_date}\t{end_date}
                print(f"REQUEST\t{request['request_id']}\t{request['status']}\t{request['created_at']}\t{start_date_str}\t{end_date_str}")

                # Format: REPO\t{name}\t{bucket}\t{path}\t{state}\t{mounted}\t{progress}
                for repo_info in repo_data:
                    print(f"REPO\t{repo_info['name']}\t{repo_info['bucket']}\t{repo_info['path']}\t{repo_info['state']}\t{repo_info['mounted']}\t{repo_info['progress']}")
            else:
                # Human-readable output: formatted display
                # Format dates for display
                if start_date_str and "T" in start_date_str:
                    start_date_display = start_date_str.replace("T", " ").split(".")[0]
                else:
                    start_date_display = start_date_str if start_date_str else "--"

                if end_date_str and "T" in end_date_str:
                    end_date_display = end_date_str.replace("T", " ").split(".")[0]
                else:
                    end_date_display = end_date_str if end_date_str else "--"

                # Display request info
                rprint(f"\n[bold cyan]Thaw Request: {request['request_id']}[/bold cyan]")
                rprint(f"[cyan]Status: {request['status']}[/cyan]")
                rprint(f"[cyan]Created: {request['created_at']}[/cyan]")
                rprint(f"[green]Date Range: {start_date_display} to {end_date_display}[/green]\n")

                # Create table for repository status
                table = Table(title="Repository Status")
                table.add_column("Repository", style="cyan", no_wrap=False, overflow="fold")
                table.add_column("Bucket", style="magenta", no_wrap=False, overflow="fold")
                table.add_column("Path", style="magenta", no_wrap=False, overflow="fold")
                table.add_column("State", style="yellow", no_wrap=False, overflow="fold")
                table.add_column("Mounted", style="green", no_wrap=False, overflow="fold")
                table.add_column("Restore Progress", style="magenta", no_wrap=False, overflow="fold")

                for repo_info in repo_data:
                    table.add_row(
                        repo_info['name'],
                        repo_info['bucket'] if repo_info['bucket'] else "--",
                        repo_info['path'] if repo_info['path'] else "--",
                        repo_info['state'],
                        repo_info['mounted'],
                        repo_info['progress'],
                    )

                self.console.print(table)

            # Mount indices if all repositories are complete and mounted
            # Check if we should mount indices:
            # - All repos are complete (restoration finished)
            # - We have date range info
            # - At least one repo is mounted
            # Note: We don't check if request is completed because we want to mount
            # indices even if the request was previously marked complete but indices
            # weren't mounted (e.g., if repo was mounted in a previous check-status call)
            should_mount_indices = (
                all_complete
                and start_date_str
                and end_date_str
                and any(repo.is_mounted for repo in repos)
            )

            if should_mount_indices:
                try:
                    start_date = decode_date(start_date_str)
                    end_date = decode_date(end_date_str)

                    self.loggit.info(
                        "Mounting indices for date range %s to %s",
                        start_date.isoformat(),
                        end_date.isoformat(),
                    )

                    # Use all mounted repos, not just newly mounted ones
                    # This handles the case where repos were mounted in a previous check
                    mounted_repos = [repo for repo in repos if repo.is_mounted]

                    mount_result = find_and_mount_indices_in_date_range(
                        self.client, mounted_repos, start_date, end_date
                    )

                    self.loggit.info(
                        "Mounted %d indices (%d skipped outside date range, %d failed, %d added to data streams)",
                        mount_result["mounted"],
                        mount_result["skipped"],
                        mount_result["failed"],
                        mount_result["datastream_successful"],
                    )

                    if not self.porcelain:
                        rprint(
                            f"[green]Mounted {mount_result['mounted']} indices "
                            f"({mount_result['skipped']} skipped outside date range, "
                            f"{mount_result['failed']} failed, "
                            f"{mount_result['datastream_successful']} added to data streams)[/green]"
                        )
                except Exception as e:
                    self.loggit.warning("Failed to mount indices: %s", e)
                    if not self.porcelain:
                        rprint(f"[yellow]Warning: Failed to mount indices: {e}[/yellow]")

            # Update thaw request status if all repositories are ready
            if all_complete:
                update_thaw_request(self.client, request_id, status="completed")
                self.loggit.info("Thaw request %s completed", request_id)
                if not self.porcelain:
                    rprint(f"[green]Request {request_id} completed[/green]")
            elif mounted_count > 0:
                if not self.porcelain:
                    rprint(
                        f"[yellow]Mounted {mounted_count} repositories. "
                        f"Some restorations still in progress.[/yellow]"
                    )

            if not self.porcelain:
                rprint()

    def do_list_requests(self) -> None:
        """
        List all thaw requests in a formatted table.

        :return: None
        :rtype: None
        """
        self.loggit.info("Listing all thaw requests")

        requests = list_thaw_requests(self.client)

        if not requests:
            if not self.porcelain:
                rprint("\n[yellow]No thaw requests found.[/yellow]\n")
            return

        if self.porcelain:
            # Machine-readable output: tab-separated values
            # Format: REQUEST\t{id}\t{status}\t{repo_count}\t{start_date}\t{end_date}\t{created_at}
            for req in requests:
                repo_count = str(len(req.get("repos", [])))
                status = req.get("status", "unknown")
                start_date = req.get("start_date", "")
                end_date = req.get("end_date", "")
                created_at = req.get("created_at", "")

                print(f"REQUEST\t{req['id']}\t{status}\t{repo_count}\t{start_date}\t{end_date}\t{created_at}")
        else:
            # Human-readable output: formatted table
            # Create table
            table = Table(title="Thaw Requests")
            table.add_column("Request ID", style="cyan", no_wrap=False, overflow="fold")
            table.add_column("St", style="magenta", no_wrap=False, overflow="fold")  # Abbreviated Status
            table.add_column("Repos", style="magenta", no_wrap=False, overflow="fold")  # Abbreviated Repositories
            table.add_column("Start Date", style="green", no_wrap=False, overflow="fold")
            table.add_column("End Date", style="green", no_wrap=False, overflow="fold")
            table.add_column("Created At", style="magenta", no_wrap=False, overflow="fold")

            # Add rows
            for req in requests:
                repo_count = str(len(req.get("repos", [])))
                created_at = req.get("created_at", "Unknown")
                # Format datetime if it's ISO format
                if "T" in created_at:
                    created_at = created_at.replace("T", " ").split(".")[0]

                # Format date range
                start_date = req.get("start_date", "")
                end_date = req.get("end_date", "")

                # Format dates to show full datetime (same format as created_at)
                if start_date and "T" in start_date:
                    start_date = start_date.replace("T", " ").split(".")[0]
                if end_date and "T" in end_date:
                    end_date = end_date.replace("T", " ").split(".")[0]

                # Use "--" for missing dates
                start_date = start_date if start_date else "--"
                end_date = end_date if end_date else "--"

                # Abbreviate status for display
                status = req.get("status", "unknown")
                status_abbrev = {
                    "in_progress": "IP",
                    "completed": "C",
                    "failed": "F",
                    "unknown": "U",
                }.get(status, status[:2].upper())

                table.add_row(
                    req["id"],  # Show full Request ID
                    status_abbrev,
                    repo_count,
                    start_date,
                    end_date,
                    created_at,
                )

            self.console.print(table)
            rprint("[dim]Status: IP=In Progress, C=Completed, F=Failed, U=Unknown[/dim]")

    def _display_thaw_status(self, request: dict, repos: list) -> None:
        """
        Display detailed status information for a thaw request.

        :param request: The thaw request document
        :type request: dict
        :param repos: List of Repository objects
        :type repos: list

        :return: None
        :rtype: None
        """
        # Get date range for display/output
        start_date_str = request.get("start_date", "")
        end_date_str = request.get("end_date", "")

        # Build repo data with restore progress
        repo_data = []
        for repo in repos:
            # Check restore status if not mounted
            if not repo.is_mounted:
                try:
                    status = check_restore_status(self.s3, repo.bucket, repo.base_path)
                    if status["complete"]:
                        progress = "Complete"
                    else:
                        progress = f"{status['restored']}/{status['total']}"
                except Exception as e:
                    self.loggit.warning("Failed to check status for %s: %s", repo.name, e)
                    progress = "Error"
            else:
                progress = "Complete"

            repo_data.append({
                "name": repo.name,
                "bucket": repo.bucket if repo.bucket else "",
                "path": repo.base_path if repo.base_path else "",
                "state": repo.thaw_state,
                "mounted": "yes" if repo.is_mounted else "no",
                "progress": progress,
            })

        if self.porcelain:
            # Machine-readable output: tab-separated values
            # Format: REQUEST\t{request_id}\t{status}\t{created_at}\t{start_date}\t{end_date}
            print(f"REQUEST\t{request['request_id']}\t{request['status']}\t{request['created_at']}\t{start_date_str}\t{end_date_str}")

            # Format: REPO\t{name}\t{bucket}\t{path}\t{state}\t{mounted}\t{progress}
            for repo_info in repo_data:
                print(f"REPO\t{repo_info['name']}\t{repo_info['bucket']}\t{repo_info['path']}\t{repo_info['state']}\t{repo_info['mounted']}\t{repo_info['progress']}")
        else:
            # Human-readable output: formatted display
            # Format dates for display
            if start_date_str and "T" in start_date_str:
                start_date_display = start_date_str.replace("T", " ").split(".")[0]
            else:
                start_date_display = start_date_str if start_date_str else "--"

            if end_date_str and "T" in end_date_str:
                end_date_display = end_date_str.replace("T", " ").split(".")[0]
            else:
                end_date_display = end_date_str if end_date_str else "--"

            rprint(f"\n[bold cyan]Thaw Request: {request['request_id']}[/bold cyan]")
            rprint(f"[cyan]Status: {request['status']}[/cyan]")
            rprint(f"[cyan]Created: {request['created_at']}[/cyan]")
            rprint(f"[green]Date Range: {start_date_display} to {end_date_display}[/green]\n")

            # Create table for repository status
            table = Table(title="Repository Status")
            table.add_column("Repository", style="cyan", no_wrap=False, overflow="fold")
            table.add_column("Bucket", style="magenta", no_wrap=False, overflow="fold")
            table.add_column("Path", style="magenta", no_wrap=False, overflow="fold")
            table.add_column("State", style="yellow", no_wrap=False, overflow="fold")
            table.add_column("Mounted", style="green", no_wrap=False, overflow="fold")
            table.add_column("Restore Progress", style="magenta", no_wrap=False, overflow="fold")

            for repo_info in repo_data:
                table.add_row(
                    repo_info['name'],
                    repo_info['bucket'] if repo_info['bucket'] else "--",
                    repo_info['path'] if repo_info['path'] else "--",
                    repo_info['state'],
                    repo_info['mounted'],
                    repo_info['progress'],
                )

            self.console.print(table)
            rprint()

    def do_dry_run(self) -> None:
        """
        Perform a dry-run of the thaw operation.

        :return: None
        :rtype: None
        """
        self.loggit.info("DRY-RUN MODE. No changes will be made.")

        if self.mode == "list":
            self.loggit.info("DRY-RUN: Would list all thaw requests")
            self.do_list_requests()
            return

        if self.mode == "check_status":
            self.loggit.info(
                "DRY-RUN: Would check status of thaw request %s", self.check_status
            )
            # Still show current status in dry-run
            request = get_thaw_request(self.client, self.check_status)
            repos = get_repositories_by_names(self.client, request["repos"])
            self._display_thaw_status(request, repos)
            self.loggit.info("DRY-RUN: Would mount any repositories with completed restoration")
            return

        if self.mode == "check_all_status":
            self.loggit.info("DRY-RUN: Would check status of all thaw requests and mount any repositories with completed restoration")
            return

        # Create mode
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
        Perform the thaw operation (routes to appropriate handler based on mode).

        :return: None
        :rtype: None
        """
        if self.mode == "list":
            self.do_list_requests()
            return

        if self.mode == "check_status":
            self.do_check_status()
            return

        if self.mode == "check_all_status":
            self.do_check_all_status()
            return

        # Create mode - original thaw logic
        self.loggit.info(
            "Thawing repositories with data between %s and %s",
            self.start_date.isoformat(),
            self.end_date.isoformat(),
        )

        # Phase 1: Find matching repositories
        if self.sync:
            self.console.print(Panel(
                f"[bold cyan]Phase 1: Finding Repositories[/bold cyan]\n\n"
                f"Date Range: [yellow]{self.start_date.isoformat()}[/yellow] to "
                f"[yellow]{self.end_date.isoformat()}[/yellow]",
                border_style="cyan",
                expand=False
            ))

        repos = find_repos_by_date_range(self.client, self.start_date, self.end_date)

        if not repos:
            self.loggit.warning("No repositories found for date range")
            if self.sync:
                self.console.print(Panel(
                    "[yellow]No repositories found matching the specified date range.[/yellow]",
                    title="[bold yellow]No Repositories Found[/bold yellow]",
                    border_style="yellow",
                    expand=False
                ))
            return

        self.loggit.info("Found %d repositories to thaw", len(repos))

        if self.sync:
            # Display found repositories
            table = Table(title=f"Found {len(repos)} Repositories")
            table.add_column("Repository", style="cyan", no_wrap=False, overflow="fold")
            table.add_column("Bucket", style="magenta", no_wrap=False, overflow="fold")
            table.add_column("Base Path", style="magenta", no_wrap=False, overflow="fold")
            for repo in repos:
                table.add_row(repo.name, repo.bucket or "--", repo.base_path or "--")
            self.console.print(table)
            self.console.print()

        # Phase 2: Initiate thaw for each repository
        if self.sync:
            self.console.print(Panel(
                f"[bold cyan]Phase 2: Initiating Glacier Restore[/bold cyan]\n\n"
                f"Retrieval Tier: [yellow]{self.retrieval_tier}[/yellow]\n"
                f"Duration: [yellow]{self.duration} days[/yellow]",
                border_style="cyan",
                expand=False
            ))

        thawed_repos = []
        for repo in repos:
            if self.sync:
                self.console.print(f"  [cyan]→[/cyan] Initiating restore for [bold]{repo.name}[/bold]...")
            if self._thaw_repository(repo):
                thawed_repos.append(repo)
                if self.sync:
                    self.console.print(f"    [green]✓[/green] Restore initiated successfully")
            else:
                if self.sync:
                    self.console.print(f"    [red]✗[/red] Failed to initiate restore")

        if not thawed_repos:
            self.loggit.error("Failed to thaw any repositories")
            if self.sync:
                self.console.print(Panel(
                    "[red]Failed to initiate restore for any repositories.[/red]",
                    title="[bold red]Thaw Failed[/bold red]",
                    border_style="red",
                    expand=False
                ))
            return

        self.loggit.info("Successfully initiated thaw for %d repositories", len(thawed_repos))
        if self.sync:
            self.console.print()

        # Handle sync vs async modes
        if self.sync:
            # Save thaw request for status tracking (will be marked completed when done)
            save_thaw_request(
                self.client,
                self.request_id,
                thawed_repos,
                "in_progress",
                self.start_date,
                self.end_date,
            )
            self.loggit.debug("Saved sync thaw request %s for status tracking", self.request_id)

            # Phase 3: Wait for restoration
            self.console.print(Panel(
                f"[bold cyan]Phase 3: Waiting for Glacier Restoration[/bold cyan]\n\n"
                f"This may take several hours depending on the retrieval tier.\n"
                f"Progress will be updated as objects are restored.",
                border_style="cyan",
                expand=False
            ))

            successfully_restored = []
            failed_restores = []

            # Wait for each repository to be restored
            for repo in thawed_repos:
                if self._wait_for_restore(repo, show_progress=True):
                    successfully_restored.append(repo)
                else:
                    failed_restores.append(repo)
                    self.loggit.warning(
                        "Skipping mount for %s due to restoration timeout", repo.name
                    )

            if not successfully_restored:
                self.console.print(Panel(
                    "[red]No repositories were successfully restored.[/red]",
                    title="[bold red]Restoration Failed[/bold red]",
                    border_style="red",
                    expand=False
                ))
                return

            self.console.print()

            # Phase 4: Mount repositories
            self.console.print(Panel(
                f"[bold cyan]Phase 4: Mounting Repositories[/bold cyan]\n\n"
                f"Mounting {len(successfully_restored)} restored "
                f"repositor{'y' if len(successfully_restored) == 1 else 'ies'}.",
                border_style="cyan",
                expand=False
            ))

            mounted_count = 0
            for repo in successfully_restored:
                self.console.print(f"  [cyan]→[/cyan] Mounting [bold]{repo.name}[/bold]...")
                try:
                    mount_repo(self.client, repo)
                    self.console.print(f"    [green]✓[/green] Mounted successfully")
                    mounted_count += 1
                except Exception as e:
                    self.console.print(f"    [red]✗[/red] Failed to mount: {e}")
                    self.loggit.error("Failed to mount %s: %s", repo.name, e)

            self.console.print()

            # Phase 5: Update date ranges
            self.console.print(Panel(
                "[bold cyan]Phase 5: Updating Repository Metadata[/bold cyan]",
                border_style="cyan",
                expand=False
            ))

            for repo in successfully_restored:
                self._update_repo_dates(repo)

            self.console.print()

            # Phase 6: Mount indices
            self.console.print(Panel(
                f"[bold cyan]Phase 6: Mounting Indices[/bold cyan]\n\n"
                f"Finding and mounting indices within the requested date range.",
                border_style="cyan",
                expand=False
            ))

            mount_result = find_and_mount_indices_in_date_range(
                self.client, successfully_restored, self.start_date, self.end_date
            )

            self.console.print(f"  [cyan]→[/cyan] Mounted [bold]{mount_result['mounted']}[/bold] indices")
            if mount_result['skipped'] > 0:
                self.console.print(
                    f"  [dim]•[/dim] Skipped [dim]{mount_result['skipped']}[/dim] indices outside date range"
                )
            if mount_result['failed'] > 0:
                self.console.print(
                    f"  [yellow]⚠[/yellow] Failed to mount [yellow]{mount_result['failed']}[/yellow] indices"
                )
            if mount_result['datastream_successful'] > 0:
                self.console.print(
                    f"  [green]✓[/green] Added [bold]{mount_result['datastream_successful']}[/bold] indices to data streams"
                )
            if mount_result['datastream_failed'] > 0:
                self.console.print(
                    f"  [yellow]⚠[/yellow] Failed to add [yellow]{mount_result['datastream_failed']}[/yellow] indices to data streams"
                )

            # Final summary
            self.console.print()
            summary_lines = [
                f"[bold green]Thaw Operation Completed Successfully![/bold green]\n",
                f"Repositories Processed: [cyan]{len(repos)}[/cyan]",
                f"Restore Initiated: [cyan]{len(thawed_repos)}[/cyan]",
                f"Successfully Restored: [cyan]{len(successfully_restored)}[/cyan]",
                f"Successfully Mounted: [cyan]{mounted_count}[/cyan]",
                f"Indices Mounted: [cyan]{mount_result['mounted']}[/cyan]",
            ]
            if failed_restores:
                summary_lines.append(f"Failed Restores: [yellow]{len(failed_restores)}[/yellow]")
            if mount_result['failed'] > 0:
                summary_lines.append(f"Failed Index Mounts: [yellow]{mount_result['failed']}[/yellow]")
            if mount_result['datastream_successful'] > 0:
                summary_lines.append(f"Data Stream Indices Added: [cyan]{mount_result['datastream_successful']}[/cyan]")

            self.console.print(Panel(
                "\n".join(summary_lines),
                title="[bold green]Summary[/bold green]",
                border_style="green",
                expand=False
            ))

            # Mark thaw request as completed
            update_thaw_request(self.client, self.request_id, status="completed")
            self.loggit.debug("Marked thaw request %s as completed", self.request_id)

            self.loggit.info("Thaw operation completed")

        else:
            # Async mode - initiate restore and return immediately
            self.loggit.info("Async mode: Saving thaw request...")

            # Save thaw request for later querying
            save_thaw_request(
                self.client,
                self.request_id,
                thawed_repos,
                "in_progress",
                self.start_date,
                self.end_date,
            )

            self.loggit.info(
                "Thaw request saved with ID: %s. "
                "Use this ID to check status and mount when ready.",
                self.request_id,
            )

            # Display the thaw ID prominently for the user
            self.console.print()
            self.console.print(Panel(
                f"[bold green]Thaw Request Initiated[/bold green]\n\n"
                f"Request ID: [cyan]{self.request_id}[/cyan]\n\n"
                f"Glacier restore has been initiated for [cyan]{len(thawed_repos)}[/cyan] "
                f"repositor{'y' if len(thawed_repos) == 1 else 'ies'}.\n"
                f"Retrieval Tier: [yellow]{self.retrieval_tier}[/yellow]\n"
                f"Duration: [yellow]{self.duration} days[/yellow]\n\n"
                f"[dim]Check status with:[/dim]\n"
                f"[yellow]curator_cli deepfreeze thaw --check-status {self.request_id}[/yellow]",
                border_style="green",
                expand=False
            ))
            self.console.print()

    def do_singleton_action(self) -> None:
        """
        Entry point for singleton CLI execution.

        :return: None
        :rtype: None
        """
        self.do_action()
