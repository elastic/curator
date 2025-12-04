"""Thaw action for deepfreeze"""

# pylint: disable=too-many-arguments,too-many-instance-attributes, raise-missing-from

import logging
import time
import uuid
from datetime import datetime, timedelta, timezone

from elasticsearch8 import Elasticsearch
from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.table import Table
from rich.markup import escape

from deepfreeze.constants import (
    STATUS_INDEX,
    THAW_STATE_THAWING,
    THAW_STATUS_COMPLETED,
    THAW_STATUS_FAILED,
    THAW_STATUS_IN_PROGRESS,
)
from deepfreeze.exceptions import ActionError, MissingIndexError, MissingSettingsError
from deepfreeze.helpers import Repository
from deepfreeze.s3client import s3_client_factory
from deepfreeze.utilities import (
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
    update_thaw_request,
)


class Thaw:
    """
    Thaw restores data from Glacier storage and mounts repositories.

    Supports multiple operation modes:
    - Date range: Identify and thaw repositories containing data in a date range
    - Request status: Check status of an existing thaw request
    - List requests: List all thaw requests

    :param client: A client connection object
    :param start_date: Start of the date range to thaw (optional)
    :param end_date: End of the date range to thaw (optional)
    :param request_id: ID of an existing thaw request to check/continue (optional)
    :param list_requests: If True, list all thaw requests
    :param restore_days: Number of days to keep data restored (default: 7)
    :param retrieval_tier: Glacier retrieval tier (Standard, Bulk, Expedited)
    :param sync: If True, wait for restoration to complete

    :methods:
        do_dry_run: Perform a dry-run of the thaw operation
        do_action: Perform the thaw operation

    :example:
        >>> from deepfreeze.actions import Thaw
        >>> thaw = Thaw(client, start_date=datetime(2023, 1, 1), end_date=datetime(2023, 1, 31))
        >>> thaw.do_action()
    """

    def __init__(
        self,
        client: Elasticsearch,
        start_date: datetime = None,
        end_date: datetime = None,
        request_id: str = None,
        list_requests: bool = False,
        restore_days: int = 7,
        retrieval_tier: str = "Standard",
        sync: bool = False,
        porcelain: bool = False,
    ) -> None:
        self.loggit = logging.getLogger("deepfreeze.actions.thaw")
        self.loggit.debug("Initializing Deepfreeze Thaw")

        # Console for STDERR output
        self.console = Console(stderr=True)

        self.client = client
        self.start_date = start_date
        self.end_date = end_date
        self.request_id = request_id
        self.list_requests = list_requests
        self.restore_days = restore_days
        self.retrieval_tier = retrieval_tier
        self.sync = sync
        self.porcelain = porcelain

        # Will be loaded during action
        self.settings = None
        self.s3 = None

        self.loggit.debug("Deepfreeze Thaw initialized")

    def _load_settings(self) -> None:
        """Load settings from the status index."""
        self.loggit.debug("Loading settings")

        # Check if status index exists
        if not self.client.indices.exists(index=STATUS_INDEX):
            raise MissingIndexError(f"Status index {STATUS_INDEX} does not exist")

        self.settings = get_settings(self.client)
        if not self.settings:
            raise MissingSettingsError("Settings document not found in status index")

        # Initialize S3 client with provider from settings
        self.s3 = s3_client_factory(self.settings.provider)

    def _list_all_requests(self) -> None:
        """List all thaw requests."""
        requests = list_thaw_requests(self.client)

        if self.porcelain:
            for req in requests:
                print(
                    f"REQUEST\t{req.get('request_id')}\t{req.get('status')}\t"
                    f"{req.get('start_date', 'N/A')}\t{req.get('end_date', 'N/A')}\t"
                    f"{','.join(req.get('repos', []))}"
                )
            return

        if not requests:
            self.console.print("[dim]No thaw requests found[/dim]")
            return

        table = Table(title="Thaw Requests")
        table.add_column("Request ID", style="cyan")
        table.add_column("Status", style="magenta")
        table.add_column("Date Range", style="white")
        table.add_column("Repositories", style="yellow")
        table.add_column("Created", style="white")

        for req in sorted(requests, key=lambda x: x.get("created_at", "")):
            status = req.get("status", "unknown")
            status_color = {
                "in_progress": "yellow",
                "completed": "green",
                "failed": "red",
                "refrozen": "blue",
            }.get(status, "white")

            date_range = ""
            if req.get("start_date") and req.get("end_date"):
                date_range = f"{req['start_date'][:10]} - {req['end_date'][:10]}"

            repos_str = ", ".join(req.get("repos", [])[:3])
            if len(req.get("repos", [])) > 3:
                repos_str += f" (+{len(req['repos']) - 3} more)"

            created = req.get("created_at", "N/A")
            if created and created != "N/A":
                created = created[:16].replace("T", " ")

            table.add_row(
                req.get("request_id", req.get("id", "N/A")),
                f"[{status_color}]{status}[/{status_color}]",
                date_range,
                repos_str,
                created,
            )

        self.console.print(table)

    def _check_request_status(self, request_id: str) -> dict:
        """
        Check the status of a thaw request and update if completed.

        :param request_id: The thaw request ID
        :return: Status information dictionary
        """
        try:
            request = get_thaw_request(self.client, request_id)
        except ActionError:
            if self.porcelain:
                print(f"ERROR\trequest_not_found\t{request_id}")
            else:
                self.console.print(
                    Panel(
                        f"[bold]Thaw request [cyan]{request_id}[/cyan] not found[/bold]",
                        title="[bold red]Request Not Found[/bold red]",
                        border_style="red",
                        expand=False,
                    )
                )
            return None

        status = request.get("status")
        repos = request.get("repos", [])

        # If already completed or failed, just return status
        if status in [THAW_STATUS_COMPLETED, THAW_STATUS_FAILED]:
            return request

        # Check S3 restore status for each repository
        repo_objs = get_repositories_by_names(self.client, repos)

        all_complete = True
        any_failed = False
        restore_statuses = []

        for repo in repo_objs:
            restore_status = check_restore_status(
                self.s3, repo.bucket, repo.base_path
            )
            restore_statuses.append(
                {
                    "repo": repo.name,
                    "total": restore_status["total"],
                    "restored": restore_status["restored"],
                    "in_progress": restore_status["in_progress"],
                    "not_restored": restore_status["not_restored"],
                    "complete": restore_status["complete"],
                }
            )

            if not restore_status["complete"]:
                all_complete = False

        # Update request status if all complete
        if all_complete and status == THAW_STATUS_IN_PROGRESS:
            self.loggit.info("All S3 restores complete, mounting repositories")

            try:
                # Mount repositories
                for repo in repo_objs:
                    mount_repo(self.client, repo)

                # Mount indices
                start_date = decode_date(request.get("start_date"))
                end_date = decode_date(request.get("end_date"))

                mount_result = find_and_mount_indices_in_date_range(
                    self.client, repo_objs, start_date, end_date
                )

                # Update request status
                update_thaw_request(self.client, request_id, status=THAW_STATUS_COMPLETED)
                request["status"] = THAW_STATUS_COMPLETED

            except Exception as e:
                self.loggit.error("Failed to complete thaw: %s", e)
                update_thaw_request(self.client, request_id, status=THAW_STATUS_FAILED)
                request["status"] = THAW_STATUS_FAILED
                any_failed = True

        request["restore_statuses"] = restore_statuses
        return request

    def _display_request_status(self, request: dict) -> None:
        """Display the status of a thaw request."""
        if self.porcelain:
            print(
                f"STATUS\t{request.get('request_id')}\t{request.get('status')}\t"
                f"{request.get('start_date', 'N/A')}\t{request.get('end_date', 'N/A')}"
            )
            for rs in request.get("restore_statuses", []):
                print(
                    f"RESTORE\t{rs['repo']}\t{rs['restored']}/{rs['total']}\t"
                    f"{rs['in_progress']} in_progress\t{rs['not_restored']} not_restored"
                )
            return

        status = request.get("status")
        status_color = {
            "in_progress": "yellow",
            "completed": "green",
            "failed": "red",
            "refrozen": "blue",
        }.get(status, "white")

        # Build status panel
        lines = [
            f"Request ID: [cyan]{request.get('request_id')}[/cyan]",
            f"Status: [{status_color}]{status}[/{status_color}]",
        ]

        if request.get("start_date") and request.get("end_date"):
            lines.append(
                f"Date Range: {request['start_date'][:10]} - {request['end_date'][:10]}"
            )

        repos = request.get("repos", [])
        if repos:
            lines.append(f"Repositories: {', '.join(repos)}")

        self.console.print(
            Panel(
                "\n".join(lines),
                title="[bold]Thaw Request Status[/bold]",
                border_style=status_color,
                expand=False,
            )
        )

        # Show restore progress
        restore_statuses = request.get("restore_statuses", [])
        if restore_statuses:
            table = Table(title="S3 Restore Progress")
            table.add_column("Repository", style="cyan")
            table.add_column("Total", style="white")
            table.add_column("Restored", style="green")
            table.add_column("In Progress", style="yellow")
            table.add_column("Not Restored", style="red")
            table.add_column("Complete", style="white")

            for rs in restore_statuses:
                complete_str = (
                    "[green]Yes[/green]" if rs["complete"] else "[yellow]No[/yellow]"
                )
                table.add_row(
                    rs["repo"],
                    str(rs["total"]),
                    str(rs["restored"]),
                    str(rs["in_progress"]),
                    str(rs["not_restored"]),
                    complete_str,
                )

            self.console.print(table)

    def _initiate_thaw(self, dry_run: bool = False) -> str:
        """
        Initiate a new thaw operation for the specified date range.

        :param dry_run: If True, don't actually initiate thaw
        :return: The request ID
        """
        # Normalize dates to UTC
        start = self.start_date
        end = self.end_date

        if start.tzinfo is None:
            start = start.replace(tzinfo=timezone.utc)
        if end.tzinfo is None:
            end = end.replace(tzinfo=timezone.utc)

        self.loggit.info(
            "Finding repositories with data between %s and %s",
            start.isoformat(),
            end.isoformat(),
        )

        # Find repositories that overlap with the date range
        repos = find_repos_by_date_range(self.client, start, end)

        if not repos:
            if self.porcelain:
                print(f"ERROR\tno_repos_found\t{start.isoformat()}\t{end.isoformat()}")
            else:
                self.console.print(
                    Panel(
                        f"[bold]No repositories found containing data between:[/bold]\n"
                        f"  Start: {start.isoformat()}\n"
                        f"  End: {end.isoformat()}\n\n"
                        f"This could mean:\n"
                        f"  - No data exists for this date range\n"
                        f"  - Repository date ranges haven't been recorded\n"
                        f"  - Repositories haven't been rotated yet",
                        title="[bold yellow]No Repositories Found[/bold yellow]",
                        border_style="yellow",
                        expand=False,
                    )
                )
            return None

        # Generate request ID
        request_id = str(uuid.uuid4())[:8]

        if dry_run:
            if self.porcelain:
                print(
                    f"DRY_RUN\tthaw\t{request_id}\t{start.isoformat()}\t{end.isoformat()}"
                )
                for repo in repos:
                    print(f"DRY_RUN\trepo\t{repo.name}\t{repo.bucket}\t{repo.base_path}")
            else:
                repo_list = "\n".join(
                    [f"  - [cyan]{r.name}[/cyan] ({r.bucket})" for r in repos]
                )
                self.console.print(
                    Panel(
                        f"[bold]Would initiate thaw for {len(repos)} repositories:[/bold]\n"
                        f"{repo_list}\n\n"
                        f"Request ID: [cyan]{request_id}[/cyan]\n"
                        f"Date Range: {start.isoformat()} - {end.isoformat()}\n"
                        f"Restore Days: {self.restore_days}\n"
                        f"Retrieval Tier: {self.retrieval_tier}",
                        title="[bold blue]Dry Run - Thaw Operation[/bold blue]",
                        border_style="blue",
                        expand=False,
                    )
                )
            return request_id

        # Save thaw request
        save_thaw_request(
            self.client,
            request_id,
            repos,
            THAW_STATUS_IN_PROGRESS,
            start,
            end,
        )

        # Calculate expiration time
        expires_at = datetime.now(timezone.utc) + timedelta(days=self.restore_days)

        # Initiate S3 restore for each repository
        if not self.porcelain:
            self.console.print(
                f"[bold]Initiating S3 restore for {len(repos)} repositories...[/bold]"
            )

        for repo in repos:
            self.loggit.info(
                "Initiating S3 restore for %s (bucket: %s, base_path: %s)",
                repo.name,
                repo.bucket,
                repo.base_path,
            )

            try:
                # List objects in the bucket path
                objects = self.s3.list_objects(repo.bucket, repo.base_path)

                if not objects:
                    self.loggit.warning(
                        "No objects found in s3://%s/%s", repo.bucket, repo.base_path
                    )
                    continue

                # Initiate restore
                self.s3.thaw(
                    repo.bucket,
                    repo.base_path,
                    objects,
                    restore_days=self.restore_days,
                    retrieval_tier=self.retrieval_tier,
                )

                # Update repository state to thawing
                repo.start_thawing(expires_at)
                repo.persist(self.client)

                if self.porcelain:
                    print(
                        f"INITIATED\t{repo.name}\t{len(objects)} objects\t{repo.bucket}"
                    )
                else:
                    self.console.print(
                        f"  [green]Initiated restore for {repo.name}[/green] ({len(objects)} objects)"
                    )

            except Exception as e:
                self.loggit.error("Failed to initiate restore for %s: %s", repo.name, e)
                if self.porcelain:
                    print(f"ERROR\trestore_failed\t{repo.name}\t{str(e)}")
                else:
                    self.console.print(
                        f"  [red]Failed to restore {repo.name}: {escape(str(e))}[/red]"
                    )

        if not self.porcelain:
            self.console.print(
                Panel(
                    f"[bold green]Thaw request initiated successfully![/bold green]\n\n"
                    f"Request ID: [cyan]{request_id}[/cyan]\n"
                    f"Repositories: {len(repos)}\n"
                    f"Retrieval Tier: {self.retrieval_tier}\n"
                    f"Restore Duration: {self.restore_days} days\n"
                    f"Expires At: {expires_at.isoformat()}\n\n"
                    f"[bold]Next steps:[/bold]\n"
                    f"  - Check status: [yellow]deepfreeze thaw --request-id {request_id}[/yellow]\n"
                    f"  - Depending on retrieval tier, data should be available in:\n"
                    f"    - Expedited: 1-5 minutes\n"
                    f"    - Standard: 3-5 hours\n"
                    f"    - Bulk: 5-12 hours",
                    title="[bold green]Thaw Request Created[/bold green]",
                    border_style="green",
                    expand=False,
                )
            )

        return request_id

    def _wait_for_completion(self, request_id: str) -> None:
        """
        Wait synchronously for the thaw operation to complete.

        :param request_id: The thaw request ID
        """
        self.loggit.info("Waiting for thaw request %s to complete", request_id)

        if self.porcelain:
            # Simple polling for porcelain mode
            while True:
                request = self._check_request_status(request_id)
                if request and request.get("status") in [
                    THAW_STATUS_COMPLETED,
                    THAW_STATUS_FAILED,
                ]:
                    print(f"COMPLETE\t{request_id}\t{request.get('status')}")
                    break
                time.sleep(60)  # Check every minute
            return

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=self.console,
        ) as progress:
            task = progress.add_task(
                f"Waiting for thaw request {request_id}...", total=None
            )

            check_count = 0
            while True:
                request = self._check_request_status(request_id)

                if not request:
                    progress.update(task, description="[red]Request not found[/red]")
                    break

                status = request.get("status")

                if status == THAW_STATUS_COMPLETED:
                    progress.update(
                        task, description="[green]Thaw completed successfully![/green]"
                    )
                    break
                elif status == THAW_STATUS_FAILED:
                    progress.update(task, description="[red]Thaw failed![/red]")
                    break

                # Update progress description with restore status
                restore_statuses = request.get("restore_statuses", [])
                total_restored = sum(rs["restored"] for rs in restore_statuses)
                total_objects = sum(rs["total"] for rs in restore_statuses)
                in_progress = sum(rs["in_progress"] for rs in restore_statuses)

                progress.update(
                    task,
                    description=f"Restoring... {total_restored}/{total_objects} objects ({in_progress} in progress)",
                )

                check_count += 1
                time.sleep(60)  # Check every minute

        # Display final status
        if request:
            self._display_request_status(request)

    def do_dry_run(self) -> None:
        """
        Perform a dry-run of the thaw operation.

        :return: None
        :rtype: None
        """
        self.loggit.info("DRY-RUN MODE.  No changes will be made.")

        try:
            self._load_settings()

            if self.list_requests:
                self._list_all_requests()
            elif self.request_id:
                request = self._check_request_status(self.request_id)
                if request:
                    self._display_request_status(request)
            elif self.start_date and self.end_date:
                self._initiate_thaw(dry_run=True)
            else:
                if self.porcelain:
                    print("ERROR\tmissing_parameters\tProvide date range or request ID")
                else:
                    self.console.print(
                        "[red]Error: Provide either --start-date/--end-date or --request-id[/red]"
                    )

        except (MissingIndexError, MissingSettingsError) as e:
            if self.porcelain:
                print(f"ERROR\t{type(e).__name__}\t{str(e)}")
            else:
                self.console.print(f"[red]Error: {e}[/red]")
            raise

    def do_action(self) -> None:
        """
        Perform the thaw operation.

        :return: None
        :rtype: None
        """
        self.loggit.debug("Starting Thaw action")

        try:
            self._load_settings()

            if self.list_requests:
                self._list_all_requests()
            elif self.request_id:
                request = self._check_request_status(self.request_id)
                if request:
                    self._display_request_status(request)

                    # If sync mode and still in progress, wait
                    if self.sync and request.get("status") == THAW_STATUS_IN_PROGRESS:
                        self._wait_for_completion(self.request_id)

            elif self.start_date and self.end_date:
                request_id = self._initiate_thaw()

                # If sync mode, wait for completion
                if request_id and self.sync:
                    self._wait_for_completion(request_id)

            else:
                if self.porcelain:
                    print("ERROR\tmissing_parameters\tProvide date range or request ID")
                else:
                    self.console.print(
                        Panel(
                            "[bold]Missing required parameters[/bold]\n\n"
                            "Provide either:\n"
                            "  - [yellow]--start-date[/yellow] and [yellow]--end-date[/yellow] to initiate a new thaw\n"
                            "  - [yellow]--request-id[/yellow] to check status of an existing request\n"
                            "  - [yellow]--list[/yellow] to list all thaw requests",
                            title="[bold red]Invalid Parameters[/bold red]",
                            border_style="red",
                            expand=False,
                        )
                    )

        except (MissingIndexError, MissingSettingsError) as e:
            if self.porcelain:
                print(f"ERROR\t{type(e).__name__}\t{str(e)}")
            else:
                self.console.print(
                    Panel(
                        f"[bold]Deepfreeze is not initialized[/bold]\n\n"
                        f"Error: {escape(str(e))}\n\n"
                        f"Run [yellow]deepfreeze setup[/yellow] first.",
                        title="[bold red]Thaw Failed[/bold red]",
                        border_style="red",
                        expand=False,
                    )
                )
            raise

        except Exception as e:
            if self.porcelain:
                print(f"ERROR\tunexpected\t{str(e)}")
            else:
                self.console.print(
                    Panel(
                        f"[bold]Thaw operation failed[/bold]\n\n"
                        f"Error: {escape(str(e))}\n\n"
                        f"Check logs for details.",
                        title="[bold red]Thaw Error[/bold red]",
                        border_style="red",
                        expand=False,
                    )
                )
            self.loggit.error("Thaw failed: %s", e, exc_info=True)
            raise
