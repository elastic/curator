"""Refreeze action for deepfreeze"""

# pylint: disable=too-many-arguments,too-many-instance-attributes, raise-missing-from

import logging

from elasticsearch import Elasticsearch
from rich import print as rprint

from curator.actions.deepfreeze.constants import STATUS_INDEX
from curator.actions.deepfreeze.utilities import (
    get_repositories_by_names,
    get_settings,
    get_thaw_request,
    list_thaw_requests,
)


class Refreeze:
    """
    The Refreeze action is a user-initiated operation to signal "I'm done with this thaw."
    It unmounts repositories that were previously thawed and resets them back to frozen state.

    Unlike the automatic Cleanup action which processes expired repositories on a schedule,
    Refreeze is explicitly invoked by users when they're finished accessing thawed data,
    even if the S3 restore hasn't expired yet.

    When you thaw from AWS Glacier, you get a temporary restored copy that exists for a
    specified duration. After that expires, AWS automatically removes the temporary copy -
    the original Glacier object never moved. Refreeze doesn't push anything back; it's about
    unmounting the repositories and resetting state.

    :param client: A client connection object
    :type client: Elasticsearch
    :param thaw_request_id: The ID of the thaw request to refreeze (optional - if None, all open requests)
    :type thaw_request_id: str

    :methods:
        do_action: Perform the refreeze operation.
        do_dry_run: Perform a dry-run of the refreeze operation.
        do_singleton_action: Entry point for singleton CLI execution.
    """

    def __init__(self, client: Elasticsearch, thaw_request_id: str = None, porcelain: bool = False) -> None:
        self.loggit = logging.getLogger("curator.actions.deepfreeze")
        self.loggit.debug("Initializing Deepfreeze Refreeze")

        self.client = client
        self.thaw_request_id = thaw_request_id
        self.porcelain = porcelain
        self.settings = get_settings(client)

        if thaw_request_id:
            self.loggit.info("Deepfreeze Refreeze initialized for request %s", thaw_request_id)
        else:
            self.loggit.info("Deepfreeze Refreeze initialized for all open requests")

    def _get_open_thaw_requests(self) -> list:
        """
        Get all open (in_progress) thaw requests.

        :return: List of thaw request dicts
        :rtype: list
        """
        all_requests = list_thaw_requests(self.client)
        return [req for req in all_requests if req.get("status") == "in_progress"]

    def _confirm_bulk_refreeze(self, requests: list) -> bool:
        """
        Display a list of thaw requests and get user confirmation to proceed.
        In porcelain mode, automatically returns True (no interactive confirmation).

        :param requests: List of thaw request dicts
        :type requests: list

        :return: True if user confirms (or in porcelain mode), False otherwise
        :rtype: bool
        """
        # In porcelain mode, skip confirmation and just proceed
        if self.porcelain:
            return True

        rprint(f"\n[bold yellow]WARNING: This will refreeze {len(requests)} open thaw request(s)[/bold yellow]\n")

        # Show the requests
        for req in requests:
            request_id = req.get("id")
            repo_count = len(req.get("repos", []))
            created_at = req.get("created_at", "Unknown")
            start_date = req.get("start_date", "--")
            end_date = req.get("end_date", "--")

            rprint(f"  [cyan]• {request_id}[/cyan]")
            rprint(f"    [dim]Created: {created_at}[/dim]")
            rprint(f"    [dim]Date Range: {start_date} to {end_date}[/dim]")
            rprint(f"    [dim]Repositories: {repo_count}[/dim]\n")

        # Get confirmation
        try:
            response = input("Do you want to proceed with refreezing all these requests? [y/N]: ").strip().lower()
            return response in ['y', 'yes']
        except (EOFError, KeyboardInterrupt):
            rprint("\n[yellow]Operation cancelled by user[/yellow]")
            return False

    def _refreeze_single_request(self, request_id: str) -> tuple[list, list]:
        """
        Refreeze a single thaw request.

        :param request_id: The thaw request ID
        :type request_id: str

        :return: Tuple of (unmounted_repos, failed_repos)
        :rtype: tuple[list, list]
        """
        self.loggit.info("Refreezing thaw request %s", request_id)

        # Get the thaw request
        try:
            request = get_thaw_request(self.client, request_id)
        except Exception as e:
            self.loggit.error("Failed to get thaw request %s: %s", request_id, e)
            if self.porcelain:
                print(f"ERROR\trequest_not_found\t{request_id}\t{str(e)}")
            else:
                rprint(f"[red]Error: Could not find thaw request '{request_id}'[/red]")
            return [], []

        # Get the repositories from the request
        repo_names = request.get("repos", [])
        if not repo_names:
            self.loggit.warning("No repositories found in thaw request %s", request_id)
            return [], []

        self.loggit.info("Found %d repositories to refreeze", len(repo_names))

        # Get the repository objects
        try:
            repos = get_repositories_by_names(self.client, repo_names)
        except Exception as e:
            self.loggit.error("Failed to get repositories: %s", e)
            return [], []

        if not repos:
            self.loggit.warning("No repository objects found for names: %s", repo_names)
            return [], []

        # Track success/failure
        unmounted = []
        failed = []

        # Process each repository
        for repo in repos:
            self.loggit.info("Processing repository %s (state: %s, mounted: %s)",
                           repo.name, repo.thaw_state, repo.is_mounted)

            try:
                # Unmount if still mounted
                if repo.is_mounted:
                    try:
                        self.client.snapshot.delete_repository(name=repo.name)
                        self.loggit.info("Unmounted repository %s", repo.name)
                        unmounted.append(repo.name)
                    except Exception as e:
                        # If it's already unmounted, that's okay
                        if "repository_missing_exception" in str(e).lower():
                            self.loggit.debug("Repository %s was already unmounted", repo.name)
                        else:
                            self.loggit.warning("Failed to unmount repository %s: %s", repo.name, e)
                            # Continue anyway to update the state
                else:
                    self.loggit.debug("Repository %s was not mounted", repo.name)

                # Reset to frozen state
                repo.reset_to_frozen()
                repo.persist(self.client)
                self.loggit.info("Repository %s reset to frozen state", repo.name)

            except Exception as e:
                self.loggit.error("Error processing repository %s: %s", repo.name, e)
                failed.append(repo.name)

        # Update the thaw request status to completed
        try:
            self.client.update(
                index=STATUS_INDEX,
                id=request_id,
                body={"doc": {"status": "completed"}}
            )
            self.loggit.info("Thaw request %s marked as completed", request_id)
        except Exception as e:
            self.loggit.error("Failed to update thaw request status: %s", e)

        return unmounted, failed

    def do_action(self) -> None:
        """
        Unmount repositories from thaw request(s) and reset them to frozen state.

        If thaw_request_id is provided, refreeze that specific request.
        If thaw_request_id is None, refreeze all open requests (with confirmation).

        :return: None
        :rtype: None
        """
        # Determine which requests to process
        if self.thaw_request_id:
            # Single request mode
            request_ids = [self.thaw_request_id]
        else:
            # Bulk mode - get all open requests
            open_requests = self._get_open_thaw_requests()

            if not open_requests:
                if not self.porcelain:
                    rprint("[yellow]No open thaw requests found to refreeze[/yellow]")
                return

            # Get confirmation
            if not self._confirm_bulk_refreeze(open_requests):
                if not self.porcelain:
                    rprint("[yellow]Refreeze operation cancelled[/yellow]")
                return

            request_ids = [req.get("id") for req in open_requests]

        # Process each request
        total_unmounted = []
        total_failed = []

        for request_id in request_ids:
            unmounted, failed = self._refreeze_single_request(request_id)
            total_unmounted.extend(unmounted)
            total_failed.extend(failed)

        # Report results
        if self.porcelain:
            # Machine-readable output: tab-separated values
            for repo_name in total_unmounted:
                print(f"UNMOUNTED\t{repo_name}")
            for repo_name in total_failed:
                print(f"FAILED\t{repo_name}")
            print(f"SUMMARY\t{len(total_unmounted)}\t{len(total_failed)}\t{len(request_ids)}")
        else:
            if len(request_ids) == 1:
                rprint(f"\n[green]Refreeze completed for thaw request '{request_ids[0]}'[/green]")
            else:
                rprint(f"\n[green]Refreeze completed for {len(request_ids)} thaw requests[/green]")

            rprint(f"[cyan]Unmounted {len(total_unmounted)} repositories[/cyan]")
            if total_failed:
                rprint(f"[red]Failed to process {len(total_failed)} repositories: {', '.join(total_failed)}[/red]")

    def do_dry_run(self) -> None:
        """
        Perform a dry-run of the refreeze operation.
        Shows which repositories would be unmounted and reset.

        If thaw_request_id is provided, show dry-run for that specific request.
        If thaw_request_id is None, show dry-run for all open requests.

        :return: None
        :rtype: None
        """
        # Determine which requests to process
        if self.thaw_request_id:
            # Single request mode
            request_ids = [self.thaw_request_id]
            if not self.porcelain:
                rprint(f"\n[cyan]DRY-RUN: Would refreeze thaw request '{self.thaw_request_id}'[/cyan]\n")
        else:
            # Bulk mode - get all open requests
            open_requests = self._get_open_thaw_requests()

            if not open_requests:
                if not self.porcelain:
                    rprint("[yellow]DRY-RUN: No open thaw requests found to refreeze[/yellow]")
                return

            if not self.porcelain:
                rprint(f"\n[cyan]DRY-RUN: Would refreeze {len(open_requests)} open thaw requests:[/cyan]\n")

                # Show the requests
                for req in open_requests:
                    request_id = req.get("id")
                    repo_count = len(req.get("repos", []))
                    created_at = req.get("created_at", "Unknown")
                    start_date = req.get("start_date", "--")
                    end_date = req.get("end_date", "--")

                    rprint(f"  [cyan]• {request_id}[/cyan]")
                    rprint(f"    [dim]Created: {created_at}[/dim]")
                    rprint(f"    [dim]Date Range: {start_date} to {end_date}[/dim]")
                    rprint(f"    [dim]Repositories: {repo_count}[/dim]\n")

            request_ids = [req.get("id") for req in open_requests]

        # Process each request in dry-run mode
        total_repos = 0
        for request_id in request_ids:
            try:
                request = get_thaw_request(self.client, request_id)
            except Exception as e:
                self.loggit.error("DRY-RUN: Failed to get thaw request %s: %s", request_id, e)
                if self.porcelain:
                    print(f"ERROR\tdry_run_request_not_found\t{request_id}\t{str(e)}")
                else:
                    rprint(f"[red]DRY-RUN: Could not find thaw request '{request_id}'[/red]")
                continue

            repo_names = request.get("repos", [])
            if not repo_names:
                continue

            try:
                repos = get_repositories_by_names(self.client, repo_names)
            except Exception as e:
                self.loggit.error("DRY-RUN: Failed to get repositories for request %s: %s", request_id, e)
                continue

            if not repos:
                continue

            # Show details if single request, or summary if bulk
            if self.porcelain:
                # Machine-readable output
                for repo in repos:
                    action = "unmount_and_reset" if repo.is_mounted else "reset"
                    print(f"DRY_RUN\t{repo.name}\t{repo.thaw_state}\t{repo.is_mounted}\t{action}")
            else:
                if len(request_ids) == 1:
                    rprint(f"[cyan]Would process {len(repos)} repositories:[/cyan]\n")
                    for repo in repos:
                        action = "unmount and reset to frozen" if repo.is_mounted else "reset to frozen"
                        rprint(f"  [cyan]- {repo.name}[/cyan] (state: {repo.thaw_state}, mounted: {repo.is_mounted})")
                        rprint(f"    [dim]Would {action}[/dim]")
                    rprint(f"\n[cyan]DRY-RUN: Would mark thaw request '{request_id}' as completed[/cyan]\n")

            total_repos += len(repos)

        # Summary for bulk mode
        if len(request_ids) > 1 and not self.porcelain:
            rprint(f"[cyan]DRY-RUN: Would process {total_repos} total repositories across {len(request_ids)} thaw requests[/cyan]")
            rprint(f"[cyan]DRY-RUN: Would mark {len(request_ids)} thaw requests as completed[/cyan]\n")

        # Porcelain mode summary
        if self.porcelain:
            print(f"DRY_RUN_SUMMARY\t{total_repos}\t{len(request_ids)}")

    def do_singleton_action(self) -> None:
        """
        Entry point for singleton CLI execution.

        :return: None
        :rtype: None
        """
        self.do_action()
