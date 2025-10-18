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
    :param thaw_request_id: The ID of the thaw request to refreeze
    :type thaw_request_id: str

    :methods:
        do_action: Perform the refreeze operation.
        do_dry_run: Perform a dry-run of the refreeze operation.
        do_singleton_action: Entry point for singleton CLI execution.
    """

    def __init__(self, client: Elasticsearch, thaw_request_id: str) -> None:
        self.loggit = logging.getLogger("curator.actions.deepfreeze")
        self.loggit.debug("Initializing Deepfreeze Refreeze")

        if not thaw_request_id:
            raise ValueError("thaw_request_id is required")

        self.client = client
        self.thaw_request_id = thaw_request_id
        self.settings = get_settings(client)

        self.loggit.info("Deepfreeze Refreeze initialized for request %s", thaw_request_id)

    def do_action(self) -> None:
        """
        Unmount repositories from a thaw request and reset them to frozen state.

        :return: None
        :rtype: None
        """
        self.loggit.info("Refreezing thaw request %s", self.thaw_request_id)

        # Get the thaw request
        try:
            request = get_thaw_request(self.client, self.thaw_request_id)
        except Exception as e:
            self.loggit.error("Failed to get thaw request %s: %s", self.thaw_request_id, e)
            rprint(f"[red]Error: Could not find thaw request '{self.thaw_request_id}'[/red]")
            return

        # Get the repositories from the request
        repo_names = request.get("repos", [])
        if not repo_names:
            self.loggit.warning("No repositories found in thaw request %s", self.thaw_request_id)
            rprint(f"[yellow]Warning: No repositories found in thaw request '{self.thaw_request_id}'[/yellow]")
            return

        self.loggit.info("Found %d repositories to refreeze", len(repo_names))

        # Get the repository objects
        try:
            repos = get_repositories_by_names(self.client, repo_names)
        except Exception as e:
            self.loggit.error("Failed to get repositories: %s", e)
            rprint(f"[red]Error: Failed to get repositories: {e}[/red]")
            return

        if not repos:
            self.loggit.warning("No repository objects found for names: %s", repo_names)
            rprint(f"[yellow]Warning: No repository objects found[/yellow]")
            return

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
                id=self.thaw_request_id,
                body={"doc": {"status": "completed"}}
            )
            self.loggit.info("Thaw request %s marked as completed", self.thaw_request_id)
        except Exception as e:
            self.loggit.error("Failed to update thaw request status: %s", e)

        # Report results
        rprint(f"\n[green]Refreeze completed for thaw request '{self.thaw_request_id}'[/green]")
        rprint(f"[cyan]Processed {len(repos)} repositories[/cyan]")
        if unmounted:
            rprint(f"[cyan]Unmounted {len(unmounted)} repositories[/cyan]")
        if failed:
            rprint(f"[red]Failed to process {len(failed)} repositories: {', '.join(failed)}[/red]")

    def do_dry_run(self) -> None:
        """
        Perform a dry-run of the refreeze operation.
        Shows which repositories would be unmounted and reset.

        :return: None
        :rtype: None
        """
        self.loggit.info("DRY-RUN: Refreezing thaw request %s", self.thaw_request_id)

        # Get the thaw request
        try:
            request = get_thaw_request(self.client, self.thaw_request_id)
        except Exception as e:
            self.loggit.error("DRY-RUN: Failed to get thaw request %s: %s", self.thaw_request_id, e)
            rprint(f"[red]DRY-RUN: Could not find thaw request '{self.thaw_request_id}'[/red]")
            return

        # Get the repositories from the request
        repo_names = request.get("repos", [])
        if not repo_names:
            self.loggit.warning("DRY-RUN: No repositories found in thaw request %s", self.thaw_request_id)
            rprint(f"[yellow]DRY-RUN: No repositories found in thaw request '{self.thaw_request_id}'[/yellow]")
            return

        self.loggit.info("DRY-RUN: Found %d repositories to refreeze", len(repo_names))

        # Get the repository objects
        try:
            repos = get_repositories_by_names(self.client, repo_names)
        except Exception as e:
            self.loggit.error("DRY-RUN: Failed to get repositories: %s", e)
            rprint(f"[red]DRY-RUN: Failed to get repositories: {e}[/red]")
            return

        if not repos:
            self.loggit.warning("DRY-RUN: No repository objects found for names: %s", repo_names)
            rprint(f"[yellow]DRY-RUN: No repository objects found[/yellow]")
            return

        rprint(f"\n[cyan]DRY-RUN: Would refreeze thaw request '{self.thaw_request_id}'[/cyan]")
        rprint(f"[cyan]DRY-RUN: Would process {len(repos)} repositories:[/cyan]\n")

        # Show what would be done for each repository
        for repo in repos:
            action = "unmount and reset to frozen" if repo.is_mounted else "reset to frozen"
            rprint(f"  [cyan]- {repo.name}[/cyan] (state: {repo.thaw_state}, mounted: {repo.is_mounted})")
            rprint(f"    [dim]Would {action}[/dim]")

        rprint(f"\n[cyan]DRY-RUN: Would mark thaw request '{self.thaw_request_id}' as completed[/cyan]\n")

    def do_singleton_action(self) -> None:
        """
        Entry point for singleton CLI execution.

        :return: None
        :rtype: None
        """
        self.do_action()
