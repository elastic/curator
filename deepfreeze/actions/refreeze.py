"""Refreeze action for deepfreeze"""

# pylint: disable=too-many-arguments,too-many-instance-attributes, raise-missing-from

import logging

from elasticsearch8 import Elasticsearch
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.markup import escape

from deepfreeze.constants import (
    STATUS_INDEX,
    THAW_STATE_THAWED,
    THAW_STATUS_COMPLETED,
    THAW_STATUS_REFROZEN,
)
from deepfreeze.exceptions import ActionError, MissingIndexError, MissingSettingsError
from deepfreeze.helpers import Repository
from deepfreeze.s3client import s3_client_factory
from deepfreeze.utilities import (
    get_all_indices_in_repo,
    get_repositories_by_names,
    get_settings,
    get_thaw_request,
    list_thaw_requests,
    unmount_repo,
    update_thaw_request,
)


class Refreeze:
    """
    Refreeze unmounts thawed repositories and returns them to frozen state.

    Supports multiple operation modes:
    - Request ID: Refreeze repositories from a specific thaw request
    - All: Refreeze all completed thaw requests

    :param client: A client connection object
    :param request_id: ID of a specific thaw request to refreeze (optional)
    :param all_requests: If True, refreeze all completed thaw requests

    :methods:
        do_dry_run: Perform a dry-run of the refreeze operation
        do_action: Perform the refreeze operation

    :example:
        >>> from deepfreeze.actions import Refreeze
        >>> refreeze = Refreeze(client, request_id="abc12345")
        >>> refreeze.do_action()
    """

    def __init__(
        self,
        client: Elasticsearch,
        request_id: str = None,
        all_requests: bool = False,
        porcelain: bool = False,
    ) -> None:
        self.loggit = logging.getLogger("deepfreeze.actions.refreeze")
        self.loggit.debug("Initializing Deepfreeze Refreeze")

        # Console for STDERR output
        self.console = Console(stderr=True)

        self.client = client
        self.request_id = request_id
        self.all_requests = all_requests
        self.porcelain = porcelain

        # Will be loaded during action
        self.settings = None
        self.s3 = None

        self.loggit.debug("Deepfreeze Refreeze initialized")

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

    def _delete_mounted_indices(self, repo: Repository) -> list:
        """
        Delete indices that were mounted from a repository.

        :param repo: The repository to delete indices from
        :return: List of deleted index names
        """
        deleted_indices = []

        try:
            # Get all indices in the repository
            indices = get_all_indices_in_repo(self.client, repo.name)

            for index_name in indices:
                # Check if index exists (might be mounted with different name patterns)
                for pattern in [index_name, f"partial-{index_name}", f"restored-{index_name}"]:
                    if self.client.indices.exists(index=pattern):
                        self.loggit.info("Deleting mounted index %s", pattern)
                        try:
                            self.client.indices.delete(index=pattern)
                            deleted_indices.append(pattern)
                        except Exception as e:
                            self.loggit.warning(
                                "Failed to delete index %s: %s", pattern, e
                            )

        except Exception as e:
            self.loggit.warning(
                "Could not get indices for repository %s: %s", repo.name, e
            )

        return deleted_indices

    def _refreeze_repository(self, repo: Repository, dry_run: bool = False) -> dict:
        """
        Refreeze a single repository.

        :param repo: The repository to refreeze
        :param dry_run: If True, don't actually refreeze
        :return: Result dictionary with status
        """
        result = {
            "repo": repo.name,
            "success": False,
            "deleted_indices": [],
            "error": None,
        }

        if dry_run:
            result["success"] = True
            return result

        try:
            # Delete mounted indices
            deleted_indices = self._delete_mounted_indices(repo)
            result["deleted_indices"] = deleted_indices

            # Unmount the repository
            self.loggit.info("Unmounting repository %s", repo.name)
            unmounted_repo = unmount_repo(self.client, repo.name)

            # Reset to frozen state
            unmounted_repo.reset_to_frozen()
            unmounted_repo.persist(self.client)

            result["success"] = True
            self.loggit.info(
                "Successfully refroze repository %s (deleted %d indices)",
                repo.name,
                len(deleted_indices),
            )

        except Exception as e:
            result["error"] = str(e)
            self.loggit.error("Failed to refreeze repository %s: %s", repo.name, e)

        return result

    def _refreeze_request(self, request_id: str, dry_run: bool = False) -> dict:
        """
        Refreeze all repositories from a thaw request.

        :param request_id: The thaw request ID
        :param dry_run: If True, don't actually refreeze
        :return: Result dictionary with status
        """
        try:
            request = get_thaw_request(self.client, request_id)
        except ActionError as e:
            return {"error": f"Request not found: {request_id}", "results": []}

        status = request.get("status")
        if status != THAW_STATUS_COMPLETED:
            if status == THAW_STATUS_REFROZEN:
                return {
                    "error": f"Request {request_id} has already been refrozen",
                    "results": [],
                }
            else:
                return {
                    "error": f"Cannot refreeze request {request_id} with status '{status}' (must be 'completed')",
                    "results": [],
                }

        repos = request.get("repos", [])
        repo_objs = get_repositories_by_names(self.client, repos)

        results = []
        for repo in repo_objs:
            result = self._refreeze_repository(repo, dry_run)
            results.append(result)

        # Update request status
        if not dry_run:
            all_success = all(r["success"] for r in results)
            if all_success:
                update_thaw_request(self.client, request_id, status=THAW_STATUS_REFROZEN)

        return {"request_id": request_id, "results": results}

    def _get_completed_requests(self) -> list:
        """Get all completed thaw requests."""
        all_requests = list_thaw_requests(self.client)
        return [r for r in all_requests if r.get("status") == THAW_STATUS_COMPLETED]

    def do_dry_run(self) -> None:
        """
        Perform a dry-run of the refreeze operation.

        :return: None
        :rtype: None
        """
        self.loggit.info("DRY-RUN MODE.  No changes will be made.")

        try:
            self._load_settings()

            if self.request_id:
                # Refreeze specific request
                result = self._refreeze_request(self.request_id, dry_run=True)

                if result.get("error"):
                    if self.porcelain:
                        print(f"ERROR\t{result['error']}")
                    else:
                        self.console.print(f"[red]Error: {result['error']}[/red]")
                    return

                if self.porcelain:
                    print(f"DRY_RUN\trefreeze_request\t{self.request_id}")
                    for r in result.get("results", []):
                        print(f"DRY_RUN\trefreeze_repo\t{r['repo']}")
                else:
                    repos = [r["repo"] for r in result.get("results", [])]
                    repo_list = "\n".join([f"  - [cyan]{r}[/cyan]" for r in repos])
                    self.console.print(
                        Panel(
                            f"[bold]Would refreeze request [cyan]{self.request_id}[/cyan][/bold]\n\n"
                            f"Repositories to refreeze:\n{repo_list}",
                            title="[bold blue]Dry Run - Refreeze[/bold blue]",
                            border_style="blue",
                            expand=False,
                        )
                    )

            elif self.all_requests:
                # Refreeze all completed requests
                completed = self._get_completed_requests()

                if not completed:
                    if self.porcelain:
                        print("INFO\tno_completed_requests")
                    else:
                        self.console.print("[dim]No completed thaw requests to refreeze[/dim]")
                    return

                if self.porcelain:
                    for req in completed:
                        print(f"DRY_RUN\trefreeze_request\t{req.get('request_id')}")
                else:
                    table = Table(title="Requests to Refreeze")
                    table.add_column("Request ID", style="cyan")
                    table.add_column("Date Range", style="white")
                    table.add_column("Repositories", style="yellow")

                    for req in completed:
                        date_range = ""
                        if req.get("start_date") and req.get("end_date"):
                            date_range = f"{req['start_date'][:10]} - {req['end_date'][:10]}"

                        repos_str = ", ".join(req.get("repos", [])[:3])
                        if len(req.get("repos", [])) > 3:
                            repos_str += f" (+{len(req['repos']) - 3} more)"

                        table.add_row(
                            req.get("request_id", "N/A"),
                            date_range,
                            repos_str,
                        )

                    self.console.print(table)
                    self.console.print(
                        f"\n[bold]Would refreeze {len(completed)} completed requests[/bold]"
                    )

            else:
                if self.porcelain:
                    print("ERROR\tmissing_parameters\tProvide --request-id or --all")
                else:
                    self.console.print(
                        "[red]Error: Provide either --request-id or --all[/red]"
                    )

        except (MissingIndexError, MissingSettingsError) as e:
            if self.porcelain:
                print(f"ERROR\t{type(e).__name__}\t{str(e)}")
            else:
                self.console.print(f"[red]Error: {e}[/red]")
            raise

    def do_action(self) -> None:
        """
        Perform the refreeze operation.

        :return: None
        :rtype: None
        """
        self.loggit.debug("Starting Refreeze action")

        try:
            self._load_settings()

            if self.request_id:
                # Refreeze specific request
                result = self._refreeze_request(self.request_id)

                if result.get("error"):
                    if self.porcelain:
                        print(f"ERROR\t{result['error']}")
                    else:
                        self.console.print(
                            Panel(
                                f"[bold]{escape(result['error'])}[/bold]",
                                title="[bold red]Refreeze Failed[/bold red]",
                                border_style="red",
                                expand=False,
                            )
                        )
                    return

                # Display results
                results = result.get("results", [])
                successful = [r for r in results if r["success"]]
                failed = [r for r in results if not r["success"]]

                if self.porcelain:
                    for r in results:
                        status = "SUCCESS" if r["success"] else "FAILED"
                        print(f"{status}\t{r['repo']}\t{len(r.get('deleted_indices', []))} indices")
                else:
                    if successful:
                        success_list = "\n".join(
                            [f"  - [green]{r['repo']}[/green] ({len(r.get('deleted_indices', []))} indices deleted)"
                             for r in successful]
                        )
                        self.console.print(
                            Panel(
                                f"[bold]Successfully refrozen {len(successful)} repositories:[/bold]\n{success_list}",
                                title="[bold green]Refreeze Successful[/bold green]",
                                border_style="green",
                                expand=False,
                            )
                        )

                    if failed:
                        fail_list = "\n".join(
                            [f"  - [red]{r['repo']}[/red]: {r.get('error', 'Unknown error')}"
                             for r in failed]
                        )
                        self.console.print(
                            Panel(
                                f"[bold]Failed to refreeze {len(failed)} repositories:[/bold]\n{fail_list}",
                                title="[bold red]Refreeze Failures[/bold red]",
                                border_style="red",
                                expand=False,
                            )
                        )

            elif self.all_requests:
                # Refreeze all completed requests
                completed = self._get_completed_requests()

                if not completed:
                    if self.porcelain:
                        print("INFO\tno_completed_requests")
                    else:
                        self.console.print(
                            "[dim]No completed thaw requests to refreeze[/dim]"
                        )
                    return

                all_results = []
                for req in completed:
                    req_id = req.get("request_id", req.get("id"))
                    if self.porcelain:
                        print(f"PROCESSING\t{req_id}")
                    else:
                        self.console.print(f"Processing request [cyan]{req_id}[/cyan]...")

                    result = self._refreeze_request(req_id)
                    all_results.append(result)

                # Summary
                total_repos = sum(len(r.get("results", [])) for r in all_results)
                successful_repos = sum(
                    len([res for res in r.get("results", []) if res["success"]])
                    for r in all_results
                )

                if self.porcelain:
                    print(f"COMPLETE\t{len(completed)} requests\t{successful_repos}/{total_repos} repos")
                else:
                    self.console.print(
                        Panel(
                            f"[bold green]Refreeze complete![/bold green]\n\n"
                            f"Requests processed: {len(completed)}\n"
                            f"Repositories refrozen: {successful_repos}/{total_repos}",
                            title="[bold green]Refreeze Complete[/bold green]",
                            border_style="green",
                            expand=False,
                        )
                    )

            else:
                if self.porcelain:
                    print("ERROR\tmissing_parameters\tProvide --request-id or --all")
                else:
                    self.console.print(
                        Panel(
                            "[bold]Missing required parameters[/bold]\n\n"
                            "Provide either:\n"
                            "  - [yellow]--request-id[/yellow] to refreeze a specific thaw request\n"
                            "  - [yellow]--all[/yellow] to refreeze all completed thaw requests",
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
                        title="[bold red]Refreeze Failed[/bold red]",
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
                        f"[bold]Refreeze operation failed[/bold]\n\n"
                        f"Error: {escape(str(e))}\n\n"
                        f"Check logs for details.",
                        title="[bold red]Refreeze Error[/bold red]",
                        border_style="red",
                        expand=False,
                    )
                )
            self.loggit.error("Refreeze failed: %s", e, exc_info=True)
            raise
