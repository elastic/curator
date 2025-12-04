"""Cleanup action for deepfreeze"""

# pylint: disable=too-many-arguments,too-many-instance-attributes, raise-missing-from

import logging
from datetime import datetime, timedelta, timezone

from elasticsearch8 import Elasticsearch, NotFoundError
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.markup import escape

from deepfreeze.constants import (
    STATUS_INDEX,
    THAW_STATE_EXPIRED,
    THAW_STATUS_COMPLETED,
    THAW_STATUS_FAILED,
    THAW_STATUS_REFROZEN,
)
from deepfreeze.exceptions import ActionError, MissingIndexError, MissingSettingsError
from deepfreeze.s3client import s3_client_factory
from deepfreeze.utilities import (
    get_matching_repos,
    get_settings,
    is_policy_safe_to_delete,
    list_thaw_requests,
    unmount_repo,
)


class Cleanup:
    """
    Cleanup removes expired repositories, old thaw requests, and orphaned ILM policies.

    :param client: A client connection object
    :param dry_run_mode: If True, show what would be cleaned up without making changes

    :methods:
        do_dry_run: Perform a dry-run showing what would be cleaned up
        do_action: Perform the cleanup operation

    :example:
        >>> from deepfreeze.actions import Cleanup
        >>> cleanup = Cleanup(client)
        >>> cleanup.do_action()
    """

    def __init__(
        self,
        client: Elasticsearch,
        porcelain: bool = False,
    ) -> None:
        self.loggit = logging.getLogger("deepfreeze.actions.cleanup")
        self.loggit.debug("Initializing Deepfreeze Cleanup")

        # Console for STDERR output
        self.console = Console(stderr=True)

        self.client = client
        self.porcelain = porcelain

        # Will be loaded during action
        self.settings = None
        self.s3 = None

        self.loggit.debug("Deepfreeze Cleanup initialized")

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

    def _find_expired_repos(self) -> list:
        """
        Find repositories that have expired (S3 restore has expired).

        :return: List of expired repository objects
        """
        expired = []

        # Get all repos from status index
        repos = get_matching_repos(self.client, self.settings.repo_name_prefix)

        now = datetime.now(timezone.utc)

        for repo in repos:
            # Check if repo has an expiration time and it's in the past
            if repo.thaw_state == THAW_STATE_EXPIRED:
                expired.append(repo)
            elif repo.expires_at and repo.expires_at < now:
                # Mark as expired
                repo.mark_expired()
                expired.append(repo)

        self.loggit.debug("Found %d expired repositories", len(expired))
        return expired

    def _find_old_thaw_requests(self) -> list:
        """
        Find thaw requests that are old enough to be cleaned up based on retention settings.

        :return: List of request dictionaries to delete
        """
        to_delete = []

        requests = list_thaw_requests(self.client)
        now = datetime.now(timezone.utc)

        for req in requests:
            status = req.get("status")
            created_at = req.get("created_at")

            if not created_at:
                continue

            # Parse created_at
            if isinstance(created_at, str):
                created_dt = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
            else:
                created_dt = created_at

            age_days = (now - created_dt).days

            # Check retention based on status
            should_delete = False
            retention_days = 0

            if status == THAW_STATUS_COMPLETED:
                retention_days = self.settings.thaw_request_retention_days_completed
                should_delete = age_days > retention_days
            elif status == THAW_STATUS_FAILED:
                retention_days = self.settings.thaw_request_retention_days_failed
                should_delete = age_days > retention_days
            elif status == THAW_STATUS_REFROZEN:
                retention_days = self.settings.thaw_request_retention_days_refrozen
                should_delete = age_days > retention_days

            if should_delete:
                to_delete.append(
                    {
                        "request": req,
                        "age_days": age_days,
                        "retention_days": retention_days,
                    }
                )

        self.loggit.debug("Found %d thaw requests to clean up", len(to_delete))
        return to_delete

    def _find_orphaned_policies(self) -> list:
        """
        Find ILM policies that reference non-existent repositories and are not in use.

        :return: List of policy names to delete
        """
        orphaned = []

        try:
            all_policies = self.client.ilm.get_lifecycle()

            # Get all existing repos
            existing_repos = set()
            try:
                es_repos = self.client.snapshot.get_repository(name="_all")
                existing_repos = set(es_repos.keys())
            except Exception as e:
                self.loggit.warning("Could not get repositories: %s", e)

            for policy_name, policy_data in all_policies.items():
                # Check if policy references a deepfreeze repo
                policy_body = policy_data.get("policy", {})
                phases = policy_body.get("phases", {})

                for phase_name, phase_config in phases.items():
                    actions = phase_config.get("actions", {})
                    if "searchable_snapshot" in actions:
                        snapshot_repo = actions["searchable_snapshot"].get(
                            "snapshot_repository"
                        )
                        # Check if it references a deepfreeze repo that doesn't exist
                        if (
                            snapshot_repo
                            and self.settings.repo_name_prefix
                            and snapshot_repo.startswith(self.settings.repo_name_prefix)
                            and snapshot_repo not in existing_repos
                        ):
                            # Check if policy is safe to delete
                            if is_policy_safe_to_delete(self.client, policy_name):
                                orphaned.append(
                                    {
                                        "policy_name": policy_name,
                                        "referenced_repo": snapshot_repo,
                                    }
                                )
                            break

        except Exception as e:
            self.loggit.warning("Error finding orphaned policies: %s", e)

        self.loggit.debug("Found %d orphaned ILM policies", len(orphaned))
        return orphaned

    def _cleanup_expired_repos(self, repos: list, dry_run: bool = False) -> list:
        """
        Clean up expired repositories by unmounting them and resetting their state.

        :param repos: List of expired repository objects
        :param dry_run: If True, don't actually clean up
        :return: List of results
        """
        results = []

        for repo in repos:
            result = {"repo": repo.name, "success": False, "error": None}

            if dry_run:
                result["success"] = True
                results.append(result)
                continue

            try:
                # Check if repo is still mounted
                try:
                    self.client.snapshot.get_repository(name=repo.name)
                    # If it exists, unmount it
                    unmount_repo(self.client, repo.name)
                except NotFoundError:
                    # Already unmounted
                    pass

                # Reset to frozen state
                repo.reset_to_frozen()
                repo.persist(self.client)

                result["success"] = True
                self.loggit.info("Cleaned up expired repository %s", repo.name)

            except Exception as e:
                result["error"] = str(e)
                self.loggit.error("Failed to clean up %s: %s", repo.name, e)

            results.append(result)

        return results

    def _cleanup_thaw_requests(self, requests: list, dry_run: bool = False) -> list:
        """
        Clean up old thaw requests by deleting them from the status index.

        :param requests: List of request info dictionaries
        :param dry_run: If True, don't actually clean up
        :return: List of results
        """
        results = []

        for req_info in requests:
            req = req_info["request"]
            request_id = req.get("request_id", req.get("id"))
            result = {"request_id": request_id, "success": False, "error": None}

            if dry_run:
                result["success"] = True
                results.append(result)
                continue

            try:
                self.client.delete(index=STATUS_INDEX, id=request_id)
                result["success"] = True
                self.loggit.info("Deleted old thaw request %s", request_id)
            except Exception as e:
                result["error"] = str(e)
                self.loggit.error("Failed to delete request %s: %s", request_id, e)

            results.append(result)

        return results

    def _cleanup_orphaned_policies(self, policies: list, dry_run: bool = False) -> list:
        """
        Clean up orphaned ILM policies by deleting them.

        :param policies: List of policy info dictionaries
        :param dry_run: If True, don't actually clean up
        :return: List of results
        """
        results = []

        for policy_info in policies:
            policy_name = policy_info["policy_name"]
            result = {"policy_name": policy_name, "success": False, "error": None}

            if dry_run:
                result["success"] = True
                results.append(result)
                continue

            try:
                self.client.ilm.delete_lifecycle(name=policy_name)
                result["success"] = True
                self.loggit.info("Deleted orphaned ILM policy %s", policy_name)
            except Exception as e:
                result["error"] = str(e)
                self.loggit.error("Failed to delete policy %s: %s", policy_name, e)

            results.append(result)

        return results

    def do_dry_run(self) -> None:
        """
        Perform a dry-run showing what would be cleaned up.

        :return: None
        :rtype: None
        """
        self.loggit.info("DRY-RUN MODE.  No changes will be made.")

        try:
            self._load_settings()

            # Find items to clean up
            expired_repos = self._find_expired_repos()
            old_requests = self._find_old_thaw_requests()
            orphaned_policies = self._find_orphaned_policies()

            # Display what would be cleaned up
            if self.porcelain:
                for repo in expired_repos:
                    print(f"DRY_RUN\texpired_repo\t{repo.name}\t{repo.expires_at}")
                for req_info in old_requests:
                    req = req_info["request"]
                    print(
                        f"DRY_RUN\told_request\t{req.get('request_id')}\t"
                        f"{req.get('status')}\t{req_info['age_days']}d old"
                    )
                for policy_info in orphaned_policies:
                    print(
                        f"DRY_RUN\torphan_policy\t{policy_info['policy_name']}\t"
                        f"{policy_info['referenced_repo']}"
                    )
                print(
                    f"SUMMARY\t{len(expired_repos)} repos\t{len(old_requests)} requests\t"
                    f"{len(orphaned_policies)} policies"
                )
            else:
                # Expired repositories
                if expired_repos:
                    table = Table(title="Expired Repositories")
                    table.add_column("Name", style="cyan")
                    table.add_column("Expired At", style="yellow")
                    table.add_column("Thaw State", style="red")

                    for repo in expired_repos:
                        expires = repo.expires_at.isoformat() if repo.expires_at else "N/A"
                        table.add_row(repo.name, expires, repo.thaw_state)

                    self.console.print(table)
                    self.console.print()
                else:
                    self.console.print("[dim]No expired repositories to clean up[/dim]")
                    self.console.print()

                # Old thaw requests
                if old_requests:
                    table = Table(title="Old Thaw Requests")
                    table.add_column("Request ID", style="cyan")
                    table.add_column("Status", style="magenta")
                    table.add_column("Age (days)", style="yellow")
                    table.add_column("Retention (days)", style="white")

                    for req_info in old_requests:
                        req = req_info["request"]
                        status = req.get("status", "unknown")
                        table.add_row(
                            req.get("request_id", "N/A"),
                            status,
                            str(req_info["age_days"]),
                            str(req_info["retention_days"]),
                        )

                    self.console.print(table)
                    self.console.print()
                else:
                    self.console.print("[dim]No old thaw requests to clean up[/dim]")
                    self.console.print()

                # Orphaned policies
                if orphaned_policies:
                    table = Table(title="Orphaned ILM Policies")
                    table.add_column("Policy Name", style="cyan")
                    table.add_column("Referenced Repo", style="yellow")

                    for policy_info in orphaned_policies:
                        table.add_row(
                            policy_info["policy_name"],
                            policy_info["referenced_repo"],
                        )

                    self.console.print(table)
                    self.console.print()
                else:
                    self.console.print("[dim]No orphaned ILM policies to clean up[/dim]")
                    self.console.print()

                # Summary
                total = len(expired_repos) + len(old_requests) + len(orphaned_policies)
                if total > 0:
                    self.console.print(
                        Panel(
                            f"[bold]Would clean up:[/bold]\n"
                            f"  - {len(expired_repos)} expired repositories\n"
                            f"  - {len(old_requests)} old thaw requests\n"
                            f"  - {len(orphaned_policies)} orphaned ILM policies",
                            title="[bold blue]Dry Run Summary[/bold blue]",
                            border_style="blue",
                            expand=False,
                        )
                    )
                else:
                    self.console.print("[green]Nothing to clean up![/green]")

        except (MissingIndexError, MissingSettingsError) as e:
            if self.porcelain:
                print(f"ERROR\t{type(e).__name__}\t{str(e)}")
            else:
                self.console.print(f"[red]Error: {e}[/red]")
            raise

    def do_action(self) -> None:
        """
        Perform the cleanup operation.

        :return: None
        :rtype: None
        """
        self.loggit.debug("Starting Cleanup action")

        try:
            self._load_settings()

            # Find items to clean up
            expired_repos = self._find_expired_repos()
            old_requests = self._find_old_thaw_requests()
            orphaned_policies = self._find_orphaned_policies()

            # Perform cleanup
            repo_results = self._cleanup_expired_repos(expired_repos)
            request_results = self._cleanup_thaw_requests(old_requests)
            policy_results = self._cleanup_orphaned_policies(orphaned_policies)

            # Display results
            if self.porcelain:
                for r in repo_results:
                    status = "SUCCESS" if r["success"] else "FAILED"
                    print(f"{status}\texpired_repo\t{r['repo']}")
                for r in request_results:
                    status = "SUCCESS" if r["success"] else "FAILED"
                    print(f"{status}\told_request\t{r['request_id']}")
                for r in policy_results:
                    status = "SUCCESS" if r["success"] else "FAILED"
                    print(f"{status}\torphan_policy\t{r['policy_name']}")

                total_success = (
                    sum(1 for r in repo_results if r["success"])
                    + sum(1 for r in request_results if r["success"])
                    + sum(1 for r in policy_results if r["success"])
                )
                total_failed = (
                    sum(1 for r in repo_results if not r["success"])
                    + sum(1 for r in request_results if not r["success"])
                    + sum(1 for r in policy_results if not r["success"])
                )
                print(f"COMPLETE\t{total_success} success\t{total_failed} failed")

            else:
                # Summarize results
                repo_success = sum(1 for r in repo_results if r["success"])
                repo_failed = sum(1 for r in repo_results if not r["success"])
                request_success = sum(1 for r in request_results if r["success"])
                request_failed = sum(1 for r in request_results if not r["success"])
                policy_success = sum(1 for r in policy_results if r["success"])
                policy_failed = sum(1 for r in policy_results if not r["success"])

                total_success = repo_success + request_success + policy_success
                total_failed = repo_failed + request_failed + policy_failed

                if total_success + total_failed == 0:
                    self.console.print(
                        Panel(
                            "[green]Nothing to clean up![/green]\n\n"
                            "All repositories, thaw requests, and ILM policies are current.",
                            title="[bold green]Cleanup Complete[/bold green]",
                            border_style="green",
                            expand=False,
                        )
                    )
                else:
                    summary_lines = [
                        f"[bold]Cleanup completed[/bold]\n",
                        f"Expired repositories: {repo_success} cleaned"
                        + (f" ({repo_failed} failed)" if repo_failed else ""),
                        f"Old thaw requests: {request_success} deleted"
                        + (f" ({request_failed} failed)" if request_failed else ""),
                        f"Orphaned ILM policies: {policy_success} deleted"
                        + (f" ({policy_failed} failed)" if policy_failed else ""),
                    ]

                    border_style = "green" if total_failed == 0 else "yellow"
                    title_style = "green" if total_failed == 0 else "yellow"

                    self.console.print(
                        Panel(
                            "\n".join(summary_lines),
                            title=f"[bold {title_style}]Cleanup Complete[/bold {title_style}]",
                            border_style=border_style,
                            expand=False,
                        )
                    )

                    # Show failures if any
                    if total_failed > 0:
                        failures = []
                        for r in repo_results:
                            if not r["success"]:
                                failures.append(f"  - Repo {r['repo']}: {r.get('error')}")
                        for r in request_results:
                            if not r["success"]:
                                failures.append(
                                    f"  - Request {r['request_id']}: {r.get('error')}"
                                )
                        for r in policy_results:
                            if not r["success"]:
                                failures.append(
                                    f"  - Policy {r['policy_name']}: {r.get('error')}"
                                )

                        self.console.print(
                            Panel(
                                "[bold]Some items failed to clean up:[/bold]\n"
                                + "\n".join(failures),
                                title="[bold yellow]Cleanup Failures[/bold yellow]",
                                border_style="yellow",
                                expand=False,
                            )
                        )

            self.loggit.info(
                "Cleanup complete: %d success, %d failed",
                total_success,
                total_failed,
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
                        title="[bold red]Cleanup Failed[/bold red]",
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
                        f"[bold]Cleanup operation failed[/bold]\n\n"
                        f"Error: {escape(str(e))}\n\n"
                        f"Check logs for details.",
                        title="[bold red]Cleanup Error[/bold red]",
                        border_style="red",
                        expand=False,
                    )
                )
            self.loggit.error("Cleanup failed: %s", e, exc_info=True)
            raise
