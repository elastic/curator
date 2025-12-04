"""Status action for deepfreeze"""

# pylint: disable=too-many-arguments,too-many-instance-attributes, raise-missing-from

import json
import logging

from elasticsearch8 import Elasticsearch
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from deepfreeze.constants import STATUS_INDEX
from deepfreeze.exceptions import ActionError, MissingIndexError, MissingSettingsError
from deepfreeze.s3client import s3_client_factory
from deepfreeze.utilities import (
    get_all_repos,
    get_matching_repo_names,
    get_settings,
    list_thaw_requests,
)


class Status:
    """
    Status action displays the current state of deepfreeze repositories,
    thaw requests, S3 buckets, ILM policies, and configuration.

    :param client: A client connection object
    :param porcelain: If True, output machine-readable format instead of rich tables

    :methods:
        do_dry_run: Same as do_action (read-only operation)
        do_action: Display status information

    :example:
        >>> from deepfreeze.actions import Status
        >>> status = Status(client)
        >>> status.do_action()
    """

    def __init__(
        self,
        client: Elasticsearch,
        porcelain: bool = False,
    ) -> None:
        self.loggit = logging.getLogger("deepfreeze.actions.status")
        self.loggit.debug("Initializing Deepfreeze Status")

        # Console for STDERR output
        self.console = Console(stderr=True)

        self.client = client
        self.porcelain = porcelain

        # Will be loaded during action
        self.settings = None
        self.s3 = None

        self.loggit.debug("Deepfreeze Status initialized")

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

    def _get_repositories_status(self) -> list:
        """Get status of all repositories."""
        repos = []
        try:
            all_repos = get_all_repos(self.client)
            for repo in all_repos:
                # Check if repo is actually mounted in ES
                mounted_repos = get_matching_repo_names(
                    self.client, self.settings.repo_name_prefix
                )
                is_mounted_in_es = repo.name in mounted_repos

                repos.append(
                    {
                        "name": repo.name,
                        "bucket": repo.bucket,
                        "base_path": repo.base_path,
                        "start": repo.start.isoformat() if repo.start else None,
                        "end": repo.end.isoformat() if repo.end else None,
                        "is_mounted": is_mounted_in_es,
                        "thaw_state": repo.thaw_state,
                        "thawed_at": repo.thawed_at.isoformat()
                        if repo.thawed_at
                        else None,
                        "expires_at": repo.expires_at.isoformat()
                        if repo.expires_at
                        else None,
                    }
                )
        except Exception as e:
            self.loggit.warning("Error getting repositories: %s", e)

        return repos

    def _get_thaw_requests(self) -> list:
        """Get all thaw requests."""
        try:
            return list_thaw_requests(self.client)
        except ActionError as e:
            self.loggit.warning("Error getting thaw requests: %s", e)
            return []

    def _get_buckets_info(self) -> list:
        """Get info about S3 buckets."""
        buckets = []
        try:
            bucket_names = self.s3.list_buckets(
                prefix=self.settings.bucket_name_prefix
            )
            for bucket_name in bucket_names:
                # Count objects in bucket
                try:
                    objects = self.s3.list_objects(bucket_name, "")
                    object_count = len(objects)
                except Exception as e:
                    self.loggit.warning(
                        "Error listing objects in bucket %s: %s", bucket_name, e
                    )
                    object_count = -1

                buckets.append({"name": bucket_name, "object_count": object_count})
        except Exception as e:
            self.loggit.warning("Error listing buckets: %s", e)

        return buckets

    def _get_ilm_policies(self) -> list:
        """Get ILM policies that reference deepfreeze repositories."""
        policies = []
        try:
            all_policies = self.client.ilm.get_lifecycle()
            for policy_name, policy_data in all_policies.items():
                policy_body = policy_data.get("policy", {})
                phases = policy_body.get("phases", {})

                # Check if any phase has a searchable_snapshot action
                for phase_name, phase_config in phases.items():
                    actions = phase_config.get("actions", {})
                    if "searchable_snapshot" in actions:
                        snapshot_repo = actions["searchable_snapshot"].get(
                            "snapshot_repository"
                        )
                        # Check if it references a deepfreeze repo
                        if (
                            snapshot_repo
                            and self.settings.repo_name_prefix
                            and snapshot_repo.startswith(
                                self.settings.repo_name_prefix
                            )
                        ):
                            in_use_by = policy_data.get("in_use_by", {})
                            policies.append(
                                {
                                    "name": policy_name,
                                    "phase": phase_name,
                                    "repository": snapshot_repo,
                                    "indices_count": len(
                                        in_use_by.get("indices", [])
                                    ),
                                    "data_streams_count": len(
                                        in_use_by.get("data_streams", [])
                                    ),
                                    "templates_count": len(
                                        in_use_by.get("composable_templates", [])
                                    ),
                                }
                            )
                            break  # Only add once per policy
        except Exception as e:
            self.loggit.warning("Error getting ILM policies: %s", e)

        return policies

    def _display_porcelain(
        self, repos: list, thaw_requests: list, buckets: list, ilm_policies: list
    ) -> None:
        """Output machine-readable format."""
        output = {
            "settings": self.settings.to_dict() if self.settings else None,
            "repositories": repos,
            "thaw_requests": thaw_requests,
            "buckets": buckets,
            "ilm_policies": ilm_policies,
        }
        print(json.dumps(output, indent=2))

    def _display_rich(
        self, repos: list, thaw_requests: list, buckets: list, ilm_policies: list
    ) -> None:
        """Output rich formatted tables."""
        # Settings panel
        if self.settings:
            settings_text = (
                f"Provider: [cyan]{self.settings.provider}[/cyan]\n"
                f"Repository Prefix: [cyan]{self.settings.repo_name_prefix}[/cyan]\n"
                f"Bucket Prefix: [cyan]{self.settings.bucket_name_prefix}[/cyan]\n"
                f"Base Path Prefix: [cyan]{self.settings.base_path_prefix}[/cyan]\n"
                f"Storage Class: [cyan]{self.settings.storage_class}[/cyan]\n"
                f"Rotation Style: [cyan]{self.settings.style}[/cyan]\n"
                f"Last Suffix: [cyan]{self.settings.last_suffix}[/cyan]"
            )
            if self.settings.ilm_policy_name:
                settings_text += f"\nILM Policy: [cyan]{self.settings.ilm_policy_name}[/cyan]"
            if self.settings.index_template_name:
                settings_text += f"\nIndex Template: [cyan]{self.settings.index_template_name}[/cyan]"

            self.console.print(
                Panel(
                    settings_text,
                    title="[bold]Deepfreeze Configuration[/bold]",
                    border_style="blue",
                    expand=False,
                )
            )
            self.console.print()

        # Repositories table
        if repos:
            table = Table(title="Repositories")
            table.add_column("Name", style="cyan")
            table.add_column("Bucket", style="yellow")
            table.add_column("Base Path", style="white")
            table.add_column("Date Range", style="white")
            table.add_column("Mounted", style="green")
            table.add_column("Thaw State", style="magenta")

            for repo in sorted(repos, key=lambda x: x["name"]):
                date_range = ""
                if repo.get("start") and repo.get("end"):
                    date_range = f"{repo['start'][:10]} - {repo['end'][:10]}"

                mounted_str = (
                    "[green]Yes[/green]"
                    if repo.get("is_mounted")
                    else "[red]No[/red]"
                )
                thaw_state = repo.get("thaw_state", "unknown")
                state_color = {
                    "active": "green",
                    "frozen": "blue",
                    "thawing": "yellow",
                    "thawed": "cyan",
                    "expired": "red",
                }.get(thaw_state, "white")

                table.add_row(
                    repo["name"],
                    repo.get("bucket", "N/A"),
                    repo.get("base_path", "N/A"),
                    date_range,
                    mounted_str,
                    f"[{state_color}]{thaw_state}[/{state_color}]",
                )

            self.console.print(table)
            self.console.print()
        else:
            self.console.print("[yellow]No repositories found[/yellow]")
            self.console.print()

        # Thaw requests table
        if thaw_requests:
            table = Table(title="Thaw Requests")
            table.add_column("Request ID", style="cyan")
            table.add_column("Status", style="magenta")
            table.add_column("Date Range", style="white")
            table.add_column("Repositories", style="yellow")
            table.add_column("Created", style="white")

            for req in sorted(thaw_requests, key=lambda x: x.get("created_at", "")):
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
                    req.get("request_id", "N/A"),
                    f"[{status_color}]{status}[/{status_color}]",
                    date_range,
                    repos_str,
                    created,
                )

            self.console.print(table)
            self.console.print()
        else:
            self.console.print("[dim]No active thaw requests[/dim]")
            self.console.print()

        # S3 Buckets table
        if buckets:
            table = Table(title="S3 Buckets")
            table.add_column("Bucket Name", style="cyan")
            table.add_column("Object Count", style="yellow")

            for bucket in sorted(buckets, key=lambda x: x["name"]):
                count_str = (
                    str(bucket["object_count"])
                    if bucket["object_count"] >= 0
                    else "Error"
                )
                table.add_row(bucket["name"], count_str)

            self.console.print(table)
            self.console.print()
        else:
            self.console.print("[dim]No S3 buckets found matching prefix[/dim]")
            self.console.print()

        # ILM Policies table
        if ilm_policies:
            table = Table(title="ILM Policies (referencing deepfreeze)")
            table.add_column("Policy Name", style="cyan")
            table.add_column("Phase", style="yellow")
            table.add_column("Repository", style="green")
            table.add_column("Indices", style="white")
            table.add_column("Data Streams", style="white")
            table.add_column("Templates", style="white")

            for policy in sorted(ilm_policies, key=lambda x: x["name"]):
                table.add_row(
                    policy["name"],
                    policy["phase"],
                    policy["repository"],
                    str(policy["indices_count"]),
                    str(policy["data_streams_count"]),
                    str(policy["templates_count"]),
                )

            self.console.print(table)
            self.console.print()
        else:
            self.console.print(
                "[dim]No ILM policies found referencing deepfreeze repositories[/dim]"
            )
            self.console.print()

    def do_dry_run(self) -> None:
        """
        Perform a dry-run of the status check.
        Status is a read-only operation, so this is the same as do_action.

        :return: None
        :rtype: None
        """
        self.loggit.info("DRY-RUN MODE.  No changes will be made.")
        self.do_action()

    def do_action(self) -> None:
        """
        Display status information about deepfreeze.

        :return: None
        :rtype: None
        """
        self.loggit.debug("Starting Status action")

        try:
            # Load settings
            self._load_settings()

            # Gather all status information
            repos = self._get_repositories_status()
            thaw_requests = self._get_thaw_requests()
            buckets = self._get_buckets_info()
            ilm_policies = self._get_ilm_policies()

            # Display output
            if self.porcelain:
                self._display_porcelain(repos, thaw_requests, buckets, ilm_policies)
            else:
                self._display_rich(repos, thaw_requests, buckets, ilm_policies)

        except MissingIndexError:
            if self.porcelain:
                print(json.dumps({"error": "status_index_missing", "message": f"Status index {STATUS_INDEX} does not exist. Run 'deepfreeze setup' first."}))
            else:
                self.console.print(
                    Panel(
                        f"[bold]Status index [cyan]{STATUS_INDEX}[/cyan] does not exist.[/bold]\n\n"
                        "Deepfreeze has not been initialized in this cluster.\n\n"
                        "Run [yellow]deepfreeze setup[/yellow] to initialize deepfreeze.",
                        title="[bold yellow]Deepfreeze Not Initialized[/bold yellow]",
                        border_style="yellow",
                        expand=False,
                    )
                )
            raise

        except MissingSettingsError:
            if self.porcelain:
                print(json.dumps({"error": "settings_missing", "message": "Settings document not found in status index."}))
            else:
                self.console.print(
                    Panel(
                        "[bold]Settings document not found in status index.[/bold]\n\n"
                        "The status index exists but appears to be corrupted or incomplete.\n\n"
                        "You may need to:\n"
                        "  1. Delete the status index and run [yellow]deepfreeze setup[/yellow] again\n"
                        "  2. Check Elasticsearch logs for index corruption",
                        title="[bold red]Settings Missing[/bold red]",
                        border_style="red",
                        expand=False,
                    )
                )
            raise

        except Exception as e:
            if self.porcelain:
                print(json.dumps({"error": "unexpected", "message": str(e)}))
            else:
                self.console.print(
                    Panel(
                        f"[bold]An unexpected error occurred[/bold]\n\n"
                        f"Error: {str(e)}\n\n"
                        "Check logs for details.",
                        title="[bold red]Status Error[/bold red]",
                        border_style="red",
                        expand=False,
                    )
                )
            self.loggit.error("Unexpected error during status: %s", e, exc_info=True)
            raise
