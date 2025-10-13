"""Status action for deepfreeze"""

# pylint: disable=too-many-arguments,too-many-instance-attributes, raise-missing-from

import logging
from datetime import datetime

from elasticsearch import Elasticsearch
from rich import print
from rich.console import Console
from rich.table import Table

from curator.actions.deepfreeze.utilities import get_all_repos, get_settings


class Status:
    """
    Get the status of the deepfreeze components. No dry_run for this action makes
    sense as it changes nothing, so the do_singleton_action method simply runs the
    do_action method directly.

    :param client: A client connection object
    :type client: Elasticsearch
    :param limit: Number of most recent repositories to show (None = show all)
    :type limit: int

    :methods:
        do_action: Perform high-level status steps in sequence.
        do_singleton_action: Perform high-level status steps in sequence.
        get_cluster_name: Get the name of the cluster.
        do_repositories: Get the status of the repositories.
        do_buckets: Get the status of the buckets.
        do_ilm_policies: Get the status of the ILM policies.
        do_thawsets: Get the status of the thawsets.
        do_config: Get the status of the configuration.
    """

    def __init__(self, client: Elasticsearch, limit: int = None) -> None:
        self.loggit = logging.getLogger("curator.actions.deepfreeze")
        self.loggit.debug("Initializing Deepfreeze Status")
        self.settings = get_settings(client)
        self.client = client
        self.limit = limit
        self.console = Console()
        self.console.clear()

    def get_cluster_name(self) -> str:
        """
        Connects to the Elasticsearch cluster and returns its name.

        :param es_host: The URL of the Elasticsearch instance (default: "http://localhost:9200").
        :type es_host: str
        :return: The name of the Elasticsearch cluster.
        :rtype: str
        """
        try:
            cluster_info = self.client.cluster.health()
            return cluster_info.get("cluster_name", "Unknown Cluster")
        except Exception as e:
            return f"Error: {e}"

    def do_action(self) -> None:
        """
        Perform the status action

        :return: None
        :rtype: None
        """
        self.loggit.info("Getting status")
        print()

        self.do_repositories()
        self.do_buckets()
        self.do_ilm_policies()
        self.do_config()

    def do_config(self):
        """
        Print the configuration settings

        :return: None
        :rtype: None
        """
        table = Table(title="Configuration")
        table.add_column("Setting", style="cyan")
        table.add_column("Value", style="magenta")

        table.add_row("Repo Prefix", self.settings.repo_name_prefix)
        table.add_row("Bucket Prefix", self.settings.bucket_name_prefix)
        table.add_row("Base Path Prefix", self.settings.base_path_prefix)
        table.add_row("Canned ACL", self.settings.canned_acl)
        table.add_row("Storage Class", self.settings.storage_class)
        table.add_row("Provider", self.settings.provider)
        table.add_row("Rotate By", self.settings.rotate_by)
        table.add_row("Style", self.settings.style)
        table.add_row("Last Suffix", self.settings.last_suffix)
        table.add_row("Cluster Name", self.get_cluster_name())

        self.console.print(table)

    def do_ilm_policies(self):
        """
        Print the ILM policies affected by deepfreeze

        :return: None
        :rtype: None
        """
        table = Table(title="ILM Policies")
        table.add_column("Policy", style="cyan")
        table.add_column("Indices", style="magenta")
        table.add_column("Datastreams", style="magenta")
        policies = self.client.ilm.get_lifecycle()
        for policy in policies:
            # print(f"  {policy}")
            for phase in policies[policy]["policy"]["phases"]:
                if (
                    "searchable_snapshot"
                    in policies[policy]["policy"]["phases"][phase]["actions"]
                    and policies[policy]["policy"]["phases"][phase]["actions"][
                        "searchable_snapshot"
                    ]["snapshot_repository"]
                    == f"{self.settings.repo_name_prefix}-{self.settings.last_suffix}"
                ):
                    num_indices = len(policies[policy]["in_use_by"]["indices"])
                    num_datastreams = len(policies[policy]["in_use_by"]["data_streams"])
                    table.add_row(policy, str(num_indices), str(num_datastreams))
                    break
        self.console.print(table)

    def do_buckets(self):
        """
        Print the buckets in use by deepfreeze

        :return: None
        :rtype: None
        """
        table = Table(title="Buckets")
        table.add_column("Provider", style="cyan")
        table.add_column("Bucket", style="magenta")
        table.add_column("Base_path", style="magenta")

        if self.settings.rotate_by == "bucket":
            table.add_row(
                self.settings.provider,
                f"{self.settings.bucket_name_prefix}-{self.settings.last_suffix}",
                self.settings.base_path_prefix,
            )
        else:
            table.add_row(
                self.settings.provider,
                f"{self.settings.bucket_name_prefix}",
                f"{self.settings.base_path_prefix}-{self.settings.last_suffix}",
            )
        self.console.print(table)

    def do_repositories(self):
        """
        Print the repositories in use by deepfreeze

        :return: None
        :rtype: None
        """
        self.loggit.debug("Showing repositories")

        # Get and sort all repositories
        active_repo = f"{self.settings.repo_name_prefix}-{self.settings.last_suffix}"
        self.loggit.debug("Getting repositories")
        unmounted_repos = get_all_repos(self.client)
        unmounted_repos.sort()
        total_repos = len(unmounted_repos)
        self.loggit.debug("Got %s repositories", total_repos)

        # Apply limit if specified
        if self.limit is not None and self.limit > 0:
            unmounted_repos = unmounted_repos[-self.limit:]
            self.loggit.debug("Limiting display to last %s repositories", self.limit)

        # Set up the table with appropriate title
        if self.limit is not None and self.limit > 0:
            table_title = f"Repositories (showing last {len(unmounted_repos)} of {total_repos})"
        else:
            table_title = "Repositories"

        table = Table(title=table_title)
        table.add_column("Repository", style="cyan")
        table.add_column("Status", style="magenta")
        table.add_column("Snapshots", style="magenta")
        table.add_column("Start", style="magenta")
        table.add_column("End", style="magenta")
        for repo in unmounted_repos:
            status = "U"
            if repo.is_mounted:
                status = "M"
                if repo.name == active_repo:
                    status = "M*"
            if repo.is_thawed:
                status = "T"
            if repo.name == active_repo:
                status = "M*"
            count = "--"
            self.loggit.debug(f"Checking mount status for {repo.name}")
            if repo.is_mounted:
                try:
                    snapshots = self.client.snapshot.get(
                        repository=repo.name, snapshot="_all"
                    )
                    count = len(snapshots.get("snapshots", []))
                    self.loggit.debug(f"Got {count} snapshots for {repo.name}")
                except Exception as e:
                    self.loggit.warning("Repository %s not mounted: %s", repo.name, e)
                    repo.unmount()
            # Format dates for display
            start_str = (
                repo.start.isoformat() if isinstance(repo.start, datetime)
                else repo.start if repo.start
                else "N/A"
            )
            end_str = (
                repo.end.isoformat() if isinstance(repo.end, datetime)
                else repo.end if repo.end
                else "N/A"
            )
            table.add_row(repo.name, status, str(count), start_str, end_str)
        self.console.print(table)

    def do_singleton_action(self) -> None:
        """
        Dry run makes no sense here, so we're just going to do this either way.

        :return: None
        :rtype: None
        """
        self.do_action()
