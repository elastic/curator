"""Status action for deepfreeae"""

# pylint: disable=too-many-arguments,too-many-instance-attributes, raise-missing-from

import logging

from elasticsearch import Elasticsearch
from rich import print
from rich.console import Console
from rich.table import Table

from curator.actions.deepfreeze.constants import STATUS_INDEX
from curator.actions.deepfreeze.utilities import (
    get_matching_repo_names,
    get_settings,
    get_unmounted_repos,
)


class Status:
    """
    Get the status of the deepfreeze components. No dry_run for this action makes
    sense as it changes nothing, so the do_singleton_action method simply runs the
    do_action method directly.

    :param client: A client connection object
    :type client: Elasticsearch

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

    def __init__(self, client: Elasticsearch) -> None:
        self.loggit = logging.getLogger("curator.actions.deepfreeze")
        self.loggit.debug("Initializing Deepfreeze Status")
        self.settings = get_settings(client)
        self.client = client
        self.console = Console()

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
        # self.do_thawsets()
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

    def do_thawsets(self):
        """
        Print the thawed repositories

        :return: None
        :rtype: None
        """
        self.loggit.debug("Getting thawsets")
        table = Table(title="ThawSets")
        table.add_column("ThawSet", style="cyan")
        table.add_column("Repositories", style="magenta")
        if not self.client.indices.exists(index=STATUS_INDEX):
            self.loggit.warning("No status index found")
            return
        thawsets = self.client.search(index=STATUS_INDEX)
        self.loggit.debug("Validating thawsets")
        for thawset in thawsets:
            table.add_column(thawset)
            for repo in thawset:
                table.add_row(thawset["_id"], repo)

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
        table = Table(title="Repositories")
        table.add_column("Repository", style="cyan")
        table.add_column("Status", style="magenta")
        table.add_column("Start", style="magenta")
        table.add_column("End", style="magenta")
        unmounted_repos = get_unmounted_repos(self.client)
        unmounted_repos.sort()
        for repo in unmounted_repos:
            status = "U"
            if repo.is_mounted:
                status = "M"
            if repo.is_thawed:
                status = "T"
            table.add_row(repo.name, status, repo.start, repo.end)
        if not self.client.indices.exists(index=STATUS_INDEX):
            self.loggit.warning("No status index found")
            return
        active_repo = f"{self.settings.repo_name_prefix}-{self.settings.last_suffix}"
        repolist = get_matching_repo_names(self.client, self.settings.repo_name_prefix)
        repolist.sort()
        for repo in repolist:
            if repo == active_repo:
                table.add_row(repo, "M*")
            else:
                table.add_row(repo, "M")
        self.console.print(table)

    def do_singleton_action(self) -> None:
        """
        Dry run makes no sense here, so we're just going to do this either way.

        :return: None
        :rtype: None
        """
        self.do_action()
