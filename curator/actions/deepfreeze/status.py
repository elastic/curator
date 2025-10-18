"""Status action for deepfreeze"""

# pylint: disable=too-many-arguments,too-many-instance-attributes, raise-missing-from

import logging
from datetime import datetime

from elasticsearch import Elasticsearch
from rich import print
from rich.console import Console
from rich.table import Table

from curator.actions.deepfreeze.utilities import get_all_repos, get_settings, check_restore_status, list_thaw_requests
from curator.s3client import s3_client_factory


class Status:
    """
    Get the status of the deepfreeze components. No dry_run for this action makes
    sense as it changes nothing, so the do_singleton_action method simply runs the
    do_action method directly.

    :param client: A client connection object
    :type client: Elasticsearch
    :param limit: Number of most recent repositories to show (None = show all)
    :type limit: int
    :param show_repos: Show repositories section
    :type show_repos: bool
    :param show_thawed: Show thawed repositories section
    :type show_thawed: bool
    :param show_buckets: Show buckets section
    :type show_buckets: bool
    :param show_ilm: Show ILM policies section
    :type show_ilm: bool
    :param show_config: Show configuration section
    :type show_config: bool
    :param porcelain: Output plain text without rich formatting
    :type porcelain: bool

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

    def __init__(
        self,
        client: Elasticsearch,
        limit: int = None,
        show_repos: bool = False,
        show_thawed: bool = False,
        show_buckets: bool = False,
        show_ilm: bool = False,
        show_config: bool = False,
        porcelain: bool = False,
    ) -> None:
        self.loggit = logging.getLogger("curator.actions.deepfreeze")
        self.loggit.debug("Initializing Deepfreeze Status")
        self.settings = get_settings(client)
        self.client = client
        self.limit = limit

        # If no specific sections are requested, show all
        self.show_all = not (show_repos or show_thawed or show_buckets or show_ilm or show_config)
        self.show_repos = show_repos or self.show_all
        self.show_thawed = show_thawed or self.show_all
        self.show_buckets = show_buckets or self.show_all
        self.show_ilm = show_ilm or self.show_all
        self.show_config = show_config or self.show_all
        self.porcelain = porcelain

        self.console = Console()
        if not porcelain:
            self.console.clear()
        # Initialize S3 client for checking restore status
        self.s3 = s3_client_factory(self.settings.provider)

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
        if not self.porcelain:
            print()

        if self.show_thawed:
            self.do_thawed_repositories()
        if self.show_repos:
            self.do_repositories()
        if self.show_buckets:
            self.do_buckets()
        if self.show_ilm:
            self.do_ilm_policies()
        if self.show_config:
            self.do_config()

    def do_config(self):
        """
        Print the configuration settings

        :return: None
        :rtype: None
        """
        config_items = [
            ("Repo Prefix", self.settings.repo_name_prefix),
            ("Bucket Prefix", self.settings.bucket_name_prefix),
            ("Base Path Prefix", self.settings.base_path_prefix),
            ("Canned ACL", self.settings.canned_acl),
            ("Storage Class", self.settings.storage_class),
            ("Provider", self.settings.provider),
            ("Rotate By", self.settings.rotate_by),
            ("Style", self.settings.style),
            ("Last Suffix", self.settings.last_suffix),
            ("Cluster Name", self.get_cluster_name()),
        ]

        if self.porcelain:
            # Output tab-separated key-value pairs for scripting
            for setting, value in config_items:
                print(f"{setting}\t{value}")
        else:
            table = Table(title="Configuration")
            table.add_column("Setting", style="cyan")
            table.add_column("Value", style="magenta")

            for setting, value in config_items:
                table.add_row(setting, value)

            self.console.print(table)

    def do_ilm_policies(self):
        """
        Print the ILM policies affected by deepfreeze

        :return: None
        :rtype: None
        """
        table = Table(title="ILM Policies")
        table.add_column("Policy", style="cyan")
        table.add_column("Repository", style="magenta")
        table.add_column("Indices", style="magenta")
        table.add_column("Datastreams", style="magenta")

        current_repo = f"{self.settings.repo_name_prefix}-{self.settings.last_suffix}"
        policies = self.client.ilm.get_lifecycle()

        for policy in policies:
            for phase in policies[policy]["policy"]["phases"]:
                if (
                    "searchable_snapshot"
                    in policies[policy]["policy"]["phases"][phase]["actions"]
                ):
                    repo_name = policies[policy]["policy"]["phases"][phase]["actions"][
                        "searchable_snapshot"
                    ]["snapshot_repository"]

                    # Check if repository starts with our prefix
                    if repo_name.startswith(self.settings.repo_name_prefix):
                        # Mark current repo with asterisk
                        repo_display = repo_name if repo_name != current_repo else f"{repo_name}*"

                        num_indices = len(policies[policy]["in_use_by"]["indices"])
                        num_datastreams = len(policies[policy]["in_use_by"]["data_streams"])

                        if self.porcelain:
                            # Output tab-separated values for scripting
                            print(f"{policy}\t{repo_display}\t{num_indices}\t{num_datastreams}")
                        else:
                            table.add_row(policy, repo_display, str(num_indices), str(num_datastreams))
                        break

        if not self.porcelain:
            self.console.print(table)

    def do_buckets(self):
        """
        Print the buckets in use by deepfreeze

        :return: None
        :rtype: None
        """
        self.loggit.debug("Showing buckets")

        # Get all repositories with our prefix
        all_repos = get_all_repos(self.client)
        matching_repos = [
            repo for repo in all_repos
            if repo.name.startswith(self.settings.repo_name_prefix)
        ]

        # Extract unique bucket/base_path combinations
        bucket_info = {}
        for repo in matching_repos:
            if repo.bucket and repo.base_path is not None:
                key = (repo.bucket, repo.base_path)
                if key not in bucket_info:
                    bucket_info[key] = repo.name

        # Sort by bucket/base_path
        sorted_buckets = sorted(bucket_info.keys())
        total_buckets = len(sorted_buckets)

        # Apply limit if specified
        if self.limit is not None and self.limit > 0:
            sorted_buckets = sorted_buckets[-self.limit:]
            self.loggit.debug("Limiting display to last %s buckets", self.limit)

        # Determine current bucket/base_path
        if self.settings.rotate_by == "bucket":
            current_bucket = f"{self.settings.bucket_name_prefix}-{self.settings.last_suffix}"
            current_base_path = self.settings.base_path_prefix
        else:
            current_bucket = self.settings.bucket_name_prefix
            current_base_path = f"{self.settings.base_path_prefix}-{self.settings.last_suffix}"

        # Set up the table with appropriate title
        if self.limit is not None and self.limit > 0 and total_buckets > self.limit:
            table_title = f"Buckets (showing last {len(sorted_buckets)} of {total_buckets})"
        else:
            table_title = "Buckets"

        table = Table(title=table_title)
        table.add_column("Provider", style="cyan")
        table.add_column("Bucket", style="magenta")
        table.add_column("Base_path", style="magenta")

        for bucket, base_path in sorted_buckets:
            # Mark current bucket/base_path with asterisk
            if bucket == current_bucket and base_path == current_base_path:
                bucket_display = f"{bucket}*"
            else:
                bucket_display = bucket

            if self.porcelain:
                # Output tab-separated values for scripting
                print(f"{self.settings.provider}\t{bucket_display}\t{base_path}")
            else:
                table.add_row(self.settings.provider, bucket_display, base_path)

        if not self.porcelain:
            self.console.print(table)

    def do_thawed_repositories(self):
        """
        Print thawed and thawing repositories in a separate section

        :return: None
        :rtype: None
        """
        self.loggit.debug("Showing thawed repositories")

        # Get all repositories
        all_repos = get_all_repos(self.client)
        all_repos.sort()

        # Get active thaw requests to track which repos are being thawed
        active_thaw_requests = []
        repos_being_thawed = set()
        try:
            all_thaw_requests = list_thaw_requests(self.client)
            active_thaw_requests = [req for req in all_thaw_requests if req.get("status") == "in_progress"]
            for req in active_thaw_requests:
                repos_being_thawed.update(req.get("repos", []))
        except Exception as e:
            self.loggit.warning("Could not retrieve thaw requests: %s", e)

        # Filter to only thawed/thawing repos
        thawed_repos = []
        for repo in all_repos:
            if repo.name in repos_being_thawed or repo.is_thawed:
                thawed_repos.append(repo)

        # If no thawed repos, don't show the section
        if not thawed_repos:
            return

        # Create the table
        table = Table(title="Thawed Repositories")
        table.add_column("Repository", style="cyan")
        table.add_column("State", style="yellow")
        table.add_column("Mounted", style="green")
        table.add_column("Snapshots", style="magenta")
        table.add_column("Expires", style="red")
        table.add_column("Start", style="magenta")
        table.add_column("End", style="magenta")

        for repo in thawed_repos:
            # Determine mounted status
            mounted_status = "yes" if repo.is_mounted else "no"

            # Get snapshot count
            count = "--"
            if repo.is_mounted:
                try:
                    snapshots = self.client.snapshot.get(
                        repository=repo.name, snapshot="_all"
                    )
                    count = len(snapshots.get("snapshots", []))
                except Exception as e:
                    self.loggit.warning("Repository %s not mounted: %s", repo.name, e)

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

            # Format expiry time
            expires_str = (
                repo.expires_at.isoformat() if isinstance(repo.expires_at, datetime)
                else repo.expires_at if repo.expires_at
                else "N/A"
            )

            if self.porcelain:
                print(f"{repo.name}\t{repo.thaw_state}\t{mounted_status}\t{count}\t{expires_str}\t{start_str}\t{end_str}")
            else:
                table.add_row(repo.name, repo.thaw_state, mounted_status, str(count), expires_str, start_str, end_str)

        if not self.porcelain:
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
        all_repos = get_all_repos(self.client)
        all_repos.sort()
        total_repos = len(all_repos)
        self.loggit.debug("Got %s repositories", total_repos)

        # Get active thaw requests to track which repos are being thawed
        active_thaw_requests = []
        repos_being_thawed = set()
        try:
            all_thaw_requests = list_thaw_requests(self.client)
            active_thaw_requests = [req for req in all_thaw_requests if req.get("status") == "in_progress"]
            for req in active_thaw_requests:
                repos_being_thawed.update(req.get("repos", []))
            self.loggit.debug("Found %d active thaw requests covering %d repos",
                            len(active_thaw_requests), len(repos_being_thawed))
        except Exception as e:
            self.loggit.warning("Could not retrieve thaw requests: %s", e)

        # Apply limit to all repos equally
        if self.limit is not None and self.limit > 0:
            repos_to_display = all_repos[-self.limit:]
            self.loggit.debug("Limiting display to last %s repositories", self.limit)
        else:
            repos_to_display = all_repos

        # Set up the table with appropriate title
        if self.limit is not None and self.limit > 0:
            table_title = f"Repositories (showing last {len(repos_to_display)} of {total_repos})"
        else:
            table_title = "Repositories"

        table = Table(title=table_title)
        table.add_column("Repository", style="cyan")
        table.add_column("State", style="yellow")
        table.add_column("Mounted", style="green")
        table.add_column("Snapshots", style="magenta")
        table.add_column("Start", style="magenta")
        table.add_column("End", style="magenta")

        for repo in repos_to_display:
            # Mark active repository with asterisk
            repo_name = f"{repo.name}*" if repo.name == active_repo else repo.name

            # Determine mounted status
            mounted_status = "yes" if repo.is_mounted else "no"

            # Get snapshot count
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

            if self.porcelain:
                # Output tab-separated values for scripting
                print(f"{repo_name}\t{repo.thaw_state}\t{mounted_status}\t{count}\t{start_str}\t{end_str}")
            else:
                table.add_row(repo_name, repo.thaw_state, mounted_status, str(count), start_str, end_str)

        if not self.porcelain:
            self.console.print(table)

    def do_singleton_action(self) -> None:
        """
        Dry run makes no sense here, so we're just going to do this either way.

        :return: None
        :rtype: None
        """
        self.do_action()
