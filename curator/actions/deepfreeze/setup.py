"""Setup action for deepfreeze"""

# pylint: disable=too-many-arguments,too-many-instance-attributes, raise-missing-from

import logging
import sys

from elasticsearch8 import Elasticsearch
from rich.console import Console
from rich.panel import Panel
from rich import print as rprint
from rich.markup import escape

from curator.s3client import s3_client_factory

from .constants import STATUS_INDEX
from .exceptions import PreconditionError, RepositoryException
from .helpers import Settings
from .utilities import (
    create_ilm_policy,
    create_repo,
    ensure_settings_index,
    get_matching_repo_names,
    save_settings,
)


class Setup:
    """
    Setup is responsible for creating the initial repository and bucket for
    deepfreeze operations.

    :param client: A client connection object
    :param repo_name_prefix: A prefix for repository names, defaults to `deepfreeze`
    :param bucket_name_prefix: A prefix for bucket names, defaults to `deepfreeze`
    :param base_path_prefix: Path within a bucket where snapshots are stored, defaults to `snapshots`
    :param canned_acl: One of the AWS canned ACL values (see
        `<https://docs.aws.amazon.com/AmazonS3/latest/userguide/acl-overview.html#canned-acl>`),
        defaults to `private`
    :param storage_class: AWS Storage class (see `<https://aws.amazon.com/s3/storage-classes/>`),
        defaults to `intelligent_tiering`
    :param provider: The provider to use (AWS only for now), defaults to `aws`, and will be saved
        to the deepfreeze status index for later reference.
    :param rotate_by: Rotate by bucket or path within a bucket?, defaults to `path`

    :raises RepositoryException: If a repository with the given prefix already exists

    :methods:
        do_dry_run: Perform a dry-run of the setup process.
        do_action: Perform create initial bucket and repository.

    :example:
        >>> from curator.actions.deepfreeze import Setup
        >>> setup = Setup(client, repo_name_prefix="deepfreeze", bucket_name_prefix="deepfreeze", base_path_prefix="snapshots", canned_acl="private", storage_class="intelligent_tiering", provider="aws", rotate_by="path")
        >>> setup.do_dry_run()
        >>> setup.do_action()
    """

    def __init__(
        self,
        client: Elasticsearch,
        year: int = None,
        month: int = None,
        repo_name_prefix: str = "deepfreeze",
        bucket_name_prefix: str = "deepfreeze",
        base_path_prefix: str = "snapshots",
        canned_acl: str = "private",
        storage_class: str = "intelligent_tiering",
        provider: str = "aws",
        rotate_by: str = "path",
        style: str = "oneup",
        create_sample_ilm_policy: bool = False,
        ilm_policy_name: str = "deepfreeze-sample-policy",
        porcelain: bool = False,
    ) -> None:
        self.loggit = logging.getLogger("curator.actions.deepfreeze")
        self.loggit.debug("Initializing Deepfreeze Setup")

        # Console for STDERR output
        self.console = Console(stderr=True)

        self.client = client
        self.porcelain = porcelain
        self.year = year
        self.month = month
        self.settings = Settings(
            repo_name_prefix=repo_name_prefix,
            bucket_name_prefix=bucket_name_prefix,
            base_path_prefix=base_path_prefix,
            canned_acl=canned_acl,
            storage_class=storage_class,
            provider=provider,
            rotate_by=rotate_by,
            style=style,
        )
        self.create_sample_ilm_policy = create_sample_ilm_policy
        self.ilm_policy_name = ilm_policy_name
        self.base_path = self.settings.base_path_prefix

        self.s3 = s3_client_factory(self.settings.provider)

        self.suffix = "000001"
        if self.settings.style != "oneup":
            self.suffix = f"{self.year:04}.{self.month:02}"
        self.settings.last_suffix = self.suffix

        self.new_repo_name = f"{self.settings.repo_name_prefix}-{self.suffix}"
        if self.settings.rotate_by == "bucket":
            self.new_bucket_name = f"{self.settings.bucket_name_prefix}-{self.suffix}"
            self.base_path = f"{self.settings.base_path_prefix}"
        else:
            self.new_bucket_name = f"{self.settings.bucket_name_prefix}"
            self.base_path = f"{self.base_path}-{self.suffix}"

        self.loggit.debug("Deepfreeze Setup initialized")

    def _check_preconditions(self) -> None:
        """
        Check preconditions before performing setup. Raise exceptions if any
        preconditions are not met. If this completes without raising an exception,
        the setup can proceed.

        :raises PreconditionError: If any preconditions are not met.

        :return: None
        :rtype: None
        """
        errors = []

        # First, make sure the status index does not exist yet
        self.loggit.debug("Checking if status index %s exists", STATUS_INDEX)
        if self.client.indices.exists(index=STATUS_INDEX):
            errors.append({
                "issue": f"Status index [cyan]{STATUS_INDEX}[/cyan] already exists",
                "solution": f"Delete the existing index before running setup:\n"
                           f"  [yellow]curator_cli --host <host> DELETE index --name {STATUS_INDEX}[/yellow]\n"
                           f"  or use the Elasticsearch API:\n"
                           f"  [yellow]curl -X DELETE 'http://<host>:9200/{STATUS_INDEX}'[/yellow]"
            })

        # Second, see if any existing repositories match the prefix
        self.loggit.debug(
            "Checking if any existing repositories match %s",
            self.settings.repo_name_prefix,
        )
        repos = self.client.snapshot.get_repository(name="_all")
        self.loggit.debug("Existing repositories: %s", repos)
        matching_repos = [repo for repo in repos.keys() if repo.startswith(self.settings.repo_name_prefix)]

        if matching_repos:
            repo_list = "\n  ".join([f"[cyan]{repo}[/cyan]" for repo in matching_repos])
            errors.append({
                "issue": f"Found {len(matching_repos)} existing repositor{'y' if len(matching_repos) == 1 else 'ies'} matching prefix [cyan]{self.settings.repo_name_prefix}[/cyan]:\n  {repo_list}",
                "solution": "Delete the existing repositories before running setup:\n"
                           f"  [yellow]curator_cli deepfreeze cleanup[/yellow]\n"
                           "  or manually delete each repository:\n"
                           f"  [yellow]curl -X DELETE 'http://<host>:9200/_snapshot/<repo_name>'[/yellow]\n"
                           "\n[bold]WARNING:[/bold] Ensure you have backups before deleting repositories!"
            })

        # Third, check if the bucket already exists
        self.loggit.debug("Checking if bucket %s exists", self.new_bucket_name)
        if self.s3.bucket_exists(self.new_bucket_name):
            errors.append({
                "issue": f"S3 bucket [cyan]{self.new_bucket_name}[/cyan] already exists",
                "solution": f"Delete the existing bucket before running setup:\n"
                           f"  [yellow]aws s3 rb s3://{self.new_bucket_name} --force[/yellow]\n"
                           "\n[bold]WARNING:[/bold] This will delete all data in the bucket!\n"
                           "Or use a different bucket_name_prefix in your configuration."
            })

        # HIGH PRIORITY FIX: Check for S3 repository plugin (only for ES 7.x and below)
        # NOTE: Elasticsearch 8.x+ has built-in S3 repository support, no plugin needed
        self.loggit.debug("Checking S3 repository support")
        try:
            # Get Elasticsearch version
            cluster_info = self.client.info()
            es_version = cluster_info.get("version", {}).get("number", "0.0.0")
            major_version = int(es_version.split(".")[0])

            if major_version < 8:
                # ES 7.x and below require the repository-s3 plugin
                self.loggit.debug(
                    "Elasticsearch %s detected - checking for S3 repository plugin",
                    es_version
                )

                # Get cluster plugins
                nodes_info = self.client.nodes.info(node_id="_all", metric="plugins")

                # Check if any node has the S3 plugin
                has_s3_plugin = False
                for node_id, node_data in nodes_info.get("nodes", {}).items():
                    plugins = node_data.get("plugins", [])
                    for plugin in plugins:
                        if plugin.get("name") == "repository-s3":
                            has_s3_plugin = True
                            self.loggit.debug("Found S3 plugin on node %s", node_id)
                            break
                    if has_s3_plugin:
                        break

                if not has_s3_plugin:
                    errors.append({
                        "issue": "Elasticsearch S3 repository plugin is not installed",
                        "solution": "Install the S3 repository plugin on all Elasticsearch nodes:\n"
                                   "  [yellow]bin/elasticsearch-plugin install repository-s3[/yellow]\n"
                                   "  Then restart all Elasticsearch nodes.\n"
                                   "  See: https://www.elastic.co/guide/en/elasticsearch/plugins/current/repository-s3.html"
                    })
                else:
                    self.loggit.debug("S3 repository plugin is installed")
            else:
                # ES 8.x+ has built-in S3 support
                self.loggit.debug(
                    "Elasticsearch %s detected - S3 repository support is built-in",
                    es_version
                )
        except Exception as e:
            self.loggit.warning("Could not verify S3 repository support: %s", e)
            # Don't add to errors - this is a soft check that may fail due to permissions

        # If any errors were found, display them all and raise exception
        if errors:
            if self.porcelain:
                # Machine-readable output: tab-separated values
                for error in errors:
                    # Extract clean text from rich markup
                    issue_text = error['issue'].replace('[cyan]', '').replace('[/cyan]', '').replace('[yellow]', '').replace('[/yellow]', '').replace('[bold]', '').replace('[/bold]', '').replace('\n', ' ')
                    print(f"ERROR\tprecondition\t{issue_text}")
            else:
                self.console.print("\n[bold red]Setup Preconditions Failed[/bold red]\n", style="bold")

                for i, error in enumerate(errors, 1):
                    self.console.print(Panel(
                        f"[bold]Issue:[/bold]\n{error['issue']}\n\n"
                        f"[bold]Solution:[/bold]\n{error['solution']}",
                        title=f"[bold red]Error {i} of {len(errors)}[/bold red]",
                        border_style="red",
                        expand=False
                    ))
                    self.console.print()  # Add spacing between panels

                # Create summary error message
                summary = f"Found {len(errors)} precondition error{'s' if len(errors) > 1 else ''} that must be resolved before setup can proceed."
                self.console.print(Panel(
                    f"[bold]{summary}[/bold]\n\n"
                    "Deepfreeze setup requires a clean environment. Please resolve the issues above and try again.",
                    title="[bold red]Setup Cannot Continue[/bold red]",
                    border_style="red",
                    expand=False
                ))

            summary = f"Found {len(errors)} precondition error{'s' if len(errors) > 1 else ''} that must be resolved before setup can proceed."
            raise PreconditionError(summary)

    def do_dry_run(self) -> None:
        """
        Perform a dry-run of the setup process.

        :return: None
        :rtype: None
        """
        self.loggit.info("DRY-RUN MODE.  No changes will be made.")
        msg = f"DRY-RUN: deepfreeze setup of {self.new_repo_name} backed by {self.new_bucket_name}, with base path {self.base_path}."
        self.loggit.info(msg)
        self._check_preconditions()

        self.loggit.info("DRY-RUN: Creating bucket %s", self.new_bucket_name)
        create_repo(
            self.client,
            self.new_repo_name,
            self.new_bucket_name,
            self.base_path,
            self.settings.canned_acl,
            self.settings.storage_class,
            dry_run=True,
        )

    def do_action(self) -> None:
        """
        Perform setup steps to create initial bucket and repository and save settings.

        :return: None
        :rtype: None
        """
        self.loggit.debug("Starting Setup action")

        try:
            # Check preconditions
            self._check_preconditions()

            # Create settings index and save settings
            self.loggit.info("Creating settings index and saving configuration")
            try:
                ensure_settings_index(self.client, create_if_missing=True)
                save_settings(self.client, self.settings)
            except Exception as e:
                if self.porcelain:
                    print(f"ERROR\tsettings_index\t{str(e)}")
                else:
                    self.console.print(Panel(
                        f"[bold]Failed to create settings index or save configuration[/bold]\n\n"
                        f"Error: {escape(str(e))}\n\n"
                        f"[bold]Possible Solutions:[/bold]\n"
                        f"  • Check Elasticsearch connection and permissions\n"
                        f"  • Verify the cluster is healthy and has capacity\n"
                        f"  • Check Elasticsearch logs for details",
                        title="[bold red]Settings Index Error[/bold red]",
                        border_style="red",
                        expand=False
                    ))
                raise

            # Create S3 bucket
            # ENHANCED LOGGING: Log bucket creation parameters
            self.loggit.info(
                "Creating S3 bucket %s with ACL=%s, storage_class=%s",
                self.new_bucket_name,
                self.settings.canned_acl,
                self.settings.storage_class
            )
            self.loggit.debug(
                "Full bucket creation parameters: bucket=%s, ACL=%s, storage_class=%s, provider=%s",
                self.new_bucket_name,
                self.settings.canned_acl,
                self.settings.storage_class,
                self.settings.provider
            )
            try:
                self.s3.create_bucket(self.new_bucket_name)
                self.loggit.info("Successfully created S3 bucket %s", self.new_bucket_name)
            except Exception as e:
                if self.porcelain:
                    print(f"ERROR\ts3_bucket\t{self.new_bucket_name}\t{str(e)}")
                else:
                    self.console.print(Panel(
                        f"[bold]Failed to create S3 bucket [cyan]{self.new_bucket_name}[/cyan][/bold]\n\n"
                        f"Error: {escape(str(e))}\n\n"
                        f"[bold]Possible Solutions:[/bold]\n"
                        f"  • Check AWS credentials and permissions\n"
                        f"  • Verify IAM policy allows s3:CreateBucket\n"
                        f"  • Check if bucket name is globally unique\n"
                        f"  • Verify AWS region settings\n"
                        f"  • Check AWS account limits for S3 buckets",
                        title="[bold red]S3 Bucket Creation Error[/bold red]",
                        border_style="red",
                        expand=False
                    ))
                raise

            # Create repository
            # ENHANCED LOGGING: Log repository configuration
            self.loggit.info("Creating repository %s", self.new_repo_name)
            self.loggit.debug(
                "Repository configuration: name=%s, bucket=%s, base_path=%s, ACL=%s, storage_class=%s",
                self.new_repo_name,
                self.new_bucket_name,
                self.base_path,
                self.settings.canned_acl,
                self.settings.storage_class
            )
            try:
                create_repo(
                    self.client,
                    self.new_repo_name,
                    self.new_bucket_name,
                    self.base_path,
                    self.settings.canned_acl,
                    self.settings.storage_class,
                )
                self.loggit.info("Successfully created repository %s", self.new_repo_name)
            except Exception as e:
                if self.porcelain:
                    print(f"ERROR\trepository\t{self.new_repo_name}\t{str(e)}")
                else:
                    self.console.print(Panel(
                        f"[bold]Failed to create repository [cyan]{self.new_repo_name}[/cyan][/bold]\n\n"
                        f"Error: {escape(str(e))}\n\n"
                        f"[bold]Possible Solutions:[/bold]\n"
                        f"  • Verify Elasticsearch has S3 plugin installed\n"
                        f"  • Check AWS credentials are configured in Elasticsearch keystore\n"
                        f"  • Verify S3 bucket [cyan]{self.new_bucket_name}[/cyan] is accessible\n"
                        f"  • Check repository settings (ACL, storage class, etc.)\n"
                        f"  • Review Elasticsearch logs for detailed error messages",
                        title="[bold red]Repository Creation Error[/bold red]",
                        border_style="red",
                        expand=False
                    ))
                raise

            # Optionally create sample ILM policy
            if self.create_sample_ilm_policy:
                policy_name = self.ilm_policy_name
                policy_body = {
                    "policy": {
                        "phases": {
                            "hot": {
                                "min_age": "0ms",
                                "actions": {
                                    "rollover": {"max_size": "45gb", "max_age": "7d"}
                                },
                            },
                            "frozen": {
                                "min_age": "14d",
                                "actions": {
                                    "searchable_snapshot": {
                                        "snapshot_repository": self.new_repo_name
                                    }
                                },
                            },
                            "delete": {
                                "min_age": "365d",
                                "actions": {
                                    "delete": {"delete_searchable_snapshot": False}
                                },
                            },
                        }
                    }
                }
                self.loggit.info("Creating ILM policy %s", policy_name)
                self.loggit.debug("ILM policy body: %s", policy_body)
                try:
                    create_ilm_policy(
                        client=self.client, policy_name=policy_name, policy_body=policy_body
                    )
                except Exception as e:
                    # ILM policy creation is optional, so just warn but don't fail
                    if self.porcelain:
                        print(f"WARNING\tilm_policy\t{policy_name}\t{str(e)}")
                    else:
                        self.console.print(Panel(
                            f"[bold yellow]Warning: Failed to create sample ILM policy[/bold yellow]\n\n"
                            f"Error: {escape(str(e))}\n\n"
                            f"Setup will continue, but you'll need to create the ILM policy manually.\n"
                            f"This is not a critical error.",
                            title="[bold yellow]ILM Policy Warning[/bold yellow]",
                            border_style="yellow",
                            expand=False
                        ))
                    self.loggit.warning("Failed to create sample ILM policy: %s", e)

            # Success!
            if self.porcelain:
                # Machine-readable output: tab-separated values
                # Format: SUCCESS\t{repo_name}\t{bucket_name}\t{base_path}
                print(f"SUCCESS\t{self.new_repo_name}\t{self.new_bucket_name}\t{self.base_path}")
                if self.create_sample_ilm_policy:
                    print(f"ILM_POLICY\t{self.ilm_policy_name}\tcreated")
            else:
                self.console.print(Panel(
                    f"[bold green]Setup completed successfully![/bold green]\n\n"
                    f"Repository: [cyan]{self.new_repo_name}[/cyan]\n"
                    f"S3 Bucket: [cyan]{self.new_bucket_name}[/cyan]\n"
                    f"Base Path: [cyan]{escape(self.base_path)}[/cyan]\n\n"
                    f"[bold]Next Steps:[/bold]\n"
                    f"  1. Update your ILM policies to use repository [cyan]{self.new_repo_name}[/cyan]\n"
                    f"  2. Ensure all ILM policies have [yellow]delete_searchable_snapshot: false[/yellow]\n"
                    f"  3. Thawed indices will automatically get per-repository ILM policies\n"
                    f"  4. See: https://www.elastic.co/guide/en/elasticsearch/reference/current/ilm-delete.html",
                    title="[bold green]Deepfreeze Setup Complete[/bold green]",
                    border_style="green",
                    expand=False
                ))

            self.loggit.info("Setup complete. Repository %s is ready to use.", self.new_repo_name)

        except PreconditionError:
            # Precondition errors are already formatted and displayed, just re-raise
            raise
        except Exception as e:
            # Catch any unexpected errors
            if self.porcelain:
                print(f"ERROR\tunexpected\t{str(e)}")
            else:
                self.console.print(Panel(
                    f"[bold]An unexpected error occurred during setup[/bold]\n\n"
                    f"Error: {escape(str(e))}\n\n"
                    f"[bold]What to do:[/bold]\n"
                    f"  • Check the logs for detailed error information\n"
                    f"  • Verify all prerequisites are met (AWS credentials, ES connection, etc.)\n"
                    f"  • You may need to manually clean up any partially created resources\n"
                    f"  • Run [yellow]curator_cli deepfreeze cleanup[/yellow] to remove any partial state",
                    title="[bold red]Unexpected Setup Error[/bold red]",
                    border_style="red",
                    expand=False
                ))
            self.loggit.error("Unexpected error during setup: %s", e, exc_info=True)
            raise
