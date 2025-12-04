"""Rotate action for deepfreeze"""

# pylint: disable=too-many-arguments,too-many-instance-attributes, raise-missing-from

import logging
from datetime import datetime

from elasticsearch8 import Elasticsearch
from rich.console import Console
from rich.panel import Panel
from rich.markup import escape

from deepfreeze.constants import STATUS_INDEX, THAW_STATE_FROZEN
from deepfreeze.exceptions import ActionError, MissingIndexError, MissingSettingsError
from deepfreeze.helpers import Settings
from deepfreeze.s3client import s3_client_factory
from deepfreeze.utilities import (
    create_repo,
    create_versioned_ilm_policy,
    get_composable_templates,
    get_ilm_policy,
    get_index_templates,
    get_matching_repos,
    get_next_suffix,
    get_settings,
    push_to_glacier,
    save_settings,
    unmount_repo,
    update_template_ilm_policy,
)


class Rotate:
    """
    Rotate creates a new repository, updates ILM policies, moves old data to Glacier,
    and optionally unmounts old repositories.

    :param client: A client connection object
    :param keep: Number of repositories to keep mounted (default: 1)
    :param year: Year override for date-style rotation
    :param month: Month override for date-style rotation

    :methods:
        do_dry_run: Perform a dry-run of the rotation
        do_action: Perform the rotation

    :example:
        >>> from deepfreeze.actions import Rotate
        >>> rotate = Rotate(client, keep=2)
        >>> rotate.do_action()
    """

    def __init__(
        self,
        client: Elasticsearch,
        keep: int = 1,
        year: int = None,
        month: int = None,
        porcelain: bool = False,
    ) -> None:
        self.loggit = logging.getLogger("deepfreeze.actions.rotate")
        self.loggit.debug("Initializing Deepfreeze Rotate")

        # Console for STDERR output
        self.console = Console(stderr=True)

        self.client = client
        self.keep = keep
        self.year = year
        self.month = month
        self.porcelain = porcelain

        # Will be loaded during action
        self.settings = None
        self.s3 = None

        self.loggit.debug("Deepfreeze Rotate initialized")

    def _load_settings(self) -> Settings:
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

        return self.settings

    def _create_new_repository(self, dry_run: bool = False) -> tuple:
        """
        Create a new repository and bucket.

        :param dry_run: If True, don't actually create anything
        :return: Tuple of (repo_name, bucket_name, base_path)
        """
        # Calculate next suffix
        next_suffix = get_next_suffix(
            self.settings.style,
            self.settings.last_suffix,
            self.year,
            self.month,
        )

        new_repo_name = f"{self.settings.repo_name_prefix}-{next_suffix}"

        if self.settings.rotate_by == "bucket":
            new_bucket_name = f"{self.settings.bucket_name_prefix}-{next_suffix}"
            base_path = self.settings.base_path_prefix
        else:
            new_bucket_name = self.settings.bucket_name_prefix
            base_path = f"{self.settings.base_path_prefix}-{next_suffix}"

        self.loggit.info(
            "Creating new repository %s (bucket: %s, base_path: %s)",
            new_repo_name,
            new_bucket_name,
            base_path,
        )

        if not dry_run:
            # Create bucket if rotating by bucket
            if self.settings.rotate_by == "bucket":
                if not self.s3.bucket_exists(new_bucket_name):
                    self.s3.create_bucket(new_bucket_name)
                    self.loggit.info("Created S3 bucket %s", new_bucket_name)
                else:
                    self.loggit.info("S3 bucket %s already exists", new_bucket_name)

            # Create repository in Elasticsearch
            create_repo(
                self.client,
                new_repo_name,
                new_bucket_name,
                base_path,
                self.settings.canned_acl,
                self.settings.storage_class,
            )

            # Update last_suffix in settings
            self.settings.last_suffix = next_suffix
            save_settings(self.client, self.settings)

        return new_repo_name, new_bucket_name, base_path, next_suffix

    def _update_ilm_policies(
        self, new_repo_name: str, old_suffix: str, new_suffix: str, dry_run: bool = False
    ) -> list:
        """
        Update ILM policies to use the new repository.

        For policies referencing the old repo:
        1. Create a versioned copy of the policy
        2. Update templates to use the new versioned policy

        :param new_repo_name: Name of the new repository
        :param old_suffix: The previous suffix (e.g., "000001")
        :param new_suffix: The new suffix (e.g., "000002")
        :param dry_run: If True, don't actually update anything
        :return: List of updated policy names
        """
        updated_policies = []

        if not self.settings.ilm_policy_name:
            self.loggit.debug("No ILM policy configured, skipping ILM updates")
            return updated_policies

        # Get the base ILM policy
        base_policy = get_ilm_policy(self.client, self.settings.ilm_policy_name)
        if not base_policy:
            self.loggit.warning(
                "ILM policy %s not found, skipping ILM updates",
                self.settings.ilm_policy_name,
            )
            return updated_policies

        old_policy_name = f"{self.settings.ilm_policy_name}-{old_suffix}"
        new_policy_name = f"{self.settings.ilm_policy_name}-{new_suffix}"

        # Check if old versioned policy exists
        old_policy = get_ilm_policy(self.client, old_policy_name)
        if old_policy:
            # Use the old versioned policy as the base
            base_policy_body = old_policy.get("policy", {})
        else:
            # Use the base policy
            base_policy_body = base_policy.get("policy", {})
            old_policy_name = self.settings.ilm_policy_name

        self.loggit.info(
            "Creating versioned ILM policy %s from %s",
            new_policy_name,
            old_policy_name,
        )

        if not dry_run:
            try:
                # Create new versioned policy
                create_versioned_ilm_policy(
                    self.client,
                    self.settings.ilm_policy_name,
                    base_policy_body,
                    new_repo_name,
                    new_suffix,
                )
                updated_policies.append(new_policy_name)

                # Update templates to use the new policy
                self._update_templates_ilm_policy(
                    old_policy_name, new_policy_name, dry_run
                )

            except Exception as e:
                self.loggit.error("Failed to update ILM policies: %s", e)

        return updated_policies

    def _update_templates_ilm_policy(
        self, old_policy_name: str, new_policy_name: str, dry_run: bool = False
    ) -> list:
        """
        Update index templates to use the new ILM policy.

        :param old_policy_name: The old policy name
        :param new_policy_name: The new policy name
        :param dry_run: If True, don't actually update anything
        :return: List of updated template names
        """
        updated_templates = []

        if dry_run:
            return updated_templates

        # Check composable templates
        try:
            composable = get_composable_templates(self.client)
            for template in composable.get("index_templates", []):
                template_name = template.get("name")
                template_data = template.get("index_template", {})

                # Check if template uses the old policy
                ilm_policy = (
                    template_data.get("template", {})
                    .get("settings", {})
                    .get("index", {})
                    .get("lifecycle", {})
                    .get("name")
                )

                if ilm_policy == old_policy_name:
                    self.loggit.info(
                        "Updating composable template %s: %s -> %s",
                        template_name,
                        old_policy_name,
                        new_policy_name,
                    )
                    if update_template_ilm_policy(
                        self.client,
                        template_name,
                        old_policy_name,
                        new_policy_name,
                        is_composable=True,
                    ):
                        updated_templates.append(template_name)

        except Exception as e:
            self.loggit.warning("Error checking composable templates: %s", e)

        # Check legacy templates
        try:
            legacy = get_index_templates(self.client)
            for template_name, template_data in legacy.items():
                ilm_policy = (
                    template_data.get("settings", {})
                    .get("index", {})
                    .get("lifecycle", {})
                    .get("name")
                )

                if ilm_policy == old_policy_name:
                    self.loggit.info(
                        "Updating legacy template %s: %s -> %s",
                        template_name,
                        old_policy_name,
                        new_policy_name,
                    )
                    if update_template_ilm_policy(
                        self.client,
                        template_name,
                        old_policy_name,
                        new_policy_name,
                        is_composable=False,
                    ):
                        updated_templates.append(template_name)

        except Exception as e:
            self.loggit.warning("Error checking legacy templates: %s", e)

        return updated_templates

    def _archive_old_repos(self, dry_run: bool = False) -> list:
        """
        Archive old repositories to Glacier and unmount them.

        :param dry_run: If True, don't actually archive anything
        :return: List of archived repository names
        """
        archived_repos = []

        # Get all repos matching our prefix
        repos = get_matching_repos(
            self.client, self.settings.repo_name_prefix, mounted=True
        )

        # Sort by name (which includes suffix)
        repos = sorted(repos, key=lambda r: r.name)

        # Keep the newest 'keep' repos mounted
        repos_to_archive = repos[: -self.keep] if len(repos) > self.keep else []

        for repo in repos_to_archive:
            self.loggit.info("Archiving repository %s to Glacier", repo.name)

            if not dry_run:
                try:
                    # Push all objects to Glacier
                    push_to_glacier(self.s3, repo)

                    # Unmount the repository
                    unmounted_repo = unmount_repo(self.client, repo.name)

                    # Update thaw state to frozen
                    unmounted_repo.thaw_state = THAW_STATE_FROZEN
                    unmounted_repo.persist(self.client)

                    archived_repos.append(repo.name)
                    self.loggit.info(
                        "Successfully archived and unmounted %s", repo.name
                    )

                except Exception as e:
                    self.loggit.error("Failed to archive %s: %s", repo.name, e)

            else:
                archived_repos.append(repo.name)

        return archived_repos

    def do_dry_run(self) -> None:
        """
        Perform a dry-run of the rotation.

        :return: None
        :rtype: None
        """
        self.loggit.info("DRY-RUN MODE.  No changes will be made.")

        try:
            self._load_settings()

            # Show what would be created
            new_repo, new_bucket, base_path, new_suffix = self._create_new_repository(
                dry_run=True
            )

            if self.porcelain:
                print(f"DRY_RUN\tnew_repository\t{new_repo}\t{new_bucket}\t{base_path}")
            else:
                self.console.print(
                    Panel(
                        f"[bold]Would create new repository:[/bold]\n"
                        f"  Name: [cyan]{new_repo}[/cyan]\n"
                        f"  Bucket: [cyan]{new_bucket}[/cyan]\n"
                        f"  Base Path: [cyan]{escape(base_path)}[/cyan]",
                        title="[bold blue]Dry Run - New Repository[/bold blue]",
                        border_style="blue",
                        expand=False,
                    )
                )

            # Show what repos would be archived
            repos_to_archive = self._archive_old_repos(dry_run=True)
            if repos_to_archive:
                if self.porcelain:
                    for repo in repos_to_archive:
                        print(f"DRY_RUN\tarchive_repository\t{repo}")
                else:
                    archive_list = "\n".join(
                        [f"  - [yellow]{r}[/yellow]" for r in repos_to_archive]
                    )
                    self.console.print(
                        Panel(
                            f"[bold]Would archive {len(repos_to_archive)} repositories to Glacier:[/bold]\n{archive_list}",
                            title="[bold blue]Dry Run - Archive Repositories[/bold blue]",
                            border_style="blue",
                            expand=False,
                        )
                    )

            # Show ILM policy updates
            if self.settings.ilm_policy_name:
                old_suffix = self.settings.last_suffix
                if self.porcelain:
                    print(
                        f"DRY_RUN\tilm_policy\t{self.settings.ilm_policy_name}-{old_suffix}\t{self.settings.ilm_policy_name}-{new_suffix}"
                    )
                else:
                    self.console.print(
                        Panel(
                            f"[bold]Would update ILM policy:[/bold]\n"
                            f"  Create: [cyan]{self.settings.ilm_policy_name}-{new_suffix}[/cyan]\n"
                            f"  From: [yellow]{self.settings.ilm_policy_name}-{old_suffix}[/yellow]",
                            title="[bold blue]Dry Run - ILM Policy[/bold blue]",
                            border_style="blue",
                            expand=False,
                        )
                    )

        except (MissingIndexError, MissingSettingsError) as e:
            if self.porcelain:
                print(f"ERROR\t{type(e).__name__}\t{str(e)}")
            else:
                self.console.print(f"[red]Error: {e}[/red]")
            raise

    def do_action(self) -> None:
        """
        Perform the rotation.

        :return: None
        :rtype: None
        """
        self.loggit.debug("Starting Rotate action")

        try:
            self._load_settings()
            old_suffix = self.settings.last_suffix

            # Create new repository
            new_repo, new_bucket, base_path, new_suffix = self._create_new_repository()

            if self.porcelain:
                print(f"CREATED\trepository\t{new_repo}\t{new_bucket}\t{base_path}")
            else:
                self.console.print(
                    Panel(
                        f"[bold green]Created new repository[/bold green]\n"
                        f"  Name: [cyan]{new_repo}[/cyan]\n"
                        f"  Bucket: [cyan]{new_bucket}[/cyan]\n"
                        f"  Base Path: [cyan]{escape(base_path)}[/cyan]",
                        title="[bold green]New Repository Created[/bold green]",
                        border_style="green",
                        expand=False,
                    )
                )

            # Update ILM policies
            updated_policies = self._update_ilm_policies(
                new_repo, old_suffix, new_suffix
            )
            if updated_policies:
                if self.porcelain:
                    for policy in updated_policies:
                        print(f"UPDATED\tilm_policy\t{policy}")
                else:
                    policy_list = "\n".join(
                        [f"  - [cyan]{p}[/cyan]" for p in updated_policies]
                    )
                    self.console.print(
                        Panel(
                            f"[bold]Updated ILM policies:[/bold]\n{policy_list}",
                            title="[bold green]ILM Policies Updated[/bold green]",
                            border_style="green",
                            expand=False,
                        )
                    )

            # Archive old repositories
            archived = self._archive_old_repos()
            if archived:
                if self.porcelain:
                    for repo in archived:
                        print(f"ARCHIVED\trepository\t{repo}")
                else:
                    archive_list = "\n".join(
                        [f"  - [yellow]{r}[/yellow]" for r in archived]
                    )
                    self.console.print(
                        Panel(
                            f"[bold]Archived {len(archived)} repositories to Glacier:[/bold]\n{archive_list}",
                            title="[bold green]Repositories Archived[/bold green]",
                            border_style="green",
                            expand=False,
                        )
                    )

            # Final summary
            if not self.porcelain:
                self.console.print(
                    Panel(
                        f"[bold green]Rotation completed successfully![/bold green]\n\n"
                        f"New repository: [cyan]{new_repo}[/cyan]\n"
                        f"Policies updated: {len(updated_policies)}\n"
                        f"Repositories archived: {len(archived)}\n\n"
                        f"[bold]Next steps:[/bold]\n"
                        f"  - Verify ILM policies are using the new repository\n"
                        f"  - Monitor searchable snapshot transitions\n"
                        f"  - Run [yellow]deepfreeze status[/yellow] to verify state",
                        title="[bold green]Rotation Complete[/bold green]",
                        border_style="green",
                        expand=False,
                    )
                )

            self.loggit.info("Rotation completed. New repository: %s", new_repo)

        except (MissingIndexError, MissingSettingsError) as e:
            if self.porcelain:
                print(f"ERROR\t{type(e).__name__}\t{str(e)}")
            else:
                self.console.print(
                    Panel(
                        f"[bold]Deepfreeze is not initialized[/bold]\n\n"
                        f"Error: {escape(str(e))}\n\n"
                        f"Run [yellow]deepfreeze setup[/yellow] first.",
                        title="[bold red]Rotation Failed[/bold red]",
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
                        f"[bold]Rotation failed[/bold]\n\n"
                        f"Error: {escape(str(e))}\n\n"
                        f"Check logs for details.",
                        title="[bold red]Rotation Error[/bold red]",
                        border_style="red",
                        expand=False,
                    )
                )
            self.loggit.error("Rotation failed: %s", e, exc_info=True)
            raise
