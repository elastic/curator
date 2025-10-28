"""Rotate action for deepfreeze"""

# pylint: disable=too-many-arguments,too-many-instance-attributes, raise-missing-from

import logging
import sys

from elasticsearch8 import Elasticsearch

from curator.actions.deepfreeze.cleanup import Cleanup
from curator.actions.deepfreeze.constants import STATUS_INDEX
from curator.actions.deepfreeze.helpers import Repository
from curator.actions.deepfreeze.utilities import (
    create_repo,
    create_versioned_ilm_policy,
    ensure_settings_index,
    get_composable_templates,
    get_index_templates,
    get_matching_repo_names,
    get_matching_repos,
    get_next_suffix,
    get_policies_by_suffix,
    get_policies_for_repo,
    get_settings,
    is_policy_safe_to_delete,
    push_to_glacier,
    save_settings,
    unmount_repo,
    update_repository_date_range,
    update_template_ilm_policy,
)
from curator.exceptions import RepositoryException
from curator.s3client import s3_client_factory


class Rotate:
    """
    The Deepfreeze is responsible for managing the repository rotation given
    a config file of user-managed options and settings.

    :param client: A client connection object
    :type client: Elasticsearch
    :param keep: How many repositories to retain, defaults to 6
    :type keep: str
    :param year: Optional year to override current year
    :type year: int
    :param month: Optional month to override current month
    :type month: int

    :raises RepositoryException: If a repository with the given prefix already exists

    :methods:
        update_ilm_policies: Update ILM policies to use the new repository.
        unmount_oldest_repos: Unmount the oldest repositories.
        is_thawed: Check if a repository is thawed.
    """

    def __init__(
        self,
        client: Elasticsearch,
        keep: str = "6",
        year: int = None,  # type: ignore
        month: int = None,  # type: ignore
    ) -> None:
        self.loggit = logging.getLogger("curator.actions.deepfreeze")
        self.loggit.debug("Initializing Deepfreeze Rotate")

        self.settings = get_settings(client)  # type: ignore
        self.loggit.debug("Settings: %s", str(self.settings))

        self.client = client
        self.keep = int(keep)
        self.year = year
        self.month = month
        self.base_path = ""
        self.suffix = get_next_suffix(
            self.settings.style, self.settings.last_suffix, year, month
        )
        self.settings.last_suffix = self.suffix

        self.s3 = s3_client_factory(self.settings.provider)

        self.new_repo_name = f"{self.settings.repo_name_prefix}-{self.suffix}"
        if self.settings.rotate_by == "bucket":
            self.new_bucket_name = f"{self.settings.bucket_name_prefix}-{self.suffix}"
            self.base_path = f"{self.settings.base_path_prefix}"
        else:
            self.new_bucket_name = f"{self.settings.bucket_name_prefix}"
            self.base_path = f"{self.settings.base_path_prefix}-{self.suffix}"

        self.loggit.debug("Getting repo list")
        self.repo_list = get_matching_repo_names(
            self.client, self.settings.repo_name_prefix  # type: ignore
        )
        self.repo_list.sort(reverse=True)
        self.loggit.debug("Repo list: %s", self.repo_list)
        self.latest_repo = ""
        try:
            self.latest_repo = self.repo_list[0]
            self.loggit.debug("Latest repo: %s", self.latest_repo)
        except IndexError:
            raise RepositoryException(
                f"no repositories match {self.settings.repo_name_prefix}"
            )
        if self.new_repo_name in self.repo_list:
            raise RepositoryException(f"repository {self.new_repo_name} already exists")
        if not self.client.indices.exists(index=STATUS_INDEX):
            self.client.indices.create(index=STATUS_INDEX)
            self.loggit.warning("Created index %s", STATUS_INDEX)

        # Validate that ILM policies exist for the current repository
        # This must be checked during initialization to fail fast
        self.loggit.debug("Checking for ILM policies that reference %s", self.latest_repo)
        policies_for_repo = get_policies_for_repo(self.client, self.latest_repo)  # type: ignore
        if not policies_for_repo:
            raise RepositoryException(
                f"No ILM policies found that reference repository {self.latest_repo}. "
                f"Rotation requires existing ILM policies to create versioned copies. "
                f"Please create ILM policies that use searchable_snapshot actions "
                f"with snapshot_repository: {self.latest_repo}, or run setup with "
                f"--create-sample-ilm-policy to create a default policy."
            )
        self.loggit.info(
            "Found %d ILM policies referencing %s",
            len(policies_for_repo),
            self.latest_repo
        )

        self.loggit.info("Deepfreeze initialized")

    def update_repo_date_range(self, dry_run=False):
        """
        Update the date ranges for all repositories in the status index.

        :return: None
        :rtype: None

        """
        self.loggit.debug("Updating repo date ranges")
        # Get ALL repo objects (not just mounted) which match our prefix
        # We need to update date ranges for all repos to avoid gaps in coverage
        repos = get_matching_repos(
            self.client, self.settings.repo_name_prefix, mounted=None  # type: ignore
        )
        self.loggit.debug("Found %s matching repos", len(repos))

        # Update date range for each repository
        for repo in repos:
            self.loggit.debug("Updating date range for %s (mounted: %s)", repo.name, repo.is_mounted)

            if dry_run:
                self.loggit.info("DRY-RUN: Would update date range for %s", repo.name)
                continue

            # Use the shared utility function to update dates
            # It handles multiple index naming patterns and persists automatically
            updated = update_repository_date_range(self.client, repo)  # type: ignore

            if updated:
                self.loggit.debug("Successfully updated date range for %s", repo.name)
            else:
                self.loggit.debug("No date range update for %s", repo.name)

    def update_ilm_policies(self, dry_run=False) -> None:
        """
        Create versioned ILM policies for the new repository and update index templates.

        Instead of modifying existing policies, this creates NEW versioned policies
        (e.g., my-policy-000005) that reference the new repository. Index templates
        are then updated to use the new versioned policies, ensuring new indices use
        the new repository while existing indices keep their old policies.

        :param dry_run: If True, do not actually create policies or update templates
        :type dry_run: bool

        :return: None
        :rtype: None

        :raises Exception: If policies or templates cannot be updated
        """
        self.loggit.debug("Creating versioned ILM policies for new repository")

        if self.latest_repo == self.new_repo_name:
            self.loggit.info("Already on the latest repo")
            sys.exit(0)

        self.loggit.info(
            "Creating versioned policies for transition from %s to %s",
            self.latest_repo,
            self.new_repo_name,
        )

        # Find all policies that reference the latest repository
        # Note: We already validated policies exist during __init__, so this should always succeed
        self.loggit.debug("Searching for policies that reference %s", self.latest_repo)
        policies_to_version = get_policies_for_repo(self.client, self.latest_repo)  # type: ignore

        self.loggit.info(
            "Found %d policies to create versioned copies for: %s",
            len(policies_to_version),
            ", ".join(policies_to_version.keys()),
        )

        # Track policy name mappings (old -> new) for template updates
        policy_mappings = {}

        # Create versioned copies of each policy
        for policy_name, policy_data in policies_to_version.items():
            policy_body = policy_data.get("policy", {})

            # Strip old suffix from policy name if it exists
            # This handles subsequent rotations where policy might be "my-policy-000002"
            # We want base name "my-policy" to create "my-policy-000003"
            base_policy_name = policy_name
            if "-" in policy_name:
                parts = policy_name.rsplit("-", 1)
                # Check if last part looks like a suffix (all digits or date format)
                potential_suffix = parts[1]
                if potential_suffix.isdigit() or (
                    "." in potential_suffix
                    and all(p.isdigit() for p in potential_suffix.split("."))
                ):
                    base_policy_name = parts[0]
                    self.loggit.debug(
                        "Stripped suffix from %s, using base name: %s",
                        policy_name,
                        base_policy_name,
                    )

            # Check for delete_searchable_snapshot setting and warn if True
            for phase_name, phase_config in policy_body.get("phases", {}).items():
                delete_action = phase_config.get("actions", {}).get("delete", {})
                if delete_action.get("delete_searchable_snapshot", False):
                    self.loggit.warning(
                        "Policy %s has delete_searchable_snapshot=true in %s phase. "
                        "Snapshots may be deleted when indices transition!",
                        policy_name,
                        phase_name,
                    )

            if not dry_run:
                try:
                    new_policy_name = create_versioned_ilm_policy(
                        self.client,  # type: ignore
                        base_policy_name,  # Use base name, not full name
                        policy_body,
                        self.new_repo_name,
                        self.suffix,
                    )
                    policy_mappings[policy_name] = new_policy_name
                    self.loggit.info(
                        "Created versioned policy: %s -> %s",
                        policy_name,
                        new_policy_name,
                    )
                except Exception as e:
                    self.loggit.error(
                        "Failed to create versioned policy for %s: %s", policy_name, e
                    )
                    raise
            else:
                new_policy_name = f"{base_policy_name}-{self.suffix}"
                policy_mappings[policy_name] = new_policy_name
                self.loggit.info(
                    "DRY-RUN: Would create policy %s -> %s",
                    policy_name,
                    new_policy_name,
                )

        # Update index templates to use the new versioned policies
        self.loggit.info("Updating index templates to use new versioned policies")
        templates_updated = 0

        # Update composable templates
        try:
            composable_templates = get_composable_templates(self.client)  # type: ignore
            for template_name in composable_templates.get("index_templates", []):
                template_name = template_name["name"]
                for old_policy, new_policy in policy_mappings.items():
                    if not dry_run:
                        try:
                            if update_template_ilm_policy(
                                self.client, template_name, old_policy, new_policy, is_composable=True  # type: ignore
                            ):
                                templates_updated += 1
                                self.loggit.info(
                                    "Updated composable template %s: %s -> %s",
                                    template_name,
                                    old_policy,
                                    new_policy,
                                )
                        except Exception as e:
                            self.loggit.debug(
                                "Could not update template %s: %s", template_name, e
                            )
                    else:
                        self.loggit.info(
                            "DRY-RUN: Would update composable template %s if it uses policy %s",
                            template_name,
                            old_policy,
                        )
        except Exception as e:
            self.loggit.warning("Could not get composable templates: %s", e)

        # Update legacy templates
        try:
            legacy_templates = get_index_templates(self.client)  # type: ignore
            for template_name in legacy_templates.keys():
                for old_policy, new_policy in policy_mappings.items():
                    if not dry_run:
                        try:
                            if update_template_ilm_policy(
                                self.client, template_name, old_policy, new_policy, is_composable=False  # type: ignore
                            ):
                                templates_updated += 1
                                self.loggit.info(
                                    "Updated legacy template %s: %s -> %s",
                                    template_name,
                                    old_policy,
                                    new_policy,
                                )
                        except Exception as e:
                            self.loggit.debug(
                                "Could not update template %s: %s", template_name, e
                            )
                    else:
                        self.loggit.info(
                            "DRY-RUN: Would update legacy template %s if it uses policy %s",
                            template_name,
                            old_policy,
                        )
        except Exception as e:
            self.loggit.warning("Could not get legacy templates: %s", e)

        if templates_updated > 0:
            self.loggit.info("Updated %d index templates", templates_updated)
        else:
            self.loggit.warning("No index templates were updated")

        self.loggit.info("Finished ILM policy versioning and template updates")

    def cleanup_policies_for_repo(self, repo_name: str, dry_run=False) -> None:
        """
        Clean up ILM policies associated with an unmounted repository.

        Finds all policies with the same suffix as the repository and deletes them
        if they are not in use by any indices, data streams, or templates.

        :param repo_name: The repository name (e.g., "deepfreeze-000003")
        :type repo_name: str
        :param dry_run: If True, do not actually delete policies
        :type dry_run: bool

        :return: None
        :rtype: None
        """
        self.loggit.debug("Cleaning up policies for repository %s", repo_name)

        # Extract suffix from repository name
        # Repository format: {prefix}-{suffix}
        try:
            suffix = repo_name.split("-")[-1]
            self.loggit.debug(
                "Extracted suffix %s from repository %s", suffix, repo_name
            )
        except Exception as e:
            self.loggit.error(
                "Could not extract suffix from repository %s: %s", repo_name, e
            )
            return

        # Find all policies with this suffix
        policies_with_suffix = get_policies_by_suffix(self.client, suffix)  # type: ignore

        if not policies_with_suffix:
            self.loggit.info("No policies found with suffix -%s", suffix)
            return

        self.loggit.info(
            "Found %d policies with suffix -%s to evaluate for deletion",
            len(policies_with_suffix),
            suffix,
        )

        deleted_count = 0
        skipped_count = 0

        for policy_name in policies_with_suffix.keys():
            # Check if the policy is safe to delete
            if is_policy_safe_to_delete(self.client, policy_name):  # type: ignore
                if not dry_run:
                    try:
                        self.client.ilm.delete_lifecycle(name=policy_name)
                        deleted_count += 1
                        self.loggit.info(
                            "Deleted policy %s (no longer in use)", policy_name
                        )
                    except Exception as e:
                        self.loggit.error(
                            "Failed to delete policy %s: %s", policy_name, e
                        )
                        skipped_count += 1
                else:
                    self.loggit.info("DRY-RUN: Would delete policy %s", policy_name)
                    deleted_count += 1
            else:
                skipped_count += 1
                self.loggit.info(
                    "Skipping policy %s (still in use by indices/datastreams/templates)",
                    policy_name,
                )

        self.loggit.info(
            "Policy cleanup complete: %d deleted, %d skipped",
            deleted_count,
            skipped_count,
        )

    def is_thawed(self, repo: str) -> bool:
        """
        Check if a repository is thawed by querying the STATUS_INDEX.

        :param repo: The name of the repository
        :returns: True if the repository is thawed, False otherwise
        """
        self.loggit.debug("Checking if %s is thawed", repo)
        try:
            repository = Repository.from_elasticsearch(self.client, repo, STATUS_INDEX)
            if repository is None:
                self.loggit.warning(
                    "Repository %s not found in STATUS_INDEX, assuming not thawed", repo
                )
                return False

            is_thawed = repository.is_thawed
            self.loggit.debug(
                "Repository %s thawed status: %s (mounted: %s)",
                repo,
                is_thawed,
                repository.is_mounted,
            )
            return is_thawed
        except Exception as e:
            self.loggit.error("Error checking thawed status for %s: %s", repo, e)
            # If we can't determine the status, err on the side of caution and assume it's thawed
            # This prevents accidentally unmounting a thawed repo if there's a database issue
            return True

    def unmount_oldest_repos(self, dry_run=False) -> None:
        """
        Take the oldest repos from the list and remove them, only retaining
        the number chosen in the config under "keep".

        :param dry_run: If True, do not actually remove the repositories
        :type dry_run: bool

        :return: None
        :rtype: None

        :raises Exception: If the repository cannot be removed
        """
        # TODO: Use a list of Repositories, not a list of names. Be consistent and always use Repositories.
        self.loggit.debug("Total list: %s", self.repo_list)
        s = self.repo_list[self.keep :]
        self.loggit.debug("Repos to remove: %s", s)
        for repo in s:
            if self.is_thawed(repo):
                self.loggit.warning("Skipping thawed repo %s", repo)
                continue
            self.loggit.info("Removing repo %s", repo)
            if not dry_run:
                # ? Do I want to check for existence of snapshots still mounted from
                # ? the repo here or in unmount_repo?
                unmounted_repo = unmount_repo(self.client, repo)  # type: ignore
                push_to_glacier(self.s3, unmounted_repo)
                try:
                    self.loggit.debug("Fetching repo %s doc", repo)
                    repository = Repository.from_elasticsearch(
                        self.client, repo, STATUS_INDEX
                    )
                    self.loggit.debug("Looking for %s, found %s", repo, repository)
                    repository.unmount()  # type: ignore
                    self.loggit.debug("preparing to persist %s", repo)
                    repository.persist(self.client)  # type: ignore
                    self.loggit.info(
                        "Updated status to unmounted for repo %s", repository.name  # type: ignore
                    )

                    # Clean up ILM policies associated with this repository
                    self.loggit.info(
                        "Cleaning up ILM policies associated with repository %s", repo
                    )
                    self.cleanup_policies_for_repo(repo, dry_run=False)

                except Exception as e:
                    self.loggit.error(
                        "Failed to update doc unmounting repo %s: %s", repo, str(e)
                    )
                    raise
            else:
                self.loggit.info("DRY-RUN: Would clean up policies for repo %s", repo)
                self.cleanup_policies_for_repo(repo, dry_run=True)

    def do_dry_run(self) -> None:
        """
        Perform a dry-run of the rotation process.

        :return: None
        :rtype: None

        :raises Exception: If the repository cannot be created
        :raises Exception: If the repository already exists
        """
        self.loggit.info("DRY-RUN MODE.  No changes will be made.")
        msg = (
            f"DRY-RUN: deepfreeze {self.latest_repo} will be rotated out"
            f" and {self.new_repo_name} will be added & made active."
        )
        self.loggit.info(msg)
        self.loggit.info("DRY-RUN: Creating bucket %s", self.new_bucket_name)
        create_repo(
            self.client,  # type: ignore
            self.new_repo_name,
            self.new_bucket_name,
            self.base_path,
            self.settings.canned_acl,
            self.settings.storage_class,
            dry_run=True,
        )
        self.update_ilm_policies(dry_run=True)
        self.unmount_oldest_repos(dry_run=True)
        self.update_repo_date_range(dry_run=True)
        # Clean up any thawed repositories that have expired
        cleanup = Cleanup(self.client)
        cleanup.do_dry_run()

    def do_action(self) -> None:
        """
        Perform high-level repo rotation steps in sequence.

        :return: None
        :rtype: None

        :raises Exception: If the repository cannot be created
        :raises Exception: If the repository already exists
        """
        ensure_settings_index(self.client)  # type: ignore
        self.loggit.debug("Saving settings")
        save_settings(self.client, self.settings)  # type: ignore

        # HIGH PRIORITY FIX: Add validation and logging for bucket/repo creation
        # Create the new bucket and repo, but only if rotate_by is bucket
        if self.settings.rotate_by == "bucket":
            self.loggit.info("Checking if bucket %s exists before creation", self.new_bucket_name)
            try:
                # create_bucket already checks bucket_exists internally
                self.s3.create_bucket(self.new_bucket_name)
            except Exception as e:
                self.loggit.error(
                    "Failed to create bucket %s: %s. Check S3 permissions and bucket naming rules.",
                    self.new_bucket_name,
                    e,
                    exc_info=True
                )
                raise

        # Verify repository doesn't already exist before creation
        self.loggit.info(
            "Creating repository %s with bucket=%s, base_path=%s, storage_class=%s",
            self.new_repo_name,
            self.new_bucket_name,
            self.base_path,
            self.settings.storage_class
        )
        try:
            existing_repos = self.client.snapshot.get_repository()
            if self.new_repo_name in existing_repos:
                error_msg = f"Repository {self.new_repo_name} already exists in Elasticsearch"
                self.loggit.error(error_msg)
                raise ActionError(error_msg)

            create_repo(
                self.client,  # type: ignore
                self.new_repo_name,
                self.new_bucket_name,
                self.base_path,
                self.settings.canned_acl,
                self.settings.storage_class,
            )
            self.loggit.info("Successfully created repository %s", self.new_repo_name)
        except Exception as e:
            self.loggit.error(
                "Failed to create repository %s: %s",
                self.new_repo_name,
                e,
                exc_info=True
            )
            raise
        # Go through mounted repos and make sure the date ranges are up-to-date
        self.update_repo_date_range()
        self.update_ilm_policies()
        self.unmount_oldest_repos()
        # Clean up any thawed repositories that have expired
        cleanup = Cleanup(self.client)
        cleanup.do_action()
