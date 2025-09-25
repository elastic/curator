"""Rotate action for deepfreeae"""

# pylint: disable=too-many-arguments,too-many-instance-attributes, raise-missing-from

import logging
import sys

from elasticsearch import Elasticsearch

from curator.actions.deepfreeze.constants import STATUS_INDEX
from curator.actions.deepfreeze.helpers import Repository
from curator.actions.deepfreeze.utilities import (
    create_repo,
    decode_date,
    ensure_settings_index,
    get_all_indices_in_repo,
    get_matching_repo_names,
    get_matching_repos,
    get_next_suffix,
    get_settings,
    get_timestamp_range,
    push_to_glacier,
    save_settings,
    unmount_repo,
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
        year: int = None,
        month: int = None,
    ) -> None:
        self.loggit = logging.getLogger("curator.actions.deepfreeze")
        self.loggit.debug("Initializing Deepfreeze Rotate")

        self.settings = get_settings(client)
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
            self.client, self.settings.repo_name_prefix
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
        self.loggit.info("Deepfreeze initialized")

    def update_repo_date_range(self, dry_run=False):
        """
        Update the date ranges for all repositories in the status index.

        :return: None
        :rtype: None

        """
        self.loggit.debug("Updating repo date ranges")
        # Get the repo objects (not names) which match our prefix
        repos = get_matching_repos(
            self.client, self.settings.repo_name_prefix, mounted=True
        )
        self.loggit.debug("Found %s matching repos", len(repos))
        # Now loop through the repos, updating the date range for each
        for repo in repos:
            self.loggit.debug("Updating date range for %s", repo.name)
            indices = get_all_indices_in_repo(self.client, repo.name)
            self.loggit.debug("Checking %s indices for existence", len(indices))
            filtered = []
            for index in indices:
                index = f"partial-{index}"
                if self.client.indices.exists(index=index):
                    filtered.append(index)
            self.loggit.debug("Found %s indices still mounted", len(filtered))
            if filtered:
                earliest, latest = get_timestamp_range(self.client, filtered)
                self.loggit.debug(
                    "BDW: For repo %s: Earliest: %s, Latest: %s",
                    repo.name,
                    earliest,
                    latest,
                )
                changed = False
                if not repo.start or earliest < decode_date(repo.start):
                    repo.start = earliest
                    changed = True
                if not repo.end or latest > decode_date(repo.end):
                    repo.end = latest
                    changed = True
                if not dry_run and changed:
                    query = {"query": {"term": {"name.keyword": repo.name}}}
                    response = self.client.search(index=STATUS_INDEX, body=query)
                    if response["hits"]["total"]["value"] > 0:
                        self.loggit.debug("UDRR: Updating Repo %s", repo.name)
                        self.client.update(
                            index=STATUS_INDEX,
                            id=response["hits"]["hits"][0]["_id"],
                            body={"doc": repo.to_dict()},
                        )
                    else:
                        self.loggit.debug("UDRR: Creating Repo %s", repo.name)
                        self.client.index(index=STATUS_INDEX, body=repo.to_dict())
                elif not changed:
                    self.loggit.debug("No change to date range for %s", repo.name)
            else:
                self.loggit.debug("No update; no indices found for %s", repo.name)

    def update_ilm_policies(self, dry_run=False) -> None:
        """
        Loop through all existing IML policies looking for ones which reference
        the latest_repo and update them to use the new repo instead.

        :param dry_run: If True, do not actually update the policies
        :type dry_run: bool

        :return: None
        :rtype: None

        :raises Exception: If the policy cannot be updated
        :raises Exception: If the policy does not exist
        """
        self.loggit.debug("Updating ILM policies")
        if self.latest_repo == self.new_repo_name:
            self.loggit.info("Already on the latest repo")
            sys.exit(0)
        self.loggit.info(
            "Switching from %s to %s", self.latest_repo, self.new_repo_name
        )
        policies = self.client.ilm.get_lifecycle()
        updated_policies = {}
        for policy in policies:
            # Go through these looking for any occurrences of self.latest_repo
            # and change those to use self.new_repo_name instead.
            # TODO: Ensure that delete_searchable_snapshot is set to false or
            # TODO: the snapshot will be deleted when the policy transitions to the
            # TODO: next phase. In this case, raise an error and skip this policy.
            # ? Maybe we don't correct this but flag it as an error?
            p = policies[policy]["policy"]["phases"]
            updated = False
            for phase in p:
                if "searchable_snapshot" in p[phase]["actions"] and (
                    p[phase]["actions"]["searchable_snapshot"]["snapshot_repository"]
                    == self.latest_repo
                ):
                    p[phase]["actions"]["searchable_snapshot"][
                        "snapshot_repository"
                    ] = self.new_repo_name
                    updated = True
            if updated:
                updated_policies[policy] = policies[policy]["policy"]

        # Now, submit the updated policies to _ilm/policy/<policyname>
        if not updated_policies:
            self.loggit.warning("No policies to update")
        else:
            self.loggit.info("Updating %d policies:", len(updated_policies.keys()))
        for pol, body in updated_policies.items():
            self.loggit.info("\t%s", pol)
            self.loggit.debug("Policy body: %s", body)
            if not dry_run:
                self.client.ilm.put_lifecycle(name=pol, policy=body)
            self.loggit.debug("Finished ILM Policy updates")

    def is_thawed(self, repo: str) -> bool:
        """
        Check if a repository is thawed

        :param repo: The name of the repository
        :returns: True if the repository is thawed, False otherwise

        :raises Exception: If the repository does not exist
        """
        # TODO: This might work, but we might also need to check our Repostories.
        self.loggit.debug("Checking if %s is thawed", repo)
        return repo.startswith("thawed-")

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
                unmounted_repo = unmount_repo(self.client, repo)
                push_to_glacier(self.s3, unmounted_repo)
                try:
                    self.loggit.debug("Fetching repo %s doc", repo)
                    repository = Repository.from_elasticsearch(
                        self.client, repo, STATUS_INDEX
                    )
                    self.loggit.debug("Looking for %s, found %s", repo, repository)
                    repository.unmount()
                    self.loggit.debug("preparing to persist %s", repo)
                    repository.persist(self.client)
                    self.loggit.info(
                        "Updated status to unmounted for repo %s", repository.name
                    )
                except Exception as e:
                    self.loggit.error(
                        "Failed to update doc unmounting repo %s: %s", repo, str(e)
                    )
                    raise

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
            self.client,
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

    def do_action(self) -> None:
        """
        Perform high-level repo rotation steps in sequence.

        :return: None
        :rtype: None

        :raises Exception: If the repository cannot be created
        :raises Exception: If the repository already exists
        """
        ensure_settings_index(self.client)
        self.loggit.debug("Saving settings")
        save_settings(self.client, self.settings)
        # Create the new bucket and repo, but only if rotate_by is bucket
        if self.settings.rotate_by == "bucket":
            self.s3.create_bucket(self.new_bucket_name)
        create_repo(
            self.client,
            self.new_repo_name,
            self.new_bucket_name,
            self.base_path,
            self.settings.canned_acl,
            self.settings.storage_class,
        )
        # Go through mounted repos and make sure the date ranges are up-to-date
        self.update_repo_date_range()
        self.update_ilm_policies()
        self.unmount_oldest_repos()
