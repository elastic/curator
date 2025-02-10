"""Deepfreeze action class"""

# pylint: disable=too-many-arguments,too-many-instance-attributes, raise-missing-from

import json
import logging
import re
import sys
from dataclasses import dataclass
from datetime import datetime

from elasticsearch8 import Elasticsearch
from elasticsearch8.exceptions import NotFoundError
from rich import print
from rich.console import Console
from rich.table import Table

from curator.exceptions import ActionError, RepositoryException
from curator.s3client import S3Client, s3_client_factory

STATUS_INDEX = "deepfreeze-status"
SETTINGS_ID = "1"

#
#
# Utility Classes
#
#


class Deepfreeze:
    """
    Allows nesting of actions under the deepfreeze command
    """


@dataclass
class ThawedRepo:
    """
    Data class for a thawed repo and indices
    """

    repo_name: str
    bucket_name: str
    base_path: str
    provider: str
    indices: list = None

    def __init__(self, repo_info: dict, indices: list[str] = None) -> None:
        self.repo_name = repo_info["name"]
        self.bucket_name = repo_info["bucket"]
        self.base_path = repo_info["base_path"]
        self.provider = "aws"
        self.indices = indices

    def add_index(self, index: str) -> None:
        """
        Add an index to the list of indices

        :param index: The index to add
        """
        self.indices.append(index)


class ThawSet(dict[str, ThawedRepo]):
    """
    Data class for thaw settings
    """

    def add(self, thawed_repo: ThawedRepo) -> None:
        """
        Add a thawed repo to the dictionary

        :param thawed_repo: A thawed repo object
        """
        self[thawed_repo.repo_name] = thawed_repo


@dataclass
class Repository:
    """
    Data class for repository
    """

    name: str
    bucket: str
    base_path: str
    start: datetime
    end: datetime
    is_thawed: bool = False
    is_mounted: bool = True
    doctype: str = "repository"

    def __init__(self, repo_hash=None) -> None:
        if repo_hash is not None:
            for key, value in repo_hash.items():
                setattr(self, key, value)

    def to_dict(self) -> dict:
        """
        Convert the Repository object to a dictionary.
        Convert datetime to ISO 8601 string format for JSON compatibility.
        """
        return {
            "name": self.name,
            "bucket": self.bucket,
            "base_path": self.base_path,
            "start": self.start.isoformat(),  # Convert datetime to string
            "end": self.end.isoformat(),  # Convert datetime to string
            "is_thawed": self.is_thawed,
            "is_mounted": self.is_mounted,
            "doctype": self.doctype,
        }

    def to_json(self) -> str:
        """
        Serialize the Repository object to a JSON string.
        """
        return json.dumps(self.to_dict(), indent=4)


@dataclass
class Settings:
    """
    Data class for settings
    """

    doctype: str = "settings"
    repo_name_prefix: str = "deepfreeze"
    bucket_name_prefix: str = "deepfreeze"
    base_path_prefix: str = "snapshots"
    canned_acl: str = "private"
    storage_class: str = "intelligent_tiering"
    provider: str = "aws"
    rotate_by: str = "path"
    style: str = "oneup"
    last_suffix: str = None

    def __init__(self, settings_hash=None) -> None:
        if settings_hash is not None:
            for key, value in settings_hash.items():
                setattr(self, key, value)


#
#
# Utility functions
#
#


def push_to_glacier(s3: S3Client, repo: Repository) -> None:
    """
    Move the repository to Glacier storage class

    :param s3: The S3 client object
    :param repo: The repository to move
    """
    response = s3.list_objects_v2(Bucket=repo.bucket, Prefix=repo.base_path)

    # Check if objects were found
    if "Contents" not in response:
        print(f"No objects found in prefix: {repo.base_path}")
        return

    # Loop through each object and initiate restore for Glacier objects
    count = 0
    for obj in response["Contents"]:
        count += 1

        # Initiate the restore request for each object
        s3.copy_object(
            Bucket=repo.bucket,
            Key=obj["key"],
            CopySource={"Bucket": repo.bucket, "Key": obj["Key"]},
            StorageClass="GLACIER",
        )

    print("Freezing to Glacier initiated for {count} objects")


def get_cluster_name(client: Elasticsearch) -> str:
    """
    Connects to the Elasticsearch cluster and returns its name.

    :param es_host: The URL of the Elasticsearch instance (default: "http://localhost:9200").
    :return: The name of the Elasticsearch cluster.
    """
    try:
        cluster_info = client.cluster.health()
        return cluster_info.get("cluster_name", "Unknown Cluster")
    except Exception as e:
        return f"Error: {e}"


def thaw_repo(
    s3: S3Client,
    bucket_name: str,
    base_path: str,
    restore_days: int = 7,
    retrieval_tier: str = "Standard",
) -> None:
    """
    Thaw a repository in Elasticsearch

    :param client: A client connection object
    :param bucket_name: The name of the bucket
    :param object_key: The key of the object
    :param restore_days: Number of days to keep the object accessible
    :param retrieval_tier: 'Standard' or 'Expedited' or 'Bulk'

    :raises: NotFoundError

    """
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=base_path)

    # Check if objects were found
    if "Contents" not in response:
        print(f"No objects found in prefix: {base_path}")
        return

    # Loop through each object and initiate restore for Glacier objects
    count = 0
    for obj in response["Contents"]:
        count += 1

        # Initiate the restore request for each object
        # s3.restore_object(
        #     Bucket=bucket_name,
        #     Key=obj["Key"],
        #     RestoreRequest={
        #         "Days": restore_days,
        #         "GlacierJobParameters": {
        #             "Tier": retrieval_tier  # You can change to 'Expedited' or 'Bulk' if needed
        #         },
        #     },
        # )

    print("Restore request initiated for {count} objects")


def get_all_indices_in_repo(client: Elasticsearch, repository: str) -> list[str]:
    """
    Retrieve all indices from snapshots in the given repository.

    :param client: A client connection object
    :param repository: The name of the repository
    :returns: A list of indices
    :rtype: list[str]
    """
    snapshots = client.snapshot.get(repository=repository, snapshot="_all")
    indices = set()

    for snapshot in snapshots["snapshots"]:
        indices.update(snapshot["indices"])

    logging.debug("Indices: %s", indices)

    return list(indices)


def get_timestamp_range(
    client: Elasticsearch, indices: list[str]
) -> tuple[datetime, datetime]:
    """
    Retrieve the earliest and latest @timestamp values from the given indices.

    :param client: A client connection object
    :param indices: A list of indices
    :returns: A tuple containing the earliest and latest @timestamp values
    :rtype: tuple[datetime, datetime]
    """
    if not indices:
        return None, None

    query = {
        "size": 0,
        "aggs": {
            "earliest": {"min": {"field": "@timestamp"}},
            "latest": {"max": {"field": "@timestamp"}},
        },
    }

    response = client.search(index=",".join(indices), body=query)

    earliest = response["aggregations"]["earliest"]["value_as_string"]
    latest = response["aggregations"]["latest"]["value_as_string"]

    logging.debug("Earliest: %s, Latest: %s", earliest, latest)

    return datetime.fromisoformat(earliest), datetime.fromisoformat(latest)


def ensure_settings_index(client: Elasticsearch) -> None:
    """
    Ensure that the status index exists in Elasticsearch.

    :param client: A client connection object
    """
    loggit = logging.getLogger("curator.actions.deepfreeze")
    if not client.indices.exists(index=STATUS_INDEX):
        loggit.info("Creating index %s", STATUS_INDEX)
        client.indices.create(index=STATUS_INDEX)


def get_settings(client: Elasticsearch) -> Settings:
    """
    Get the settings for the deepfreeze operation from the status index.

    :param client: A client connection object
    :returns: The settings
    :rtype: dict
    """
    loggit = logging.getLogger("curator.actions.deepfreeze")
    try:
        doc = client.get(index=STATUS_INDEX, id=SETTINGS_ID)
        loggit.info("Settings document found")
        return Settings(doc["_source"])
    except NotFoundError:
        loggit.info("Settings document not found")
        return None


def get_repos_to_thaw(
    client: Elasticsearch, start: datetime, end: datetime
) -> list[Repository]:
    """
    Get the list of repos that were active during the given time range.

    :param client: A client connection object
    :param start: The start of the time range
    :param end: The end of the time range
    :returns: The repos
    :rtype: list[Repository] A list of repository names
    """
    loggit = logging.getLogger("curator.actions.deepfreeze")
    repos = get_unmounted_repos(client)
    overlapping_repos = []
    for repo in repos:
        if repo.start <= end and repo.end >= start:
            overlapping_repos.append(repo)
    loggit.info("Found overlapping repos: %s", overlapping_repos)
    return overlapping_repos


def save_settings(client: Elasticsearch, settings: Settings) -> None:
    """
    Save the settings for the deepfreeze operation to the status index.

    :param client: A client connection object
    :param provider: The provider to use (AWS only for now)
    """
    loggit = logging.getLogger("curator.actions.deepfreeze")
    try:
        client.get(index=STATUS_INDEX, id=SETTINGS_ID)
        loggit.info("Settings document already exists, updating it")
        client.update(index=STATUS_INDEX, id=SETTINGS_ID, doc=settings.__dict__)
    except NotFoundError:
        loggit.info("Settings document does not exist, creating it")
        client.create(index=STATUS_INDEX, id=SETTINGS_ID, document=settings.__dict__)
    loggit.info("Settings saved")


def create_new_repo(
    client,
    repo_name: str,
    bucket_name: str,
    base_path: str,
    canned_acl: str,
    storage_class: str,
    dry_run: bool = False,
) -> None:
    """
    Creates a new repo using the previously-created bucket.

    :param client: A client connection object
    :param repo_name: The name of the repository to create
    :param bucket_name: The name of the bucket to use for the repository
    :param base_path_prefix: Path within a bucket where snapshots are stored
    :param canned_acl: One of the AWS canned ACL values
    :param storage_class: AWS Storage class
    :param dry_run: If True, do not actually create the repository
    """
    loggit = logging.getLogger("curator.actions.deepfreeze")
    loggit.info("Creating repo %s using bucket %s", repo_name, bucket_name)
    if dry_run:
        return
    try:
        response = client.snapshot.create_repository(
            name=repo_name,
            body={
                "type": "s3",
                "settings": {
                    "bucket": bucket_name,
                    "base_path": base_path,
                    "canned_acl": canned_acl,
                    "storage_class": storage_class,
                },
            },
        )
    except Exception as e:
        loggit.error(e)
        print(
            f"[magenta]Error creating repository. Ensure AWS credentials have been added to keystore:[/magenta] {e}"
        )
        raise ActionError(e)
    #
    # TODO: Gather the reply and parse it to make sure this succeeded
    #       It should simply bring back '{ "acknowledged": true }' but I
    #       don't know how client will wrap it.
    loggit.info("Response: %s", response)


def get_next_suffix(style: str, last_suffix: str, year: int, month: int) -> str:
    """
    Gets the next suffix

    :param year: Optional year to override current year
    :param month: Optional month to override current month
    :returns: The next suffix in the format YYYY.MM
    :rtype: str
    """
    if style == "oneup":
        return str(int(last_suffix) + 1).zfill(6)
    elif style == "date":
        current_year = year or datetime.now().year
        current_month = month or datetime.now().month
        return f"{current_year:04}.{current_month:02}"
    else:
        raise ValueError("Invalid style")


def get_unmounted_repos(client: Elasticsearch) -> list[Repository]:
    """
    Get the complete list of repos from our index and return a Repository object for each.

    :param client: A client connection object
    :returns: The unmounted repos.
    :rtype: list[Repository]
    """
    # logging.debug("Looking for unmounted repos")
    # # Perform search in ES for all repos in the status index
    query = {"query": {"match": {"doctype": "repository"}}}
    response = client.search(index=STATUS_INDEX, body=query)
    repos = response["hits"]["hits"]
    # return a Repository object for each
    return [Repository(repo["_source"]) for repo in repos]


def get_repos(client: Elasticsearch, repo_name_prefix: str) -> list[str]:
    """
    Get the complete list of repos and return just the ones whose names
    begin with the given prefix.

    :param client: A client connection object
    :param repo_name_prefix: A prefix for repository names
    :returns: The repos.
    :rtype: list[object]
    """
    repos = client.snapshot.get_repository()
    pattern = re.compile(repo_name_prefix)
    logging.debug("Looking for repos matching %s", repo_name_prefix)
    return [repo for repo in repos if pattern.search(repo)]


def unmount_repo(client: Elasticsearch, repo: str) -> Repository:
    """
    Encapsulate the actions of deleting the repo and, at the same time,
    doing any record-keeping we need.

    :param client: A client connection object
    :param repo: The name of the repository to unmount
    :param status_index: The name of the status index
    """
    loggit = logging.getLogger("curator.actions.deepfreeze")
    repo_info = client.snapshot.get_repository(name=repo)[repo]
    bucket = repo_info["settings"]["bucket"]
    base_path = repo_info["settings"]["base_path"]
    earliest, latest = get_timestamp_range(
        client, get_all_indices_in_repo(client, repo)
    )
    repodoc = Repository(
        {
            "name": repo,
            "bucket": bucket,
            "base_path": base_path,
            "is_mounted": False,
            "start": decode_date(earliest),
            "end": decode_date(latest),
            "doctype": "repository",
        }
    )
    msg = f"Recording repository details as {repodoc}"
    loggit.debug(msg)
    client.index(index=STATUS_INDEX, document=repodoc.to_dict())
    loggit.debug("Removing repo %s", repo)
    # Now that our records are complete, go ahead and remove the repo.
    client.snapshot.delete_repository(name=repo)
    return repodoc


def decode_date(date_in: str) -> datetime:
    if isinstance(date_in, datetime):
        return date_in
    elif isinstance(date_in, str):
        return datetime.fromisoformat(date_in)
    else:
        return datetime.now()  # FIXME: This should be a value error
        # raise ValueError("Invalid date format")


class Setup:
    """
    Setup is responsible for creating the initial repository and bucket for
    deepfreeze operations.
    """

    def __init__(
        self,
        client: Elasticsearch,
        year: int,
        month: int,
        repo_name_prefix: str = "deepfreeze",
        bucket_name_prefix: str = "deepfreeze",
        base_path_prefix: str = "snapshots",
        canned_acl: str = "private",
        storage_class: str = "intelligent_tiering",
        provider: str = "aws",
        rotate_by: str = "path",
        style: str = "oneup",
    ) -> None:
        """
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
        """
        self.loggit = logging.getLogger("curator.actions.deepfreeze")
        self.loggit.debug("Initializing Deepfreeze Setup")

        self.client = client
        self.year = year
        self.month = month
        self.settings = Settings()
        self.settings.repo_name_prefix = repo_name_prefix
        self.settings.bucket_name_prefix = bucket_name_prefix
        self.settings.base_path_prefix = base_path_prefix
        self.settings.canned_acl = canned_acl
        self.settings.storage_class = storage_class
        self.settings.provider = provider
        self.settings.rotate_by = rotate_by
        self.settings.style = style
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

        self.loggit.debug("Getting repo list")
        self.repo_list = get_repos(self.client, self.settings.repo_name_prefix)
        self.repo_list.sort()
        self.loggit.debug("Repo list: %s", self.repo_list)

        if len(self.repo_list) > 0:
            raise RepositoryException(
                f"repositories matching {self.settings.repo_name_prefix}-* already exist"
            )
        self.loggit.debug("Deepfreeze Setup initialized")

    def do_dry_run(self) -> None:
        """
        Perform a dry-run of the setup process.
        """
        self.loggit.info("DRY-RUN MODE.  No changes will be made.")
        msg = f"DRY-RUN: deepfreeze setup of {self.new_repo_name} backed by {self.new_bucket_name}, with base path {self.base_path}."
        self.loggit.info(msg)
        self.loggit.info("DRY-RUN: Creating bucket %s", self.new_bucket_name)
        create_new_repo(
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
        Perform create initial bucket and repository.
        """
        self.loggit.debug("Starting Setup action")
        ensure_settings_index(self.client)
        save_settings(self.client, self.settings)
        self.s3.create_bucket(self.new_bucket_name)
        create_new_repo(
            self.client,
            self.new_repo_name,
            self.new_bucket_name,
            self.base_path,
            self.settings.canned_acl,
            self.settings.storage_class,
        )
        self.loggit.info(
            "Setup complete. You now need to update ILM policies to use %s.",
            self.new_repo_name,
        )
        self.loggit.info(
            "Ensure that all ILM policies using this repository have delete_searchable_snapshot set to false. "
            "See https://www.elastic.co/guide/en/elasticsearch/reference/current/ilm-delete.html"
        )


class Rotate:
    """
    The Deepfreeze is responsible for managing the repository rotation given
    a config file of user-managed options and settings.
    """

    def __init__(
        self,
        client: Elasticsearch,
        keep: str = "6",
        year: int = None,
        month: int = None,
    ) -> None:
        """
        :param client: A client connection object
        # :param repo_name_prefix: A prefix for repository names, defaults to `deepfreeze`
        # :param bucket_name_prefix: A prefix for bucket names, defaults to `deepfreeze`
        # :param base_path_prefix: Path within a bucket where snapshots are stored, defaults to `snapshots`
        # :param canned_acl: One of the AWS canned ACL values (see
        #     `<https://docs.aws.amazon.com/AmazonS3/latest/userguide/acl-overview.html#canned-acl>`),
        #     defaults to `private`
        # :param storage_class: AWS Storage class (see `<https://aws.amazon.com/s3/storage-classes/>`),
        #     defaults to `intelligent_tiering`
        :param keep: How many repositories to retain, defaults to 6
        :param year: Optional year to override current year
        :param month: Optional month to override current month
        """
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
        self.repo_list = get_repos(self.client, self.settings.repo_name_prefix)
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

    def update_ilm_policies(self, dry_run=False) -> None:
        """
        Loop through all existing IML policies looking for ones which reference
        the latest_repo and update them to use the new repo instead.
        """
        if self.latest_repo == self.new_repo_name:
            self.loggit.warning("Already on the latest repo")
            sys.exit(0)
        self.loggit.warning(
            "Switching from %s to %s", self.latest_repo, self.new_repo_name
        )
        policies = self.client.ilm.get_lifecycle()
        updated_policies = {}
        for policy in policies:
            # Go through these looking for any occurrences of self.latest_repo
            # and change those to use self.new_repo_name instead.
            # TODO: Ensure that delete_searchable_snapshot is set to false or
            # the snapshot will be deleted when the policy transitions to the next phase.
            # in this case, raise an error and skip this policy.
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

    def is_thawed(repo: str) -> bool:
        """
        Check if a repository is thawed

        :param repo: The name of the repository
        :returns: True if the repository is thawed, False otherwise
        """
        return repo.startswith("thawed-")

    def unmount_oldest_repos(self, dry_run=False) -> None:
        """
        Take the oldest repos from the list and remove them, only retaining
        the number chosen in the config under "keep".
        """
        # TODO: Look at snapshot.py for date-based calculations
        # Also, how to embed mutliple classes in a single action file
        # Alias action may be using multiple filter blocks. Look at that since we'll
        # need to do the same thing.:
        self.loggit.debug("Total list: %s", self.repo_list)
        s = self.repo_list[self.keep :]
        self.loggit.debug("Repos to remove: %s", s)
        for repo in s:
            if self.is_thawed(repo):
                self.loggit.warning("Skipping thawed repo %s", repo)
                continue
            self.loggit.info("Removing repo %s", repo)
            if not dry_run:
                repo = unmount_repo(self.client, repo)
                push_to_glacier(self.s3, repo)

    def get_repo_details(self, repo: str) -> Repository:
        """
        Get all the relevant details about this repo and build a Repository object
        using them.

        Args:
            repo (str): Name of the repository

        Returns:
            Repository: A fleshed-out Repository object for persisting to ES.
        """
        response = self.client.get_repository(repo)
        earliest, latest = get_timestamp_range(self.client, [repo])
        return Repository(
            {
                "name": repo,
                "bucket": response["bucket"],
                "base_path": response["base_path"],
                "start": earliest,
                "end": latest,
                "is_mounted": False,
            }
        )

    def do_dry_run(self) -> None:
        """
        Perform a dry-run of the rotation process.
        """
        self.loggit.info("DRY-RUN MODE.  No changes will be made.")
        msg = (
            f"DRY-RUN: deepfreeze {self.latest_repo} will be rotated out"
            f" and {self.new_repo_name} will be added & made active."
        )
        self.loggit.info(msg)
        self.loggit.info("DRY-RUN: Creating bucket %s", self.new_bucket_name)
        create_new_repo(
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

    def do_action(self) -> None:
        """
        Perform high-level repo rotation steps in sequence.
        """
        ensure_settings_index(self.client)
        self.loggit.debug("Saving settings")
        save_settings(self.client, self.settings)
        self.s3.create_bucket(self.new_bucket_name)
        create_new_repo(
            self.client,
            self.new_repo_name,
            self.new_bucket_name,
            self.base_path,
            self.settings.canned_acl,
            self.settings.storage_class,
        )
        self.update_ilm_policies()
        self.unmount_oldest_repos()


class Thaw:
    """
    Thaw a deepfreeze repository
    """

    def __init__(
        self,
        client: Elasticsearch,
        start: datetime,
        end: datetime,
        retain: int,
        storage_class: str,
        enable_multiple_buckets: bool = False,
    ) -> None:
        self.loggit = logging.getLogger("curator.actions.deepfreeze")
        self.loggit.debug("Initializing Deepfreeze Rotate")

        self.settings = get_settings(client)
        self.loggit.debug("Settings: %s", str(self.settings))

        self.client = client
        self.start = decode_date(start)
        self.end = decode_date(end)
        self.retain = retain
        self.storage_class = storage_class
        self.enable_multiple_buckets = enable_multiple_buckets

        self.s3 = s3_client_factory(self.settings.provider)

    def do_action(self) -> None:
        """
        Perform high-level repo thawing steps in sequence.
        """
        # We don't save the settings here because nothing should change our settings.
        # What we _will_ do though, is save a ThawSet showing what indices and repos
        # were thawed out.

        thawset = ThawSet()

        for repo in get_repos_to_thaw(self.client, self.start, self.end):
            self.loggit.info("Thawing %s", repo)
            if self.provider == "aws":
                if self.setttings.rotate_by == "bucket":
                    bucket = f"{self.settings.bucket_name_prefix}-{self.settings.last_suffix}"
                    path = self.settings.base_path_prefix
                else:
                    bucket = f"{self.settings.bucket_name_prefix}"
                    path = (
                        f"{self.settings.base_path_prefix}-{self.settings.last_suffix}"
                    )
            else:
                raise ValueError("Invalid provider")
            thaw_repo(self.s3, bucket, path, self.retain, self.storage_class)
            repo_info = self.client.get_repository(repo)
            thawset.add(ThawedRepo(repo_info))


class Refreeze:
    """
    Refreeze a thawed deepfreeze repository (if provider does not allow for thawing
    with a retention period, or if the user wants to re-freeze early)
    """

    pass


class Status:
    """
    Get the status of the deepfreeze components
    """

    def __init__(self, client: Elasticsearch) -> None:
        """
        Setup the status action

        Args:
            client (elasticsearch): Elasticsearch client object
        """
        self.loggit = logging.getLogger("curator.actions.deepfreeze")
        self.loggit.debug("Initializing Deepfreeze Status")
        self.settings = get_settings(client)
        self.client = client
        self.console = Console()

    def do_action(self) -> None:
        """
        Perform the status action
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
        table.add_row("Cluster Name", get_cluster_name(self.client))

        self.console.print(table)

    def do_thawsets(self):
        """
        Print the thawed repositories
        """
        table = Table(title="ThawSets")
        if not self.client.indices.exists(index=STATUS_INDEX):
            self.loggit.warning("No status index found")
            return
        thawsets = self.client.search(index=STATUS_INDEX)
        for thawset in thawsets:
            table.add_column(thawset)
            for repo in thawsets[thawset]:
                table.add_row(repo)

    def do_ilm_policies(self):
        """
        Print the ILM policies affected by deepfreeze
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
        """
        table = Table(title="Repositories")
        table.add_column("Repository", style="cyan")
        table.add_column("Status", style="magenta")
        table.add_column("Start", style="magenta")
        table.add_column("End", style="magenta")
        for repo in get_unmounted_repos(self.client):
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
        repolist = get_repos(self.client, self.settings.repo_name_prefix)
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
        """
        self.do_action()
