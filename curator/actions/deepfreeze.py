"""Deepfreeze action class"""

# pylint: disable=too-many-arguments,too-many-instance-attributes, raise-missing-from

import json
import logging
import re
import sys
from dataclasses import dataclass
from datetime import datetime, time

from elasticsearch8 import Elasticsearch
from elasticsearch8.exceptions import NotFoundError
from rich import print
from rich.console import Console
from rich.table import Table

from curator.actions import CreateIndex
from curator.exceptions import ActionError, RepositoryException
from curator.s3client import S3Client, s3_client_factory

STATUS_INDEX = "deepfreeze-status"
SETTINGS_ID = "1"


class Deepfreeze:
    """
    Allows nesting of actions under the deepfreeze command
    """


@dataclass
class ThawedRepo:
    """
    ThawedRepo is a data class representing a thawed repository and its indices.

    Attributes:
        repo_name (str): The name of the repository.
        bucket_name (str): The name of the bucket where the repository is stored.
        base_path (str): The base path of the repository.
        provider (str): The provider of the repository, default is "aws".
        indices (list): A list of indices associated with the repository.

    Methods:
        __init__(repo_info: dict, indices: list[str] = None) -> None:
            Initializes a ThawedRepo instance with repository information and optional indices.

        add_index(index: str) -> None:
            Adds an index to the list of indices.

    Example:
        thawed_repo = ThawedRepo(repo_info, indices)
        thawed_repo.add_index("index_name")
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

        Params:
            index (str): The index to add

        Returns:
            None
        """
        self.indices.append(index)


@dataclass
class ThawSet(dict[str, ThawedRepo]):
    """
    Data class for thaw settings

    Attributes:
        doctype (str): The document type of the thaw settings.

    Methods:
        add(thawed_repo: ThawedRepo) -> None:
            Add a thawed repo to the dictionary

    Example:
        thawset = ThawSet()
        thawset.add(ThawedRepo(repo_info, indices))
    """

    doctype: str = "thawset"

    def add(self, thawed_repo: ThawedRepo) -> None:
        """
        Add a thawed repo to the dictionary

        Params:
            thawed_repo (ThawedRepo): The thawed repo to add

        Returns:
            None
        """
        self[thawed_repo.repo_name] = thawed_repo


@dataclass
class Repository:
    """
    Data class for repository

    Attributes:
        name (str): The name of the repository.
        bucket (str): The name of the bucket.
        base_path (str): The base path of the repository.
        start (datetime): The start date of the repository.
        end (datetime): The end date of the repository.
        is_thawed (bool): Whether the repository is thawed.
        is_mounted (bool): Whether the repository is mounted.
        doctype (str): The document type of the repository.

    Methods:
        to_dict() -> dict:
            Convert the Repository object to a dictionary.

        to_json() -> str:
            Convert the Repository object to a JSON string.

    Example:
        repo = Repository({name="repo1", bucket="bucket1", base_path="path1", start=datetime.now(), end=datetime.now()})
        repo = Repository(name="deepfreeze-000032")
        repo_dict = repo.to_dict()
        repo_json = repo.to_json()
    """

    name: str
    bucket: str
    base_path: str
    start: datetime
    end: datetime
    is_thawed: bool = False
    is_mounted: bool = True
    doctype: str = "repository"

    def __init__(self, repo_hash=None, name=None) -> None:
        if name is not None:
            repo_hash = self.client.get(index=STATUS_INDEX, id=name)["_source"]
        if repo_hash is not None:
            for key, value in repo_hash.items():
                setattr(self, key, value)

    def to_dict(self) -> dict:
        """
        Convert the Repository object to a dictionary.
        Convert datetime to ISO 8601 string format for JSON compatibility.

        Params:
            None

        Returns:
            dict: A dictionary representation of the Repository object.
        """
        start_str = self.start.isoformat() if self.start else None
        end_str = self.end.isoformat() if self.end else None
        return {
            "name": self.name,
            "bucket": self.bucket,
            "base_path": self.base_path,
            "start": start_str,
            "end": end_str,
            "is_thawed": self.is_thawed,
            "is_mounted": self.is_mounted,
            "doctype": self.doctype,
        }

    def to_json(self) -> str:
        """
        Convert the Repository object to a JSON string.

        Params:
            None

        Returns:
            str: A JSON string representation of the Repository object.
        """
        return json.dumps(self.to_dict(), indent=4)

    def __lt__(self, other):
        """
        Less than comparison based on the repository name.

        Params:
            other (Repository): Another Repository object to compare with.

        Returns:
            bool: True if this repository's name is less than the other repository's name, False otherwise.
        """
        return self.name < other.name


@dataclass
class Settings:
    """
    Data class for settings

    Attributes:
        doctype (str): The document type of the settings.
        repo_name_prefix (str): The prefix for repository names.
        bucket_name_prefix (str): The prefix for bucket names.
        base_path_prefix (str): The base path prefix.
        canned_acl (str): The canned ACL.
        storage_class (str): The storage class.
        provider (str): The provider.
        rotate_by (str): The rotation style.
        style (str): The style of the settings.
        last_suffix (str): The last suffix.

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


def push_to_glacier(s3: S3Client, repo: Repository) -> None:
    """Push objects to Glacier storage

    :param s3: The S3 client object
    :type s3: S3Client
    :param repo: The repository to push to Glacier
    :type repo: Repository

    :return: None
    :rtype: None

    :raises Exception: If the object is not in the restoration process
    """
    logging.debug("Pushing objects to Glacier storage")
    response = s3.list_objects(repo.bucket, repo.base_path)

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
            Key=obj["Key"],
            CopySource={"Bucket": repo.bucket, "Key": obj["Key"]},
            StorageClass="GLACIER",
        )

    print("Freezing to Glacier initiated for {count} objects")


def check_restore_status(s3: S3Client, repo: Repository) -> bool:
    """
    Check the status of the restore request for each object in the repository.

    :param s3:  The S3 client object
    :type s3: S3Client
    :param repo: The repository to check
    :type repo: Repository
    :raises Exception:  If the object is not in the restoration process
    :return:   True if the restore request is complete, False otherwise
    :rtype: bool
    """
    response = s3.list_objects(repo.bucket, repo.base_path)

    # Check if objects were found
    if "Contents" not in response:
        print(f"No objects found in prefix: {repo.base_path}")
        return

    # Loop through each object and initiate restore for Glacier objects
    for obj in response["Contents"]:
        try:
            response = s3.head_object(Bucket=repo.bucket, Key=obj["Key"])

            # Check if the object has the 'Restore' header
            restore_status = response.get("Restore")

            if restore_status:
                if 'ongoing-request="true"' in restore_status:
                    print(f"Object {obj['Key']} is still being restored.")
                    return False
            else:
                raise Exception(
                    f"Object {obj['Key']} is not in the restoration process."
                )

        except Exception as e:
            print(f"Error checking restore status: {e}")
            return None
    return True


def thaw_repo(
    s3: S3Client,
    bucket_name: str,
    base_path: str,
    restore_days: int = 7,
    retrieval_tier: str = "Standard",
) -> None:
    """
    Restore objects from Glacier storage

    :param s3: The S3 client object
    :type s3: S3Client
    :param bucket_name: Bucket name
    :type bucket_name: str
    :param base_path: Base path of the repository
    :type base_path: str
    :param restore_days: Number of days to retain before returning to Glacier, defaults to 7
    :type restore_days: int, optional
    :param retrieval_tier: Storage tier to return objects to, defaults to "Standard"
    :type retrieval_tier: str, optional

    :raises Exception: If the object is not in the restoration process

    :return: None
    :rtype: None
    """
    response = s3.list_objects(bucket_name, base_path)

    # Check if objects were found
    if "Contents" not in response:
        print(f"No objects found in prefix: {base_path}")
        return

    # Loop through each object and initiate restore for Glacier objects
    count = 0
    for obj in response["Contents"]:
        count += 1

        # Initiate the restore request for each object
        s3.restore_object(
            Bucket=bucket_name,
            Key=obj["Key"],
            RestoreRequest={
                "Days": restore_days,
                "GlacierJobParameters": {
                    "Tier": retrieval_tier  # You can change to 'Expedited' or 'Bulk' if needed
                },
            },
        )

    print(f"Restore request initiated for {count} objects")


def get_all_indices_in_repo(client: Elasticsearch, repository: str) -> list[str]:
    """
    Retrieve all indices from snapshots in the given repository.

    :param client: A client connection object
    :param repository: The name of the repository
    :returns: A list of indices
    :rtype: list[str]

    :raises Exception: If the repository does not exist
    :raises Exception: If the repository is empty
    :raises Exception: If the repository is not mounted
    """
    indices = set()

    # TODO: Convert these three lines to use an existing Curator function?
    snapshots = client.snapshot.get(repository=repository, snapshot="_all")
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

    :raises Exception: If the indices list is empty
    :raises Exception: If the indices do not exist
    :raises Exception: If the indices are empty

    :example:
        >>> get_timestamp_range(client, ["index1", "index2"])
        (datetime.datetime(2021, 1, 1, 0, 0), datetime.datetime(2021, 1, 2, 0, 0))
    """
    logging.debug("Determining timestamp range for indices: %s", indices)
    if not indices:
        return None, None
    # TODO: Consider using Curator filters to accomplish this
    query = {
        "size": 0,
        "aggs": {
            "earliest": {"min": {"field": "@timestamp"}},
            "latest": {"max": {"field": "@timestamp"}},
        },
    }
    response = client.search(index=",".join(indices), body=query)
    logging.debug("Response: %s", response)

    earliest = response["aggregations"]["earliest"]["value_as_string"]
    latest = response["aggregations"]["latest"]["value_as_string"]

    logging.debug("Earliest: %s, Latest: %s", earliest, latest)

    return datetime.fromisoformat(earliest), datetime.fromisoformat(latest)


def ensure_settings_index(client: Elasticsearch) -> None:
    """
    Ensure that the status index exists in Elasticsearch.

    :param client: A client connection object
    :type client: Elasticsearch

    :return: None
    :rtype: None

    :raises Exception: If the index cannot be created
    :raises Exception: If the index already exists
    :raises Exception: If the index cannot be retrieved
    :raises Exception: If the index is not empty

    """
    loggit = logging.getLogger("curator.actions.deepfreeze")
    if not client.indices.exists(index=STATUS_INDEX):
        loggit.info("Creating index %s", STATUS_INDEX)
        CreateIndex(client, STATUS_INDEX).do_action()
        # client.indices.create(index=STATUS_INDEX)


def get_settings(client: Elasticsearch) -> Settings:
    """
    Get the settings for the deepfreeze operation from the status index.

    :param client: A client connection object
    :type client: Elasticsearch

    :returns: The settings
    :rtype: dict

    :raises Exception: If the settings document does not exist

    :example:
        >>> get_settings(client)
        {'repo_name_prefix': 'deepfreeze', 'bucket_name_prefix': 'deepfreeze', 'base_path_prefix': 'snapshots', 'canned_acl': 'private', 'storage_class': 'intelligent_tiering', 'provider': 'aws', 'rotate_by': 'path', 'style': 'oneup', 'last_suffix': '000001'}
    """
    loggit = logging.getLogger("curator.actions.deepfreeze")
    try:
        doc = client.get(index=STATUS_INDEX, id=SETTINGS_ID)
        loggit.info("Settings document found")
        return Settings(doc["_source"])
    except NotFoundError:
        loggit.info("Settings document not found")
        return None


def save_settings(client: Elasticsearch, settings: Settings) -> None:
    """
    Save the settings for the deepfreeze operation to the status index.

    :param client: A client connection object
    :type client: Elasticsearch
    :param settings: The settings to save
    :type settings: Settings

    :return: None
    :rtype: None

    :raises Exception: If the settings document cannot be created
    :raises Exception: If the settings document cannot be updated
    :raises Exception: If the settings document cannot be retrieved
    :raises Exception: If the settings document is not empty
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


def create_repo(
    client: Elasticsearch,
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
    :type client: Elasticsearch
    :param repo_name: The name of the repository to create
    :type repo_name: str
    :param bucket_name: The name of the bucket to use for the repository
    :type bucket_name: str
    :param base_path_prefix: Path within a bucket where snapshots are stored
    :type base_path_prefix: str
    :param canned_acl: One of the AWS canned ACL values
    :type canned_acl: str
    :param storage_class: AWS Storage class
    :type storage_class: str
    :param dry_run: If True, do not actually create the repository
    :type dry_run: bool

    :raises Exception: If the repository cannot be created
    :raises Exception: If the repository already exists
    :raises Exception: If the repository cannot be retrieved
    :raises Exception: If the repository is not empty
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

    :param style: The style of the suffix
    :type style: str
    :param last_suffix: The last suffix
    :type last_suffix: str
    :param year: Optional year to override current year
    :type year: int
    :param month: Optional month to override current month
    :type month: int

    :returns: The next suffix in the format YYYY.MM
    :rtype: str

    :raises ValueError: If the style is not valid
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
    :type client: Elasticsearch

    :returns: The unmounted repos.
    :rtype: list[Repository]

    :raises Exception: If the repository does not exist

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
    :type client: Elasticsearch
    :param repo_name_prefix: A prefix for repository names
    :type repo_name_prefix: str

    :returns: The repos.
    :rtype: list[object]

    :raises Exception: If the repository does not exist
    """
    repos = client.snapshot.get_repository()
    logging.debug("Repos retrieved: %s", repos)
    pattern = re.compile(repo_name_prefix)
    logging.debug("Looking for repos matching %s", repo_name_prefix)
    return [repo for repo in repos if pattern.search(repo)]


def get_thawset(client: Elasticsearch, thawset_id: str) -> ThawSet:
    """
    Get the thawset from the status index.

    :param client: A client connection object
    :type client: Elasticsearch
    :param thawset_id: The ID of the thawset
    :type thawset_id: str

    :returns: The thawset
    :rtype: ThawSet

    :raises Exception: If the thawset document does not exist
    """
    loggit = logging.getLogger("curator.actions.deepfreeze")
    try:
        doc = client.get(index=STATUS_INDEX, id=thawset_id)
        loggit.info("ThawSet document found")
        return ThawSet(doc["_source"])
    except NotFoundError:
        loggit.info("ThawSet document not found")
        return None


def unmount_repo(client: Elasticsearch, repo: str) -> Repository:
    """
    Encapsulate the actions of deleting the repo and, at the same time,
    doing any record-keeping we need.

    :param client: A client connection object
    :type client: Elasticsearch
    :param repo: The name of the repository to unmount
    :type repo: str

    :returns: The repo.
    :rtype: Repository

    :raises Exception: If the repository does not exist
    :raises Exception: If the repository is not empty
    :raises Exception: If the repository cannot be deleted
    """
    loggit = logging.getLogger("curator.actions.deepfreeze")
    repo_info = client.snapshot.get_repository(name=repo)[repo]
    bucket = repo_info["settings"]["bucket"]
    base_path = repo_info["settings"]["base_path"]
    indices = get_all_indices_in_repo(client, repo)
    repodoc = {}
    if indices:
        earliest, latest = get_timestamp_range(client, indices)
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
    else:
        repodoc = Repository(
            {
                "name": repo,
                "bucket": bucket,
                "base_path": base_path,
                "is_mounted": False,
                "start": None,
                "end": None,
                "doctype": "repository",
            }
        )
    msg = f"Recording repository details as {repodoc}"
    loggit.debug(msg)
    client.index(index=STATUS_INDEX, document=repodoc.to_dict())
    loggit.debug("Removing repo %s", repo)
    # Now that our records are complete, go ahead and remove the repo.
    client.snapshot.delete_repository(name=repo)
    loggit.debug("Repo %s removed", repo)
    return repodoc


def wait_for_s3_restore(
    s3: S3Client, thawset: ThawSet, wait_interval: int = 60, max_wait: int = -1
) -> None:
    """
    Wait for the S3 objects to be restored.

    :param s3: The S3 client object
    :type s3: S3Client
    :param thawset: The thawset to wait for
    :type thawset: ThawSet
    :param wait_interval: The interval to wait between checks
    :type wait_interval: int
    :param max_wait: The maximum time to wait
    :type max_wait: int

    :return: None
    :rtype: None

    :raises Exception: If the S3 objects are not restored
    :raises Exception: If the S3 objects are not found
    :raises Exception: If the S3 objects are not in the restoration process
    :raises Exception: If the S3 objects are not in the correct storage class
    :raises Exception: If the S3 objects are not in the correct bucket
    :raises Exception: If the S3 objects are not in the correct base path
    """
    loggit = logging.getLogger("curator.actions.deepfreeze")
    loggit.info("Waiting for S3 objects to be restored")
    start_time = datetime.now()
    while True:
        if check_is_s3_thawed(s3, thawset):
            loggit.info("S3 objects restored")
            break
        if max_wait > 0 and (datetime.now() - start_time).seconds > max_wait:
            loggit.warning("Max wait time exceeded")
            break
        loggit.info("Waiting for S3 objects to be restored")
        time.sleep(wait_interval)


def decode_date(date_in: str) -> datetime:
    """
    Decode a date from a string or datetime object.

    :param date_in: The date to decode
    :type date_in: str or datetime

    :returns: The decoded date
    :rtype: datetime

    :raises ValueError: If the date is not valid
    """
    if isinstance(date_in, datetime):
        return date_in
    elif isinstance(date_in, str):
        return datetime.fromisoformat(date_in)
    else:
        raise ValueError("Invalid date format")


def check_is_s3_thawed(s3: S3Client, thawset: ThawSet) -> bool:
    """
    Check the status of the thawed repositories.

    :param s3: The S3 client object
    :type s3: S3Client
    :param thawset: The thawset to check
    :type thawset: ThawSet

    :returns: True if the repositories are thawed, False otherwise
    :rtype: bool

    :raises Exception: If the repository does not exist
    :raises Exception: If the repository is not empty
    :raises Exception: If the repository is not mounted
    :raises Exception: If the repository is not thawed
    :raises Exception: If the repository is not in the correct storage class
    :raises Exception: If the repository is not in the correct bucket
    :raises Exception: If the repository is not in the correct base path
    """
    for repo in thawset:
        logging.info("Checking status of %s", repo)
        if not check_restore_status(s3, repo):
            logging.warning("Restore not complete for %s", repo)
            print("Restore not complete for %s", repo)
            return False
    return True


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
                f"repositories matching {self.settings.repo_jname_prefix}-* already exist"
            )
        self.loggit.debug("Deepfreeze Setup initialized")

    def do_dry_run(self) -> None:
        """
        Perform a dry-run of the setup process.

        :return: None
        :rtype: None
        """
        self.loggit.info("DRY-RUN MODE.  No changes will be made.")
        msg = f"DRY-RUN: deepfreeze setup of {self.new_repo_name} backed by {self.new_bucket_name}, with base path {self.base_path}."
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

    def do_action(self) -> None:
        """
        Perform setup steps to create initial bucket and repository and save settings.

        :return: None
        :rtype: None
        """
        self.loggit.debug("Starting Setup action")
        ensure_settings_index(self.client)
        save_settings(self.client, self.settings)
        self.s3.create_bucket(self.new_bucket_name)
        create_repo(
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

    def update_repo_date_range(self, dry_run=False):
        """
        Update the date ranges for all repositories in the status index.

        :return: None
        :rtype: None

        :raises Exception: If the repository does not exist
        :raises Exception: If the repository is not empty
        :raises Exception: If the repository is not mounted
        :raises Exception: If the repository is not thawed
        """
        self.loggit.debug("Updating repo date ranges")
        # Get the repo objects (not names) which match our prefix
        repos = get_repos(self.client, self.settings.repo_name_prefix)
        # Now loop through the repos, updating the date range for each
        for repo in repos:
            self.loggit.debug("Updating date range for %s", repo.name)
            indices = get_all_indices_in_repo(self.client, repo.name)
            if indices:
                earliest, latest = get_timestamp_range(self.client, indices)
                repo.start = (
                    decode_date(earliest) if earliest <= repo.start else repo.start
                )
                repo.end = decode_date(latest) if latest >= repo.end else repo.end
                # ? Will this produce too many updates? Do I need to only update if one
                # ? of the dates has changed?
                if not dry_run:
                    self.client.update(index=STATUS_INDEX, doc=repo.to_dict())
                self.loggit.debug("Updated date range for %s", repo.name)
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
        # TODO: Look at snapshot.py for date-based calculations
        # Also, how to embed mutliple classes in a single action file
        # Alias action may be using multiple filter blocks. Look at that since we
        # may need to do the same thing.
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
        """Return a Repository object given a repo name

        :param repo: The name of the repository
        :type repo: str

        :return: The repository object
        :rtype: Repository

        :raises Exception: If the repository does not exist
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
        self.s3.create_bucket(self.new_bucket_name)
        create_repo(
            self.client,
            self.new_repo_name,
            self.new_bucket_name,
            self.base_path,
            self.settings.canned_acl,
            self.settings.storage_class,
        )
        self.update_ilm_policies()
        self.unmount_oldest_repos()
        self.update_repo_date_range()


class Thaw:
    """
    Thaw a deepfreeze repository and make it ready to be remounted. If
    wait_for_completion is True, wait for the thawed repository to be ready and then
    proceed to remount it. This is the default.

    :param client: A client connection object
    :param start: The start of the time range
    :param end: The end of the time range
    :param retain: The number of days to retain the thawed repository
    :param storage_class: The storage class to use for the thawed repository
    :param wait_for_completion: If True, wait for the thawed repository to be ready
    :param wait_interval: The interval to wait between checks
    :param max_wait: The maximum time to wait (-1 for no limit)
    :param enable_multiple_buckets: If True, enable multiple buckets

    :raises Exception: If the repository does not exist
    :raises Exception: If the repository is not empty
    :raises Exception: If the repository is not mounted

    :methods:
        get_repos_to_thaw: Get the list of repos that were active during the given time range.
        do_dry_run: Perform a dry-run of the thawing process.
        do_action: Perform high-level repo thawing steps in sequence.
    """

    def __init__(
        self,
        client: Elasticsearch,
        start: datetime,
        end: datetime,
        retain: int,
        storage_class: str,
        wait_for_completion: bool = True,
        wait_interval: int = 60,
        max_wait: int = -1,
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
        self.wfc = wait_for_completion
        self.wait_interval = wait_interval
        self.max_wait = max_wait
        self.enable_multiple_buckets = enable_multiple_buckets
        self.s3 = s3_client_factory(self.settings.provider)

    def get_repos_to_thaw(self, start: datetime, end: datetime) -> list[Repository]:
        """
        Get the list of repos that were active during the given time range.

        :param start: The start of the time range
        :type start: datetime
        :param end: The end of the time range
        :type start: datetime

        :returns: The repos
        :rtype: list[Repository] A list of repository names

        :raises Exception: If the repository does not exist
        :raises Exception: If the repository is not empty
        """
        loggit = logging.getLogger("curator.actions.deepfreeze")
        repos = get_unmounted_repos(self.client)
        overlapping_repos = []
        for repo in repos:
            if repo.start <= end and repo.end >= start:
                overlapping_repos.append(repo)
        loggit.info("Found overlapping repos: %s", overlapping_repos)
        return overlapping_repos

    def do_dry_run(self) -> None:
        """
        Perform a dry-run of the thawing process.

        :return: None
        :rtype: None
        """
        thawset = ThawSet()

        for repo in self.get_repos_to_thaw(self.start, self.end):
            self.loggit.info("Thawing %s", repo)
            repo_info = self.client.get_repository(repo)
            thawset.add(ThawedRepo(repo_info))
        print(f"Dry Run ThawSet: {thawset}")

    def do_action(self) -> None:
        """
        Perform high-level repo thawing steps in sequence.

        :return: None
        :rtype: None
        """
        # We don't save the settings here because nothing should change our settings.
        # What we _will_ do though, is save a ThawSet showing what indices and repos
        # were thawed out.

        thawset = ThawSet()

        for repo in self.get_repos_to_thaw(self.start, self.end):
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
        response = self.client.index(index=STATUS_INDEX, document=thawset)
        if not self.wfc:
            thawset_id = response["_id"]
            print(
                f"ThawSet {thawset_id} created. Plase use this ID to remount the thawed repositories."
            )
        else:
            wait_for_s3_restore(self.s3, thawset_id, self.wait_interval, self.max_wait)
            remount = Remount(
                self.client, thawset_id, self.wfc, self.wait_interval, self.max_wait
            )
            remount.do_action()


class Remount:
    """
    Remount a thawed deepfreeze repository. Remount indices as "thawed-<repo>".

    :param client: A client connection object
    :type client: Elasticsearch
    :param thawset: The thawset to remount
    :type thawset: str
    :param wait_for_completion: If True, wait for the remounted repository to be ready
    :type wait_for_completion: bool
    :param wait_interval: The interval to wait between checks
    :type wait_interval: int
    :param max_wait: The maximum time to wait (-1 for no limit)
    :type max_wait: int

    :methods:
        do_dry_run: Perform a dry-run of the remounting process.
        do_action: Perform high-level repo remounting steps in sequence.
    """

    def __init__(
        self,
        client: Elasticsearch,
        thawset: str,
        wait_for_completion: bool = True,
        wait_interval: int = 9,
        max_wait: int = -1,
    ) -> None:
        self.loggit = logging.getLogger("curator.actions.deepfreeze")
        self.loggit.debug("Initializing Deepfreeze Rotate")

        self.settings = get_settings(client)
        self.loggit.debug("Settings: %s", str(self.settings))

        self.client = client
        self.thawset = get_thawset(thawset)
        self.wfc = wait_for_completion
        self.wait_interval = wait_interval
        self.max_wait = max_wait

    def do_dry_run(self) -> None:
        """
        Perform a dry-run of the remounting process.

        :return: None
        :rtype: None
        """
        if not check_is_s3_thawed(self.s3, self.thawset):
            print("Dry Run Remount: Not all repos thawed")

        for repo in self.thawset_id.repos:
            self.loggit.info("Remounting %s", repo)

    def do_action(self) -> None:
        """
        Perform high-level repo remounting steps in sequence.

        :return: None
        :rtype: None
        """
        if not check_is_s3_thawed(self.s3, self.thawset):
            print("Remount: Not all repos thawed")
            return

        for repo in self.thawset_id.repos:
            self.loggit.info("Remounting %s", repo)
            create_repo(
                self.client,
                f"thawed-{repo.name}",
                repo.bucket,
                repo.base_path,
                self.settings.canned_acl,
                self.settings.storage_class,
            )


class Refreeze:
    """
    First unmount a repo, then refreeze it requested (or let it age back to Glacier
    naturally)

    :param client: A client connection object
    :type client: Elasticsearch
    :param thawset: The thawset to refreeze
    :type thawset: str

    :methods:
        do_dry_run: Perform a dry-run of the refreezing process.
        do_action: Perform high-level repo refreezing steps in sequence.
    """

    def __init__(self, client: Elasticsearch, thawset: str) -> None:
        self.loggit = logging.getLogger("curator.actions.deepfreeze")
        self.loggit.debug("Initializing Deepfreeze Rotate")

        self.settings = get_settings(client)
        self.loggit.debug("Settings: %s", str(self.settings))

        self.client = client
        self.thawset = ThawSet(thawset)

    def do_dry_run(self) -> None:
        """
        Perform a dry-run of the refreezing process.

        :return: None
        :rtype: None
        """
        pass

    def do_action(self) -> None:
        """
        Perform high-level repo refreezing steps in sequence.

        :return: None
        :rtype: None
        """
        pass


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

        :return: None
        :rtype: None
        """
        self.do_action()
