"""Utility functions for deepfreeae"""

# pylint: disable=too-many-arguments,too-many-instance-attributes, raise-missing-from

import logging
import re
from datetime import datetime, time

from elasticsearch8 import Elasticsearch, NotFoundError

from curator.actions import CreateIndex
from curator.actions.deepfreeze import Repository
from curator.actions.deepfreeze.exceptions import MissingIndexError
from curator.exceptions import ActionError
from curator.s3client import S3Client

from .constants import SETTINGS_ID, STATUS_INDEX
from .helpers import Repository, Settings, ThawSet


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
        return

    # Loop through each object and initiate restore for Glacier objects
    for obj in response["Contents"]:
        try:
            response = s3.head_object(Bucket=repo.bucket, Key=obj["Key"])

            # Check if the object has the 'Restore' header
            restore_status = response.get("Restore")

            if restore_status:
                if 'ongoing-request="true"' in restore_status:
                    return False
            else:
                raise Exception(
                    f"Object {obj['Key']} is not in the restoration process."
                )

        except Exception as e:
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


def ensure_settings_index(
    client: Elasticsearch, create_if_missing: bool = False
) -> None:
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
    if create_if_missing:
        if not client.indices.exists(index=STATUS_INDEX):
            loggit.info("Creating index %s", STATUS_INDEX)
            CreateIndex(client, STATUS_INDEX).do_action()
    else:
        if not client.indices.exists(index=STATUS_INDEX):
            raise MissingIndexError(
                f"Status index {STATUS_INDEX} is missing but should exist"
            )


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
        return Settings(**doc["_source"])
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


def get_repository(client: Elasticsearch, name: str) -> Repository:
    """
    Get the repository object from the status index.

    :param client: A client connection object
    :type client: Elasticsearch
    :param name: The name of the repository
    :type name: str

    :returns: The repository
    :rtype: Repository

    :raises Exception: If the repository does not exist
    """
    loggit = logging.getLogger("curator.actions.deepfreeze")
    try:
        doc = client.get(index=STATUS_INDEX, id=name)
        return Repository(**doc["_source"])
    except NotFoundError:
        loggit.warning("Repository document not found")
        return None


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
    # ! This will now include mounted and unmounted repos both!
    query = {"query": {"match": {"doctype": "repository"}}}
    response = client.search(index=STATUS_INDEX, body=query)
    repos = response["hits"]["hits"]
    # return a Repository object for each
    return [Repository(**repo["_source"]) for repo in repos]


def get_matching_repo_names(client: Elasticsearch, repo_name_prefix: str) -> list[str]:
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


def get_matching_repos(
    client: Elasticsearch, repo_name_prefix: str
) -> list[Repository]:
    """
    Get the list of repos from our index and return a Repository object for each one
    which matches the given prefix.

    :param client: A client connection object
    :type client: Elasticsearch
    :param repo_name_prefix: A prefix for repository names
    :type repo_name_prefix: str

    :returns: The repos.
    :rtype: list[Repository]

    :raises Exception: If the repository does not exist
    """
    query = {"query": {"match": {"doctype": "repository"}}}
    response = client.search(index=STATUS_INDEX, body=query)
    repos = response["hits"]["hits"]
    # ? Make sure this works
    repos = [repo for repo in repos if repo["name"].startswith(repo_name_prefix)]
    # return a Repository object for each
    return [Repository(**repo["_source"]) for repo in repos]


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
        # ! TODO: This can't be done here; we have to calculate the date range while
        # ! TODO: the indices are still mounted.
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
    loggit.debug("Removing repo %s", repo)
    try:
        client.snapshot.delete_repository(name=repo)
    except Exception as e:
        loggit.error(e)
        raise ActionError(e)
    # Don't update the records until the repo has been succesfully removed.
    client.index(index=STATUS_INDEX, document=repodoc.to_dict())
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
            return False
    return True


def create_ilm_policy(
    client: Elasticsearch, policy_name: str, policy_body: str
) -> None:
    """
    Create a sample ILM policy.

    :param client: A client connection object
    :type client: Elasticsearch
    :param policy_name: The name of the policy to create
    :type policy_name: str

    :return: None
    :rtype: None

    :raises Exception: If the policy cannot be created
    :raises Exception: If the policy already exists
    :raises Exception: If the policy cannot be retrieved
    :raises Exception: If the policy is not empty
    """
    loggit = logging.getLogger("curator.actions.deepfreeze")
    loggit.info("Creating ILM policy %s", policy_name)
    try:
        response = client.ilm.put_lifecycle(name=policy_name, body=policy_body)
    except Exception as e:
        loggit.error(e)
        raise ActionError(e)
