"""Utility functions for deepfreeae"""

# pylint: disable=too-many-arguments,too-many-instance-attributes, raise-missing-from

import logging
import re
from datetime import datetime, timezone

from elasticsearch8 import Elasticsearch, NotFoundError

from curator.actions import CreateIndex
from curator.actions.deepfreeze.exceptions import MissingIndexError
from curator.exceptions import ActionError
from curator.s3client import S3Client

from .constants import SETTINGS_ID, STATUS_INDEX
from .helpers import Repository, Settings


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

    # logging.debug("Indices: %s", indices)
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
    logging.debug("starting with %s indices", len(indices))
    # Remove any indices that do not exist
    filtered = [index for index in indices if client.indices.exists(index=index)]
    logging.debug("after removing non-existent indices: %s", len(filtered))

    try:
        response = client.search(
            index=",".join(filtered), body=query, allow_partial_search_results=True
        )
        logging.debug("Response: %s", response)
    except Exception as e:
        logging.error("Error retrieving timestamp range: %s", e)
        return None, None

    earliest = response["aggregations"]["earliest"]["value_as_string"]
    latest = response["aggregations"]["latest"]["value_as_string"]

    logging.debug("BDW from query: Earliest: %s, Latest: %s", earliest, latest)

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
    if not client.indices.exists(index=STATUS_INDEX):
        raise MissingIndexError(f"Status index {STATUS_INDEX} is missing")
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
        client.snapshot.create_repository(
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
    # Get and save a repository object for this repo
    loggit.debug("Saving repo %s to status index", repo_name)
    repository = get_repository(client, repo_name)
    repository.bucket = bucket_name if not repository.bucket else repository.bucket
    repository.base_path = (
        base_path if not repository.base_path else repository.base_path
    )
    loggit.debug("Repo = %s", repository)
    client.index(index=STATUS_INDEX, body=repository.to_dict())
    loggit.debug("Repo %s saved to status index", repo_name)
    #
    # TODO: Gather the reply and parse it to make sure this succeeded


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
    logging.debug("Getting repository %s", name)
    try:
        doc = client.search(
            index=STATUS_INDEX, body={"query": {"match": {"name": name}}}
        )
        logging.debug("Got: %s", doc)
        if doc["hits"]["total"]["value"] == 0:
            logging.debug("Got no hits")
            return Repository(name=name)
        for n in range(len(doc["hits"]["hits"])):
            if doc["hits"]["hits"][n]["_source"]["name"] == name:
                logging.debug("Got a match")
                return Repository(
                    **doc["hits"]["hits"][n]["_source"],
                    docid=doc["hits"]["hits"][n]["_id"],
                )
        # If we get here, we have no match
        logging.debug("No match found")
        return Repository(name=name)
    except NotFoundError:
        loggit.warning("Repository document not found")
        return Repository(name=name)


def get_all_repos(client: Elasticsearch) -> list[Repository]:
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
    logging.debug("Searching for repos")
    response = client.search(index=STATUS_INDEX, body=query, size=10000)
    logging.debug("Response: %s", response)
    repos = response["hits"]["hits"]
    logging.debug("Repos retrieved: %s", repos)
    # return a Repository object for each
    # TEMP:
    rv = []
    for repo in repos:
        logging.debug("Repo: %s", repo)
        logging.debug("Repo ID: %s", repo["_id"])
        logging.debug("Repo Source: %s", repo["_source"])
        rv.append(Repository(**repo["_source"], docid=repo["_id"]))
        logging.debug("Repo object: %s", rv[-1])
    return rv


#    return [Repository(**repo["_source"], docid=response["_id"]) for repo in repos]


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
    client: Elasticsearch, repo_name_prefix: str, mounted: bool = False
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
    response = client.search(index=STATUS_INDEX, body=query, size=10000)
    logging.debug("Response: %s", response)
    repos = response["hits"]["hits"]
    logging.debug("Repos retrieved: %s", repos)
    repos = [
        repo for repo in repos if repo["_source"]["name"].startswith(repo_name_prefix)
    ]
    if mounted:
        mounted_repos = [
            repo for repo in repos if repo["_source"]["is_mounted"] is True
        ]
        logging.debug("Mounted repos: %s", mounted_repos)
        return [Repository(**repo["_source"]) for repo in mounted_repos]
    # return a Repository object for each
    return [Repository(**repo["_source"], docid=response["_id"]) for repo in repos]


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
    # ? Why am I doing it this way? Is there a reason or could this be done using get_repository and the resulting repo object?
    repo_info = client.snapshot.get_repository(name=repo)[repo]
    bucket = repo_info["settings"]["bucket"]
    base_path = repo_info["settings"]["base_path"]
    indices = get_all_indices_in_repo(client, repo)
    repo_obj = get_repository(client, repo)
    repo_obj.bucket = bucket if not repo_obj.bucket else repo_obj.bucket
    repo_obj.base_path = base_path if not repo_obj.base_path else repo_obj.base_path
    if indices:
        earliest, latest = get_timestamp_range(client, indices)
        loggit.debug("Confirming Earliest: %s, Latest: %s", earliest, latest)
        repo_obj.start = decode_date(earliest)
        repo_obj.end = decode_date(latest)
    repo_obj.unmount()
    msg = f"Recording repository details as {repo_obj}"
    loggit.debug(msg)
    loggit.debug("Removing repo %s", repo)
    try:
        client.snapshot.delete_repository(name=repo)
    except Exception as e:
        loggit.warning("Repository %s could not be unmounted due to %s", repo, e)
        loggit.warning("Another attempt will be made when rotate runs next")
    # Don't update the records until the repo has been succesfully removed.
    loggit.debug("Updating repo: %s", repo_obj)
    client.update(index=STATUS_INDEX, doc=repo_obj.to_dict(), id=repo_obj.docid)
    loggit.debug("Repo %s removed", repo)
    return repo_obj


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
        dt = date_in
    elif isinstance(date_in, str):
        logging.debug("Decoding date %s", date_in)
        dt = datetime.fromisoformat(date_in)
    else:
        raise ValueError("Invalid date format")
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


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
        client.ilm.put_lifecycle(name=policy_name, body=policy_body)
    except Exception as e:
        loggit.error(e)
        raise ActionError(e)
