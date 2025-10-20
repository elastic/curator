"""Utility functions for deepfreeze"""

# pylint: disable=too-many-arguments,too-many-instance-attributes, raise-missing-from

import logging
import re
from datetime import datetime, timezone

import botocore
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
    try:
        # Normalize base_path: remove leading/trailing slashes, ensure it ends with /
        base_path = repo.base_path.strip('/')
        if base_path:
            base_path += '/'

        # Initialize variables for pagination
        success = True
        object_count = 0

        # List objects
        objects = s3.list_objects(repo.bucket, base_path)

        # Process each object
        for obj in objects:
            key = obj['Key']
            current_storage_class = obj.get('StorageClass', 'STANDARD')

            # Log the object being processed
            logging.info(
                f"Processing object: s3://{repo.bucket}/{key} (Current: {current_storage_class})"
            )

            try:
                # Copy object to itself with new storage class
                copy_source = {'Bucket': repo.bucket, 'Key': key}
                s3.copy_object(
                    Bucket=repo.bucket,
                    Key=key,
                    CopySource=copy_source,
                    StorageClass='GLACIER',
                )

                # Log success
                logging.info(f"Successfully moved s3://{repo.bucket}/{key} to GLACIER")
                object_count += 1

            except botocore.exceptions.ClientError as e:
                logging.error(f"Failed to move s3://{repo.bucket}/{key}: {e}")
                success = False
                continue
        # Log summary
        logging.info(
            f"Processed {object_count} objects in s3://{repo.bucket}/{base_path}"
        )
        if success:
            logging.info("All objects successfully moved to GLACIER")
        else:
            logging.warning("Some objects failed to move to GLACIER")

        return success

    except botocore.exceptions.ClientError as e:
        logging.error(f"Failed to process bucket s3://{repo.bucket}: {e}")
        return False


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
        # Filter out doctype as it's not accepted by Settings constructor
        source_data = doc["_source"].copy()
        source_data.pop('doctype', None)
        return Settings(**source_data)
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
    query = {"query": {"match": {"doctype": "repository"}}, "size": 10000}
    logging.debug("Searching for repos")
    response = client.search(index=STATUS_INDEX, body=query)
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
    query = {"query": {"match": {"doctype": "repository"}}, "size": 10000}
    response = client.search(index=STATUS_INDEX, body=query)
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
    return [Repository(**repo["_source"], docid=repo["_id"]) for repo in repos]


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
    # Get repository info from Elasticsearch
    repo_info = client.snapshot.get_repository(name=repo)[repo]
    bucket = repo_info["settings"]["bucket"]
    base_path = repo_info["settings"]["base_path"]

    # Get repository object from status index
    repo_obj = get_repository(client, repo)
    repo_obj.bucket = bucket if not repo_obj.bucket else repo_obj.bucket
    repo_obj.base_path = base_path if not repo_obj.base_path else repo_obj.base_path

    # Try to update date ranges using the shared utility function
    # This will fall back gracefully if indices aren't available
    updated = update_repository_date_range(client, repo_obj)
    if updated:
        loggit.info("Successfully updated date range for %s before unmounting", repo)
    else:
        loggit.debug(
            "Could not update date range for %s (keeping existing dates: %s to %s)",
            repo,
            repo_obj.start.isoformat() if repo_obj.start else "None",
            repo_obj.end.isoformat() if repo_obj.end else "None"
        )

    # Mark repository as unmounted
    repo_obj.unmount()
    msg = f"Recording repository details as {repo_obj}"
    loggit.debug(msg)

    # Remove the repository from Elasticsearch
    loggit.debug("Removing repo %s", repo)
    try:
        client.snapshot.delete_repository(name=repo)
    except Exception as e:
        loggit.warning("Repository %s could not be unmounted due to %s", repo, e)
        loggit.warning("Another attempt will be made when rotate runs next")

    # Update the status index with final repository state
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


def update_repository_date_range(client: Elasticsearch, repo: Repository) -> bool:
    """
    Update the date range for a repository by querying mounted indices.

    Tries multiple index naming patterns (original, partial-, restored-) to find
    mounted indices, queries their timestamp ranges, and updates the Repository
    object and persists it to the status index.

    :param client: A client connection object
    :type client: Elasticsearch
    :param repo: The repository to update
    :type repo: Repository

    :returns: True if dates were updated, False otherwise
    :rtype: bool

    :raises Exception: If the repository does not exist
    """
    loggit = logging.getLogger("curator.actions.deepfreeze")
    loggit.debug("Updating date range for repository %s", repo.name)

    try:
        # Get all indices from snapshots in this repository
        snapshot_indices = get_all_indices_in_repo(client, repo.name)
        loggit.debug("Found %d indices in snapshots", len(snapshot_indices))

        # Find which indices are actually mounted (try multiple naming patterns)
        mounted_indices = []
        for idx in snapshot_indices:
            # Try original name
            if client.indices.exists(index=idx):
                mounted_indices.append(idx)
                loggit.debug("Found mounted index: %s", idx)
            # Try with partial- prefix (searchable snapshots)
            elif client.indices.exists(index=f"partial-{idx}"):
                mounted_indices.append(f"partial-{idx}")
                loggit.debug("Found mounted searchable snapshot: partial-%s", idx)
            # Try with restored- prefix (fully restored indices)
            elif client.indices.exists(index=f"restored-{idx}"):
                mounted_indices.append(f"restored-{idx}")
                loggit.debug("Found restored index: restored-%s", idx)

        if not mounted_indices:
            loggit.debug("No mounted indices found for repository %s", repo.name)
            return False

        loggit.debug("Found %d mounted indices", len(mounted_indices))

        # Query timestamp ranges
        earliest, latest = get_timestamp_range(client, mounted_indices)

        if not earliest or not latest:
            loggit.warning("Could not determine timestamp range for repository %s", repo.name)
            return False

        loggit.debug("Timestamp range: %s to %s", earliest, latest)

        # Update repository dates to reflect currently mounted indices
        # Always replace (not expand) to accurately track what's actually mounted
        earliest_dt = decode_date(earliest)
        latest_dt = decode_date(latest)

        # Check if dates have actually changed
        changed = False
        if repo.start != earliest_dt or repo.end != latest_dt:
            repo.start = earliest_dt
            repo.end = latest_dt
            changed = True
            loggit.debug(
                "Updated date range to %s - %s (was %s - %s)",
                earliest_dt,
                latest_dt,
                repo.start,
                repo.end,
            )

        if changed:
            # Persist to status index
            query = {"query": {"term": {"name.keyword": repo.name}}}
            response = client.search(index=STATUS_INDEX, body=query)

            if response["hits"]["total"]["value"] > 0:
                doc_id = response["hits"]["hits"][0]["_id"]
                client.update(
                    index=STATUS_INDEX,
                    id=doc_id,
                    body={"doc": repo.to_dict()}
                )
                loggit.info(
                    "Updated date range for %s: %s to %s",
                    repo.name,
                    repo.start.isoformat() if repo.start else None,
                    repo.end.isoformat() if repo.end else None
                )
            else:
                # Create new document if it doesn't exist
                client.index(index=STATUS_INDEX, body=repo.to_dict())
                loggit.info("Created status document for %s with date range", repo.name)

            return True
        else:
            loggit.debug("No date range changes for repository %s", repo.name)
            return False

    except Exception as e:
        loggit.error("Error updating date range for repository %s: %s", repo.name, e)
        return False


def find_repos_by_date_range(
    client: Elasticsearch, start: datetime, end: datetime
) -> list[Repository]:
    """
    Find repositories that contain data overlapping with the given date range.

    :param client: A client connection object
    :type client: Elasticsearch
    :param start: The start of the date range
    :type start: datetime
    :param end: The end of the date range
    :type end: datetime

    :returns: A list of repositories with overlapping date ranges
    :rtype: list[Repository]

    :raises Exception: If the status index does not exist
    """
    loggit = logging.getLogger("curator.actions.deepfreeze")
    loggit.debug(
        "Finding repositories with data between %s and %s",
        start.isoformat(),
        end.isoformat(),
    )

    # Query for repositories where the date range overlaps with the requested range
    # Overlap occurs if: repo.start <= end AND repo.end >= start
    query = {
        "query": {
            "bool": {
                "must": [
                    {"term": {"doctype": "repository"}},
                    {"range": {"start": {"lte": end.isoformat()}}},
                    {"range": {"end": {"gte": start.isoformat()}}},
                ]
            }
        },
        "size": 10000
    }

    try:
        response = client.search(index=STATUS_INDEX, body=query)
        repos = response["hits"]["hits"]
        loggit.debug("Found %d repositories matching date range", len(repos))
        return [Repository(**repo["_source"], docid=repo["_id"]) for repo in repos]
    except NotFoundError:
        loggit.warning("Status index not found")
        return []


def check_restore_status(s3: S3Client, bucket: str, base_path: str) -> dict:
    """
    Check the restoration status of objects in an S3 bucket.

    Uses head_object to check the Restore metadata field, which is the only way
    to determine if a Glacier object has been restored (storage class remains GLACIER
    even after restoration).

    :param s3: The S3 client object
    :type s3: S3Client
    :param bucket: The bucket name
    :type bucket: str
    :param base_path: The base path in the bucket
    :type base_path: str

    :returns: A dictionary with restoration status information
    :rtype: dict

    :raises Exception: If the bucket or objects cannot be accessed
    """
    loggit = logging.getLogger("curator.actions.deepfreeze")
    loggit.debug("Checking restore status for s3://%s/%s", bucket, base_path)

    # Normalize base_path
    normalized_path = base_path.strip("/")
    if normalized_path:
        normalized_path += "/"

    objects = s3.list_objects(bucket, normalized_path)

    total_count = len(objects)
    restored_count = 0
    in_progress_count = 0
    not_restored_count = 0

    for obj in objects:
        key = obj["Key"]
        storage_class = obj.get("StorageClass", "STANDARD")

        # For objects in instant-access tiers, no need to check restore status
        if storage_class in [
            "STANDARD",
            "STANDARD_IA",
            "ONEZONE_IA",
            "INTELLIGENT_TIERING",
        ]:
            restored_count += 1
            continue

        # For Glacier objects, must use head_object to check Restore metadata
        try:
            metadata = s3.head_object(bucket, key)
            restore_header = metadata.get("Restore")

            if restore_header:
                # Restore header exists - parse it to check status
                # Format: 'ongoing-request="true"' or 'ongoing-request="false", expiry-date="..."'
                if 'ongoing-request="true"' in restore_header:
                    in_progress_count += 1
                    loggit.debug("Object %s: restoration in progress", key)
                else:
                    # ongoing-request="false" means restoration is complete
                    restored_count += 1
                    loggit.debug("Object %s: restored (expiry in header)", key)
            else:
                # No Restore header means object is in Glacier and not being restored
                not_restored_count += 1
                loggit.debug("Object %s: in %s, not restored", key, storage_class)

        except Exception as e:
            loggit.warning("Failed to check restore status for %s: %s", key, e)
            # Count as not restored if we can't determine status
            not_restored_count += 1

    status = {
        "total": total_count,
        "restored": restored_count,
        "in_progress": in_progress_count,
        "not_restored": not_restored_count,
        "complete": (restored_count == total_count) if total_count > 0 else False,
    }

    loggit.debug("Restore status: %s", status)
    return status


def mount_repo(client: Elasticsearch, repo: Repository) -> None:
    """
    Mount a repository by creating it in Elasticsearch and updating its status.

    :param client: A client connection object
    :type client: Elasticsearch
    :param repo: The repository to mount
    :type repo: Repository

    :return: None
    :rtype: None

    :raises Exception: If the repository cannot be created
    """
    loggit = logging.getLogger("curator.actions.deepfreeze")
    loggit.info("Mounting repository %s", repo.name)

    # Get settings to retrieve canned_acl and storage_class
    settings = get_settings(client)

    # Create the repository in Elasticsearch
    try:
        client.snapshot.create_repository(
            name=repo.name,
            body={
                "type": "s3",
                "settings": {
                    "bucket": repo.bucket,
                    "base_path": repo.base_path,
                    "canned_acl": settings.canned_acl,
                    "storage_class": settings.storage_class,
                },
            },
        )
        loggit.info("Repository %s created successfully", repo.name)

        # Mark repository as thawed (uses new state machine)
        repo.mark_thawed()
        repo.persist(client)
        loggit.info("Repository %s status updated to 'thawed'", repo.name)

    except Exception as e:
        loggit.error("Failed to mount repository %s: %s", repo.name, e)
        raise ActionError(f"Failed to mount repository {repo.name}: {e}")


def save_thaw_request(
    client: Elasticsearch,
    request_id: str,
    repos: list[Repository],
    status: str,
    start_date: datetime = None,
    end_date: datetime = None,
) -> None:
    """
    Save a thaw request to the status index for later querying.

    :param client: A client connection object
    :type client: Elasticsearch
    :param request_id: A unique identifier for this thaw request
    :type request_id: str
    :param repos: The list of repositories being thawed
    :type repos: list[Repository]
    :param status: The current status of the thaw request
    :type status: str
    :param start_date: Start of the date range for this thaw request
    :type start_date: datetime
    :param end_date: End of the date range for this thaw request
    :type end_date: datetime

    :return: None
    :rtype: None

    :raises Exception: If the request cannot be saved
    """
    loggit = logging.getLogger("curator.actions.deepfreeze")
    loggit.debug("Saving thaw request %s", request_id)

    request_doc = {
        "doctype": "thaw_request",
        "request_id": request_id,
        "repos": [repo.name for repo in repos],
        "status": status,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }

    # Add date range if provided
    if start_date:
        request_doc["start_date"] = start_date.isoformat()
    if end_date:
        request_doc["end_date"] = end_date.isoformat()

    try:
        client.index(index=STATUS_INDEX, id=request_id, body=request_doc)
        loggit.info("Thaw request %s saved successfully", request_id)
    except Exception as e:
        loggit.error("Failed to save thaw request %s: %s", request_id, e)
        raise ActionError(f"Failed to save thaw request {request_id}: {e}")


def get_thaw_request(client: Elasticsearch, request_id: str) -> dict:
    """
    Retrieve a thaw request from the status index by ID.

    :param client: A client connection object
    :type client: Elasticsearch
    :param request_id: The thaw request ID
    :type request_id: str

    :returns: The thaw request document
    :rtype: dict

    :raises Exception: If the request is not found
    """
    loggit = logging.getLogger("curator.actions.deepfreeze")
    loggit.debug("Retrieving thaw request %s", request_id)

    try:
        response = client.get(index=STATUS_INDEX, id=request_id)
        return response["_source"]
    except NotFoundError:
        loggit.error("Thaw request %s not found", request_id)
        raise ActionError(f"Thaw request {request_id} not found")
    except Exception as e:
        loggit.error("Failed to retrieve thaw request %s: %s", request_id, e)
        raise ActionError(f"Failed to retrieve thaw request {request_id}: {e}")


def list_thaw_requests(client: Elasticsearch) -> list[dict]:
    """
    List all thaw requests from the status index.

    :param client: A client connection object
    :type client: Elasticsearch

    :returns: List of thaw request documents
    :rtype: list[dict]

    :raises Exception: If the query fails
    """
    loggit = logging.getLogger("curator.actions.deepfreeze")
    loggit.debug("Listing all thaw requests")

    query = {"query": {"term": {"doctype": "thaw_request"}}, "size": 10000}

    try:
        response = client.search(index=STATUS_INDEX, body=query)
        requests = response["hits"]["hits"]
        loggit.debug("Found %d thaw requests", len(requests))
        return [{"id": req["_id"], **req["_source"]} for req in requests]
    except NotFoundError:
        loggit.warning("Status index not found")
        return []
    except Exception as e:
        loggit.error("Failed to list thaw requests: %s", e)
        raise ActionError(f"Failed to list thaw requests: {e}")


def update_thaw_request(
    client: Elasticsearch, request_id: str, status: str = None, **fields
) -> None:
    """
    Update a thaw request in the status index.

    :param client: A client connection object
    :type client: Elasticsearch
    :param request_id: The thaw request ID
    :type request_id: str
    :param status: New status value (optional)
    :type: str
    :param fields: Additional fields to update
    :type fields: dict

    :return: None
    :rtype: None

    :raises Exception: If the update fails
    """
    loggit = logging.getLogger("curator.actions.deepfreeze")
    loggit.debug("Updating thaw request %s", request_id)

    update_doc = {}
    if status:
        update_doc["status"] = status
    update_doc.update(fields)

    try:
        client.update(index=STATUS_INDEX, id=request_id, doc=update_doc)
        loggit.info("Thaw request %s updated successfully", request_id)
    except Exception as e:
        loggit.error("Failed to update thaw request %s: %s", request_id, e)
        raise ActionError(f"Failed to update thaw request {request_id}: {e}")


def get_repositories_by_names(
    client: Elasticsearch, repo_names: list[str]
) -> list[Repository]:
    """
    Get Repository objects by a list of repository names.

    :param client: A client connection object
    :type client: Elasticsearch
    :param repo_names: List of repository names
    :type repo_names: list[str]

    :returns: List of Repository objects
    :rtype: list[Repository]

    :raises Exception: If the query fails
    """
    loggit = logging.getLogger("curator.actions.deepfreeze")
    loggit.debug("Getting repositories by names: %s", repo_names)

    if not repo_names:
        return []

    query = {
        "query": {
            "bool": {
                "must": [
                    {"term": {"doctype": "repository"}},
                    {"terms": {"name.keyword": repo_names}},
                ]
            }
        },
        "size": 10000
    }

    try:
        response = client.search(index=STATUS_INDEX, body=query)
        repos = response["hits"]["hits"]
        loggit.debug("Found %d repositories", len(repos))
        return [Repository(**repo["_source"], docid=repo["_id"]) for repo in repos]
    except NotFoundError:
        loggit.warning("Status index not found")
        return []
    except Exception as e:
        loggit.error("Failed to get repositories: %s", e)
        raise ActionError(f"Failed to get repositories: {e}")


def get_index_templates(client: Elasticsearch) -> dict:
    """
    Get all legacy index templates.

    :param client: A client connection object
    :type client: Elasticsearch

    :returns: Dictionary of legacy index templates
    :rtype: dict

    :raises Exception: If the query fails
    """
    loggit = logging.getLogger("curator.actions.deepfreeze")
    loggit.debug("Getting legacy index templates")
    try:
        return client.indices.get_template()
    except Exception as e:
        loggit.error("Failed to get legacy index templates: %s", e)
        raise ActionError(f"Failed to get legacy index templates: {e}")


def get_composable_templates(client: Elasticsearch) -> dict:
    """
    Get all composable index templates.

    :param client: A client connection object
    :type client: Elasticsearch

    :returns: Dictionary of composable index templates
    :rtype: dict

    :raises Exception: If the query fails
    """
    loggit = logging.getLogger("curator.actions.deepfreeze")
    loggit.debug("Getting composable index templates")
    try:
        return client.indices.get_index_template()
    except Exception as e:
        loggit.error("Failed to get composable index templates: %s", e)
        raise ActionError(f"Failed to get composable index templates: {e}")


def update_template_ilm_policy(
    client: Elasticsearch,
    template_name: str,
    old_policy_name: str,
    new_policy_name: str,
    is_composable: bool = True,
) -> bool:
    """
    Update an index template to use a new ILM policy.

    :param client: A client connection object
    :type client: Elasticsearch
    :param template_name: The name of the template to update
    :type template_name: str
    :param old_policy_name: The old policy name to replace
    :type old_policy_name: str
    :param new_policy_name: The new policy name
    :type new_policy_name: str
    :param is_composable: Whether this is a composable template
    :type is_composable: bool

    :returns: True if template was updated, False otherwise
    :rtype: bool

    :raises Exception: If the update fails
    """
    loggit = logging.getLogger("curator.actions.deepfreeze")
    loggit.debug(
        "Updating template %s from policy %s to %s",
        template_name,
        old_policy_name,
        new_policy_name,
    )

    try:
        if is_composable:
            # Get composable template
            templates = client.indices.get_index_template(name=template_name)
            if not templates or "index_templates" not in templates:
                loggit.warning("Template %s not found", template_name)
                return False

            template = templates["index_templates"][0]["index_template"]

            # Check if template uses the old policy
            ilm_policy = template.get("template", {}).get("settings", {}).get("index", {}).get("lifecycle", {}).get("name")

            if ilm_policy == old_policy_name:
                # Update the policy name
                if "template" not in template:
                    template["template"] = {}
                if "settings" not in template["template"]:
                    template["template"]["settings"] = {}
                if "index" not in template["template"]["settings"]:
                    template["template"]["settings"]["index"] = {}
                if "lifecycle" not in template["template"]["settings"]["index"]:
                    template["template"]["settings"]["index"]["lifecycle"] = {}

                template["template"]["settings"]["index"]["lifecycle"]["name"] = new_policy_name

                # Put the updated template
                client.indices.put_index_template(name=template_name, body=template)
                loggit.info("Updated composable template %s to use policy %s", template_name, new_policy_name)
                return True
        else:
            # Get legacy template
            templates = client.indices.get_template(name=template_name)
            if not templates or template_name not in templates:
                loggit.warning("Template %s not found", template_name)
                return False

            template = templates[template_name]

            # Check if template uses the old policy
            ilm_policy = template.get("settings", {}).get("index", {}).get("lifecycle", {}).get("name")

            if ilm_policy == old_policy_name:
                # Update the policy name
                if "settings" not in template:
                    template["settings"] = {}
                if "index" not in template["settings"]:
                    template["settings"]["index"] = {}
                if "lifecycle" not in template["settings"]["index"]:
                    template["settings"]["index"]["lifecycle"] = {}

                template["settings"]["index"]["lifecycle"]["name"] = new_policy_name

                # Put the updated template
                client.indices.put_template(name=template_name, body=template)
                loggit.info("Updated legacy template %s to use policy %s", template_name, new_policy_name)
                return True

        return False
    except Exception as e:
        loggit.error("Failed to update template %s: %s", template_name, e)
        raise ActionError(f"Failed to update template {template_name}: {e}")


def create_versioned_ilm_policy(
    client: Elasticsearch,
    base_policy_name: str,
    base_policy_body: dict,
    new_repo_name: str,
    suffix: str,
) -> str:
    """
    Create a versioned ILM policy with updated repository reference.

    :param client: A client connection object
    :type client: Elasticsearch
    :param base_policy_name: The base policy name
    :type base_policy_name: str
    :param base_policy_body: The base policy body
    :type base_policy_body: dict
    :param new_repo_name: The new repository name
    :type new_repo_name: str
    :param suffix: The suffix to append to the policy name
    :type suffix: str

    :returns: The new versioned policy name
    :rtype: str

    :raises Exception: If policy creation fails
    """
    loggit = logging.getLogger("curator.actions.deepfreeze")

    # Create versioned policy name
    new_policy_name = f"{base_policy_name}-{suffix}"

    loggit.debug(
        "Creating versioned policy %s referencing repository %s",
        new_policy_name,
        new_repo_name,
    )

    # Deep copy the policy body to avoid modifying the original
    import copy
    new_policy_body = copy.deepcopy(base_policy_body)

    # Update all searchable_snapshot repository references
    if "phases" in new_policy_body:
        for phase_name, phase_config in new_policy_body["phases"].items():
            if "actions" in phase_config and "searchable_snapshot" in phase_config["actions"]:
                phase_config["actions"]["searchable_snapshot"]["snapshot_repository"] = new_repo_name
                loggit.debug(
                    "Updated %s phase to reference repository %s",
                    phase_name,
                    new_repo_name,
                )

    # Create the new policy
    try:
        client.ilm.put_lifecycle(name=new_policy_name, policy=new_policy_body)
        loggit.info("Created versioned ILM policy %s", new_policy_name)
        return new_policy_name
    except Exception as e:
        loggit.error("Failed to create policy %s: %s", new_policy_name, e)
        raise ActionError(f"Failed to create policy {new_policy_name}: {e}")


def get_policies_for_repo(client: Elasticsearch, repo_name: str) -> dict:
    """
    Find all ILM policies that reference a specific repository.

    :param client: A client connection object
    :type client: Elasticsearch
    :param repo_name: The repository name
    :type repo_name: str

    :returns: Dictionary of policy names to policy bodies
    :rtype: dict
    """
    loggit = logging.getLogger("curator.actions.deepfreeze")
    loggit.debug("Finding policies that reference repository %s", repo_name)

    policies = client.ilm.get_lifecycle()
    matching_policies = {}

    for policy_name, policy_data in policies.items():
        policy_body = policy_data.get("policy", {})
        phases = policy_body.get("phases", {})

        for phase_name, phase_config in phases.items():
            actions = phase_config.get("actions", {})
            if "searchable_snapshot" in actions:
                snapshot_repo = actions["searchable_snapshot"].get("snapshot_repository")
                if snapshot_repo == repo_name:
                    matching_policies[policy_name] = policy_data
                    loggit.debug("Found policy %s referencing %s", policy_name, repo_name)
                    break

    loggit.info("Found %d policies referencing repository %s", len(matching_policies), repo_name)
    return matching_policies


def get_policies_by_suffix(client: Elasticsearch, suffix: str) -> dict:
    """
    Find all ILM policies that end with a specific suffix.

    :param client: A client connection object
    :type client: Elasticsearch
    :param suffix: The suffix to search for (e.g., "000003")
    :type suffix: str

    :returns: Dictionary of policy names to policy bodies
    :rtype: dict
    """
    loggit = logging.getLogger("curator.actions.deepfreeze")
    loggit.debug("Finding policies ending with suffix -%s", suffix)

    policies = client.ilm.get_lifecycle()
    matching_policies = {}

    suffix_pattern = f"-{suffix}"

    for policy_name, policy_data in policies.items():
        if policy_name.endswith(suffix_pattern):
            matching_policies[policy_name] = policy_data
            loggit.debug("Found policy %s with suffix %s", policy_name, suffix)

    loggit.info("Found %d policies with suffix -%s", len(matching_policies), suffix)
    return matching_policies


def is_policy_safe_to_delete(client: Elasticsearch, policy_name: str) -> bool:
    """
    Check if an ILM policy is safe to delete (not in use by any indices/datastreams/templates).

    :param client: A client connection object
    :type client: Elasticsearch
    :param policy_name: The policy name
    :type policy_name: str

    :returns: True if safe to delete, False otherwise
    :rtype: bool
    """
    loggit = logging.getLogger("curator.actions.deepfreeze")
    loggit.debug("Checking if policy %s is safe to delete", policy_name)

    try:
        policies = client.ilm.get_lifecycle(name=policy_name)
        if policy_name not in policies:
            loggit.warning("Policy %s not found", policy_name)
            return False

        policy_data = policies[policy_name]
        in_use_by = policy_data.get("in_use_by", {})

        indices_count = len(in_use_by.get("indices", []))
        datastreams_count = len(in_use_by.get("data_streams", []))
        templates_count = len(in_use_by.get("composable_templates", []))

        total_usage = indices_count + datastreams_count + templates_count

        if total_usage > 0:
            loggit.info(
                "Policy %s is in use by %d indices, %d data streams, %d templates",
                policy_name,
                indices_count,
                datastreams_count,
                templates_count,
            )
            return False

        loggit.debug("Policy %s is safe to delete (not in use)", policy_name)
        return True
    except NotFoundError:
        loggit.warning("Policy %s not found", policy_name)
        return False
    except Exception as e:
        loggit.error("Error checking policy %s: %s", policy_name, e)
        return False


def find_snapshots_for_index(
    client: Elasticsearch, repo_name: str, index_name: str
) -> list[str]:
    """
    Find all snapshots in a repository that contain a specific index.

    :param client: A client connection object
    :type client: Elasticsearch
    :param repo_name: The repository name
    :type repo_name: str
    :param index_name: The index name to search for
    :type index_name: str

    :returns: List of snapshot names containing the index
    :rtype: list[str]
    """
    loggit = logging.getLogger("curator.actions.deepfreeze")
    loggit.debug("Finding snapshots containing index %s in repo %s", index_name, repo_name)

    try:
        snapshots = client.snapshot.get(repository=repo_name, snapshot="_all")
        matching_snapshots = []

        for snapshot in snapshots["snapshots"]:
            if index_name in snapshot["indices"]:
                matching_snapshots.append(snapshot["snapshot"])
                loggit.debug(
                    "Found index %s in snapshot %s", index_name, snapshot["snapshot"]
                )

        loggit.info(
            "Found %d snapshots containing index %s", len(matching_snapshots), index_name
        )
        return matching_snapshots

    except Exception as e:
        loggit.error("Failed to find snapshots for index %s: %s", index_name, e)
        return []


def mount_snapshot_index(
    client: Elasticsearch, repo_name: str, snapshot_name: str, index_name: str
) -> bool:
    """
    Mount an index from a snapshot as a searchable snapshot.

    :param client: A client connection object
    :type client: Elasticsearch
    :param repo_name: The repository name
    :type repo_name: str
    :param snapshot_name: The snapshot name
    :type snapshot_name: str
    :param index_name: The index name to mount
    :type index_name: str

    :returns: True if successful, False otherwise
    :rtype: bool
    """
    loggit = logging.getLogger("curator.actions.deepfreeze")
    loggit.info(
        "Mounting index %s from snapshot %s/%s", index_name, repo_name, snapshot_name
    )

    # Check if index is already mounted
    if client.indices.exists(index=index_name):
        loggit.info("Index %s is already mounted", index_name)
        return True

    try:
        client.searchable_snapshots.mount(
            repository=repo_name,
            snapshot=snapshot_name,
            body={"index": index_name},
        )
        loggit.info("Successfully mounted index %s", index_name)
        return True

    except Exception as e:
        loggit.error("Failed to mount index %s: %s", index_name, e)
        return False


def get_index_datastream_name(client: Elasticsearch, index_name: str) -> str:
    """
    Get the data stream name for an index by checking its settings.

    Only returns a data stream name if the index has concrete metadata
    indicating it was part of a data stream.

    :param client: A client connection object
    :type client: Elasticsearch
    :param index_name: The index name
    :type index_name: str

    :returns: The data stream name if the index was part of one, None otherwise
    :rtype: str
    """
    loggit = logging.getLogger("curator.actions.deepfreeze")

    try:
        # Get index settings to check for data stream metadata
        settings = client.indices.get_settings(index=index_name)

        if index_name in settings:
            index_settings = settings[index_name].get("settings", {})
            index_metadata = index_settings.get("index", {})

            # Check if this index has data stream metadata
            # Data stream backing indices have a hidden setting indicating their data stream
            datastream_name = index_metadata.get("provided_name")

            # Also check if index was created by a data stream
            if datastream_name and datastream_name.startswith(".ds-"):
                # Extract the actual data stream name from the backing index name
                # Pattern: .ds-{name}-{date}-{number}
                remaining = datastream_name[4:]
                parts = remaining.rsplit("-", 2)
                if len(parts) >= 3:
                    ds_name = parts[0]
                    loggit.debug("Index %s belongs to data stream %s", index_name, ds_name)
                    return ds_name

        return None

    except Exception as e:
        loggit.debug("Could not determine data stream for index %s: %s", index_name, e)
        return None


def add_index_to_datastream(
    client: Elasticsearch, datastream_name: str, index_name: str
) -> bool:
    """
    Add a backing index back to its data stream.

    :param client: A client connection object
    :type client: Elasticsearch
    :param datastream_name: The data stream name
    :type datastream_name: str
    :param index_name: The backing index name
    :type index_name: str

    :returns: True if successful, False otherwise
    :rtype: bool
    """
    loggit = logging.getLogger("curator.actions.deepfreeze")
    loggit.info("Adding index %s to data stream %s", index_name, datastream_name)

    try:
        # First check if data stream exists
        try:
            client.indices.get_data_stream(name=datastream_name)
        except NotFoundError:
            loggit.warning("Data stream %s does not exist", datastream_name)
            return False

        # Add the backing index to the data stream
        client.indices.modify_data_stream(
            body={
                "actions": [
                    {"add_backing_index": {"data_stream": datastream_name, "index": index_name}}
                ]
            }
        )
        loggit.info("Successfully added index %s to data stream %s", index_name, datastream_name)
        return True

    except Exception as e:
        loggit.error("Failed to add index %s to data stream %s: %s", index_name, datastream_name, e)
        return False


def find_and_mount_indices_in_date_range(
    client: Elasticsearch, repos: list[Repository], start_date: datetime, end_date: datetime
) -> dict:
    """
    Find and mount all indices within a date range from the given repositories.

    For each index found:
    1. Mount it as a searchable snapshot
    2. Check if its @timestamp range overlaps with the requested date range
    3. If no overlap, unmount the index
    4. If overlap and it's a data stream backing index, add it back to the data stream

    :param client: A client connection object
    :type client: Elasticsearch
    :param repos: List of repositories to search
    :type repos: list[Repository]
    :param start_date: Start of date range
    :type start_date: datetime
    :param end_date: End of date range
    :type end_date: datetime

    :returns: Dictionary with mounted, skipped, and failed counts
    :rtype: dict
    """
    loggit = logging.getLogger("curator.actions.deepfreeze")
    loggit.info(
        "Finding and mounting indices between %s and %s",
        start_date.isoformat(),
        end_date.isoformat(),
    )

    mounted_indices = []
    skipped_indices = []
    failed_indices = []
    datastream_adds = {"successful": [], "failed": []}

    for repo in repos:
        try:
            # Get all indices from snapshots in this repository
            all_indices = get_all_indices_in_repo(client, repo.name)
            loggit.debug("Found %d indices in repository %s", len(all_indices), repo.name)

            # For each index, check if it overlaps with the date range
            for index_name in all_indices:
                # Find the snapshot containing this index (use the latest one)
                snapshots = find_snapshots_for_index(client, repo.name, index_name)
                if not snapshots:
                    loggit.warning("No snapshots found for index %s", index_name)
                    continue

                # Use the most recent snapshot
                snapshot_name = snapshots[-1]

                # Mount the index temporarily to check its date range
                if not mount_snapshot_index(client, repo.name, snapshot_name, index_name):
                    failed_indices.append(index_name)
                    continue

                # Query the index to get its actual @timestamp range
                try:
                    index_start, index_end = get_timestamp_range(client, [index_name])

                    if not index_start or not index_end:
                        loggit.warning(
                            "Could not determine date range for %s, keeping mounted",
                            index_name
                        )
                        mounted_indices.append(index_name)
                    else:
                        # Check if index date range overlaps with requested range
                        # Overlap occurs if: index_start <= end_date AND index_end >= start_date
                        index_start_dt = decode_date(index_start)
                        index_end_dt = decode_date(index_end)

                        if index_start_dt <= end_date and index_end_dt >= start_date:
                            loggit.info(
                                "Index %s overlaps date range (%s to %s), keeping mounted",
                                index_name,
                                index_start_dt.isoformat(),
                                index_end_dt.isoformat(),
                            )
                            mounted_indices.append(index_name)

                            # Check if this index was actually part of a data stream
                            # by examining its metadata (not just naming patterns)
                            datastream_name = get_index_datastream_name(client, index_name)
                            if datastream_name:
                                loggit.info(
                                    "Index %s was part of data stream %s, attempting to re-add",
                                    index_name,
                                    datastream_name,
                                )
                                if add_index_to_datastream(client, datastream_name, index_name):
                                    datastream_adds["successful"].append(
                                        {"index": index_name, "datastream": datastream_name}
                                    )
                                else:
                                    datastream_adds["failed"].append(
                                        {"index": index_name, "datastream": datastream_name}
                                    )
                            else:
                                loggit.debug(
                                    "Index %s is not a data stream backing index, skipping data stream step",
                                    index_name,
                                )
                        else:
                            loggit.info(
                                "Index %s does not overlap date range (%s to %s), unmounting",
                                index_name,
                                index_start_dt.isoformat(),
                                index_end_dt.isoformat(),
                            )
                            # Unmount the index since it's outside the date range
                            try:
                                client.indices.delete(index=index_name)
                                loggit.debug("Unmounted index %s", index_name)
                            except Exception as e:
                                loggit.warning("Failed to unmount index %s: %s", index_name, e)
                            skipped_indices.append(index_name)

                except Exception as e:
                    loggit.warning(
                        "Error checking date range for index %s: %s, keeping mounted",
                        index_name,
                        e
                    )
                    mounted_indices.append(index_name)

        except Exception as e:
            loggit.error("Error processing repository %s: %s", repo.name, e)

    result = {
        "mounted": len(mounted_indices),
        "skipped": len(skipped_indices),
        "failed": len(failed_indices),
        "mounted_indices": mounted_indices,
        "skipped_indices": skipped_indices,
        "failed_indices": failed_indices,
        "datastream_successful": len(datastream_adds["successful"]),
        "datastream_failed": len(datastream_adds["failed"]),
        "datastream_details": datastream_adds,
    }

    loggit.info(
        "Mounted %d indices, skipped %d outside date range, failed %d. Added %d to data streams.",
        result["mounted"],
        result["skipped"],
        result["failed"],
        result["datastream_successful"],
    )

    return result
