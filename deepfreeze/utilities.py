"""Utility functions for deepfreeze

This module contains utility functions for the standalone deepfreeze package.
These are extracted from curator/actions/deepfreeze/utilities.py with curator
dependencies removed. The CreateIndex dependency has been replaced with direct
Elasticsearch client calls.
"""

# pylint: disable=too-many-arguments,too-many-instance-attributes, raise-missing-from

import copy
import logging
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import botocore
from elasticsearch8 import Elasticsearch, NotFoundError

from deepfreeze.exceptions import ActionError, MissingIndexError
from deepfreeze.constants import SETTINGS_ID, STATUS_INDEX
from deepfreeze.helpers import Repository, Settings
from deepfreeze.s3client import S3Client


def push_to_glacier(s3: S3Client, repo: Repository) -> bool:
    """Push objects to Glacier storage

    :param s3: The S3 client object
    :type s3: S3Client
    :param repo: The repository to push to Glacier
    :type repo: Repository

    :return: True if all objects were successfully moved, False otherwise
    :rtype: bool

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
                "Processing object: s3://%s/%s (Current: %s)",
                repo.bucket, key, current_storage_class
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
                logging.info("Successfully moved s3://%s/%s to GLACIER", repo.bucket, key)
                object_count += 1

            except botocore.exceptions.ClientError as e:
                logging.error("Failed to move s3://%s/%s: %s", repo.bucket, key, e)
                success = False
                continue

        # Log summary
        logging.info(
            "Processed %d objects in s3://%s/%s", object_count, repo.bucket, base_path
        )
        if success:
            logging.info("All objects successfully moved to GLACIER")
        else:
            logging.warning("Some objects failed to move to GLACIER")

        return success

    except botocore.exceptions.ClientError as e:
        logging.error("Failed to process bucket s3://%s: %s", repo.bucket, e)
        return False


def get_all_indices_in_repo(client: Elasticsearch, repository: str) -> list:
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

    snapshots = client.snapshot.get(repository=repository, snapshot="_all")
    for snapshot in snapshots["snapshots"]:
        indices.update(snapshot["indices"])

    return list(indices)


def get_timestamp_range(
    client: Elasticsearch, indices: list
) -> tuple:
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

    logging.debug("Earliest: %s, Latest: %s", earliest, latest)

    return datetime.fromisoformat(earliest), datetime.fromisoformat(latest)


def ensure_settings_index(
    client: Elasticsearch, create_if_missing: bool = False
) -> None:
    """
    Ensure that the status index exists in Elasticsearch.

    This function replaces the curator.actions.CreateIndex dependency with
    direct Elasticsearch client calls.

    :param client: A client connection object
    :type client: Elasticsearch
    :param create_if_missing: If True, create the index if it doesn't exist

    :return: None
    :rtype: None

    :raises MissingIndexError: If the index doesn't exist and create_if_missing is False
    """
    loggit = logging.getLogger("deepfreeze.utilities")

    if create_if_missing:
        if not client.indices.exists(index=STATUS_INDEX):
            loggit.info("Creating index %s", STATUS_INDEX)
            # Direct index creation - no curator dependency
            client.indices.create(
                index=STATUS_INDEX,
                body={
                    "settings": {
                        "number_of_shards": 1,
                        "number_of_replicas": 1,
                    },
                    "mappings": {
                        "properties": {
                            "doctype": {"type": "keyword"},
                            "name": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                            "bucket": {"type": "keyword"},
                            "base_path": {"type": "keyword"},
                            "start": {"type": "date"},
                            "end": {"type": "date"},
                            "is_thawed": {"type": "boolean"},
                            "is_mounted": {"type": "boolean"},
                            "thaw_state": {"type": "keyword"},
                            "thawed_at": {"type": "date"},
                            "expires_at": {"type": "date"},
                            "request_id": {"type": "keyword"},
                            "repos": {"type": "keyword"},
                            "status": {"type": "keyword"},
                            "created_at": {"type": "date"},
                            "start_date": {"type": "date"},
                            "end_date": {"type": "date"},
                            # Settings fields
                            "repo_name_prefix": {"type": "keyword"},
                            "bucket_name_prefix": {"type": "keyword"},
                            "base_path_prefix": {"type": "keyword"},
                            "canned_acl": {"type": "keyword"},
                            "storage_class": {"type": "keyword"},
                            "provider": {"type": "keyword"},
                            "rotate_by": {"type": "keyword"},
                            "style": {"type": "keyword"},
                            "last_suffix": {"type": "keyword"},
                            "ilm_policy_name": {"type": "keyword"},
                            "index_template_name": {"type": "keyword"},
                            "thaw_request_retention_days_completed": {"type": "integer"},
                            "thaw_request_retention_days_failed": {"type": "integer"},
                            "thaw_request_retention_days_refrozen": {"type": "integer"},
                        }
                    }
                }
            )
            loggit.info("Index %s created successfully", STATUS_INDEX)
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
    :rtype: Settings

    :raises MissingIndexError: If the settings document does not exist

    :example:
        >>> get_settings(client)
        Settings(repo_name_prefix='deepfreeze', ...)
    """
    loggit = logging.getLogger("deepfreeze.utilities")
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

    :raises ActionError: If the settings document cannot be created or updated
    """
    loggit = logging.getLogger("deepfreeze.utilities")
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
    :param base_path: Path within a bucket where snapshots are stored
    :type base_path: str
    :param canned_acl: One of the AWS canned ACL values
    :type canned_acl: str
    :param storage_class: AWS Storage class
    :type storage_class: str
    :param dry_run: If True, do not actually create the repository
    :type dry_run: bool

    :raises ActionError: If the repository cannot be created
    """
    loggit = logging.getLogger("deepfreeze.utilities")
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
    loggit = logging.getLogger("deepfreeze.utilities")
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


def get_all_repos(client: Elasticsearch) -> list:
    """
    Get the complete list of repos from our index and return a Repository object for each.

    :param client: A client connection object
    :type client: Elasticsearch

    :returns: The repos.
    :rtype: list[Repository]

    :raises Exception: If the repository does not exist
    """
    query = {"query": {"match": {"doctype": "repository"}}, "size": 10000}
    logging.debug("Searching for repos")
    response = client.search(index=STATUS_INDEX, body=query)
    logging.debug("Response: %s", response)
    repos = response["hits"]["hits"]
    logging.debug("Repos retrieved: %s", repos)

    rv = []
    for repo in repos:
        logging.debug("Repo: %s", repo)
        logging.debug("Repo ID: %s", repo["_id"])
        logging.debug("Repo Source: %s", repo["_source"])
        rv.append(Repository(**repo["_source"], docid=repo["_id"]))
        logging.debug("Repo object: %s", rv[-1])
    return rv


def get_matching_repo_names(client: Elasticsearch, repo_name_prefix: str) -> list:
    """
    Get the complete list of repos and return just the ones whose names
    begin with the given prefix.

    :param client: A client connection object
    :type client: Elasticsearch
    :param repo_name_prefix: A prefix for repository names
    :type repo_name_prefix: str

    :returns: The repos.
    :rtype: list[str]

    :raises Exception: If the repository does not exist
    """
    repos = client.snapshot.get_repository()
    logging.debug("Repos retrieved: %s", repos)
    pattern = re.compile(repo_name_prefix)
    logging.debug("Looking for repos matching %s", repo_name_prefix)
    return [repo for repo in repos if pattern.search(repo)]


def get_matching_repos(
    client: Elasticsearch, repo_name_prefix: str, mounted: bool = False
) -> list:
    """
    Get the list of repos from our index and return a Repository object for each one
    which matches the given prefix.

    :param client: A client connection object
    :type client: Elasticsearch
    :param repo_name_prefix: A prefix for repository names
    :type repo_name_prefix: str
    :param mounted: If True, only return mounted repos
    :type mounted: bool

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
    """
    loggit = logging.getLogger("deepfreeze.utilities")
    # Get repository info from Elasticsearch
    repo_info = client.snapshot.get_repository(name=repo)[repo]
    bucket = repo_info["settings"]["bucket"]
    base_path = repo_info["settings"]["base_path"]

    # Get repository object from status index
    repo_obj = get_repository(client, repo)
    repo_obj.bucket = bucket if not repo_obj.bucket else repo_obj.bucket
    repo_obj.base_path = base_path if not repo_obj.base_path else repo_obj.base_path

    # Try to update date ranges using the shared utility function
    updated = update_repository_date_range(client, repo_obj)
    if updated:
        loggit.info("Successfully updated date range for %s before unmounting", repo)
    else:
        loggit.debug(
            "Could not update date range for %s (keeping existing dates: %s to %s)",
            repo,
            repo_obj.start.isoformat() if repo_obj.start else "None",
            repo_obj.end.isoformat() if repo_obj.end else "None",
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


def decode_date(date_in) -> datetime:
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
    client: Elasticsearch, policy_name: str, policy_body: dict
) -> None:
    """
    Create an ILM policy.

    :param client: A client connection object
    :type client: Elasticsearch
    :param policy_name: The name of the policy to create
    :type policy_name: str
    :param policy_body: The policy body dictionary
    :type policy_body: dict

    :return: None
    :rtype: None

    :raises ActionError: If the policy cannot be created
    """
    loggit = logging.getLogger("deepfreeze.utilities")
    loggit.info("Creating ILM policy %s", policy_name)
    try:
        client.ilm.put_lifecycle(name=policy_name, body=policy_body)
    except Exception as e:
        loggit.error(e)
        raise ActionError(e)


def get_ilm_policy(client: Elasticsearch, policy_name: str) -> dict:
    """
    Get an ILM policy by name.

    :param client: A client connection object
    :type client: Elasticsearch
    :param policy_name: The name of the policy to retrieve
    :type policy_name: str

    :returns: The policy dictionary if found, None otherwise
    :rtype: dict | None
    """
    loggit = logging.getLogger("deepfreeze.utilities")
    loggit.debug("Getting ILM policy %s", policy_name)
    try:
        policies = client.ilm.get_lifecycle(name=policy_name)
        if policy_name in policies:
            return policies[policy_name]
        return None
    except NotFoundError:
        loggit.debug("ILM policy %s not found", policy_name)
        return None
    except Exception as e:
        loggit.error("Error getting ILM policy %s: %s", policy_name, e)
        return None


def create_or_update_ilm_policy(
    client: Elasticsearch, policy_name: str, repo_name: str
) -> dict:
    """
    Create a new ILM policy or update an existing one to use the deepfreeze repository.

    If the policy does not exist, creates a new policy with a reasonable tiering strategy:
    - Hot: 7 days (with rollover at 45GB or 7d)
    - Cold: 30 days
    - Frozen: 365 days (searchable snapshot to deepfreeze repo)
    - Delete: after frozen phase (delete_searchable_snapshot=false)

    If the policy exists, updates any searchable_snapshot actions to use the new repository.

    :param client: A client connection object
    :type client: Elasticsearch
    :param policy_name: The name of the policy to create or update
    :type policy_name: str
    :param repo_name: The repository name to use in the frozen phase
    :type repo_name: str

    :returns: Dictionary with 'action' ('created' or 'updated') and 'policy_body'
    :rtype: dict

    :raises ActionError: If the policy cannot be created or updated
    """
    loggit = logging.getLogger("deepfreeze.utilities")

    # Define the default policy with reasonable tiering strategy
    default_policy_body = {
        "policy": {
            "phases": {
                "hot": {
                    "min_age": "0ms",
                    "actions": {"rollover": {"max_size": "45gb", "max_age": "7d"}},
                },
                "cold": {
                    "min_age": "30d",
                    "actions": {"set_priority": {"priority": 0}},
                },
                "frozen": {
                    "min_age": "365d",
                    "actions": {
                        "searchable_snapshot": {"snapshot_repository": repo_name}
                    },
                },
                "delete": {
                    "min_age": "0d",  # Relative to frozen phase completion
                    "actions": {"delete": {"delete_searchable_snapshot": False}},
                },
            }
        }
    }

    existing_policy = get_ilm_policy(client, policy_name)

    if existing_policy is None:
        # Create new policy
        loggit.info(
            "Creating new ILM policy %s with default tiering strategy", policy_name
        )
        create_ilm_policy(client, policy_name, default_policy_body)
        return {"action": "created", "policy_body": default_policy_body}
    else:
        # Update existing policy to use the new repository
        loggit.info(
            "Updating existing ILM policy %s to use repository %s",
            policy_name,
            repo_name,
        )

        # Deep copy the existing policy to modify it
        updated_policy = copy.deepcopy(existing_policy)
        policy_phases = updated_policy.get("policy", {}).get("phases", {})

        # Update any searchable_snapshot actions to use the new repository
        modified = False
        for phase_name, phase_config in policy_phases.items():
            actions = phase_config.get("actions", {})
            if "searchable_snapshot" in actions:
                old_repo = actions["searchable_snapshot"].get(
                    "snapshot_repository", "N/A"
                )
                actions["searchable_snapshot"]["snapshot_repository"] = repo_name
                loggit.info(
                    "Updated %s phase: snapshot_repository %s -> %s",
                    phase_name,
                    old_repo,
                    repo_name,
                )
                modified = True

        # Also ensure delete phase has delete_searchable_snapshot=false
        if "delete" in policy_phases:
            delete_actions = policy_phases["delete"].get("actions", {})
            if "delete" in delete_actions:
                if (
                    delete_actions["delete"].get("delete_searchable_snapshot")
                    is not False
                ):
                    delete_actions["delete"]["delete_searchable_snapshot"] = False
                    loggit.info(
                        "Updated delete phase: delete_searchable_snapshot -> false"
                    )
                    modified = True

        if modified:
            # Re-structure for the API call
            policy_body = {"policy": updated_policy.get("policy", {})}
            create_ilm_policy(client, policy_name, policy_body)
            return {"action": "updated", "policy_body": policy_body}
        else:
            loggit.info(
                "ILM policy %s has no searchable_snapshot actions to update",
                policy_name,
            )
            return {
                "action": "unchanged",
                "policy_body": {"policy": updated_policy.get("policy", {})},
            }


def update_index_template_ilm_policy(
    client: Elasticsearch, template_name: str, ilm_policy_name: str
) -> dict:
    """
    Update an index template to use a specific ILM policy.

    Supports both composable templates (ES 7.8+) and legacy templates.

    :param client: A client connection object
    :type client: Elasticsearch
    :param template_name: The name of the template to update
    :type template_name: str
    :param ilm_policy_name: The name of the ILM policy to assign
    :type ilm_policy_name: str

    :returns: Dictionary with 'action' ('updated' or 'not_found') and details
    :rtype: dict

    :raises ActionError: If the template cannot be updated
    """
    loggit = logging.getLogger("deepfreeze.utilities")
    loggit.info(
        "Updating index template %s to use ILM policy %s",
        template_name,
        ilm_policy_name,
    )

    # First try composable templates (ES 7.8+)
    try:
        templates = client.indices.get_index_template(name=template_name)
        if (
            templates
            and "index_templates" in templates
            and len(templates["index_templates"]) > 0
        ):
            template_data = templates["index_templates"][0]["index_template"]
            loggit.debug("Found composable template %s", template_name)

            # Ensure template structure exists
            if "template" not in template_data:
                template_data["template"] = {}
            if "settings" not in template_data["template"]:
                template_data["template"]["settings"] = {}
            if "index" not in template_data["template"]["settings"]:
                template_data["template"]["settings"]["index"] = {}
            if "lifecycle" not in template_data["template"]["settings"]["index"]:
                template_data["template"]["settings"]["index"]["lifecycle"] = {}

            # Get old policy name for logging
            old_policy = template_data["template"]["settings"]["index"][
                "lifecycle"
            ].get("name", "none")

            # Set the new ILM policy
            template_data["template"]["settings"]["index"]["lifecycle"][
                "name"
            ] = ilm_policy_name

            # Put the updated template
            client.indices.put_index_template(name=template_name, body=template_data)
            loggit.info(
                "Updated composable template %s: ILM policy %s -> %s",
                template_name,
                old_policy,
                ilm_policy_name,
            )
            return {
                "action": "updated",
                "template_type": "composable",
                "old_policy": old_policy,
                "new_policy": ilm_policy_name,
            }
    except NotFoundError:
        loggit.debug(
            "Composable template %s not found, trying legacy template", template_name
        )
    except Exception as e:
        loggit.debug("Error checking composable template %s: %s", template_name, e)

    # Try legacy templates
    try:
        templates = client.indices.get_template(name=template_name)
        if templates and template_name in templates:
            template_data = templates[template_name]
            loggit.debug("Found legacy template %s", template_name)

            # Ensure template structure exists
            if "settings" not in template_data:
                template_data["settings"] = {}
            if "index" not in template_data["settings"]:
                template_data["settings"]["index"] = {}
            if "lifecycle" not in template_data["settings"]["index"]:
                template_data["settings"]["index"]["lifecycle"] = {}

            # Get old policy name for logging
            old_policy = template_data["settings"]["index"]["lifecycle"].get(
                "name", "none"
            )

            # Set the new ILM policy
            template_data["settings"]["index"]["lifecycle"]["name"] = ilm_policy_name

            # Put the updated template
            client.indices.put_template(name=template_name, body=template_data)
            loggit.info(
                "Updated legacy template %s: ILM policy %s -> %s",
                template_name,
                old_policy,
                ilm_policy_name,
            )
            return {
                "action": "updated",
                "template_type": "legacy",
                "old_policy": old_policy,
                "new_policy": ilm_policy_name,
            }
    except NotFoundError:
        loggit.warning(
            "Template %s not found (checked both composable and legacy)", template_name
        )
        return {
            "action": "not_found",
            "template_type": None,
            "error": f"Template {template_name} not found",
        }
    except Exception as e:
        loggit.error("Error updating legacy template %s: %s", template_name, e)
        raise ActionError(f"Failed to update template {template_name}: {e}")


def create_thawed_ilm_policy(client: Elasticsearch, repo_name: str) -> str:
    """
    Create an ILM policy for thawed indices from a specific repository.

    The policy is named {repo_name}-thawed and includes only a delete phase
    since the indices are already mounted as searchable snapshots.

    :param client: A client connection object
    :type client: Elasticsearch
    :param repo_name: The repository name (e.g., "deepfreeze-000010")
    :type repo_name: str

    :returns: The created policy name
    :rtype: str
    """
    loggit = logging.getLogger("deepfreeze.utilities")

    policy_name = f"{repo_name}-thawed"
    policy_body = {
        "policy": {
            "phases": {
                "delete": {
                    "min_age": "29d",
                    "actions": {"delete": {"delete_searchable_snapshot": True}},
                },
            }
        }
    }

    loggit.info(
        "Creating thawed ILM policy %s for repository %s", policy_name, repo_name
    )
    loggit.debug("Thawed ILM policy body: %s", policy_body)

    try:
        # Check if policy already exists
        try:
            client.ilm.get_lifecycle(name=policy_name)
            loggit.info(
                "Thawed ILM policy %s already exists, skipping creation", policy_name
            )
            return policy_name
        except Exception:
            # Policy doesn't exist, create it
            pass

        client.ilm.put_lifecycle(name=policy_name, body=policy_body)
        loggit.info("Successfully created thawed ILM policy %s", policy_name)
        return policy_name

    except Exception as e:
        loggit.error("Failed to create thawed ILM policy %s: %s", policy_name, e)
        raise ActionError(f"Failed to create thawed ILM policy {policy_name}: {e}")


def update_repository_date_range(client: Elasticsearch, repo: Repository) -> bool:
    """
    Update the date range for a repository by querying document @timestamp values.

    Gets the actual min/max @timestamp from all indices contained in the repository's
    snapshots. The date range can only EXTEND (never shrink) as new data is added.

    For mounted repos: Queries mounted indices directly.
    For unmounted repos: Cannot query without mounting, so skips update.

    :param client: A client connection object
    :type client: Elasticsearch
    :param repo: The repository to update
    :type repo: Repository

    :returns: True if dates were updated, False otherwise
    :rtype: bool
    """
    loggit = logging.getLogger("deepfreeze.utilities")
    loggit.debug(
        "Updating date range for repository %s (mounted: %s)",
        repo.name,
        repo.is_mounted,
    )

    # Store existing range to ensure we only extend, never shrink
    existing_start = repo.start
    existing_end = repo.end

    earliest = None
    latest = None

    try:
        # Get all indices from snapshots in this repository
        snapshot_indices = get_all_indices_in_repo(client, repo.name)
        loggit.debug("Found %d indices in repository snapshots", len(snapshot_indices))

        if not snapshot_indices:
            loggit.debug("No indices found in repository %s", repo.name)
            return False

        # If repo is mounted, query the mounted indices
        if repo.is_mounted:
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

            if mounted_indices:
                loggit.debug(
                    "Found %d mounted indices, querying timestamp ranges",
                    len(mounted_indices),
                )
                # Query actual @timestamp ranges from mounted indices
                earliest, latest = get_timestamp_range(client, mounted_indices)
            else:
                loggit.debug("Repo is mounted but no searchable snapshot indices found")
                return False
        else:
            # Repo is not mounted - we cannot query @timestamp without mounting
            loggit.debug(
                "Repository %s is not mounted, cannot query document timestamps. "
                "Keeping existing date range: %s to %s",
                repo.name,
                existing_start.isoformat() if existing_start else "None",
                existing_end.isoformat() if existing_end else "None",
            )
            return False

        if not earliest or not latest:
            loggit.warning(
                "Could not determine timestamp range for repository %s", repo.name
            )
            return False

        loggit.debug("Queried timestamp range: %s to %s", earliest, latest)

        # CRITICAL: Only EXTEND the date range, never shrink it
        if existing_start and existing_end:
            # We have existing dates - extend them
            final_start = min(existing_start, earliest)
            final_end = max(existing_end, latest)

            if final_start == existing_start and final_end == existing_end:
                loggit.debug("Date range unchanged for %s", repo.name)
                return False

            loggit.info(
                "Extending date range for %s: (%s to %s) -> (%s to %s)",
                repo.name,
                existing_start.isoformat(),
                existing_end.isoformat(),
                final_start.isoformat(),
                final_end.isoformat(),
            )
        else:
            # No existing dates - use the queried range
            final_start = earliest
            final_end = latest
            loggit.info(
                "Setting initial date range for %s: %s to %s",
                repo.name,
                final_start.isoformat(),
                final_end.isoformat(),
            )

        # Update the repository object
        repo.start = final_start
        repo.end = final_end

        # Persist to status index
        query = {"query": {"term": {"name.keyword": repo.name}}}
        response = client.search(index=STATUS_INDEX, body=query)

        if response["hits"]["total"]["value"] > 0:
            doc_id = response["hits"]["hits"][0]["_id"]
            client.update(index=STATUS_INDEX, id=doc_id, body={"doc": repo.to_dict()})
        else:
            # Create new document if it doesn't exist
            client.index(index=STATUS_INDEX, body=repo.to_dict())

        return True

    except Exception as e:
        loggit.error("Error updating date range for repository %s: %s", repo.name, e)
        return False


def find_repos_by_date_range(
    client: Elasticsearch, start: datetime, end: datetime
) -> list:
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
    loggit = logging.getLogger("deepfreeze.utilities")
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
        "size": 10000,
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
    to determine if a Glacier object has been restored.

    :param s3: The S3 client object
    :type s3: S3Client
    :param bucket: The bucket name
    :type bucket: str
    :param base_path: The base path in the bucket
    :type base_path: str

    :returns: A dictionary with restoration status information
    :rtype: dict
    """
    loggit = logging.getLogger("deepfreeze.utilities")
    loggit.debug("Checking restore status for s3://%s/%s", bucket, base_path)

    # Normalize base_path
    normalized_path = base_path.strip("/")
    if normalized_path:
        normalized_path += "/"

    objects = s3.list_objects(bucket, normalized_path)
    total_count = len(objects)

    # Separate objects by storage class
    instant_access_count = 0
    glacier_objects = []

    for obj in objects:
        storage_class = obj.get("StorageClass", "STANDARD")

        if storage_class in [
            "STANDARD",
            "STANDARD_IA",
            "ONEZONE_IA",
            "INTELLIGENT_TIERING",
        ]:
            instant_access_count += 1
        else:
            glacier_objects.append(obj["Key"])

    loggit.debug(
        "Found %d instant-access objects and %d Glacier objects to check",
        instant_access_count,
        len(glacier_objects),
    )

    if not glacier_objects:
        status = {
            "total": total_count,
            "restored": instant_access_count,
            "in_progress": 0,
            "not_restored": 0,
            "complete": True if total_count > 0 else False,
        }
        loggit.debug("Restore status: %s", status)
        return status

    def check_single_object(key: str) -> tuple:
        """Check restore status for a single object."""
        try:
            metadata = s3.head_object(bucket, key)
            restore_header = metadata.get("Restore")

            if restore_header:
                if 'ongoing-request="true"' in restore_header:
                    loggit.debug("Object %s: restoration in progress", key)
                    return ("in_progress", key)
                else:
                    loggit.debug("Object %s: restored (expiry in header)", key)
                    return ("restored", key)
            else:
                loggit.debug("Object %s: in Glacier, not restored", key)
                return ("not_restored", key)

        except Exception as e:
            loggit.warning("Failed to check restore status for %s: %s", key, e)
            return ("not_restored", key)

    # Check Glacier objects in parallel
    restored_count = instant_access_count
    in_progress_count = 0
    not_restored_count = 0

    max_workers = min(15, len(glacier_objects))

    loggit.debug(
        "Checking %d Glacier objects using %d workers",
        len(glacier_objects),
        max_workers,
    )

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_key = {
            executor.submit(check_single_object, key): key for key in glacier_objects
        }

        for future in as_completed(future_to_key):
            status_result, key = future.result()

            if status_result == "restored":
                restored_count += 1
            elif status_result == "in_progress":
                in_progress_count += 1
            else:
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

    :raises ActionError: If the repository cannot be created
    """
    loggit = logging.getLogger("deepfreeze.utilities")
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
    repos: list,
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

    :raises ActionError: If the request cannot be saved
    """
    loggit = logging.getLogger("deepfreeze.utilities")
    loggit.debug("Saving thaw request %s", request_id)

    request_doc = {
        "doctype": "thaw_request",
        "request_id": request_id,
        "repos": [repo.name for repo in repos],
        "status": status,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }

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

    :raises ActionError: If the request is not found
    """
    loggit = logging.getLogger("deepfreeze.utilities")
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


def list_thaw_requests(client: Elasticsearch) -> list:
    """
    List all thaw requests from the status index.

    :param client: A client connection object
    :type client: Elasticsearch

    :returns: List of thaw request documents
    :rtype: list[dict]

    :raises ActionError: If the query fails
    """
    loggit = logging.getLogger("deepfreeze.utilities")
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

    :raises ActionError: If the update fails
    """
    loggit = logging.getLogger("deepfreeze.utilities")
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
    client: Elasticsearch, repo_names: list
) -> list:
    """
    Get Repository objects by a list of repository names.

    :param client: A client connection object
    :type client: Elasticsearch
    :param repo_names: List of repository names
    :type repo_names: list[str]

    :returns: List of Repository objects
    :rtype: list[Repository]

    :raises ActionError: If the query fails
    """
    loggit = logging.getLogger("deepfreeze.utilities")
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
        "size": 10000,
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

    :raises ActionError: If the query fails
    """
    loggit = logging.getLogger("deepfreeze.utilities")
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

    :raises ActionError: If the query fails
    """
    loggit = logging.getLogger("deepfreeze.utilities")
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

    :raises ActionError: If the update fails
    """
    loggit = logging.getLogger("deepfreeze.utilities")
    loggit.debug(
        "Updating template %s from policy %s to %s",
        template_name,
        old_policy_name,
        new_policy_name,
    )

    try:
        if is_composable:
            templates = client.indices.get_index_template(name=template_name)
            if not templates or "index_templates" not in templates:
                loggit.warning("Template %s not found", template_name)
                return False

            template = templates["index_templates"][0]["index_template"]

            ilm_policy = (
                template.get("template", {})
                .get("settings", {})
                .get("index", {})
                .get("lifecycle", {})
                .get("name")
            )

            if ilm_policy == old_policy_name:
                if "template" not in template:
                    template["template"] = {}
                if "settings" not in template["template"]:
                    template["template"]["settings"] = {}
                if "index" not in template["template"]["settings"]:
                    template["template"]["settings"]["index"] = {}
                if "lifecycle" not in template["template"]["settings"]["index"]:
                    template["template"]["settings"]["index"]["lifecycle"] = {}

                template["template"]["settings"]["index"]["lifecycle"][
                    "name"
                ] = new_policy_name

                client.indices.put_index_template(name=template_name, body=template)
                loggit.info(
                    "Updated composable template %s to use policy %s",
                    template_name,
                    new_policy_name,
                )
                return True
        else:
            templates = client.indices.get_template(name=template_name)
            if not templates or template_name not in templates:
                loggit.warning("Template %s not found", template_name)
                return False

            template = templates[template_name]

            ilm_policy = (
                template.get("settings", {})
                .get("index", {})
                .get("lifecycle", {})
                .get("name")
            )

            if ilm_policy == old_policy_name:
                if "settings" not in template:
                    template["settings"] = {}
                if "index" not in template["settings"]:
                    template["settings"]["index"] = {}
                if "lifecycle" not in template["settings"]["index"]:
                    template["settings"]["index"]["lifecycle"] = {}

                template["settings"]["index"]["lifecycle"]["name"] = new_policy_name

                client.indices.put_template(name=template_name, body=template)
                loggit.info(
                    "Updated legacy template %s to use policy %s",
                    template_name,
                    new_policy_name,
                )
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

    :raises ActionError: If policy creation fails
    """
    loggit = logging.getLogger("deepfreeze.utilities")

    new_policy_name = f"{base_policy_name}-{suffix}"

    loggit.debug(
        "Creating versioned policy %s referencing repository %s",
        new_policy_name,
        new_repo_name,
    )

    new_policy_body = copy.deepcopy(base_policy_body)

    if "phases" in new_policy_body:
        for phase_name, phase_config in new_policy_body["phases"].items():
            if (
                "actions" in phase_config
                and "searchable_snapshot" in phase_config["actions"]
            ):
                phase_config["actions"]["searchable_snapshot"][
                    "snapshot_repository"
                ] = new_repo_name
                loggit.debug(
                    "Updated %s phase to reference repository %s",
                    phase_name,
                    new_repo_name,
                )

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
    loggit = logging.getLogger("deepfreeze.utilities")
    loggit.debug("Finding policies that reference repository %s", repo_name)

    policies = client.ilm.get_lifecycle()
    matching_policies = {}

    for policy_name, policy_data in policies.items():
        policy_body = policy_data.get("policy", {})
        phases = policy_body.get("phases", {})

        for phase_name, phase_config in phases.items():
            actions = phase_config.get("actions", {})
            if "searchable_snapshot" in actions:
                snapshot_repo = actions["searchable_snapshot"].get(
                    "snapshot_repository"
                )
                if snapshot_repo == repo_name:
                    matching_policies[policy_name] = policy_data
                    loggit.debug(
                        "Found policy %s referencing %s", policy_name, repo_name
                    )
                    break

    loggit.info(
        "Found %d policies referencing repository %s", len(matching_policies), repo_name
    )
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
    loggit = logging.getLogger("deepfreeze.utilities")
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
    loggit = logging.getLogger("deepfreeze.utilities")
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
) -> list:
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
    loggit = logging.getLogger("deepfreeze.utilities")
    loggit.debug(
        "Finding snapshots containing index %s in repo %s", index_name, repo_name
    )

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
            "Found %d snapshots containing index %s",
            len(matching_snapshots),
            index_name,
        )
        return matching_snapshots

    except Exception as e:
        loggit.error("Failed to find snapshots for index %s: %s", index_name, e)
        return []


def mount_snapshot_index(
    client: Elasticsearch,
    repo_name: str,
    snapshot_name: str,
    index_name: str,
    ilm_policy: str = None,
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
    :param ilm_policy: Optional ILM policy to assign to the index
    :type ilm_policy: str

    :returns: True if successful, False otherwise
    :rtype: bool
    """
    loggit = logging.getLogger("deepfreeze.utilities")
    loggit.info(
        "Mounting index %s from snapshot %s/%s", index_name, repo_name, snapshot_name
    )

    # Check if index is already mounted
    already_mounted = client.indices.exists(index=index_name)
    if already_mounted:
        loggit.info("Index %s is already mounted", index_name)
        if ilm_policy:
            try:
                loggit.debug(
                    "Removing old ILM policy from %s before assigning new policy",
                    index_name,
                )
                try:
                    client.ilm.remove_policy(index=index_name)
                except Exception as remove_err:
                    loggit.debug(
                        "Could not remove ILM policy from %s (may not have one): %s",
                        index_name,
                        remove_err,
                    )

                client.indices.put_settings(
                    index=index_name, body={"index.lifecycle.name": ilm_policy}
                )
                loggit.info(
                    "Assigned ILM policy %s to already-mounted index %s",
                    ilm_policy,
                    index_name,
                )
            except Exception as e:
                loggit.warning(
                    "Failed to assign ILM policy to already-mounted index %s: %s",
                    index_name,
                    e,
                )
        return True

    try:
        client.searchable_snapshots.mount(
            repository=repo_name,
            snapshot=snapshot_name,
            body={"index": index_name},
        )
        loggit.info("Successfully mounted index %s", index_name)

        if ilm_policy:
            try:
                loggit.debug(
                    "Removing old ILM policy from %s before assigning new policy",
                    index_name,
                )
                try:
                    client.ilm.remove_policy(index=index_name)
                except Exception as remove_err:
                    loggit.debug(
                        "Could not remove ILM policy from %s (may not have one): %s",
                        index_name,
                        remove_err,
                    )

                client.indices.put_settings(
                    index=index_name, body={"index.lifecycle.name": ilm_policy}
                )
                loggit.info(
                    "Assigned ILM policy %s to index %s", ilm_policy, index_name
                )
            except Exception as e:
                loggit.warning(
                    "Failed to assign ILM policy to index %s: %s", index_name, e
                )

        return True

    except Exception as e:
        loggit.error("Failed to mount index %s: %s", index_name, e)
        return False


def wait_for_index_ready(
    client: Elasticsearch, index_name: str, max_wait_seconds: int = 30
) -> bool:
    """
    Wait for an index to become ready for search queries after mounting.

    :param client: A client connection object
    :type client: Elasticsearch
    :param index_name: The index name to wait for
    :type index_name: str
    :param max_wait_seconds: Maximum time to wait in seconds
    :type max_wait_seconds: int

    :returns: True if index is ready, False if timeout
    :rtype: bool
    """
    loggit = logging.getLogger("deepfreeze.utilities")
    loggit.debug("Waiting for index %s to be ready", index_name)

    start_time = time.time()
    while time.time() - start_time < max_wait_seconds:
        try:
            health = client.cluster.health(
                index=index_name, wait_for_active_shards=1, timeout="5s"
            )
            if health.get("active_shards", 0) > 0:
                loggit.debug(
                    "Index %s is ready (active shards: %d)",
                    index_name,
                    health["active_shards"],
                )
                return True
        except Exception as e:
            loggit.debug("Index %s not ready yet: %s", index_name, e)

        time.sleep(2)

    loggit.warning(
        "Index %s did not become ready within %d seconds", index_name, max_wait_seconds
    )
    return False


def get_index_datastream_name(client: Elasticsearch, index_name: str) -> str:
    """
    Get the data stream name for an index by checking its settings.

    :param client: A client connection object
    :type client: Elasticsearch
    :param index_name: The index name
    :type index_name: str

    :returns: The data stream name if the index was part of one, None otherwise
    :rtype: str
    """
    loggit = logging.getLogger("deepfreeze.utilities")

    try:
        settings = client.indices.get_settings(index=index_name)

        if index_name in settings:
            index_settings = settings[index_name].get("settings", {})
            index_metadata = index_settings.get("index", {})

            datastream_name = index_metadata.get("provided_name")

            if datastream_name and datastream_name.startswith(".ds-"):
                remaining = datastream_name[4:]
                parts = remaining.rsplit("-", 2)
                if len(parts) >= 3:
                    ds_name = parts[0]
                    loggit.debug(
                        "Index %s belongs to data stream %s (from metadata)",
                        index_name,
                        ds_name,
                    )
                    return ds_name

        if index_name.startswith(".ds-"):
            loggit.debug("Checking index name %s for data stream pattern", index_name)
            remaining = index_name[4:]
            parts = remaining.rsplit("-", 2)
            if len(parts) >= 3:
                ds_name = parts[0]
                loggit.debug(
                    "Index %s belongs to data stream %s (from index name)",
                    index_name,
                    ds_name,
                )
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
    loggit = logging.getLogger("deepfreeze.utilities")
    loggit.info("Adding index %s to data stream %s", index_name, datastream_name)

    try:
        try:
            client.indices.get_data_stream(name=datastream_name)
        except NotFoundError:
            loggit.warning("Data stream %s does not exist", datastream_name)
            return False

        client.indices.modify_data_stream(
            body={
                "actions": [
                    {
                        "add_backing_index": {
                            "data_stream": datastream_name,
                            "index": index_name,
                        }
                    }
                ]
            }
        )
        loggit.info(
            "Successfully added index %s to data stream %s", index_name, datastream_name
        )
        return True

    except Exception as e:
        loggit.error(
            "Failed to add index %s to data stream %s: %s",
            index_name,
            datastream_name,
            e,
        )
        return False


def find_and_mount_indices_in_date_range(
    client: Elasticsearch,
    repos: list,
    start_date: datetime,
    end_date: datetime,
    ilm_policy: str = None,
) -> dict:
    """
    Find and mount all indices within a date range from the given repositories.

    :param client: A client connection object
    :type client: Elasticsearch
    :param repos: List of repositories to search
    :type repos: list[Repository]
    :param start_date: Start of date range
    :type start_date: datetime
    :param end_date: End of date range
    :type end_date: datetime
    :param ilm_policy: Deprecated - per-repo policies are now created automatically
    :type ilm_policy: str

    :returns: Dictionary with mounted, skipped, failed counts, and created policies
    :rtype: dict
    """
    loggit = logging.getLogger("deepfreeze.utilities")
    loggit.info(
        "Finding and mounting indices between %s and %s",
        start_date.isoformat(),
        end_date.isoformat(),
    )

    mounted_indices = []
    skipped_indices = []
    failed_indices = []
    datastream_adds = {"successful": [], "failed": []}
    created_policies = []

    for repo in repos:
        try:
            thawed_policy = create_thawed_ilm_policy(client, repo.name)
            created_policies.append(thawed_policy)
            loggit.info(
                "Using thawed ILM policy %s for repository %s", thawed_policy, repo.name
            )
        except Exception as e:
            loggit.error("Failed to create thawed ILM policy for %s: %s", repo.name, e)
            thawed_policy = None

        try:
            all_indices = get_all_indices_in_repo(client, repo.name)
            loggit.debug(
                "Found %d indices in repository %s", len(all_indices), repo.name
            )

            for index_name in all_indices:
                snapshots = find_snapshots_for_index(client, repo.name, index_name)
                if not snapshots:
                    loggit.warning("No snapshots found for index %s", index_name)
                    continue

                snapshot_name = snapshots[-1]

                already_mounted = client.indices.exists(index=index_name)
                if already_mounted:
                    loggit.debug(
                        "Index %s is already mounted, skipping mount operation",
                        index_name,
                    )
                    if thawed_policy and not mount_snapshot_index(
                        client, repo.name, snapshot_name, index_name, thawed_policy
                    ):
                        loggit.warning(
                            "Failed to assign ILM policy to already-mounted index %s",
                            index_name,
                        )
                else:
                    if not mount_snapshot_index(
                        client, repo.name, snapshot_name, index_name, thawed_policy
                    ):
                        failed_indices.append(index_name)
                        continue

                    if not wait_for_index_ready(client, index_name):
                        loggit.warning(
                            "Index %s did not become ready in time, may have query issues",
                            index_name,
                        )

                keep_mounted = True

                try:
                    index_start, index_end = get_timestamp_range(client, [index_name])

                    if index_start and index_end:
                        index_start_dt = decode_date(index_start)
                        index_end_dt = decode_date(index_end)

                        if index_start_dt <= end_date and index_end_dt >= start_date:
                            loggit.info(
                                "Index %s overlaps date range (%s to %s), keeping mounted",
                                index_name,
                                index_start_dt.isoformat(),
                                index_end_dt.isoformat(),
                            )
                        else:
                            loggit.info(
                                "Index %s does not overlap date range (%s to %s), unmounting",
                                index_name,
                                index_start_dt.isoformat(),
                                index_end_dt.isoformat(),
                            )
                            keep_mounted = False
                            try:
                                client.indices.delete(index=index_name)
                                loggit.debug("Unmounted index %s", index_name)
                            except Exception as e:
                                loggit.warning(
                                    "Failed to unmount index %s: %s", index_name, e
                                )
                            skipped_indices.append(index_name)
                    else:
                        loggit.warning(
                            "Could not determine date range for %s, keeping mounted",
                            index_name,
                        )

                except Exception as e:
                    loggit.warning(
                        "Error checking date range for index %s: %s, keeping mounted",
                        index_name,
                        e,
                    )

                if keep_mounted:
                    mounted_indices.append(index_name)

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
        "created_policies": created_policies,
    }

    loggit.info(
        "Mounted %d indices, skipped %d outside date range, failed %d. Added %d to data streams.",
        result["mounted"],
        result["skipped"],
        result["failed"],
        result["datastream_successful"],
    )

    return result
