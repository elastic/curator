"""Deepfreeze action class"""

import logging
import re
import sys
from datetime import datetime

import boto3
from botocore.exceptions import ClientError

from curator.exceptions import ActionError, RepositoryException

STATUS_INDEX = ".deepfreeze-status"

class Deepfreeze:
    pass


def save_settings(client, provider):
    """
    Save the settings for the deepfreeze operation to the status index.

    :param client: A client connection object
    :param provider: The provider to use (AWS only for now)
    """
    loggit = logging.getLogger("curator.actions.deepfreeze")
    loggit.info("Saving settings to status index")
    doc = {
        "type": "settings",
        "provider": provider,
        "timestamp": datetime.now().isoformat(),
    }
    client.create(index=STATUS_INDEX, document=doc)


def create_new_bucket(bucket_name, dry_run=False):
    """
    Creates a new S3 bucket using the aws config in the environment.

    :param bucket_name: The name of the bucket to create
    :param dry_run: If True, do not actually create the bucket
    :returns:   whether the bucket was created or not
    :rtype:     bool
    """
    loggit = logging.getLogger("curator.actions.deepfreeze")
    loggit.info("Creating bucket %s", bucket_name)
    if dry_run:
        return
    try:
        s3 = boto3.client("s3")
        s3.create_bucket(Bucket=bucket_name)
    except ClientError as e:
        loggit.error(e)
        raise ActionError(e)


def create_new_repo(client, repo_name, bucket_name, base_path, canned_acl, storage_class, dry_run=False):
    """
    Creates a new repo using the previously-created bucket.

    :param client: A client connection object
    :param repo_name: The name of the repository to create
    :param bucket_name: The name of the bucket to use for the repository
    :param base_path: Path within a bucket where snapshots are stored
    :param canned_acl: One of the AWS canned ACL values
    :param storage_class: AWS Storage class
    :param dry_run: If True, do not actually create the repository
    """
    loggit = logging.getLogger("curator.actions.deepfreeze")
    loggit.info("Creating repo %s using bucket %s", repo_name, bucket_name)
    if dry_run:
        return
    response = client.snapshot.create_repository(
        name=repo_name,
        type="s3",
        settings={
            "bucket": bucket_name,
            "base_path": base_path,
            "canned_acl": canned_acl,
            "storage_class": storage_class,
        },
    )
    # TODO: Gather the reply and parse it to make sure this succeeded
    #       It should simply bring back '{ "acknowledged": true }' but I
    #       don't know how client will wrap it.
    print(f"Response: {response}")
    loggit.info("Response: %s", response)


def get_next_suffix(year=None, month=None):
    """
    Gets the next suffix

    :param year: Optional year to override current year
    :param month: Optional month to override current month
    :returns: The next suffix in the format YYYY.MM
    :rtype: str
    """
    current_year = year or datetime.now().year
    current_month = month or datetime.now().month
    return f"{current_year:04}.{current_month:02}"


def get_repos(client, repo_name_prefix):
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
    return [repo for repo in repos if pattern.search(repo)]


def unmount_repo(client, repo, status_index):
    """
    Encapsulate the actions of deleting the repo and, at the same time,
    doing any record-keeping we need.

    :param client: A client connection object
    :param repo: The name of the repository to unmount
    :param status_index: The name of the status index
    """
    loggit = logging.getLogger("curator.actions.deepfreeze")
    repo_info = client.get_repository(name=repo)
    bucket = repo_info["settings"]["bucket"]
    doc = {
        "repo": repo,
        "state": "deepfreeze",
        "timestamp": datetime.now().isoformat(),
        "bucket": bucket,
        "start": None,  # TODO: Add the earliest @timestamp value here
        "end": None,  # TODO: Add the latest @timestamp value here
    }
    client.create(index=status_index, document=doc)
    # Now that our records are complete, go ahead and remove the repo.
    client.snapshot.delete_repository(name=repo)


class Setup:
    """
    Setup is responsible for creating the initial repository and bucket for
    deepfreeze operations.
    """

    def __init__(
        self,
        client,
        repo_name_prefix="deepfreeze-",
        bucket_name_prefix="deepfreeze-",
        base_path="snapshots",
        canned_acl="private",
        storage_class="intelligent_tiering",
        provider="aws",
    ):
        """
        :param client: A client connection object
        :param repo_name_prefix: A prefix for repository names, defaults to `deepfreeze-`
        :param bucket_name_prefix: A prefix for bucket names, defaults to `deepfreeze-`
        :param base_path: Path within a bucket where snapshots are stored, defaults to `snapshots`
        :param canned_acl: One of the AWS canned ACL values (see
            `<https://docs.aws.amazon.com/AmazonS3/latest/userguide/acl-overview.html#canned-acl>`),
            defaults to `private`
        :param storage_class: AWS Storage class (see `<https://aws.amazon.com/s3/storage-classes/>`),
            defaults to `intelligent_tiering`
        :param provider: The provider to use (AWS only for now), defaults to `aws`, and will be saved
            to the deepfreeze status index for later reference.
        """
        self.client = client
        self.repo_name_prefix = repo_name_prefix
        self.bucket_name_prefix = bucket_name_prefix
        self.base_path = base_path
        self.canned_acl = canned_acl
        self.storage_class = storage_class
        self.provider = provider
        self.loggit = logging.getLogger("curator.actions.deepfreeze")
        self.loggit.info("Initializing Deepfreeze Setup")

        suffix = get_next_suffix(self.year, self.month)
        self.new_repo_name = f"{self.repo_name_prefix}{suffix}"
        self.new_bucket_name = f"{self.bucket_name_prefix}{suffix}"

        self.loggit.debug("Deepfreeze Setup initialized")

    def do_dry_run(self):
        """
        Perform a dry-run of the setup process.
        """
        self.loggit.info("DRY-RUN MODE.  No changes will be made.")
        msg = (
            f"DRY-RUN: deepfreeze setup of {self.latest_repo} will be rotated out"
            f" and {self.new_repo_name} will be added & made active."
        )
        self.loggit.info(msg)
        create_new_bucket(self.new_bucket_name, dry_run=True)
        create_new_repo(
            self.client, 
            self.new_repo_name, 
            self.new_bucket_name, 
            self.base_path, 
            self.canned_acl, 
            self.storage_class, 
            dry_run=True
        )

    def do_action(self):
        """
        Perform create initial bucket and repository.
        """
        save_settings(self.client, self.provider))
        create_new_bucket(self.new_bucket_name)
        create_new_repo(
            self.client, 
            self.new_repo_name, 
            self.new_bucket_name, 
            self.base_path, 
            self.canned_acl, 
            self.storage_class
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
        client,
        repo_name_prefix="deepfreeze-",
        bucket_name_prefix="deepfreeze-",
        base_path="snapshots",
        canned_acl="private",
        storage_class="intelligent_tiering",
        keep="6",
        year=None,
        month=None,
    ):
        """
        :param client: A client connection object
        :param repo_name_prefix: A prefix for repository names, defaults to `deepfreeze-`
        :param bucket_name_prefix: A prefix for bucket names, defaults to `deepfreeze-`
        :param base_path: Path within a bucket where snapshots are stored, defaults to `snapshots`
        :param canned_acl: One of the AWS canned ACL values (see
            `<https://docs.aws.amazon.com/AmazonS3/latest/userguide/acl-overview.html#canned-acl>`),
            defaults to `private`
        :param storage_class: AWS Storage class (see `<https://aws.amazon.com/s3/storage-classes/>`),
            defaults to `intelligent_tiering`
        :param keep: How many repositories to retain, defaults to 6
        :param year: Optional year to override current year
        :param month: Optional month to override current month
        """
        print("Initializing Deepfreeze")

        self.client = client
        self.repo_name_prefix = repo_name_prefix
        self.bucket_name_prefix = bucket_name_prefix
        self.base_path = base_path
        self.canned_acl = canned_acl
        self.storage_class = storage_class
        self.keep = int(keep)
        self.year = year
        self.month = month

        suffix = get_next_suffix(self.year, self.month)

        self.new_repo_name = f"{self.repo_name_prefix}{suffix}"
        self.new_bucket_name = f"{self.bucket_name_prefix}{suffix}"

        self.repo_list = get_repos(self.client, self.repo_name_prefix)
        self.repo_list.sort()
        try:
            self.latest_repo = self.repo_list[-1]
        except IndexError:
            raise RepositoryException(f"no repositories match {self.repo_name_prefix}")

        if self.new_repo_name in self.repo_list:
            raise RepositoryException(f"repository {self.new_repo_name} already exists")
        self.loggit = logging.getLogger("curator.actions.deepfreeze")
        if not self.client.indices.exists(index=STATUS_INDEX):
            self.client.indices.create(index=STATUS_INDEX)
            self.loggit.warning("Created index %s", STATUS_INDEX)
        self.loggit.info("Deepfreeze initialized")

    def update_ilm_policies(self, dry_run=False):
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
            if not dry_run:
                self.client.ilm.put_lifecycle(policy_id=pol, body=body)

    def unmount_oldest_repos(self, dry_run=False):
        """
        Take the oldest repos from the list and remove them, only retaining
        the number chosen in the config under "keep".

        TODO: Do we need to maintain a system index for our use, which tracks
        the state of the repos? I can see a situation where we thaw some indices and
        then need to ensure they stay mounted when deepfreeze runs the following time.
        """
        # TODO: Look at snapshot.py for date-based calculations
        # Also, how to embed mutliple classes in a single action file
        # Alias action may be using multiple filter blocks. Look at that since we'll
        # need to do the same thing.:
        s = slice(0, len(self.repo_list) - self.keep)
        self.loggit.info("Repo list: %s", self.repo_list)
        for repo in self.repo_list[s]:
            self.loggit.info("Removing repo %s", repo)
            if not dry_run:
                unmount_repo(self.client, repo, STATUS_INDEX)

    def do_dry_run(self):
        """
        Perform a dry-run of the rotation process.
        """
        self.loggit.info("DRY-RUN MODE.  No changes will be made.")
        msg = (
            f"DRY-RUN: deepfreeze {self.latest_repo} will be rotated out"
            f" and {self.new_repo_name} will be added & made active."
        )
        self.loggit.info(msg)
        create_new_bucket(self.new_bucket_name, dry_run=True)
        create_new_repo(self.client, self.new_repo_name, self.new_bucket_name, self.base_path, self.canned_acl, self.storage_class, dry_run=True)
        self.update_ilm_policies(dry_run=True)
        self.unmount_oldest_repos(dry_run=True)

    def do_action(self):
        """
        Perform high-level repo rotation steps in sequence.
        """
        create_new_bucket(self.new_bucket_name)
        create_new_repo(self.client, self.new_repo_name, self.new_bucket_name, self.base_path, self.canned_acl, self.storage_class)
        self.update_ilm_policies()
        self.unmount_oldest_repos()

class Thaw:
    """
    Thaw a deepfreeze repository
    """
    pass

class Refreeze:
    """
    Refreeze a thawed deepfreeze repository
    """
    pass