"""Deepfreeze action class"""
import logging
import re
import sys
import boto3
from botocore.exceptions import ClientError
from datetime import datetime
from curator.exceptions import ActionError, RepositoryException


class Deepfreeze:
    """
    The Deepfreeze is responsible for managing the repository rotation given
    a config file of user-managed options and settings.
    """

    def __init__(
        self,
        client,
        repo_name_prefix='deepfreeze-',
        bucket_name_prefix='deepfreeze-',
        base_path='snapshots',
        canned_acl='private',
        storage_class='intelligent_tiering',
        keep='6',
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
        self.client = client
        self.repo_name_prefix = repo_name_prefix
        self.bucket_name_prefix = bucket_name_prefix
        self.base_path = base_path
        self.canned_acl = canned_acl
        self.storage_class = storage_class
        self.keep = keep
        self.year = year
        self.month = month

        suffix = self.get_next_suffix()
        self.new_repo_name = f"{self.repo_name_prefix}{suffix}"
        self.new_bucket_name = f"{self.bucket_name_prefix}{suffix}"

        self.repo_list = self.get_repos()
        self.repo_list.sort()
        try:
            self.latest_repo = self.repo_list[-1]
        except IndexError:
            raise RepositoryException(f"no repositories match {self.repo_name_prefix}")

        if self.new_repo_name in self.repo_list:
            raise RepositoryException(f"repository {self.new_repo_name} already exists")
        self.loggit = logging.getLogger('curator.actions.deepfreeze')

    def create_new_bucket(self, dry_run=False):
        """
        Creates a new S3 bucket using the aws config in the environment.

        :returns:   whether the bucket was created or not
        :rtype:     bool
        """
        self.loggit.info(f"Creating bucket {self.new_bucket_name}")
        if dry_run:
            return
        try:
            s3 = boto3.client("s3")
            s3.create_bucket(Bucket=self.new_bucket_name)
        except ClientError as e:
            self.loggit.error(e)
            raise ActionError(e)

    def create_new_repo(self, dry_run=False):
        """
        Creates a new repo using the previously-created bucket.
        """
        self.loggit.info(
            f"Creating repo {self.new_repo_name} using bucket {self.new_bucket_name}"
        )
        if dry_run:
            return
        self.client.snapshot.create_repository(
            name=self.new_repo_name,
            type="s3",
            settings={
                "bucket": self.new_bucket_name,
                "base_path": self.base_path,
                "canned_acl": self.canned_acl,
                "storage_class": self.storage_class,
            },
        )
        # TODO: Gather the reply and parse it to make sure this succeeded

    def update_ilm_policies(self, dry_run=False):
        """
        Loop through all existing IML policies looking for ones which reference
        the latest_repo and update them to use the new repo instead.
        """
        if self.latest_repo == self.new_repo_name:
            self.loggit.warning("Already on the latest repo")
            sys.exit(0)
        self.loggit.info(
            f"Switching from {self.latest_repo} to " f"{self.new_repo_name}"
        )
        policies = self.client.ilm.get_lifecycle()
        updated_policies = {}
        for policy in policies:
            # Go through these looking for any occurrences of self.latest_repo
            # and change those to use self.new_repo_name instead.
            p = policies[policy]["policy"]["phases"]
            updated = False
            for phase in p:
                if "searchable_snapshot" in p[phase]["actions"]:
                    if (
                        p[phase]["actions"]["searchable_snapshot"][
                            "snapshot_repository"
                        ]
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
            self.loggit.info(f"Updating {len(updated_policies.keys())} policies:")
        for pol in updated_policies:
            self.loggit.info(f"\t{pol}")
            if not dry_run:
                self.client.ilm.put_lifecycle(name=pol, policy=updated_policies[pol])

    def get_next_suffix(self):
        """
        Gets the next suffix
        """
        year = self.year if self.year else datetime.now().year
        month = self.month if self.month else datetime.now().month
        return f"{year:04}.{month:02}"

    def unmount_oldest_repos(self, dry_run=False):
        """
        Take the oldest repos from the list and remove them, only retaining
        the number chosen in the config under "keep".
        """
        s = slice(0, len(self.repo_list) - self.keep)
        self.loggit.info(f"Repo list: {self.repo_list}")
        for repo in self.repo_list[s]:
            self.loggit.info(f"Removing repo {repo}")
            if not dry_run:
                self.client.snapshot.delete_repository(name=repo)

    def get_repos(self) -> list[object]:
        """
        Get the complete list of repos and return just the ones whose names
        begin with our prefix.

        :returns:   The repos.
        :rtype:     list[object]
        """
        repos = self.client.snapshot.get_repository()
        pattern = re.compile(self.repo_name_prefix)
        return [repo for repo in repos if pattern.search(repo)]

    def do_dry_run(self):
        self.loggit.info('DRY-RUN MODE.  No changes will be made.')
        msg = (
            f'DRY-RUN: deepfreeze {self.latest_repo} will be rotated out'
            f' and {self.new_repo_name} will be added & made active.'
        )
        self.loggit.info(msg)
        self.create_new_bucket(dry_run=True)
        self.create_new_repo(dry_run=True)
        self.update_ilm_policies(dry_run=True)
        self.unmount_oldest_repos(dry_run=True)

    def do_action(self):
        """
        Perform high-level steps in sequence.
        """
        self.create_new_bucket()
        self.create_new_repo()
        self.update_ilm_policies()
        self.unmount_oldest_repos()
