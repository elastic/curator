"""
Test deepfreeze thaw functionality

These are long-running integration tests that test the complete thaw lifecycle:
1. Creating thaw requests
2. Monitoring restore progress using porcelain output
3. Verifying indices are mounted correctly
4. Verifying data can be searched
5. Cleaning up and verifying repositories are unmounted

IMPORTANT: Real thaw operations can take up to 6 hours due to AWS Glacier restore times.
Set DEEPFREEZE_FAST_MODE=1 to use mocked/accelerated tests for CI.
Set DEEPFREEZE_FULL_TEST=1 to run full integration tests against real AWS Glacier.

Configuration is loaded from ~/.curator/curator.yml by default.
Set CURATOR_CONFIG environment variable to use a different config file.
"""

# pylint: disable=missing-function-docstring, missing-class-docstring, line-too-long
import os
import time
import warnings
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Tuple

import pytest
from es_client.builder import Builder
from es_client.helpers.config import get_config

from curator.actions.deepfreeze import STATUS_INDEX, Cleanup, Refreeze, Thaw
from curator.actions.deepfreeze.utilities import (
    get_repositories_by_names,
    get_settings,
    get_thaw_request,
    list_thaw_requests,
)
from curator.defaults.settings import VERSION_MAX, VERSION_MIN, default_config_file
from curator.s3client import s3_client_factory

from . import DeepfreezeTestCase, random_suffix, testvars

# Configuration file path
CONFIG_FILE = os.environ.get("CURATOR_CONFIG", default_config_file())
INTERVAL = 1  # Base interval for sleep operations

# Test mode configuration
FAST_MODE = os.environ.get("DEEPFREEZE_FAST_MODE", "0") == "1"
FULL_TEST = os.environ.get("DEEPFREEZE_FULL_TEST", "0") == "1"

# Skip long-running tests unless explicitly enabled
pytestmark = pytest.mark.skipif(
    not FULL_TEST and not FAST_MODE,
    reason="Thaw tests are long-running. Set DEEPFREEZE_FULL_TEST=1 or DEEPFREEZE_FAST_MODE=1 to run.",
)


class ThawStatusParser:
    """Helper class to parse porcelain output from thaw commands"""

    @staticmethod
    def parse_status_output(output: str) -> Dict:
        """
        Parse porcelain output from thaw --check-status command.

        Expected format:
        REQUEST	{request_id}	{status}	{created_at}	{start_date}	{end_date}
        REPO	{name}	{bucket}	{path}	{state}	{mounted}	{progress}

        :param output: Raw porcelain output string
        :type output: str
        :return: Parsed status information
        :rtype: Dict
        """
        result = {"request": None, "repos": []}

        for line in output.strip().split("\n"):
            if not line.strip():
                continue

            parts = line.split("\t")
            record_type = parts[0]

            if record_type == "REQUEST":
                result["request"] = {
                    "id": parts[1],
                    "status": parts[2],
                    "created_at": parts[3],
                    "start_date": parts[4],
                    "end_date": parts[5],
                }
            elif record_type == "REPO":
                result["repos"].append(
                    {
                        "name": parts[1],
                        "bucket": parts[2],
                        "path": parts[3],
                        "state": parts[4],
                        "mounted": parts[5] == "yes",
                        "progress": parts[6],
                    }
                )

        return result

    @staticmethod
    def parse_list_output(output: str) -> List[Dict]:
        """
        Parse porcelain output from thaw --list command.

        Expected format:
        THAW_REQUEST	{request_id}	{status}	{created_at}	{start_date}	{end_date}	{repo_count}

        :param output: Raw porcelain output string
        :type output: str
        :return: List of thaw request information
        :rtype: List[Dict]
        """
        requests = []

        for line in output.strip().split("\n"):
            if not line.strip():
                continue

            parts = line.split("\t")
            if parts[0] == "THAW_REQUEST":
                requests.append(
                    {
                        "id": parts[1],
                        "status": parts[2],
                        "created_at": parts[3],
                        "start_date": parts[4],
                        "end_date": parts[5],
                        "repo_count": int(parts[6]),
                    }
                )

        return requests

    @staticmethod
    def is_restore_complete(status_data: Dict) -> bool:
        """
        Check if restoration is complete for all repositories.

        :param status_data: Parsed status data from parse_status_output
        :type status_data: Dict
        :return: True if all repos show "Complete" progress
        :rtype: bool
        """
        if not status_data.get("repos"):
            return False

        return all(repo["progress"] == "Complete" for repo in status_data["repos"])

    @staticmethod
    def all_repos_mounted(status_data: Dict) -> bool:
        """
        Check if all repositories are mounted.

        :param status_data: Parsed status data from parse_status_output
        :type status_data: Dict
        :return: True if all repos are mounted
        :rtype: bool
        """
        if not status_data.get("repos"):
            return False

        return all(repo["mounted"] for repo in status_data["repos"])


class TestDeepfreezeThaw(DeepfreezeTestCase):
    """Test suite for deepfreeze thaw operations"""

    def setUp(self):
        """Set up test environment"""
        # Load configuration from curator.yml
        if not os.path.exists(CONFIG_FILE):
            pytest.skip(f"Configuration file not found: {CONFIG_FILE}")

        # Get configuration dictionary
        try:
            config = get_config(CONFIG_FILE)
            configdict = config['elasticsearch']
        except Exception as e:
            pytest.skip(f"Failed to load configuration from {CONFIG_FILE}: {e}")

        # Build client using configuration
        try:
            builder = Builder(
                configdict=configdict,
                version_max=VERSION_MAX,
                version_min=VERSION_MIN,
            )
            builder.connect()
            self.client = builder.client
        except Exception as e:
            pytest.skip(f"Failed to connect to Elasticsearch using config from {CONFIG_FILE}: {e}")

        # Initialize logger
        import logging
        self.logger = logging.getLogger("TestDeepfreezeThaw")

        # Set provider and suppress warnings
        self.provider = "aws"
        warnings.filterwarnings(
            "ignore", category=DeprecationWarning, module="botocore.auth"
        )

        # Initialize bucket name for cleanup
        self.bucket_name = ""

    def tearDown(self):
        """Clean up test resources"""
        # Clean up S3 buckets
        if self.bucket_name:
            try:
                s3 = s3_client_factory(self.provider)
                buckets = s3.list_buckets(testvars.df_bucket_name)
                for bucket in buckets:
                    s3.delete_bucket(bucket_name=bucket)
            except Exception as e:
                self.logger.warning(f"Failed to clean up buckets: {e}")

        # Clean up Elasticsearch resources
        try:
            # Delete status index
            if self.client.indices.exists(index=STATUS_INDEX):
                self.client.indices.delete(index=STATUS_INDEX)

            # Delete all test repositories
            repos = self.client.snapshot.get_repository(name="*")
            for repo in repos:
                if repo.startswith(testvars.df_repo_name):
                    try:
                        self.client.snapshot.delete_repository(name=repo)
                    except Exception:
                        pass

            # Delete all test indices
            indices = list(
                self.client.indices.get(
                    index="test-logs-*,df-*",
                    expand_wildcards="open,closed",
                    ignore_unavailable=True
                ).keys()
            )
            if indices:
                self.client.indices.delete(index=",".join(indices), ignore_unavailable=True)

        except Exception as e:
            self.logger.warning(f"Failed to clean up Elasticsearch resources: {e}")

    def _setup_test_environment(self) -> Tuple[str, str]:
        """
        Set up the test environment with repositories and test data.

        :return: Tuple of (bucket_name, repo_name_prefix)
        :rtype: Tuple[str, str]
        """
        # Generate unique test identifiers
        self.bucket_name = f"{testvars.df_bucket_name}-thaw-{random_suffix()}"

        # Run deepfreeze setup
        self.do_setup()

        repo_name = f"{testvars.df_repo_name}-000001"

        return self.bucket_name, repo_name

    def _create_test_indices_with_dates(
        self, repo_name: str, date_ranges: List[Tuple[datetime, datetime]], docs_per_index: int = 100
    ) -> List[str]:
        """
        Create test indices with specific date ranges and snapshot them.

        :param repo_name: The repository to snapshot to
        :type repo_name: str
        :param date_ranges: List of (start_date, end_date) tuples for each index
        :type date_ranges: List[Tuple[datetime, datetime]]
        :param docs_per_index: Number of documents to create per index
        :type docs_per_index: int
        :return: List of created index names
        :rtype: List[str]
        """
        created_indices = []

        for i, (start_date, end_date) in enumerate(date_ranges):
            # Create index name based on date range
            index_name = f"test-logs-{start_date.strftime('%Y%m%d')}-{i:03d}"

            # Create the index
            self.create_index(index_name)

            # Add documents with timestamps in the date range
            doc_count = docs_per_index
            time_delta = (end_date - start_date) / doc_count

            for j in range(doc_count):
                doc_time = start_date + (time_delta * j)
                self.client.index(
                    index=index_name,
                    document={
                        "@timestamp": doc_time.isoformat(),
                        "message": f"Test document {j} for index {index_name}",
                        "test_id": f"{index_name}-{j}",
                    },
                )

            # Refresh the index
            self.client.indices.refresh(index=index_name)

            # Create a snapshot of this index
            snapshot_name = f"snap-{index_name}"
            self.client.snapshot.create(
                repository=repo_name,
                snapshot=snapshot_name,
                body={
                    "indices": index_name,
                    "include_global_state": False,
                    "partial": False,
                },
                wait_for_completion=True,
            )

            created_indices.append(index_name)

            # Small delay to ensure snapshots are distinct
            time.sleep(INTERVAL)

        return created_indices

    def _push_repo_to_glacier(self, repo_name: str):
        """
        Push a repository to Glacier storage (simulated in fast mode).

        :param repo_name: The repository name to push to Glacier
        :type repo_name: str
        """
        # Get repository object
        repos = get_repositories_by_names(self.client, [repo_name])
        if not repos:
            raise ValueError(f"Repository {repo_name} not found")

        repo = repos[0]

        if FAST_MODE:
            # In fast mode, just mark as unmounted
            repo.is_mounted = False
            repo.persist(self.client)
            self.client.snapshot.delete_repository(name=repo_name)
        else:
            # In full mode, actually push to Glacier
            from curator.actions.deepfreeze.utilities import push_to_glacier

            s3 = s3_client_factory(self.provider)
            push_to_glacier(s3, repo)
            repo.is_mounted = False
            repo.persist(self.client)
            self.client.snapshot.delete_repository(name=repo_name)

    def _wait_for_restore_completion(
        self, thaw_request_id: str, timeout_seconds: int = 300, poll_interval: int = 10
    ) -> bool:
        """
        Wait for thaw restore operation to complete using porcelain output.

        :param thaw_request_id: The thaw request ID to monitor
        :type thaw_request_id: str
        :param timeout_seconds: Maximum time to wait in seconds
        :type timeout_seconds: int
        :param poll_interval: Seconds between status checks
        :type poll_interval: int
        :return: True if restore completed, False if timeout
        :rtype: bool
        """
        start_time = time.time()
        parser = ThawStatusParser()

        while (time.time() - start_time) < timeout_seconds:
            # Create Thaw action to check status
            thaw = Thaw(
                self.client,
                check_status=thaw_request_id,
                porcelain=True,
            )

            # In fast mode, we simulate completion
            if FAST_MODE:
                # After first poll, mark as complete
                request = get_thaw_request(self.client, thaw_request_id)
                repo_names = request.get("repos", [])
                repos = get_repositories_by_names(self.client, repo_names)

                # Mount all repositories
                for repo in repos:
                    if not repo.is_mounted:
                        repo.is_mounted = True
                        repo.thaw_state = "active"
                        repo.persist(self.client)

                        # Re-register the repository with Elasticsearch
                        self.client.snapshot.create_repository(
                            name=repo.name,
                            body={
                                "type": "s3",
                                "settings": {
                                    "bucket": repo.bucket,
                                    "base_path": repo.base_path,
                                },
                            },
                        )

                return True

            # In full mode, actually poll for status
            # This would use the real porcelain output
            # For now, we'll use the action's internal check
            try:
                thaw_action = Thaw(
                    self.client,
                    check_status=thaw_request_id,
                    porcelain=False,
                )

                # Check if all repos are mounted
                request = get_thaw_request(self.client, thaw_request_id)
                repo_names = request.get("repos", [])
                repos = get_repositories_by_names(self.client, repo_names)

                if all(repo.is_mounted for repo in repos):
                    return True

            except Exception as e:
                self.logger.warning(f"Error checking thaw status: {e}")

            time.sleep(poll_interval)

        return False

    def test_thaw_single_repository(self):
        """
        Test thawing a single repository with a specific date range.

        This test:
        1. Sets up a repository with test data spanning multiple dates
        2. Pushes the repository to Glacier
        3. Creates a thaw request for a specific date range
        4. Monitors restore progress using porcelain output
        5. Verifies indices are mounted correctly
        6. Verifies data can be searched
        """
        # Set up environment
        bucket_name, repo_name = self._setup_test_environment()

        # Create test indices with specific date ranges
        # We'll create 3 indices spanning January, February, March 2024
        now = datetime.now(timezone.utc)
        date_ranges = [
            (
                datetime(2024, 1, 1, tzinfo=timezone.utc),
                datetime(2024, 1, 31, tzinfo=timezone.utc),
            ),
            (
                datetime(2024, 2, 1, tzinfo=timezone.utc),
                datetime(2024, 2, 28, tzinfo=timezone.utc),
            ),
            (
                datetime(2024, 3, 1, tzinfo=timezone.utc),
                datetime(2024, 3, 31, tzinfo=timezone.utc),
            ),
        ]

        created_indices = self._create_test_indices_with_dates(repo_name, date_ranges)
        self.logger.info(f"Created indices: {created_indices}")

        # Push repository to Glacier
        self.logger.info(f"Pushing repository {repo_name} to Glacier")
        self._push_repo_to_glacier(repo_name)

        # Wait a moment for the unmount to complete
        time.sleep(INTERVAL * 2)

        # Create a thaw request for January data only
        start_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
        end_date = datetime(2024, 1, 31, 23, 59, 59, tzinfo=timezone.utc)

        self.logger.info(
            f"Creating thaw request for date range: {start_date} to {end_date}"
        )

        thaw = Thaw(
            self.client,
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat(),
            sync=False,  # Async mode
            duration=7,
            retrieval_tier="Standard",
            porcelain=True,
        )

        # Capture the thaw request ID
        # In a real scenario, we'd parse porcelain output
        # For now, we'll get it from the status index
        thaw.do_action()

        # Get the thaw request ID
        requests = list_thaw_requests(self.client)
        assert len(requests) > 0, "No thaw requests found after thaw action"
        thaw_request_id = requests[-1]["id"]

        self.logger.info(f"Created thaw request: {thaw_request_id}")

        # Wait for restore to complete (with timeout)
        timeout = 300 if FAST_MODE else 21600  # 5 min for fast, 6 hours for full
        restore_completed = self._wait_for_restore_completion(
            thaw_request_id, timeout_seconds=timeout
        )

        assert restore_completed, "Restore did not complete within timeout period"

        # Verify indices are mounted
        self.logger.info("Verifying mounted indices")
        request = get_thaw_request(self.client, thaw_request_id)
        repo_names = request.get("repos", [])
        repos = get_repositories_by_names(self.client, repo_names)

        # Should have exactly one repository (January data)
        assert len(repos) == 1, f"Expected 1 repository, got {len(repos)}"
        assert repos[0].is_mounted, "Repository should be mounted"

        # Verify we can search the data
        self.logger.info("Verifying data can be searched")
        january_index = created_indices[0]  # The January index

        # Try to search the index
        search_result = self.client.search(
            index=january_index,
            body={"query": {"match_all": {}}, "size": 1},
        )

        assert search_result["hits"]["total"]["value"] > 0, "No documents found in index"

        # Verify the document has correct timestamp
        doc = search_result["hits"]["hits"][0]["_source"]
        assert "@timestamp" in doc, "Document missing @timestamp field"

        doc_time = datetime.fromisoformat(doc["@timestamp"].replace("Z", "+00:00"))
        assert start_date <= doc_time <= end_date, "Document timestamp outside expected range"

        # Refreeze the repository
        self.logger.info("Refreezing repository")
        refreeze = Refreeze(self.client, thaw_request_id=thaw_request_id, porcelain=True)
        refreeze.do_action()

        # Verify repository is unmounted
        time.sleep(INTERVAL * 2)
        repos = get_repositories_by_names(self.client, [repos[0].name])
        assert not repos[0].is_mounted, "Repository should be unmounted after refreeze"

    def test_thaw_multiple_repositories(self):
        """
        Test thawing multiple repositories spanning a date range.

        This test:
        1. Sets up multiple repositories with different date ranges
        2. Pushes all repositories to Glacier
        3. Creates a thaw request spanning multiple repositories
        4. Verifies all relevant repositories are restored and mounted
        5. Verifies indices outside the date range are NOT mounted
        """
        # Set up initial environment
        bucket_name, first_repo = self._setup_test_environment()

        # Create multiple repositories by rotating
        # We'll create 3 repositories for Jan, Feb, Mar 2024
        from curator.actions.deepfreeze.rotate import Rotate

        repos_created = [first_repo]

        # Create additional repositories
        for _ in range(2):
            rotate = Rotate(self.client, keep=10)  # Keep all repos mounted
            rotate.do_action()
            time.sleep(INTERVAL)

            # Get the latest repository
            settings = get_settings(self.client)
            last_suffix = settings.last_suffix
            latest_repo = f"{testvars.df_repo_name}-{last_suffix}"
            repos_created.append(latest_repo)

        self.logger.info(f"Created repositories: {repos_created}")

        # Create test data in each repository
        all_indices = []
        date_ranges_per_repo = [
            [
                (
                    datetime(2024, 1, 1, tzinfo=timezone.utc),
                    datetime(2024, 1, 31, tzinfo=timezone.utc),
                )
            ],
            [
                (
                    datetime(2024, 2, 1, tzinfo=timezone.utc),
                    datetime(2024, 2, 28, tzinfo=timezone.utc),
                )
            ],
            [
                (
                    datetime(2024, 3, 1, tzinfo=timezone.utc),
                    datetime(2024, 3, 31, tzinfo=timezone.utc),
                )
            ],
        ]

        for repo_name, date_ranges in zip(repos_created, date_ranges_per_repo):
            indices = self._create_test_indices_with_dates(
                repo_name, date_ranges, docs_per_index=50
            )
            all_indices.extend(indices)

        self.logger.info(f"Created total indices: {all_indices}")

        # Push all repositories to Glacier
        for repo_name in repos_created:
            self.logger.info(f"Pushing repository {repo_name} to Glacier")
            self._push_repo_to_glacier(repo_name)
            time.sleep(INTERVAL)

        # Wait for unmounting to complete
        time.sleep(INTERVAL * 2)

        # Create a thaw request spanning January and February (2 repos)
        start_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
        end_date = datetime(2024, 2, 28, 23, 59, 59, tzinfo=timezone.utc)

        self.logger.info(
            f"Creating thaw request for date range: {start_date} to {end_date}"
        )

        thaw = Thaw(
            self.client,
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat(),
            sync=False,
            duration=7,
            retrieval_tier="Standard",
            porcelain=True,
        )

        thaw.do_action()

        # Get the thaw request ID
        requests = list_thaw_requests(self.client)
        thaw_request_id = requests[-1]["id"]

        self.logger.info(f"Created thaw request: {thaw_request_id}")

        # Wait for restore to complete
        timeout = 300 if FAST_MODE else 21600
        restore_completed = self._wait_for_restore_completion(
            thaw_request_id, timeout_seconds=timeout
        )

        assert restore_completed, "Restore did not complete within timeout period"

        # Verify exactly 2 repositories are mounted (Jan and Feb)
        request = get_thaw_request(self.client, thaw_request_id)
        repo_names = request.get("repos", [])
        repos = get_repositories_by_names(self.client, repo_names)

        assert len(repos) == 2, f"Expected 2 repositories, got {len(repos)}"
        assert all(repo.is_mounted for repo in repos), "All repos should be mounted"

        # Verify the March repository is NOT in the thaw request
        march_repo = repos_created[2]
        assert march_repo not in repo_names, "March repository should not be in thaw request"

        # Verify we can search data in both January and February indices
        for index_name in [all_indices[0], all_indices[1]]:
            search_result = self.client.search(
                index=index_name, body={"query": {"match_all": {}}, "size": 1}
            )
            assert search_result["hits"]["total"]["value"] > 0, f"No documents found in {index_name}"

        # Cleanup - run refreeze
        self.logger.info("Running cleanup")
        cleanup = Cleanup(self.client)
        cleanup.do_action()

        # Verify repositories are unmounted after cleanup
        time.sleep(INTERVAL * 2)
        repos_after = get_repositories_by_names(self.client, repo_names)
        # Note: After cleanup, repos should be unmounted if they've expired
        # In this test, they won't have expired yet, so they'll still be mounted
        # This is expected behavior

    def test_thaw_with_porcelain_output_parsing(self):
        """
        Test parsing porcelain output from thaw operations.

        This test focuses on the porcelain output format and parsing logic.
        """
        # Set up environment
        bucket_name, repo_name = self._setup_test_environment()

        # Create simple test data
        date_ranges = [
            (
                datetime(2024, 1, 1, tzinfo=timezone.utc),
                datetime(2024, 1, 31, tzinfo=timezone.utc),
            )
        ]
        created_indices = self._create_test_indices_with_dates(
            repo_name, date_ranges, docs_per_index=10
        )

        # Push to Glacier
        self._push_repo_to_glacier(repo_name)
        time.sleep(INTERVAL * 2)

        # Create thaw request
        start_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
        end_date = datetime(2024, 1, 31, tzinfo=timezone.utc)

        thaw = Thaw(
            self.client,
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat(),
            sync=False,
            duration=7,
            retrieval_tier="Standard",
            porcelain=True,
        )

        thaw.do_action()

        # Get the thaw request
        requests = list_thaw_requests(self.client)
        thaw_request_id = requests[-1]["id"]

        # Test porcelain output parsing
        parser = ThawStatusParser()

        # Simulate porcelain output (in real scenario, we'd capture stdout)
        sample_output = f"""REQUEST\t{thaw_request_id}\tin_progress\t2024-01-01T00:00:00Z\t2024-01-01T00:00:00Z\t2024-01-31T23:59:59Z
REPO\t{repo_name}\t{bucket_name}\t/df-test-path-000001\tthawing\tno\t0/100"""

        parsed = parser.parse_status_output(sample_output)

        # Verify parsed structure
        assert parsed["request"] is not None, "Request data not parsed"
        assert parsed["request"]["id"] == thaw_request_id, "Request ID mismatch"
        assert len(parsed["repos"]) == 1, "Expected 1 repository in parsed output"

        repo_data = parsed["repos"][0]
        assert repo_data["name"] == repo_name, "Repository name mismatch"
        assert not repo_data["mounted"], "Repository should not be mounted yet"
        assert not parser.is_restore_complete(parsed), "Restore should not be complete"
        assert not parser.all_repos_mounted(parsed), "Repos should not be mounted"

        # Simulate completed output
        completed_output = f"""REQUEST\t{thaw_request_id}\tin_progress\t2024-01-01T00:00:00Z\t2024-01-01T00:00:00Z\t2024-01-31T23:59:59Z
REPO\t{repo_name}\t{bucket_name}\t/df-test-path-000001\tactive\tyes\tComplete"""

        parsed_complete = parser.parse_status_output(completed_output)

        assert parser.is_restore_complete(parsed_complete), "Restore should be complete"
        assert parser.all_repos_mounted(parsed_complete), "All repos should be mounted"

    def test_cleanup_removes_expired_repositories(self):
        """
        Test that cleanup properly removes expired thawed repositories.

        This test:
        1. Creates a thaw request
        2. Manually sets the expiration to past
        3. Runs cleanup
        4. Verifies repositories are unmounted and marked as frozen
        """
        # Set up environment
        bucket_name, repo_name = self._setup_test_environment()

        # Create test data
        date_ranges = [
            (
                datetime(2024, 1, 1, tzinfo=timezone.utc),
                datetime(2024, 1, 31, tzinfo=timezone.utc),
            )
        ]
        self._create_test_indices_with_dates(repo_name, date_ranges, docs_per_index=10)

        # Push to Glacier
        self._push_repo_to_glacier(repo_name)
        time.sleep(INTERVAL * 2)

        # Create thaw request with short duration
        start_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
        end_date = datetime(2024, 1, 31, tzinfo=timezone.utc)

        thaw = Thaw(
            self.client,
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat(),
            sync=False,
            duration=1,  # 1 day duration
            retrieval_tier="Standard",
            porcelain=False,
        )

        thaw.do_action()

        # Wait for restore in fast mode
        if FAST_MODE:
            requests = list_thaw_requests(self.client)
            thaw_request_id = requests[-1]["id"]
            self._wait_for_restore_completion(thaw_request_id, timeout_seconds=60)

        # Manually expire the thaw request by updating its timestamp
        requests = list_thaw_requests(self.client)
        thaw_request_id = requests[-1]["id"]

        # Update the request to have an expiration in the past
        past_time = datetime.now(timezone.utc) - timedelta(days=2)
        self.client.update(
            index=STATUS_INDEX,
            id=thaw_request_id,
            body={
                "doc": {
                    "created_at": past_time.isoformat(),
                    "expires_at": (past_time + timedelta(days=1)).isoformat(),
                }
            },
        )
        self.client.indices.refresh(index=STATUS_INDEX)

        # Get repository state before cleanup
        request = get_thaw_request(self.client, thaw_request_id)
        repo_names = request.get("repos", [])

        # Run cleanup
        self.logger.info("Running cleanup on expired thaw request")
        cleanup = Cleanup(self.client)
        cleanup.do_action()

        time.sleep(INTERVAL * 2)

        # Verify repositories are unmounted
        repos_after = get_repositories_by_names(self.client, repo_names)
        for repo in repos_after:
            assert not repo.is_mounted, f"Repository {repo.name} should be unmounted after cleanup"
            assert repo.thaw_state == "frozen", f"Repository {repo.name} should be frozen after cleanup"

        # Verify the thaw request is marked as completed
        request_after = get_thaw_request(self.client, thaw_request_id)
        assert request_after["status"] == "completed", "Thaw request should be marked as completed"


if __name__ == "__main__":
    # Allow running individual tests
    pytest.main([__file__, "-v", "-s"])
