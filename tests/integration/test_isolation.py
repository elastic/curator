"""
Test isolation utilities for parallel deepfreeze integration tests.

Provides repository locking to prevent multiple tests from operating on
the same repository when running tests in parallel with pytest-xdist.
"""

import logging
import time
from datetime import datetime, timezone
from typing import List, Optional

from elasticsearch8 import Elasticsearch

LOCK_INDEX = ".deepfreeze_test_locks"
LOCK_TIMEOUT_SECONDS = 7200  # 2 hours - longer than any single test

logger = logging.getLogger(__name__)


class RepositoryLock:
    """
    Distributed lock for test repositories using Elasticsearch.

    Uses optimistic locking with document versioning to ensure
    only one test can reserve a repository at a time.
    """

    def __init__(self, client: Elasticsearch, repo_name: str, test_id: str):
        """
        Initialize repository lock.

        :param client: Elasticsearch client
        :param repo_name: Repository name to lock
        :param test_id: Unique test identifier (pytest node ID)
        """
        self.client = client
        self.repo_name = repo_name
        self.test_id = test_id
        self.locked = False

    def acquire(self, timeout: int = 30) -> bool:
        """
        Attempt to acquire lock on repository.

        :param timeout: Maximum time to wait for lock (seconds)
        :return: True if lock acquired, False otherwise
        """
        start_time = time.time()

        while (time.time() - start_time) < timeout:
            try:
                # Try to create lock document
                lock_doc = {
                    "repo_name": self.repo_name,
                    "locked_by": self.test_id,
                    "locked_at": datetime.now(timezone.utc).isoformat(),
                    "expires_at": datetime.now(timezone.utc).timestamp() + LOCK_TIMEOUT_SECONDS,
                }

                self.client.index(
                    index=LOCK_INDEX,
                    id=self.repo_name,
                    body=lock_doc,
                    op_type="create",  # Fails if document exists
                )

                self.locked = True
                logger.info(f"Acquired lock on repository {self.repo_name} for test {self.test_id}")
                return True

            except Exception as e:
                # Lock exists - check if it's expired
                try:
                    doc = self.client.get(index=LOCK_INDEX, id=self.repo_name, ignore=[404])
                    if doc.get("found"):
                        source = doc["_source"]
                        expires_at = source.get("expires_at", 0)

                        # If lock is expired, try to delete and retry
                        if time.time() > expires_at:
                            logger.warning(
                                f"Found expired lock on {self.repo_name} "
                                f"by {source.get('locked_by')}. Releasing..."
                            )
                            self.client.delete(index=LOCK_INDEX, id=self.repo_name, ignore=[404])
                            continue

                        logger.debug(
                            f"Repository {self.repo_name} locked by {source.get('locked_by')}, "
                            f"waiting..."
                        )
                except Exception as check_error:
                    logger.debug(f"Error checking lock: {check_error}")

                # Wait before retry
                time.sleep(1)

        logger.warning(
            f"Failed to acquire lock on repository {self.repo_name} "
            f"after {timeout} seconds"
        )
        return False

    def release(self):
        """Release lock on repository."""
        if not self.locked:
            return

        try:
            self.client.delete(index=LOCK_INDEX, id=self.repo_name, ignore=[404])
            logger.info(f"Released lock on repository {self.repo_name}")
            self.locked = False
        except Exception as e:
            logger.error(f"Error releasing lock on {self.repo_name}: {e}")

    def __enter__(self):
        """Context manager entry."""
        if not self.acquire():
            raise RuntimeError(f"Failed to acquire lock on repository {self.repo_name}")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.release()


def get_available_unlocked_repositories(
    client: Elasticsearch,
    available_repos: List[dict],
    count: int = 1,
) -> List[dict]:
    """
    Get N available repositories that are not currently locked by other tests.

    :param client: Elasticsearch client
    :param available_repos: List of available repository dictionaries
    :param count: Number of repositories needed
    :return: List of unlocked repositories (may be fewer than count)
    """
    unlocked = []

    # Ensure lock index exists
    try:
        if not client.indices.exists(index=LOCK_INDEX):
            client.indices.create(
                index=LOCK_INDEX,
                body={
                    "settings": {"number_of_shards": 1, "number_of_replicas": 0},
                    "mappings": {
                        "properties": {
                            "repo_name": {"type": "keyword"},
                            "locked_by": {"type": "keyword"},
                            "locked_at": {"type": "date"},
                            "expires_at": {"type": "date"},
                        }
                    },
                },
            )
    except Exception:
        pass  # Index already exists

    # Check each repository
    for repo in available_repos:
        if len(unlocked) >= count:
            break

        repo_name = repo["name"]

        try:
            # Check if repository is locked
            doc = client.get(index=LOCK_INDEX, id=repo_name, ignore=[404])

            if not doc.get("found"):
                # Not locked
                unlocked.append(repo)
                continue

            # Check if lock is expired
            source = doc["_source"]
            expires_at = source.get("expires_at", 0)

            if time.time() > expires_at:
                # Lock expired, can use this repo
                logger.info(f"Found expired lock on {repo_name}, marking as available")
                unlocked.append(repo)
            else:
                logger.debug(f"Repository {repo_name} is locked by {source.get('locked_by')}")

        except Exception as e:
            logger.warning(f"Error checking lock for {repo_name}: {e}")
            # Assume available on error
            unlocked.append(repo)

    return unlocked


def cleanup_expired_locks(client: Elasticsearch):
    """
    Clean up expired test locks from the lock index.

    Should be called before test suite starts.
    """
    try:
        if not client.indices.exists(index=LOCK_INDEX):
            return

        # Query for all locks
        response = client.search(
            index=LOCK_INDEX,
            body={"query": {"match_all": {}}, "size": 1000},
        )

        current_time = time.time()
        expired_count = 0

        for hit in response.get("hits", {}).get("hits", []):
            expires_at = hit["_source"].get("expires_at", 0)

            if current_time > expires_at:
                repo_name = hit["_id"]
                locked_by = hit["_source"].get("locked_by", "unknown")

                logger.info(f"Cleaning up expired lock on {repo_name} (was locked by {locked_by})")
                client.delete(index=LOCK_INDEX, id=repo_name, ignore=[404])
                expired_count += 1

        if expired_count > 0:
            logger.info(f"Cleaned up {expired_count} expired lock(s)")

    except Exception as e:
        logger.error(f"Error cleaning up expired locks: {e}")
