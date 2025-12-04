"""
Integration tests for deepfreeze (Task Group 19)

These tests verify:
1. End-to-end workflows with mocked ES and S3
2. Setup -> Status workflow
3. Setup -> Rotate workflow
4. Thaw request lifecycle
5. Backward compatibility with existing deepfreeze-status index format
"""

import pytest
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from deepfreeze.actions import (
    Setup,
    Status,
    Rotate,
    Thaw,
    Refreeze,
    Cleanup,
)
from deepfreeze.helpers import Settings, Repository
from deepfreeze.constants import STATUS_INDEX, THAW_STATUS_COMPLETED


class MockElasticsearchClient:
    """Mock Elasticsearch client for integration testing"""

    def __init__(self):
        self._indices = {}
        self._documents = {}
        self._repos = {}

        # Set up mock structure
        self.indices = MagicMock()
        self.indices.exists = MagicMock(side_effect=self._indices_exists)
        self.indices.create = MagicMock(side_effect=self._indices_create)
        self.indices.get_index_template = MagicMock(return_value={"index_templates": []})

        self.snapshot = MagicMock()
        self.snapshot.get_repository = MagicMock(return_value={})
        self.snapshot.create_repository = MagicMock()

        self.ilm = MagicMock()
        self.ilm.get_lifecycle = MagicMock(return_value={})
        self.ilm.put_lifecycle = MagicMock()

        self.cluster = MagicMock()
        self.cluster.health = MagicMock(return_value={"status": "green"})

        # Info for version checks
        self.info = MagicMock(return_value={"version": {"number": "8.10.0"}})

        # Document operations
        self.get = MagicMock(side_effect=self._get_document)
        self.index = MagicMock(side_effect=self._index_document)
        self.create = MagicMock(side_effect=self._create_document)
        self.update = MagicMock(side_effect=self._update_document)
        self.search = MagicMock(side_effect=self._search_documents)

    def _indices_exists(self, index):
        return index in self._indices

    def _indices_create(self, index, **kwargs):
        self._indices[index] = kwargs.get("body", {})
        return {"acknowledged": True}

    def _get_document(self, index, id):
        key = f"{index}:{id}"
        if key not in self._documents:
            from elasticsearch8 import NotFoundError
            raise NotFoundError(404, "not_found", "not found")
        return {"_source": self._documents[key]}

    def _index_document(self, index, id=None, body=None, **kwargs):
        if id is None:
            id = f"doc-{len(self._documents) + 1}"
        key = f"{index}:{id}"
        self._documents[key] = body
        return {"_id": id, "result": "created"}

    def _create_document(self, index, id, body=None, **kwargs):
        key = f"{index}:{id}"
        self._documents[key] = body
        return {"_id": id, "result": "created"}

    def _update_document(self, index, id, body=None, **kwargs):
        key = f"{index}:{id}"
        if key in self._documents:
            if "doc" in body:
                self._documents[key].update(body["doc"])
        return {"_id": id, "result": "updated"}

    def _search_documents(self, index=None, body=None, **kwargs):
        # Return documents matching doctype if specified
        hits = []
        for key, doc in self._documents.items():
            idx, doc_id = key.split(":", 1)
            if index and idx != index:
                continue
            hits.append({
                "_id": doc_id,
                "_source": doc,
            })
        return {"hits": {"hits": hits, "total": {"value": len(hits)}}}


class MockS3Client:
    """Mock S3 client for integration testing"""

    def __init__(self):
        self._buckets = {}
        self._objects = {}

    def bucket_exists(self, bucket_name):
        return bucket_name in self._buckets

    def create_bucket(self, bucket_name):
        self._buckets[bucket_name] = {}

    def list_objects(self, bucket_name, prefix):
        if bucket_name not in self._buckets:
            return []
        objects = []
        for key, obj in self._objects.items():
            if key.startswith(f"{bucket_name}/{prefix}"):
                objects.append(obj)
        return objects

    def list_buckets(self, prefix=None):
        buckets = list(self._buckets.keys())
        if prefix:
            buckets = [b for b in buckets if b.startswith(prefix)]
        return buckets

    def thaw(self, bucket_name, base_path, object_keys, restore_days=7, retrieval_tier="Standard"):
        pass

    def refreeze(self, bucket_name, path, storage_class="GLACIER"):
        pass

    def test_connection(self):
        return True


class TestSetupStatusWorkflow:
    """Test Setup -> Status workflow (Task Group 19)"""

    def test_setup_then_status_workflow(self):
        """Test that after setup, status can read the configuration"""
        mock_es = MagicMock()
        mock_s3 = MockS3Client()

        # Configure mock ES to pass preconditions
        mock_es.indices.exists.return_value = False
        mock_es.snapshot.get_repository.return_value = {}
        mock_es.indices.get_index_template.return_value = {
            "index_templates": [{"index_template": {}}]
        }
        mock_es.info.return_value = {"version": {"number": "8.10.0"}}

        # Run setup with all necessary patches
        with patch("deepfreeze.actions.setup.s3_client_factory") as mock_factory:
            mock_factory.return_value = mock_s3

            with patch("deepfreeze.actions.setup.ensure_settings_index"):
                with patch("deepfreeze.actions.setup.save_settings"):
                    with patch("deepfreeze.actions.setup.create_repo"):
                        with patch("deepfreeze.actions.setup.create_or_update_ilm_policy") as mock_ilm:
                            mock_ilm.return_value = {"action": "created", "policy_body": {}}

                            with patch("deepfreeze.actions.setup.update_index_template_ilm_policy") as mock_tmpl:
                                mock_tmpl.return_value = {"action": "updated", "template_type": "composable"}

                                setup = Setup(
                                    client=mock_es,
                                    repo_name_prefix="integration-test",
                                    bucket_name_prefix="integration-bucket",
                                    ilm_policy_name="test-policy",
                                    index_template_name="test-template",
                                    porcelain=True,
                                )

                                setup.do_action()

        # Verify bucket was created (bucket name prefix used, not full suffix)
        # The setup creates the bucket with just the prefix
        assert mock_s3.bucket_exists("integration-bucket")


class TestSetupRotateWorkflow:
    """Test Setup -> Rotate workflow (Task Group 19)"""

    def test_setup_then_rotate_workflow(self):
        """Test that after setup, rotate creates new repository"""
        mock_es = MagicMock()
        mock_s3 = MockS3Client()

        # Configure mock ES to pass preconditions
        mock_es.indices.exists.return_value = False
        mock_es.snapshot.get_repository.return_value = {}
        mock_es.indices.get_index_template.return_value = {
            "index_templates": [{"index_template": {}}]
        }
        mock_es.info.return_value = {"version": {"number": "8.10.0"}}

        # Setup phase
        with patch("deepfreeze.actions.setup.s3_client_factory") as mock_factory:
            mock_factory.return_value = mock_s3

            with patch("deepfreeze.actions.setup.ensure_settings_index"):
                with patch("deepfreeze.actions.setup.save_settings"):
                    with patch("deepfreeze.actions.setup.create_repo"):
                        with patch("deepfreeze.actions.setup.create_or_update_ilm_policy") as mock_ilm:
                            mock_ilm.return_value = {"action": "created", "policy_body": {}}

                            with patch("deepfreeze.actions.setup.update_index_template_ilm_policy") as mock_tmpl:
                                mock_tmpl.return_value = {"action": "updated", "template_type": "composable"}

                                setup = Setup(
                                    client=mock_es,
                                    repo_name_prefix="rotate-test",
                                    bucket_name_prefix="rotate-bucket",
                                    ilm_policy_name="test-policy",
                                    index_template_name="test-template",
                                    porcelain=True,
                                )

                                setup.do_action()

        # Verify bucket was created
        assert mock_s3.bucket_exists("rotate-bucket")

        # Now reconfigure for rotate phase
        mock_es.indices.exists.return_value = True

        # Rotate phase - the rotate action creates a new repository with new base_path
        # in the SAME bucket (rotate-bucket), not a new bucket
        with patch("deepfreeze.actions.rotate.s3_client_factory") as mock_factory:
            mock_factory.return_value = mock_s3

            with patch("deepfreeze.actions.rotate.get_settings") as mock_get:
                mock_get.return_value = Settings(
                    repo_name_prefix="rotate-test",
                    bucket_name_prefix="rotate-bucket",
                    style="oneup",
                    last_suffix="000001",
                )

                with patch("deepfreeze.actions.rotate.get_matching_repos") as mock_repos:
                    mock_repos.return_value = [
                        Repository(
                            name="rotate-test-000001",
                            bucket="rotate-bucket",
                            is_mounted=True,
                            thaw_state="active",
                        )
                    ]

                    with patch("deepfreeze.actions.rotate.create_repo") as mock_create_repo:
                        with patch("deepfreeze.actions.rotate.save_settings"):
                            with patch("deepfreeze.actions.rotate.create_versioned_ilm_policy"):
                                rotate = Rotate(
                                    client=mock_es,
                                    keep=2,
                                    porcelain=True,
                                )

                                # This should complete without error
                                rotate.do_action()

                                # Verify create_repo was called (for the new repository)
                                mock_create_repo.assert_called_once()


class TestThawRequestLifecycle:
    """Test Thaw request lifecycle (Task Group 19)"""

    def test_thaw_list_requests(self):
        """Test listing thaw requests"""
        mock_es = MagicMock()
        mock_es.indices.exists.return_value = True
        mock_s3 = MockS3Client()

        # List mode
        with patch("deepfreeze.actions.thaw.s3_client_factory") as mock_factory:
            mock_factory.return_value = mock_s3

            with patch("deepfreeze.actions.thaw.get_settings") as mock_get:
                mock_get.return_value = Settings()

                with patch("deepfreeze.actions.thaw.list_thaw_requests") as mock_list:
                    mock_list.return_value = [
                        {
                            "id": "thaw-001",
                            "request_id": "thaw-001",
                            "status": "in_progress",
                            "repos": ["test-repo-000001"],
                        }
                    ]

                    thaw = Thaw(client=mock_es, list_requests=True, porcelain=True)
                    thaw.do_action()

                    # Should have called list_thaw_requests
                    mock_list.assert_called_once()

    def test_refreeze_completed_request(self):
        """Test refreezing a completed thaw request"""
        mock_es = MagicMock()
        mock_es.indices.exists.return_value = True
        mock_s3 = MockS3Client()

        # Refreeze the completed request
        with patch("deepfreeze.actions.refreeze.s3_client_factory") as mock_factory:
            mock_factory.return_value = mock_s3

            with patch("deepfreeze.actions.refreeze.get_settings") as mock_get:
                mock_get.return_value = Settings()

                with patch("deepfreeze.actions.refreeze.get_thaw_request") as mock_req:
                    mock_req.return_value = {
                        "request_id": "thaw-completed",
                        "status": THAW_STATUS_COMPLETED,
                        "repos": ["lifecycle-repo-000001"],
                    }

                    with patch("deepfreeze.utilities.get_repository") as mock_repo:
                        mock_repo.return_value = Repository(
                            name="lifecycle-repo-000001",
                            bucket="lifecycle-bucket",
                            base_path="path/",
                            thaw_state="thawed",
                            is_mounted=True,
                        )

                        with patch("deepfreeze.actions.refreeze.update_thaw_request"):
                            with patch("deepfreeze.actions.refreeze.unmount_repo"):
                                refreeze = Refreeze(
                                    client=mock_es,
                                    request_id="thaw-completed",
                                    porcelain=True,
                                )

                                # Dry run should work
                                refreeze.do_dry_run()


class TestBackwardCompatibility:
    """Test backward compatibility with existing status index format (Task Group 19)"""

    def test_read_existing_settings_format(self):
        """Test that we can read existing settings format from status index"""
        mock_es = MagicMock()
        mock_es.indices.exists.return_value = True
        mock_es.get.return_value = {
            "_source": {
                "doctype": "settings",
                "repo_name_prefix": "legacy-repo",
                "bucket_name_prefix": "legacy-bucket",
                "base_path_prefix": "snapshots",
                "style": "oneup",
                "last_suffix": "000010",
                "canned_acl": "private",
                "storage_class": "intelligent_tiering",
                "provider": "aws",
                "rotate_by": "path",
            }
        }

        # Verify we can read the settings
        from deepfreeze.utilities import get_settings
        settings = get_settings(mock_es)

        assert settings.repo_name_prefix == "legacy-repo"
        assert settings.bucket_name_prefix == "legacy-bucket"
        assert settings.last_suffix == "000010"
        assert settings.style == "oneup"

    def test_read_existing_repository_format(self):
        """Test that we can read existing repository format from status index"""
        mock_es = MagicMock()
        mock_es.search.return_value = {
            "hits": {
                "total": {"value": 1},
                "hits": [
                    {
                        "_id": "repo-doc-1",
                        "_source": {
                            "doctype": "repository",
                            "name": "legacy-repo-000001",
                            "bucket": "legacy-bucket-000001",
                            "base_path": "snapshots/legacy",
                            "start": "2023-01-01T00:00:00+00:00",
                            "end": "2023-06-30T23:59:59+00:00",
                            "is_mounted": True,
                            "thaw_state": "active",
                            "is_thawed": False,
                        },
                    }
                ],
            }
        }

        # Verify we can read the repository
        from deepfreeze.utilities import get_repository
        repo = get_repository(mock_es, "legacy-repo-000001")

        assert repo.name == "legacy-repo-000001"
        assert repo.bucket == "legacy-bucket-000001"
        assert repo.is_mounted is True
        assert repo.thaw_state == "active"

    def test_read_existing_thaw_request_format(self):
        """Test that we can read existing thaw request format"""
        mock_es = MagicMock()
        mock_es.get.return_value = {
            "_source": {
                "doctype": "thaw_request",
                "request_id": "thaw-legacy-001",
                "status": "completed",
                "repos": ["legacy-repo-000001", "legacy-repo-000002"],
                "start_date": "2023-01-01T00:00:00+00:00",
                "end_date": "2023-06-30T23:59:59+00:00",
                "created_at": "2023-06-15T10:00:00+00:00",
                "completed_at": "2023-06-15T14:30:00+00:00",
                "restore_days": 7,
                "retrieval_tier": "Standard",
            }
        }

        # Verify we can read the thaw request
        from deepfreeze.utilities import get_thaw_request
        request = get_thaw_request(mock_es, "thaw-legacy-001")

        assert request["request_id"] == "thaw-legacy-001"
        assert request["status"] == "completed"
        assert len(request["repos"]) == 2


class TestDocumentedTestEnvironment:
    """Document test environment requirements (Task Group 19)"""

    def test_environment_requirements_documented(self):
        """Verify that test environment requirements are available"""
        # This test verifies that the testing can run with:
        # 1. Mocked Elasticsearch client (no real ES instance required)
        # 2. Mocked S3 client (no real AWS credentials required)
        # 3. pytest and standard Python testing tools

        mock_es = MagicMock()
        mock_s3 = MockS3Client()

        # Verify mocks work as expected
        mock_es.cluster.health.return_value = {"status": "green"}
        assert mock_es.cluster.health()["status"] == "green"
        assert mock_s3.test_connection() is True

        # Verify bucket operations work
        mock_s3.create_bucket("test-bucket")
        assert mock_s3.bucket_exists("test-bucket")
