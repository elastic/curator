"""Test deepfreeze Status action"""
# pylint: disable=attribute-defined-outside-init
from unittest import TestCase
from unittest.mock import Mock, patch, MagicMock
import pytest

from curator.actions.deepfreeze.status import Status
from curator.actions.deepfreeze.helpers import Settings, Repository


class TestDeepfreezeStatus(TestCase):
    """Test Deepfreeze Status action"""

    def setUp(self):
        """Set up test fixtures"""
        self.client = Mock()
        self.mock_settings = Settings(
            repo_name_prefix="deepfreeze",
            bucket_name_prefix="deepfreeze",
            base_path_prefix="snapshots",
            canned_acl="private",
            storage_class="GLACIER",
            provider="aws",
            rotate_by="path",
            style="oneup",
            last_suffix="000003"
        )

    def test_init(self):
        """Test Status initialization"""
        with patch('curator.actions.deepfreeze.status.get_settings', return_value=self.mock_settings):
            with patch('curator.actions.deepfreeze.status.Console') as mock_console:
                status = Status(self.client)

                assert status.client == self.client
                assert status.settings == self.mock_settings
                mock_console.assert_called_once()
                mock_console.return_value.clear.assert_called_once()

    def test_get_cluster_name_success(self):
        """Test successful cluster name retrieval"""
        self.client.cluster.health.return_value = {
            'cluster_name': 'test-cluster',
            'status': 'green'
        }

        with patch('curator.actions.deepfreeze.status.get_settings', return_value=self.mock_settings):
            status = Status(self.client)
            cluster_name = status.get_cluster_name()

            assert cluster_name == 'test-cluster'

    def test_get_cluster_name_error(self):
        """Test cluster name retrieval with error"""
        self.client.cluster.health.side_effect = Exception("Connection failed")

        with patch('curator.actions.deepfreeze.status.get_settings', return_value=self.mock_settings):
            status = Status(self.client)
            cluster_name = status.get_cluster_name()

            assert cluster_name.startswith("Error:")
            assert "Connection failed" in cluster_name

    def test_do_config(self):
        """Test configuration display"""
        with patch('curator.actions.deepfreeze.status.get_settings', return_value=self.mock_settings):
            with patch('curator.actions.deepfreeze.status.Table') as mock_table_class:
                with patch('curator.actions.deepfreeze.status.Console'):
                    mock_table = Mock()
                    mock_table_class.return_value = mock_table

                    status = Status(self.client)
                    status.get_cluster_name = Mock(return_value="test-cluster")

                    status.do_config()

                    # Should create table with title "Configuration"
                    mock_table_class.assert_called_with(title="Configuration")

                    # Should add columns
                    mock_table.add_column.assert_any_call("Setting", style="cyan")
                    mock_table.add_column.assert_any_call("Value", style="magenta")

                    # Should add rows for all settings
                    expected_calls = [
                        ("Repo Prefix", "deepfreeze"),
                        ("Bucket Prefix", "deepfreeze"),
                        ("Base Path Prefix", "snapshots"),
                        ("Canned ACL", "private"),
                        ("Storage Class", "GLACIER"),
                        ("Provider", "aws"),
                        ("Rotate By", "path"),
                        ("Style", "oneup"),
                        ("Last Suffix", "000003"),
                        ("Cluster Name", "test-cluster")
                    ]

                    for expected_call in expected_calls:
                        mock_table.add_row.assert_any_call(*expected_call)

    def test_do_ilm_policies(self):
        """Test ILM policies display"""
        self.client.ilm.get_lifecycle.return_value = {
            'policy1': {
                'policy': {
                    'phases': {
                        'frozen': {
                            'actions': {
                                'searchable_snapshot': {
                                    'snapshot_repository': 'deepfreeze-000003'
                                }
                            }
                        }
                    }
                },
                'in_use_by': {
                    'indices': ['index1', 'index2'],
                    'data_streams': ['stream1']
                }
            },
            'policy2': {
                'policy': {
                    'phases': {
                        'cold': {
                            'actions': {
                                'searchable_snapshot': {
                                    'snapshot_repository': 'deepfreeze-000003'
                                }
                            }
                        }
                    }
                },
                'in_use_by': {
                    'indices': ['index3'],
                    'data_streams': []
                }
            },
            'policy3': {
                'policy': {
                    'phases': {
                        'hot': {
                            'actions': {}
                        }
                    }
                },
                'in_use_by': {
                    'indices': [],
                    'data_streams': []
                }
            }
        }

        with patch('curator.actions.deepfreeze.status.get_settings', return_value=self.mock_settings):
            with patch('curator.actions.deepfreeze.status.Table') as mock_table_class:
                with patch('curator.actions.deepfreeze.status.Console'):
                    mock_table = Mock()
                    mock_table_class.return_value = mock_table

                    status = Status(self.client)

                    status.do_ilm_policies()

                    # Should create table with title "ILM Policies"
                    mock_table_class.assert_called_with(title="ILM Policies")

                    # Should add columns
                    mock_table.add_column.assert_any_call("Policy", style="cyan")
                    mock_table.add_column.assert_any_call("Repository", style="magenta")
                    mock_table.add_column.assert_any_call("Indices", style="magenta")
                    mock_table.add_column.assert_any_call("Datastreams", style="magenta")

                    # Should add rows for matching policies (policy1 and policy2)
                    mock_table.add_row.assert_any_call("policy1", "deepfreeze-000003*", "2", "1")
                    mock_table.add_row.assert_any_call("policy2", "deepfreeze-000003*", "1", "0")

    def test_do_buckets_path_rotation(self):
        """Test buckets display for path rotation"""
        mock_repos = [
            Repository(
                name="deepfreeze-000003",
                bucket="deepfreeze",
                base_path="snapshots-000003"
            )
        ]

        with patch('curator.actions.deepfreeze.status.get_settings', return_value=self.mock_settings):
            with patch('curator.actions.deepfreeze.status.get_all_repos', return_value=mock_repos):
                with patch('curator.actions.deepfreeze.status.Table') as mock_table_class:
                    with patch('curator.actions.deepfreeze.status.Console'):
                        mock_table = Mock()
                        mock_table_class.return_value = mock_table

                        status = Status(self.client)

                        status.do_buckets()

                        # Should create table with title "Buckets"
                        mock_table_class.assert_called_with(title="Buckets")

                        # Should add columns
                        mock_table.add_column.assert_any_call("Provider", style="cyan")
                        mock_table.add_column.assert_any_call("Bucket", style="magenta")
                        mock_table.add_column.assert_any_call("Base_path", style="magenta")

                        # For path rotation, should show single bucket with suffixed path
                        # Bucket gets marked with asterisk since it matches current bucket/base_path
                        mock_table.add_row.assert_called_with(
                            "aws",
                            "deepfreeze*",
                            "snapshots-000003"
                        )

    def test_do_buckets_bucket_rotation(self):
        """Test buckets display for bucket rotation"""
        bucket_rotation_settings = Settings(
            repo_name_prefix="deepfreeze",
            bucket_name_prefix="deepfreeze",
            base_path_prefix="snapshots",
            rotate_by="bucket",
            style="oneup",
            last_suffix="000003",
            provider="aws"
        )

        mock_repos = [
            Repository(
                name="deepfreeze-000003",
                bucket="deepfreeze-000003",
                base_path="snapshots"
            )
        ]

        with patch('curator.actions.deepfreeze.status.get_settings', return_value=bucket_rotation_settings):
            with patch('curator.actions.deepfreeze.status.get_all_repos', return_value=mock_repos):
                with patch('curator.actions.deepfreeze.status.Table') as mock_table_class:
                    with patch('curator.actions.deepfreeze.status.Console'):
                        mock_table = Mock()
                        mock_table_class.return_value = mock_table

                        status = Status(self.client)

                        status.do_buckets()

                        # For bucket rotation, should show suffixed bucket with static path
                        mock_table.add_row.assert_called_with(
                            "aws",
                            "deepfreeze-000003*",
                            "snapshots"
                        )


    def test_do_action(self):
        """Test main action execution"""
        with patch('curator.actions.deepfreeze.status.get_settings', return_value=self.mock_settings):
            with patch('curator.actions.deepfreeze.status.Console'):
                status = Status(self.client)

                # Mock all sub-methods
                status.do_repositories = Mock()
                status.do_buckets = Mock()
                status.do_ilm_policies = Mock()
                status.do_config = Mock()

                with patch('curator.actions.deepfreeze.status.print') as mock_print:
                    status.do_action()

                    # Should call all display methods in order
                    status.do_repositories.assert_called_once()
                    status.do_buckets.assert_called_once()
                    status.do_ilm_policies.assert_called_once()
                    status.do_config.assert_called_once()

                    # Should print empty line
                    mock_print.assert_called_once()

    def test_do_singleton_action(self):
        """Test singleton action execution"""
        with patch('curator.actions.deepfreeze.status.get_settings', return_value=self.mock_settings):
            with patch('curator.actions.deepfreeze.status.Console'):
                status = Status(self.client)

                with patch.object(status, 'do_action') as mock_do_action:
                    status.do_singleton_action()

                    mock_do_action.assert_called_once()


    def test_repository_status_with_snapshots(self):
        """Test repository status display with snapshot counts"""
        mock_repos = [
            Repository(
                name="deepfreeze-000001",
                is_mounted=True,
                is_thawed=False
            )
        ]

        # Mock successful snapshot retrieval
        self.client.snapshot.get.return_value = {
            'snapshots': [
                {'name': 'snap1'},
                {'name': 'snap2'},
                {'name': 'snap3'}
            ]
        }

        with patch('curator.actions.deepfreeze.status.get_settings', return_value=self.mock_settings):
            with patch('curator.actions.deepfreeze.status.get_all_repos', return_value=mock_repos):
                with patch('curator.actions.deepfreeze.status.Table') as mock_table_class:
                    with patch('curator.actions.deepfreeze.status.Console'):
                        mock_table = Mock()
                        mock_table_class.return_value = mock_table

                        status = Status(self.client)

                        status.do_repositories()

                        # Should show snapshot count
                        mock_table.add_row.assert_called_with(
                            "deepfreeze-000001", "M", "3", "N/A", "N/A"
                        )

    def test_repository_unmount_on_error(self):
        """Test repository gets unmounted when snapshot check fails"""
        mock_repo = Repository(
            name="deepfreeze-000001",
            is_mounted=True,
            is_thawed=False
        )

        # Mock snapshot retrieval error
        self.client.snapshot.get.side_effect = Exception("Repository not accessible")

        with patch('curator.actions.deepfreeze.status.get_settings', return_value=self.mock_settings):
            with patch('curator.actions.deepfreeze.status.get_all_repos', return_value=[mock_repo]):
                with patch('curator.actions.deepfreeze.status.Table') as mock_table_class:
                    with patch('curator.actions.deepfreeze.status.Console'):
                        mock_table = Mock()
                        mock_table_class.return_value = mock_table

                        status = Status(self.client)

                        status.do_repositories()

                        # Repository should be unmounted after error
                        assert mock_repo.is_mounted is False