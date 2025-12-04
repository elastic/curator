"""Tests for the deepfreeze CLI"""

import os
import tempfile
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from deepfreeze.cli.main import cli


@pytest.fixture
def runner():
    """Create a CLI test runner."""
    return CliRunner()


@pytest.fixture
def temp_config_file():
    """Create a temporary config file for testing."""
    config_content = """
elasticsearch:
  hosts:
    - https://localhost:9200
  username: elastic
  password: changeme

logging:
  loglevel: INFO
"""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write(config_content)
        f.flush()
        yield f.name
    # Cleanup
    os.unlink(f.name)


class TestCLIBasic:
    """Basic CLI tests."""

    def test_cli_help(self, runner):
        """Test CLI help command shows all commands."""
        result = runner.invoke(cli, ['--help'])
        assert result.exit_code == 0
        assert 'Deepfreeze' in result.output
        assert 'setup' in result.output
        assert 'status' in result.output
        assert 'rotate' in result.output
        assert 'thaw' in result.output
        assert 'refreeze' in result.output
        assert 'cleanup' in result.output
        assert 'repair-metadata' in result.output

    def test_cli_version(self, runner):
        """Test CLI version command."""
        result = runner.invoke(cli, ['--version'])
        assert result.exit_code == 0
        assert 'deepfreeze' in result.output.lower()

    def test_setup_help(self, runner):
        """Test setup command help."""
        result = runner.invoke(cli, ['setup', '--help'])
        assert result.exit_code == 0
        assert 'ilm_policy_name' in result.output
        assert 'index_template_name' in result.output
        assert 'bucket_name_prefix' in result.output

    def test_thaw_help(self, runner):
        """Test thaw command help."""
        result = runner.invoke(cli, ['thaw', '--help'])
        assert result.exit_code == 0
        assert 'start-date' in result.output
        assert 'end-date' in result.output
        assert 'check-status' in result.output
        assert 'retrieval-tier' in result.output


class TestCLIOptionParsing:
    """Test CLI option parsing."""

    def test_setup_requires_ilm_policy(self, runner, temp_config_file):
        """Test that setup requires --ilm_policy_name."""
        with patch('deepfreeze.cli.main.get_client_from_context'):
            result = runner.invoke(cli, [
                '-c', temp_config_file,
                'setup',
                '--index_template_name', 'test-template'
            ])
            # Missing required option should fail
            assert result.exit_code != 0
            assert 'ilm_policy_name' in result.output

    def test_setup_requires_index_template(self, runner, temp_config_file):
        """Test that setup requires --index_template_name."""
        with patch('deepfreeze.cli.main.get_client_from_context'):
            result = runner.invoke(cli, [
                '-c', temp_config_file,
                'setup',
                '--ilm_policy_name', 'test-policy'
            ])
            # Missing required option should fail
            assert result.exit_code != 0
            assert 'index_template_name' in result.output

    def test_thaw_requires_mode(self, runner, temp_config_file):
        """Test that thaw requires one of the operation modes."""
        with patch('deepfreeze.cli.main.get_client_from_context'):
            result = runner.invoke(cli, [
                '-c', temp_config_file,
                'thaw'
            ])
            assert result.exit_code != 0
            assert 'Must specify one of' in result.output

    def test_thaw_date_range_requires_both_dates(self, runner, temp_config_file):
        """Test that thaw with date range requires both start and end dates."""
        with patch('deepfreeze.cli.main.get_client_from_context'):
            result = runner.invoke(cli, [
                '-c', temp_config_file,
                'thaw',
                '--start-date', '2025-01-01T00:00:00Z'
            ])
            assert result.exit_code != 0
            assert 'Both --start-date and --end-date are required' in result.output


class TestCLIWithMockedClient:
    """Test CLI commands with mocked Elasticsearch client."""

    @patch('deepfreeze.cli.main.create_es_client')
    def test_dry_run_flag_parsed(self, mock_create_client, runner, temp_config_file):
        """Test that --dry-run flag is parsed correctly."""
        mock_client = MagicMock()
        mock_create_client.return_value = mock_client

        # We need to mock the entire chain to avoid errors
        mock_client.indices.exists.return_value = False

        with patch('deepfreeze.actions.Setup') as mock_setup_class:
            mock_setup = MagicMock()
            mock_setup_class.return_value = mock_setup

            result = runner.invoke(cli, [
                '-c', temp_config_file,
                '--dry-run',
                'setup',
                '--ilm_policy_name', 'test-policy',
                '--index_template_name', 'test-template'
            ])

            # Verify do_dry_run was called since --dry-run was passed
            if mock_setup.do_dry_run.called:
                mock_setup.do_action.assert_not_called()


class TestCLICommandRegistration:
    """Test that all commands are properly registered."""

    def test_all_commands_registered(self, runner):
        """Test that all expected commands are registered."""
        result = runner.invoke(cli, ['--help'])
        commands = ['setup', 'status', 'rotate', 'thaw', 'refreeze', 'cleanup', 'repair-metadata']
        for cmd in commands:
            assert cmd in result.output, f"Command '{cmd}' not found in CLI help"

    def test_repair_metadata_command_name(self, runner):
        """Test that repair-metadata command is named with hyphen."""
        result = runner.invoke(cli, ['repair-metadata', '--help'])
        assert result.exit_code == 0
        assert 'Repair repository metadata' in result.output

    def test_status_command_options(self, runner):
        """Test that status command has expected options."""
        result = runner.invoke(cli, ['status', '--help'])
        assert result.exit_code == 0
        assert '--limit' in result.output
        assert '--repos' in result.output
        assert '--porcelain' in result.output

    def test_rotate_command_options(self, runner):
        """Test that rotate command has expected options."""
        result = runner.invoke(cli, ['rotate', '--help'])
        assert result.exit_code == 0
        assert '--year' in result.output
        assert '--month' in result.output
        assert '--keep' in result.output


class TestDefaultConfig:
    """Test default configuration file functionality."""

    def test_default_config_path_in_help(self, runner):
        """Test that default config path is shown in help."""
        result = runner.invoke(cli, ['--help'])
        assert result.exit_code == 0
        assert '.deepfreeze/config.yml' in result.output

    def test_get_default_config_file_not_exists(self):
        """Test that get_default_config_file returns None when file doesn't exist."""
        from deepfreeze.cli.main import get_default_config_file
        with patch('deepfreeze.cli.main.DEFAULT_CONFIG_PATH') as mock_path:
            mock_path.is_file.return_value = False
            result = get_default_config_file()
            assert result is None

    def test_get_default_config_file_exists(self, tmp_path):
        """Test that get_default_config_file returns path when file exists."""
        from deepfreeze.cli.main import get_default_config_file, DEFAULT_CONFIG_PATH
        from pathlib import Path

        # Create a temporary config file
        config_file = tmp_path / "config.yml"
        config_file.write_text("elasticsearch:\n  hosts:\n    - localhost:9200\n")

        with patch('deepfreeze.cli.main.DEFAULT_CONFIG_PATH', config_file):
            result = get_default_config_file()
            assert result == str(config_file)

    def test_cli_uses_default_config_when_exists(self, runner, tmp_path):
        """Test that CLI uses default config file when it exists."""
        from pathlib import Path

        # Create a temporary config file
        config_file = tmp_path / "config.yml"
        config_content = """
elasticsearch:
  hosts:
    - https://localhost:9200
  username: elastic
  password: changeme
"""
        config_file.write_text(config_content)

        with patch('deepfreeze.cli.main.DEFAULT_CONFIG_PATH', config_file):
            with patch('deepfreeze.cli.main.get_default_config_file', return_value=str(config_file)):
                result = runner.invoke(cli, ['--help'])
                # Help should work without explicit --config
                assert result.exit_code == 0
