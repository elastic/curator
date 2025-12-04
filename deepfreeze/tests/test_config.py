"""Tests for the deepfreeze config module"""

import os
import tempfile
from unittest.mock import patch

import pytest

from deepfreeze.config import (
    configure_logging,
    get_elasticsearch_config,
    get_logging_config,
    load_config,
    validate_config,
)
from deepfreeze.exceptions import ActionError


class TestLoadConfig:
    """Tests for load_config function."""

    def test_load_config_from_file(self):
        """Test loading configuration from a YAML file."""
        config_content = """
elasticsearch:
  hosts:
    - https://localhost:9200
  username: elastic
  password: changeme

logging:
  loglevel: DEBUG
"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write(config_content)
            f.flush()

            config = load_config(f.name)

            assert 'elasticsearch' in config
            assert config['elasticsearch']['hosts'] == ['https://localhost:9200']
            assert config['elasticsearch']['username'] == 'elastic'
            assert config['logging']['loglevel'] == 'DEBUG'

            os.unlink(f.name)

    def test_load_config_file_not_found(self):
        """Test that missing config file raises ActionError."""
        with pytest.raises(ActionError) as exc_info:
            load_config('/nonexistent/path/config.yaml')
        assert 'not found' in str(exc_info.value)

    def test_load_config_invalid_yaml(self):
        """Test that invalid YAML raises ActionError."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write("invalid: yaml: content: :")
            f.flush()

            with pytest.raises(ActionError) as exc_info:
                load_config(f.name)
            assert 'Invalid YAML' in str(exc_info.value)

            os.unlink(f.name)

    def test_load_config_environment_override(self):
        """Test that environment variables override file config."""
        config_content = """
elasticsearch:
  hosts:
    - https://localhost:9200
  username: elastic
  password: file_password
"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write(config_content)
            f.flush()

            with patch.dict(os.environ, {'DEEPFREEZE_ES_PASSWORD': 'env_password'}):
                config = load_config(f.name)

                # Environment variable should override file value
                assert config['elasticsearch']['password'] == 'env_password'

            os.unlink(f.name)

    def test_load_config_no_file(self):
        """Test loading config with no file (environment only)."""
        with patch.dict(os.environ, {
            'DEEPFREEZE_ES_HOSTS': 'https://es.example.com:9200',
            'DEEPFREEZE_ES_USERNAME': 'admin',
            'DEEPFREEZE_ES_PASSWORD': 'secret'
        }):
            config = load_config(None)

            assert config['elasticsearch']['hosts'] == ['https://es.example.com:9200']
            assert config['elasticsearch']['username'] == 'admin'
            assert config['elasticsearch']['password'] == 'secret'


class TestGetElasticsearchConfig:
    """Tests for get_elasticsearch_config function."""

    def test_get_es_config_nested_format(self):
        """Test extracting ES config from nested format."""
        config = {
            'elasticsearch': {
                'client': {
                    'hosts': ['https://localhost:9200'],
                    'username': 'elastic'
                }
            }
        }

        es_config = get_elasticsearch_config(config)
        assert es_config['hosts'] == ['https://localhost:9200']
        assert es_config['username'] == 'elastic'

    def test_get_es_config_flat_format(self):
        """Test extracting ES config from flat format."""
        config = {
            'elasticsearch': {
                'hosts': ['https://localhost:9200'],
                'username': 'elastic'
            }
        }

        es_config = get_elasticsearch_config(config)
        assert es_config['hosts'] == ['https://localhost:9200']
        assert es_config['username'] == 'elastic'

    def test_get_es_config_missing(self):
        """Test that missing ES config raises ActionError."""
        with pytest.raises(ActionError) as exc_info:
            get_elasticsearch_config({})
        assert 'No Elasticsearch configuration found' in str(exc_info.value)


class TestGetLoggingConfig:
    """Tests for get_logging_config function."""

    def test_get_logging_config_with_values(self):
        """Test extracting logging config with values."""
        config = {
            'logging': {
                'loglevel': 'DEBUG',
                'logfile': '/var/log/deepfreeze.log'
            }
        }

        log_config = get_logging_config(config)
        assert log_config['loglevel'] == 'DEBUG'
        assert log_config['logfile'] == '/var/log/deepfreeze.log'

    def test_get_logging_config_defaults(self):
        """Test that logging config applies defaults."""
        config = {}

        log_config = get_logging_config(config)
        assert log_config['loglevel'] == 'INFO'
        assert log_config['logformat'] == 'default'


class TestValidateConfig:
    """Tests for validate_config function."""

    def test_validate_config_with_hosts(self):
        """Test validating config with hosts."""
        config = {
            'elasticsearch': {
                'hosts': ['https://localhost:9200']
            }
        }

        # Should not raise
        validate_config(config)

    def test_validate_config_with_cloud_id(self):
        """Test validating config with cloud_id."""
        config = {
            'elasticsearch': {
                'cloud_id': 'deployment:base64string'
            }
        }

        # Should not raise
        validate_config(config)

    def test_validate_config_missing_connection(self):
        """Test that config without hosts or cloud_id raises error."""
        config = {
            'elasticsearch': {
                'username': 'elastic',
                'password': 'changeme'
            }
        }

        with pytest.raises(ActionError) as exc_info:
            validate_config(config)
        assert 'hosts' in str(exc_info.value) or 'cloud_id' in str(exc_info.value)
