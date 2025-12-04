"""
Tests for ES client wrapper (Task Group 5)

These tests verify that:
1. ES client can be created with username/password authentication
2. ES client can be created with API key authentication
3. ES client can be created with cloud_id
4. ES client handles connection validation
5. YAML configuration loading works
"""

import pytest
import tempfile
import os
from unittest.mock import MagicMock, patch, PropertyMock


class TestCreateEsClient:
    """Tests for create_es_client function"""

    @patch("deepfreeze.esclient.Elasticsearch")
    def test_create_with_username_password(self, mock_es_class):
        """Test creating client with username/password authentication"""
        from deepfreeze.esclient import create_es_client

        # Mock successful connection
        mock_client = MagicMock()
        mock_client.cluster.health.return_value = {
            "cluster_name": "test-cluster",
            "status": "green",
        }
        mock_es_class.return_value = mock_client

        client = create_es_client(
            hosts=["https://localhost:9200"],
            username="elastic",
            password="changeme",
        )

        assert client is not None
        mock_es_class.assert_called_once()
        call_kwargs = mock_es_class.call_args[1]
        assert call_kwargs["hosts"] == ["https://localhost:9200"]
        assert call_kwargs["basic_auth"] == ("elastic", "changeme")
        mock_client.cluster.health.assert_called_once()

    @patch("deepfreeze.esclient.Elasticsearch")
    def test_create_with_api_key(self, mock_es_class):
        """Test creating client with API key authentication"""
        from deepfreeze.esclient import create_es_client

        mock_client = MagicMock()
        mock_client.cluster.health.return_value = {
            "cluster_name": "test-cluster",
            "status": "green",
        }
        mock_es_class.return_value = mock_client

        client = create_es_client(
            hosts=["https://localhost:9200"],
            api_key="base64-encoded-key",
        )

        assert client is not None
        call_kwargs = mock_es_class.call_args[1]
        assert call_kwargs["api_key"] == "base64-encoded-key"

    @patch("deepfreeze.esclient.Elasticsearch")
    def test_create_with_cloud_id(self, mock_es_class):
        """Test creating client with Elastic Cloud ID"""
        from deepfreeze.esclient import create_es_client

        mock_client = MagicMock()
        mock_client.cluster.health.return_value = {
            "cluster_name": "cloud-cluster",
            "status": "green",
        }
        mock_es_class.return_value = mock_client

        client = create_es_client(
            cloud_id="deployment:base64string",
            api_key="cloud-api-key",
        )

        assert client is not None
        call_kwargs = mock_es_class.call_args[1]
        assert call_kwargs["cloud_id"] == "deployment:base64string"

    @patch("deepfreeze.esclient.Elasticsearch")
    def test_create_with_certificates(self, mock_es_class):
        """Test creating client with SSL certificates"""
        from deepfreeze.esclient import create_es_client

        mock_client = MagicMock()
        mock_client.cluster.health.return_value = {
            "cluster_name": "cert-cluster",
            "status": "green",
        }
        mock_es_class.return_value = mock_client

        client = create_es_client(
            hosts=["https://localhost:9200"],
            ca_certs="/path/to/ca.crt",
            client_cert="/path/to/client.crt",
            client_key="/path/to/client.key",
        )

        assert client is not None
        call_kwargs = mock_es_class.call_args[1]
        assert call_kwargs["ca_certs"] == "/path/to/ca.crt"
        assert call_kwargs["client_cert"] == "/path/to/client.crt"
        assert call_kwargs["client_key"] == "/path/to/client.key"

    def test_create_missing_hosts_and_cloud_id(self):
        """Test that ValueError is raised when neither hosts nor cloud_id provided"""
        from deepfreeze.esclient import create_es_client

        with pytest.raises(ValueError) as exc_info:
            create_es_client(username="user", password="pass")

        assert "hosts" in str(exc_info.value) or "cloud_id" in str(exc_info.value)


class TestConnectionValidation:
    """Tests for connection validation"""

    @patch("deepfreeze.esclient.Elasticsearch")
    def test_connection_validation_success(self, mock_es_class):
        """Test that connection validation calls cluster health"""
        from deepfreeze.esclient import create_es_client

        mock_client = MagicMock()
        mock_client.cluster.health.return_value = {
            "cluster_name": "validation-cluster",
            "status": "green",
        }
        mock_es_class.return_value = mock_client

        client = create_es_client(
            hosts=["https://localhost:9200"],
            username="elastic",
            password="changeme",
        )

        mock_client.cluster.health.assert_called_with(timeout="5s")

    @patch("deepfreeze.esclient.Elasticsearch")
    def test_authentication_failure(self, mock_es_class):
        """Test that authentication failure raises ActionError"""
        from deepfreeze.esclient import create_es_client
        from deepfreeze.exceptions import ActionError

        # Create a mock AuthenticationException that behaves properly
        mock_auth_exception = MagicMock()
        mock_auth_exception.__str__ = MagicMock(return_value="Authentication failed")

        # Create the actual exception class behavior
        from elasticsearch8.exceptions import AuthenticationException

        # Create a mock that properly simulates the exception
        mock_client = MagicMock()

        # Use a simple Exception subclass that behaves predictably
        class MockAuthException(Exception):
            pass

        # Patch the AuthenticationException check
        mock_client.cluster.health.side_effect = MockAuthException("auth failed")

        mock_es_class.return_value = mock_client

        # The exception should be caught and re-raised as ActionError
        with pytest.raises(ActionError):
            create_es_client(
                hosts=["https://localhost:9200"],
                username="wrong",
                password="credentials",
            )

    @patch("deepfreeze.esclient.Elasticsearch")
    def test_generic_exception_raises_action_error(self, mock_es_class):
        """Test that generic exceptions are converted to ActionError"""
        from deepfreeze.esclient import create_es_client
        from deepfreeze.exceptions import ActionError

        mock_client = MagicMock()
        mock_client.cluster.health.side_effect = Exception("Something went wrong")
        mock_es_class.return_value = mock_client

        with pytest.raises(ActionError) as exc_info:
            create_es_client(
                hosts=["https://localhost:9200"],
            )

        assert "Something went wrong" in str(exc_info.value)


class TestYamlConfigLoading:
    """Tests for YAML configuration loading"""

    def test_load_config_from_yaml(self):
        """Test loading configuration from YAML file"""
        from deepfreeze.esclient import load_config_from_yaml

        yaml_content = """
elasticsearch:
  client:
    hosts:
      - https://localhost:9200
    username: elastic
    password: changeme
    request_timeout: 30

logging:
  loglevel: INFO
"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as f:
            f.write(yaml_content)
            config_path = f.name

        try:
            config = load_config_from_yaml(config_path)

            assert "elasticsearch" in config
            assert "client" in config["elasticsearch"]
            assert config["elasticsearch"]["client"]["username"] == "elastic"
            assert config["elasticsearch"]["client"]["hosts"][0] == "https://localhost:9200"
        finally:
            os.unlink(config_path)

    def test_load_config_file_not_found(self):
        """Test that missing file raises ActionError"""
        from deepfreeze.esclient import load_config_from_yaml
        from deepfreeze.exceptions import ActionError

        with pytest.raises(ActionError) as exc_info:
            load_config_from_yaml("/nonexistent/path/config.yml")

        assert "not found" in str(exc_info.value)

    def test_load_config_invalid_yaml(self):
        """Test that invalid YAML raises ActionError"""
        from deepfreeze.esclient import load_config_from_yaml
        from deepfreeze.exceptions import ActionError

        with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as f:
            f.write("invalid: yaml: content: [")
            config_path = f.name

        try:
            with pytest.raises(ActionError) as exc_info:
                load_config_from_yaml(config_path)

            assert "Invalid YAML" in str(exc_info.value)
        finally:
            os.unlink(config_path)

    @patch("deepfreeze.esclient.Elasticsearch")
    def test_create_client_from_config(self, mock_es_class):
        """Test creating client directly from config file"""
        from deepfreeze.esclient import create_es_client_from_config

        mock_client = MagicMock()
        mock_client.cluster.health.return_value = {
            "cluster_name": "config-cluster",
            "status": "green",
        }
        mock_es_class.return_value = mock_client

        yaml_content = """
elasticsearch:
  client:
    hosts:
      - https://localhost:9200
    username: config-user
    password: config-pass
"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as f:
            f.write(yaml_content)
            config_path = f.name

        try:
            client = create_es_client_from_config(config_path)

            assert client is not None
            call_kwargs = mock_es_class.call_args[1]
            assert call_kwargs["basic_auth"] == ("config-user", "config-pass")
        finally:
            os.unlink(config_path)


class TestValidateConnection:
    """Tests for validate_connection function"""

    def test_validate_connection_returns_cluster_info(self):
        """Test that validate_connection returns cluster information"""
        from deepfreeze.esclient import validate_connection

        mock_client = MagicMock()
        mock_client.cluster.health.return_value = {
            "cluster_name": "info-cluster",
            "status": "yellow",
            "number_of_nodes": 3,
        }
        mock_client.info.return_value = {
            "version": {"number": "8.10.0"},
        }

        result = validate_connection(mock_client)

        assert result["cluster_name"] == "info-cluster"
        assert result["status"] == "yellow"
        assert result["number_of_nodes"] == 3
        assert result["version"] == "8.10.0"


class TestESClientWrapper:
    """Tests for ESClientWrapper class"""

    @patch("deepfreeze.esclient.Elasticsearch")
    def test_wrapper_init_with_params(self, mock_es_class):
        """Test ESClientWrapper initialization with direct parameters"""
        from deepfreeze.esclient import ESClientWrapper

        mock_client = MagicMock()
        mock_client.cluster.health.return_value = {
            "cluster_name": "wrapper-cluster",
            "status": "green",
            "number_of_nodes": 1,
        }
        mock_client.info.return_value = {
            "version": {"number": "8.11.0"},
        }
        mock_es_class.return_value = mock_client

        wrapper = ESClientWrapper(
            hosts=["https://localhost:9200"],
            username="wrapper-user",
            password="wrapper-pass",
        )

        assert wrapper.client is not None
        assert wrapper.cluster_name == "wrapper-cluster"
        assert wrapper.cluster_status == "green"
        assert wrapper.version == "8.11.0"

    @patch("deepfreeze.esclient.Elasticsearch")
    def test_wrapper_is_healthy(self, mock_es_class):
        """Test ESClientWrapper.is_healthy() method"""
        from deepfreeze.esclient import ESClientWrapper

        mock_client = MagicMock()
        mock_client.cluster.health.return_value = {
            "cluster_name": "health-cluster",
            "status": "yellow",
            "number_of_nodes": 2,
        }
        mock_client.info.return_value = {
            "version": {"number": "8.10.0"},
        }
        mock_es_class.return_value = mock_client

        wrapper = ESClientWrapper(
            hosts=["https://localhost:9200"],
        )

        assert wrapper.is_healthy() is True

    def test_wrapper_requires_config_or_params(self):
        """Test that ESClientWrapper raises error without config or params"""
        from deepfreeze.esclient import ESClientWrapper

        with pytest.raises(ValueError) as exc_info:
            ESClientWrapper()

        assert "config_path" in str(exc_info.value) or "parameters" in str(exc_info.value)
