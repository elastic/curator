"""
Elasticsearch client wrapper for the standalone deepfreeze package.

This module provides a lightweight wrapper around the elasticsearch8 client
with support for all authentication methods. It does NOT depend on es_client.builder.Builder.
"""

import logging
import ssl
from typing import Optional

import yaml
from elasticsearch8 import Elasticsearch
from elasticsearch8.exceptions import AuthenticationException, ConnectionError as ESConnectionError

from deepfreeze.exceptions import ActionError


def create_es_client(
    hosts: list = None,
    username: str = None,
    password: str = None,
    api_key: str = None,
    cloud_id: str = None,
    ca_certs: str = None,
    client_cert: str = None,
    client_key: str = None,
    verify_certs: bool = True,
    request_timeout: int = 30,
    **kwargs,
) -> Elasticsearch:
    """
    Create an Elasticsearch client with the specified authentication method.

    Supports multiple authentication methods:
    - Username/password: hosts + username + password
    - API key: hosts + api_key (or cloud_id + api_key)
    - Cloud ID: cloud_id + (username/password or api_key)
    - Client certificates: hosts + client_cert + client_key (+ ca_certs)

    Args:
        hosts: List of Elasticsearch host URLs (e.g., ['https://localhost:9200'])
        username: Username for basic authentication
        password: Password for basic authentication
        api_key: API key for authentication (single string or tuple of (id, key))
        cloud_id: Elastic Cloud deployment ID
        ca_certs: Path to CA certificate file for SSL verification
        client_cert: Path to client certificate file for mutual TLS
        client_key: Path to client private key file for mutual TLS
        verify_certs: Whether to verify SSL certificates (default: True)
        request_timeout: Request timeout in seconds (default: 30)
        **kwargs: Additional arguments passed to Elasticsearch client

    Returns:
        Elasticsearch: Configured Elasticsearch client

    Raises:
        ActionError: If authentication fails or connection cannot be established
        ValueError: If required parameters are missing or conflicting
    """
    loggit = logging.getLogger("deepfreeze.esclient")

    # Build client kwargs
    client_kwargs = {
        "request_timeout": request_timeout,
        **kwargs,
    }

    # Determine connection method
    if cloud_id:
        loggit.debug("Using Elastic Cloud connection with cloud_id")
        client_kwargs["cloud_id"] = cloud_id
    elif hosts:
        loggit.debug("Using direct hosts connection: %s", hosts)
        client_kwargs["hosts"] = hosts if isinstance(hosts, list) else [hosts]
    else:
        raise ValueError("Either 'hosts' or 'cloud_id' must be provided")

    # Determine authentication method
    if api_key:
        loggit.debug("Using API key authentication")
        client_kwargs["api_key"] = api_key
    elif username and password:
        loggit.debug("Using basic authentication with username: %s", username)
        client_kwargs["basic_auth"] = (username, password)

    # SSL/TLS configuration
    if ca_certs:
        loggit.debug("Using CA certificate: %s", ca_certs)
        client_kwargs["ca_certs"] = ca_certs

    if client_cert and client_key:
        loggit.debug("Using client certificate: %s", client_cert)
        client_kwargs["client_cert"] = client_cert
        client_kwargs["client_key"] = client_key

    client_kwargs["verify_certs"] = verify_certs

    # Create the client
    try:
        loggit.info("Creating Elasticsearch client")
        client = Elasticsearch(**client_kwargs)

        # Validate connection
        loggit.debug("Validating connection with cluster health check")
        health = client.cluster.health(timeout="5s")
        loggit.info(
            "Connected to Elasticsearch cluster: %s (status: %s)",
            health.get("cluster_name", "unknown"),
            health.get("status", "unknown"),
        )

        return client

    except AuthenticationException as e:
        loggit.error("Authentication failed: %s", e)
        raise ActionError(
            f"Elasticsearch authentication failed. Check your credentials. Error: {e}"
        )
    except ESConnectionError as e:
        loggit.error("Connection failed: %s", e)
        raise ActionError(
            f"Could not connect to Elasticsearch. Check your host configuration. Error: {e}"
        )
    except Exception as e:
        loggit.error("Failed to create Elasticsearch client: %s", e, exc_info=True)
        raise ActionError(f"Failed to create Elasticsearch client: {e}")


def load_config_from_yaml(config_path: str) -> dict:
    """
    Load configuration from a YAML file.

    The YAML file should match the curator configuration format for compatibility:

    ```yaml
    elasticsearch:
      client:
        hosts:
          - https://localhost:9200
        username: elastic
        password: changeme
        # Or for API key:
        # api_key: "base64-encoded-api-key"
        # Or for Cloud:
        # cloud_id: "deployment:base64string"
        ca_certs: /path/to/ca.crt
        client_cert: /path/to/client.crt
        client_key: /path/to/client.key
        request_timeout: 30
      other_settings:
        # Curator-specific settings (optional)

    logging:
      loglevel: INFO
      logfile: /path/to/log
      logformat: default
    ```

    Args:
        config_path: Path to the YAML configuration file

    Returns:
        dict: Parsed configuration dictionary

    Raises:
        ActionError: If the file cannot be read or parsed
    """
    loggit = logging.getLogger("deepfreeze.esclient")
    loggit.debug("Loading configuration from: %s", config_path)

    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)

        if not config:
            raise ActionError(f"Configuration file is empty: {config_path}")

        return config

    except FileNotFoundError:
        raise ActionError(f"Configuration file not found: {config_path}")
    except yaml.YAMLError as e:
        raise ActionError(f"Invalid YAML in configuration file: {e}")
    except Exception as e:
        raise ActionError(f"Error reading configuration file: {e}")


def create_es_client_from_config(config_path: str) -> Elasticsearch:
    """
    Create an Elasticsearch client from a YAML configuration file.

    This function reads the configuration file and extracts the elasticsearch
    client settings to create a connected client.

    Args:
        config_path: Path to the YAML configuration file

    Returns:
        Elasticsearch: Configured and connected Elasticsearch client

    Raises:
        ActionError: If configuration is invalid or connection fails
    """
    loggit = logging.getLogger("deepfreeze.esclient")

    config = load_config_from_yaml(config_path)

    # Extract elasticsearch client configuration
    es_config = config.get("elasticsearch", {})
    client_config = es_config.get("client", es_config)  # Support both nested and flat

    if not client_config:
        raise ActionError(
            "No Elasticsearch client configuration found. "
            "Configuration must include 'elasticsearch.client' section."
        )

    # Map configuration to client parameters
    client_params = {}

    # Hosts configuration
    if "hosts" in client_config:
        hosts = client_config["hosts"]
        client_params["hosts"] = hosts if isinstance(hosts, list) else [hosts]

    # Cloud ID
    if "cloud_id" in client_config:
        client_params["cloud_id"] = client_config["cloud_id"]

    # Authentication - username/password
    if "username" in client_config:
        client_params["username"] = client_config["username"]
    if "password" in client_config:
        client_params["password"] = client_config["password"]

    # Authentication - API key
    if "api_key" in client_config:
        client_params["api_key"] = client_config["api_key"]

    # SSL/TLS certificates
    if "ca_certs" in client_config:
        client_params["ca_certs"] = client_config["ca_certs"]
    if "client_cert" in client_config:
        client_params["client_cert"] = client_config["client_cert"]
    if "client_key" in client_config:
        client_params["client_key"] = client_config["client_key"]

    # SSL verification
    if "ssl_no_validate" in client_config:
        client_params["verify_certs"] = not client_config["ssl_no_validate"]
    if "verify_certs" in client_config:
        client_params["verify_certs"] = client_config["verify_certs"]

    # Timeout
    if "request_timeout" in client_config:
        client_params["request_timeout"] = client_config["request_timeout"]
    if "timeout" in client_config:
        client_params["request_timeout"] = client_config["timeout"]

    loggit.debug("Client parameters extracted from config: %s",
                 {k: v for k, v in client_params.items() if k not in ["password", "api_key"]})

    return create_es_client(**client_params)


def validate_connection(client: Elasticsearch) -> dict:
    """
    Validate an Elasticsearch connection and return cluster information.

    Args:
        client: Elasticsearch client to validate

    Returns:
        dict: Cluster health and info including:
            - cluster_name: Name of the cluster
            - status: Health status (green, yellow, red)
            - version: Elasticsearch version
            - number_of_nodes: Number of nodes in the cluster

    Raises:
        ActionError: If the connection is invalid or cluster is unreachable
    """
    loggit = logging.getLogger("deepfreeze.esclient")

    try:
        # Get cluster health
        health = client.cluster.health(timeout="10s")

        # Get cluster info
        info = client.info()

        result = {
            "cluster_name": health.get("cluster_name"),
            "status": health.get("status"),
            "number_of_nodes": health.get("number_of_nodes"),
            "version": info.get("version", {}).get("number"),
        }

        loggit.info(
            "Connection validated - Cluster: %s, Status: %s, Version: %s, Nodes: %d",
            result["cluster_name"],
            result["status"],
            result["version"],
            result["number_of_nodes"],
        )

        return result

    except Exception as e:
        loggit.error("Connection validation failed: %s", e)
        raise ActionError(f"Elasticsearch connection validation failed: {e}")


class ESClientWrapper:
    """
    A wrapper class for Elasticsearch client that provides connection management
    and convenience methods for deepfreeze operations.

    This is an optional wrapper that can be used for stateful client management.
    For simple use cases, the create_es_client() function can be used directly.

    Attributes:
        client: The underlying Elasticsearch client
        cluster_name: Name of the connected cluster
        cluster_status: Health status of the cluster
    """

    def __init__(
        self,
        config_path: str = None,
        **kwargs,
    ):
        """
        Initialize the ES client wrapper.

        Args:
            config_path: Path to YAML configuration file (optional)
            **kwargs: Direct client parameters (hosts, username, password, etc.)
                      These override config file values if both are provided.
        """
        self.loggit = logging.getLogger("deepfreeze.esclient.ESClientWrapper")
        self._client = None
        self._cluster_info = None

        if config_path:
            # Load base config from file
            config = load_config_from_yaml(config_path)
            es_config = config.get("elasticsearch", {})
            client_config = es_config.get("client", es_config)

            # Merge with any overrides
            merged_config = {**client_config, **kwargs}

            self._client = create_es_client(**self._map_config(merged_config))
        elif kwargs:
            self._client = create_es_client(**kwargs)
        else:
            raise ValueError(
                "Either 'config_path' or connection parameters must be provided"
            )

        # Validate and cache cluster info
        self._cluster_info = validate_connection(self._client)

    def _map_config(self, config: dict) -> dict:
        """Map configuration dict to create_es_client parameters."""
        params = {}

        # Direct mappings
        for key in ["hosts", "username", "password", "api_key", "cloud_id",
                    "ca_certs", "client_cert", "client_key", "verify_certs",
                    "request_timeout"]:
            if key in config:
                params[key] = config[key]

        # Handle alternative names
        if "timeout" in config and "request_timeout" not in params:
            params["request_timeout"] = config["timeout"]
        if "ssl_no_validate" in config and "verify_certs" not in params:
            params["verify_certs"] = not config["ssl_no_validate"]

        return params

    @property
    def client(self) -> Elasticsearch:
        """Get the underlying Elasticsearch client."""
        return self._client

    @property
    def cluster_name(self) -> str:
        """Get the cluster name."""
        return self._cluster_info.get("cluster_name")

    @property
    def cluster_status(self) -> str:
        """Get the cluster health status."""
        return self._cluster_info.get("status")

    @property
    def version(self) -> str:
        """Get the Elasticsearch version."""
        return self._cluster_info.get("version")

    def refresh_cluster_info(self) -> dict:
        """Refresh cluster information."""
        self._cluster_info = validate_connection(self._client)
        return self._cluster_info

    def is_healthy(self) -> bool:
        """Check if the cluster is healthy (green or yellow status)."""
        try:
            self.refresh_cluster_info()
            return self._cluster_info.get("status") in ["green", "yellow"]
        except Exception:
            return False
