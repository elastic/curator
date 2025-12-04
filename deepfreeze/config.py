"""
Configuration management for the standalone deepfreeze package.

This module handles loading and validating configuration from YAML files
and environment variables. It matches the curator configuration format
for backward compatibility.
"""

import logging
import os
from typing import Any, Optional

import yaml

from deepfreeze.exceptions import ActionError

# Environment variable prefix for deepfreeze configuration
ENV_PREFIX = "DEEPFREEZE_"

# Mapping of environment variables to config paths
# Format: ENV_VAR_NAME -> (config_section, config_key)
ENV_MAPPING = {
    # Elasticsearch connection
    "DEEPFREEZE_ES_HOSTS": ("elasticsearch", "hosts"),
    "DEEPFREEZE_ES_HOST": ("elasticsearch", "hosts"),  # Singular form
    "DEEPFREEZE_ES_USERNAME": ("elasticsearch", "username"),
    "DEEPFREEZE_ES_PASSWORD": ("elasticsearch", "password"),
    "DEEPFREEZE_ES_API_KEY": ("elasticsearch", "api_key"),
    "DEEPFREEZE_ES_CLOUD_ID": ("elasticsearch", "cloud_id"),
    "DEEPFREEZE_ES_CA_CERTS": ("elasticsearch", "ca_certs"),
    "DEEPFREEZE_ES_CLIENT_CERT": ("elasticsearch", "client_cert"),
    "DEEPFREEZE_ES_CLIENT_KEY": ("elasticsearch", "client_key"),
    "DEEPFREEZE_ES_SSL_NO_VALIDATE": ("elasticsearch", "ssl_no_validate"),
    "DEEPFREEZE_ES_VERIFY_CERTS": ("elasticsearch", "verify_certs"),
    "DEEPFREEZE_ES_TIMEOUT": ("elasticsearch", "request_timeout"),
    # Logging
    "DEEPFREEZE_LOG_LEVEL": ("logging", "loglevel"),
    "DEEPFREEZE_LOG_FILE": ("logging", "logfile"),
    "DEEPFREEZE_LOG_FORMAT": ("logging", "logformat"),
}


def _deep_set(config: dict, path: tuple, value) -> None:
    """
    Set a value in a nested dictionary using a path tuple.

    Args:
        config: The dictionary to modify
        path: Tuple of keys representing the path (e.g., ("elasticsearch", "hosts"))
        value: The value to set
    """
    current = config
    for key in path[:-1]:
        if key not in current:
            current[key] = {}
        current = current[key]
    current[path[-1]] = value


def _parse_env_value(value: str, key: str) -> Any:
    """
    Parse an environment variable value, converting types as needed.

    Args:
        value: The string value from the environment
        key: The config key name (used to determine type)

    Returns:
        The parsed value with appropriate type
    """
    # Boolean conversions
    if key in ("ssl_no_validate", "verify_certs"):
        return value.lower() in ("true", "1", "yes")

    # Integer conversions
    if key in ("request_timeout", "timeout"):
        try:
            return int(value)
        except ValueError:
            return value

    # List conversions (comma-separated)
    if key == "hosts":
        # Support comma-separated hosts
        if "," in value:
            return [h.strip() for h in value.split(",")]
        return [value]

    return value


def load_config(config_path: Optional[str] = None) -> dict:
    """
    Load configuration from a YAML file and environment variables.

    Environment variables take precedence over file configuration.

    The configuration format matches curator for compatibility:

    ```yaml
    elasticsearch:
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

    logging:
      loglevel: INFO
      logfile: /path/to/log
      logformat: default
    ```

    Args:
        config_path: Optional path to YAML configuration file.
                     If not provided, only environment variables are used.

    Returns:
        dict: Merged configuration dictionary

    Raises:
        ActionError: If the file cannot be read or parsed
    """
    loggit = logging.getLogger("deepfreeze.config")
    config = {}

    # Load from file if provided
    if config_path:
        loggit.debug("Loading configuration from: %s", config_path)
        try:
            with open(config_path) as f:
                file_config = yaml.safe_load(f)

            if file_config:
                config = file_config
            else:
                loggit.warning("Configuration file is empty: %s", config_path)

        except FileNotFoundError as e:
            raise ActionError(f"Configuration file not found: {config_path}") from e
        except yaml.YAMLError as e:
            raise ActionError(f"Invalid YAML in configuration file: {e}") from e
        except Exception as e:
            raise ActionError(f"Error reading configuration file: {e}") from e

    # Apply environment variable overrides
    for env_var, config_path_tuple in ENV_MAPPING.items():
        value = os.environ.get(env_var)
        if value is not None:
            loggit.debug("Applying environment override: %s", env_var)
            parsed_value = _parse_env_value(value, config_path_tuple[-1])
            _deep_set(config, config_path_tuple, parsed_value)

    return config


def get_elasticsearch_config(config: dict) -> dict:
    """
    Extract Elasticsearch client configuration from the full config.

    This function handles both nested formats:
    - `elasticsearch.client` (full curator format)
    - `elasticsearch` (simplified format)

    Args:
        config: Full configuration dictionary

    Returns:
        dict: Elasticsearch client configuration

    Raises:
        ActionError: If no Elasticsearch configuration is found
    """
    es_config = config.get("elasticsearch", {})

    # Support both nested (curator format) and flat formats
    if "client" in es_config:
        client_config = es_config["client"]
    else:
        client_config = es_config

    if not client_config:
        raise ActionError(
            "No Elasticsearch configuration found. "
            "Configuration must include 'elasticsearch' section with connection details."
        )

    return client_config


def get_logging_config(config: dict) -> dict:
    """
    Extract logging configuration from the full config.

    Args:
        config: Full configuration dictionary

    Returns:
        dict: Logging configuration with defaults applied
    """
    logging_config = config.get("logging", {})

    # Apply defaults
    defaults = {
        "loglevel": "INFO",
        "logformat": "default",
    }

    for key, default_value in defaults.items():
        if key not in logging_config:
            logging_config[key] = default_value

    return logging_config


def configure_logging(config: dict) -> None:
    """
    Configure logging based on the configuration.

    Args:
        config: Full configuration dictionary (will extract logging section)
    """
    log_config = get_logging_config(config)

    loglevel = log_config.get("loglevel", "INFO").upper()
    logfile = log_config.get("logfile")
    logformat = log_config.get("logformat", "default")

    # Set up logging
    level = getattr(logging, loglevel, logging.INFO)

    # Format strings
    if logformat == "default":
        format_str = "%(asctime)s %(levelname)s %(name)s - %(message)s"
    elif logformat == "json":
        format_str = '{"time": "%(asctime)s", "level": "%(levelname)s", "logger": "%(name)s", "message": "%(message)s"}'
    elif logformat == "logstash":
        format_str = '{"@timestamp": "%(asctime)s", "level": "%(levelname)s", "logger_name": "%(name)s", "message": "%(message)s"}'
    else:
        # Custom format
        format_str = logformat

    # Configure root logger
    handlers = []

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter(format_str))
    handlers.append(console_handler)

    # File handler if specified
    if logfile:
        file_handler = logging.FileHandler(logfile)
        file_handler.setFormatter(logging.Formatter(format_str))
        handlers.append(file_handler)

    # Configure the deepfreeze logger
    logger = logging.getLogger("deepfreeze")
    logger.setLevel(level)
    logger.handlers = handlers

    # Also configure the root logger for any other loggers
    logging.basicConfig(level=level, format=format_str, handlers=handlers)


def validate_config(config: dict) -> None:
    """
    Validate that the configuration has required elements.

    Args:
        config: Configuration dictionary to validate

    Raises:
        ActionError: If required configuration is missing
    """
    es_config = get_elasticsearch_config(config)

    # Must have either hosts or cloud_id
    if "hosts" not in es_config and "cloud_id" not in es_config:
        raise ActionError(
            "Elasticsearch configuration must include either 'hosts' or 'cloud_id'"
        )

    # If hosts is provided, it should be a list or single string
    if "hosts" in es_config:
        hosts = es_config["hosts"]
        if not isinstance(hosts, (list, str)):
            raise ActionError(
                "Elasticsearch 'hosts' must be a list of URLs or a single URL string"
            )
