"""Test the deepfreee utility function get_settings"""

# pylint: disable=missing-function-docstring, redefined-outer-name, pointless-statement, missing-class-docstring, protected-access, attribute-defined-outside-init
from unittest.mock import Mock

import pytest
from elasticsearch8.exceptions import NotFoundError  # Adjust import paths as needed

from curator.actions.deepfreeze import Settings, get_settings

# Constants used in the function (mock their values)
STATUS_INDEX = "status_index"
SETTINGS_ID = "settings_id"


@pytest.fixture
def mock_client():
    """Fixture to provide a mock client object."""
    return Mock()


def test_get_settings_success(mock_client):
    """Test when client.get successfully returns a settings document."""
    mock_response = {"_source": {"key": "value"}}  # Example settings data
    mock_client.get.return_value = mock_response

    result = get_settings(mock_client)

    assert isinstance(result, Settings)
    assert result == Settings()  # Assuming Settings stores data in `data` attribute


def test_get_settings_not_found(mock_client):
    """Test when client.get raises NotFoundError and function returns None."""
    mock_client.get.side_effect = NotFoundError(
        404, "Not Found Error", "Document not found"
    )

    result = get_settings(mock_client)

    assert result is None


def test_get_settings_unexpected_exception(mock_client):
    """Test when an unexpected exception is raised (ensures no silent failures)."""
    mock_client.get.side_effect = ValueError("Unexpected error")

    with pytest.raises(ValueError, match="Unexpected error"):
        get_settings(mock_client)
