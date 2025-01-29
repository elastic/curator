from unittest.mock import Mock

import pytest
from elasticsearch8.exceptions import NotFoundError

from curator.actions.deepfreeze import save_settings

# pylint: disable=missing-function-docstring, redefined-outer-name, pointless-statement, missing-class-docstring, protected-access, attribute-defined-outside-init


# Constants used in the function (mock their values)
STATUS_INDEX = "deepfreeze-status"
SETTINGS_ID = "101"


class MockSettings:
    """Mock representation of a Settings object."""

    def __init__(self, data):
        self.__dict__ = data


@pytest.fixture
def mock_client():
    """Fixture to provide a mock client object."""
    return Mock()


@pytest.fixture
def mock_settings():
    """Fixture to provide a mock settings object."""
    return MockSettings({"key": "value"})


def test_save_settings_updates_existing(mock_client, mock_settings):
    """Test when settings already exist, they should be updated."""
    mock_client.get.return_value = {"_source": {"key": "old_value"}}

    save_settings(mock_client, mock_settings)

    mock_client.update.assert_called_once_with(
        index=STATUS_INDEX, id=SETTINGS_ID, doc=mock_settings.__dict__
    )
    mock_client.create.assert_not_called()


def test_save_settings_creates_new(mock_client, mock_settings):
    """Test when settings do not exist, they should be created."""
    mock_client.get.side_effect = NotFoundError(
        404, "Not Found Error", "Document not found"
    )

    save_settings(mock_client, mock_settings)

    mock_client.create.assert_called_once_with(
        index=STATUS_INDEX, id=SETTINGS_ID, document=mock_settings.__dict__
    )
    mock_client.update.assert_not_called()


def test_save_settings_unexpected_exception(mock_client, mock_settings):
    """Test that unexpected exceptions propagate properly."""
    mock_client.get.side_effect = ValueError("Unexpected error")

    with pytest.raises(ValueError, match="Unexpected error"):
        save_settings(mock_client, mock_settings)
