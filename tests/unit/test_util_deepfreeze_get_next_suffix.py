"""Unit tests for the get_next_suffix function in the deepfreeze module."""

# pylint: disable=missing-function-docstring, redefined-outer-name, pointless-statement, missing-class-docstring, protected-access, attribute-defined-outside-init

from datetime import datetime
from unittest.mock import patch

import pytest

from curator.actions.deepfreeze import get_next_suffix


def test_get_next_suffix_oneup():
    """Test for the 'oneup' style, ensuring the suffix is incremented and zero-padded."""
    style = "oneup"
    last_suffix = "001234"
    year = None  # Not needed for "oneup" style
    month = None  # Not needed for "oneup" style

    result = get_next_suffix(style, last_suffix, year, month)

    assert result == "001235"  # Last suffix incremented by 1, zero-padded to 6 digits


def test_get_next_suffix_year_month():
    """Test for other styles where year and month are returned."""
    style = "date"
    last_suffix = "001234"  # Not used for this style
    year = 2025
    month = 5

    result = get_next_suffix(style, last_suffix, year, month)

    assert result == "2025.05"  # Formatted as YYYY.MM


def test_get_next_suffix_missing_year_month():
    """Test when year and month are not provided, defaults to current year and month."""
    style = "date"
    last_suffix = "001234"  # Not used for this style
    year = None
    month = None

    result = get_next_suffix(style, last_suffix, 2025, 1)

    assert result == "2025.01"  # Default to current year and month (January 2025)


def test_get_next_suffix_invalid_style():
    """Test when an invalid style is passed."""
    style = "invalid_style"
    last_suffix = "001234"  # Not used for this style
    year = 2025
    month = 5

    with pytest.raises(ValueError, match="Invalid style"):
        get_next_suffix(style, last_suffix, year, month)
