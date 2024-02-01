"""Unit testing for helpers.creators functions"""
from unittest import TestCase
import pytest
from curator.defaults.settings import CURATOR_DOCS, footer
from curator.exceptions import CuratorException
from curator._version import __version__

class TestFooter(TestCase):
    """Test defaults.settings.footer functionality."""
    mytail = 'tail.html'
    ver = __version__.split('.')
    majmin = f'{ver[0]}.{ver[1]}'
    def test_basic_functionality(self):
        """Should return a URL with the major/minor version in the path"""
        expected = f'Learn more at {CURATOR_DOCS}/{self.majmin}/{self.mytail}'
        assert expected == footer(__version__, tail=self.mytail)
    def test_raises_with_nonstring(self):
        """Should raise an exception if a non-string value is passed as version"""
        with pytest.raises(CuratorException):
            footer(1234)
    def test_raises_with_unsplittable_string(self):
        """Should raise an exception if a non-period delimited string value is passed as version"""
        with pytest.raises(CuratorException):
            footer('invalid')
