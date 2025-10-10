"""Unit tests for utils"""

from unittest import TestCase

# import pytest
from unittest.mock import Mock

# from curator.exceptions import MissingArgument
from curator.indexlist import IndexList
from curator.helpers.utils import (
    chunk_index_list,
    show_dry_run,
    to_csv,
    multitarget_fix,
    multitarget_match,
)
from . import testvars

FAKE_FAIL = Exception('Simulated Failure')


class TestShowDryRun(TestCase):
    """TestShowDryRun

    Test helpers.utils.show_dry_run functionality.
    """

    # For now, since it's a pain to capture logging output, this is just a
    # simple code coverage run
    def test_index_list(self):
        """test_index_list

        Should split really long index list (well, really long index names) into
        2 chunks
        """
        client = Mock()
        client.info.return_value = {'version': {'number': '8.0.0'}}
        client.cat.indices.return_value = testvars.state_two
        client.indices.get_settings.return_value = testvars.settings_two
        client.indices.stats.return_value = testvars.stats_two
        client.indices.exists_alias.return_value = False
        client.field_stats.return_value = testvars.fieldstats_two
        ilst = IndexList(client)
        assert None is show_dry_run(ilst, 'test_action')


class TestChunkIndexList(TestCase):
    """TestToCSV

    Test helpers.utils.chunk_index_list functionality.
    """

    def test_big_list(self):
        """test_big_list

        Should split really long index list (well, really long index names) into
        2 chunks
        """
        indices = []
        for i in range(100, 150):
            indices.append(
                (
                    'superlongindexnamebyanystandardyouchoosethis'
                    'issillyhowbigcanthisgetbeforeitbreaks'
                )
                + str(i)
            )
        assert 2 == len(chunk_index_list(indices))

    def test_small_list(self):
        """test_small_list

        Should not split short index list
        """
        assert 1 == len(chunk_index_list(['short', 'list', 'of', 'indices']))


class TestToCSV(TestCase):
    """TestToCSV

    Test helpers.utils.to_csv functionality.
    """

    def test_to_csv_will_return_csv(self):
        """test_to_csv_will_return_csv

        Should return csv version of provided list
        """
        lst = ["a", "b", "c", "d"]
        csv = "a,b,c,d"
        # self.assertEqual(csv, to_csv(lst))
        assert csv == to_csv(lst)

    def test_to_csv_will_return_single(self):
        """test_to_csv_will_return_single

        Should return single string of one-element list, no comma
        """
        lst = ["a"]
        csv = "a"
        # self.assertEqual(csv, to_csv(lst))
        assert csv == to_csv(lst)

    def test_to_csv_will_return_none(self):
        """test_to_csv_will_return_None

        Should return :py:class:`None` if empty list is passed
        """
        lst = []
        # self.assertIsNone(to_csv(lst))
        assert None is to_csv(lst)


class TestMultiTarget(TestCase):
    """TestMultiTarget

    Test helpers.utils.multitarget_* functionality.
    """

    SIMPLE = ['index1', 'index2', 'index3']
    COMPLEX = ['index1', 'index2', 'index3', 'not-index1', 'not-index2', 'not-index3']

    def test_multitarget_fix_no_elements(self):
        """test_multitarget_fix_no_elements

        If there are no elements in the pattern, return a wildcard
        """
        assert '*' == multitarget_fix('')

    def test_multitarget_fix_only_negative(self):
        """test_multitarget_fix_only_negative

        If there are only negative elements in the pattern, return those prepended
        by a wildcard
        """
        assert '*,-index1,-index2' == multitarget_fix('-index1,-index2')

    def test_multitarget_fix_positive_and_negative(self):
        """test_multitarget_fix_positive_and_negative

        If there are both positive and negative elements in the pattern, return the
        original pattern with no added wildcard
        """
        assert 'index1,-index2' == multitarget_fix('index1,-index2')

    def test_multitarget_fix_only_positive(self):
        """test_multitarget_fix_only_positive

        If there are only positive elements in the pattern, return the original
        pattern with no added wildcard
        """
        assert 'index1,index2' == multitarget_fix('index1,index2')

    def test_multitarget_match_no_elements(self):
        """test_multitarget_match_no_elements

        If there are no elements in the pattern, return all indices
        """
        assert self.SIMPLE == multitarget_match('', self.SIMPLE)

    def test_multitarget_match_only_negative(self):
        """test_multitarget_match_only_negative

        Return the indices matching the pattern, which has only negatives
        containing no wildcards
        """
        assert ['index3'] == multitarget_match('-index1,-index2', self.SIMPLE)

    def test_multitarget_match_only_negative_complex(self):
        """test_multitarget_match_only_negative_complex

        Return the indices matching the pattern, which has wildcard negatives
        """
        assert ['index3', 'not-index3'] == multitarget_match('-*1,-*2', self.COMPLEX)

    def test_multitarget_match_positive_and_negative(self):
        """test_multitarget_match_positive_and_negative

        Return the indices matching the pattern, which has both positive and
        negative elements
        """
        assert ['index1'] == multitarget_match('index1,-index2', self.SIMPLE)

    def test_multitarget_match_only_positive(self):
        """test_multitarget_match_only_positive

        Return the indices matching the pattern, which has one positive element
        and no wildcards
        """
        assert ['index2'] == multitarget_match('index2', self.COMPLEX)

    def test_multitarget_match_only_positive_wildcard(self):
        """test_multitarget_match_only_positive_wildcard

        Return the indices matching the pattern, which has one positive element
        that contains a wildcard
        """
        assert ['index2', 'not-index2'] == multitarget_match('*2', self.COMPLEX)
