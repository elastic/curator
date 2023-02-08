"""Unit tests for utils"""
from unittest import TestCase
# import pytest
from mock import Mock
# from curator.exceptions import MissingArgument
from curator.indexlist import IndexList
from curator.helpers.utils import chunk_index_list, show_dry_run, to_csv
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

        Should split really long index list (well, really long index names) into 2 chunks
        """
        client = Mock()
        client.info.return_value = {'version': {'number': '8.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_two
        client.cluster.state.return_value = testvars.clu_state_two
        client.indices.stats.return_value = testvars.stats_two
        client.field_stats.return_value = testvars.fieldstats_two
        ilst = IndexList(client)
        assert None is show_dry_run(ilst, 'test_action')

class TestChunkIndexList(TestCase):
    """TestToCSV

    Test helpers.utils.chunk_index_list functionality.
    """
    def test_big_list(self):
        """test_big_list

        Should split really long index list (well, really long index names) into 2 chunks
        """
        indices = []
        for i in range(100, 150):
            indices.append(
                'superlongindexnamebyanystandardyouchoosethisissillyhowbigcanthisgetbeforeitbreaks'
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
