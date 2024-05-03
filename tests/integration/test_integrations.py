"""Test integrations"""
# pylint: disable=missing-function-docstring, missing-class-docstring, line-too-long
import os
import pytest
from curator.exceptions import ConfigurationError
from curator.helpers.getters import get_indices
from curator import IndexList
from . import CuratorTestCase
from . import testvars

HOST = os.environ.get('TEST_ES_SERVER', 'http://127.0.0.1:9200')

class TestFilters(CuratorTestCase):
    def test_filter_by_alias(self):
        alias = 'testalias'
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(self.args['actionfile'], testvars.filter_by_alias.format('testalias', False))
        self.create_index('my_index')
        self.create_index('dummy')
        self.client.indices.put_alias(index='dummy', name=alias)
        self.invoke_runner()
        assert 1 == len(get_indices(self.client))
    def test_filter_by_array_of_aliases(self):
        alias = 'testalias'
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(self.args['actionfile'], testvars.filter_by_alias.format(' [ testalias, foo ]', False))
        self.create_index('my_index')
        self.create_index('dummy')
        self.client.indices.put_alias(index='dummy', name=alias)
        self.invoke_runner()
        assert 2 == len(get_indices(self.client))
    def test_filter_by_alias_bad_aliases(self):
        alias = 'testalias'
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(self.args['actionfile'], testvars.filter_by_alias.format('{"this":"isadict"}', False))
        self.create_index('my_index')
        self.create_index('dummy')
        self.client.indices.put_alias(index='dummy', name=alias)
        self.invoke_runner()
        assert isinstance(self.result.exception, ConfigurationError)
        assert 2 == len(get_indices(self.client))
    def test_field_stats_skips_empty_index(self):
        delete_field_stats = ('---\n'
            'actions:\n'
            '  1:\n'
            '    action: delete_indices\n'
            '    filters:\n'
            '      - filtertype: age\n'
            '        source: field_stats\n'
            '        direction: older\n'
            '        field: "{0}"\n'
            '        unit: days\n'
            '        unit_count: 1\n'
            '        stats_result: min_value\n'
        )
        idx = 'my_index'
        zero = 'zero'
        field = '@timestamp'
        time = '2017-12-31T23:59:59.999Z'
        # Create idx with a single, @timestamped doc
        self.client.create(index=idx, id=1, document={field: time})
        # Flush to ensure it's written
        # Decorators make this pylint exception necessary
        # pylint: disable=E1123
        self.client.indices.flush(index=idx, force=True)
        self.client.indices.refresh(index=idx)
        # Create zero with no docs
        self.create_index(zero)
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(self.args['actionfile'], delete_field_stats.format(field))
        self.invoke_runner()
        # It should skip deleting 'zero', as it has 0 docs
        assert [zero] == get_indices(self.client)

class TestIndexList(CuratorTestCase):
    """Test some of the IndexList particulars using a live ES instance/cluster"""
    IDX1 = 'dummy1'
    IDX2 = 'dummy2'
    IDX3 = 'my_index'

    @pytest.fixture(autouse=True)
    def inject_fixtures(self, caplog):
        # pylint: disable=attribute-defined-outside-init
        self._caplog = caplog
    def test_get_index_stats_with_404(self):
        """Check to ensure that index_stats are being collected if one index is missing"""
        # expected = f'Index was initiallly present, but now is not: {self.IDX2}'
        self.create_index(self.IDX1)
        self.create_index(self.IDX2)
        self.create_index(self.IDX3)
        ilo = IndexList(self.client)
        assert ilo.indices == [self.IDX1, self.IDX2, self.IDX3]
        self.client.indices.delete(index=f'{self.IDX1},{self.IDX2}')
        ilo.get_index_stats()
        # with self._caplog.at_level(logging.WARNING):
        #     ilo.get_index_stats()
        #     # Guarantee we're getting the expected WARNING level message
        #     assert self._caplog.records[-1].message == expected
        assert ilo.indices == [self.IDX3]
    def test_get_index_state(self):
        """Check to ensure that open/close status is properly being recognized"""
        self.create_index(self.IDX1)
        self.create_index(self.IDX2)
        ilo = IndexList(self.client)
        ilo.get_index_state()
        assert ilo.indices == [self.IDX1, self.IDX2]
        assert ilo.index_info[self.IDX1]['state'] == 'open'
        assert ilo.index_info[self.IDX2]['state'] == 'open'
        self.client.indices.close(index=self.IDX2)
        ilo.get_index_state()
        assert ilo.index_info[self.IDX2]['state'] == 'close'
    def test_get_index_state_alias(self):
        """Check to ensure that open/close status catches an alias"""
        alias = {self.IDX2: {}}
        self.create_index(self.IDX1)
        self.create_index(self.IDX2)
        ilo = IndexList(self.client)
        assert ilo.indices == [self.IDX1, self.IDX2]
        self.client.indices.delete(index=self.IDX2)
        self.client.indices.create(index=self.IDX3, aliases=alias)
        ilo.get_index_state()
        assert ilo.indices == [self.IDX1, self.IDX3]
    def test_population_check_missing_index(self):
        """If index_info is missing an index, test to ensure it is populated with the zero value"""
        key = 'docs'
        self.create_index(self.IDX1)
        ilo = IndexList(self.client)
        ilo.population_check(self.IDX1, key)
        assert ilo.index_info[self.IDX1][key] == 0
    def test_population_check_missing_key(self):
        """If index_info is missing an index, test to ensure it is populated with the zero value"""
        key = 'docs'
        self.create_index(self.IDX1)
        ilo = IndexList(self.client)
        ilo.get_index_stats()
        assert ilo.indices == [self.IDX1]
        assert ilo.index_info[self.IDX1][key] == 0
        del ilo.index_info[self.IDX1][key]
        ilo.population_check(self.IDX1, key)
        assert ilo.index_info[self.IDX1][key] == 0
    def test_not_needful(self):
        """Check if get_index_stats can be skipped if already populated
        
        THIS IS LITERALLY FOR CODE COVERAGE, so a ``continue`` line in the function is tested.
        """
        key = 'docs'
        self.create_index(self.IDX1)
        ilo = IndexList(self.client)
        ilo.get_index_stats()
        ilo.get_index_stats() # This time index_info is already populated and it will skip
        assert ilo.index_info[self.IDX1][key] == 0
    def test_search_pattern_1(self):
        """Check to see if providing a search_pattern limits the index list"""
        pattern = 'd*'
        self.create_index(self.IDX1)
        self.create_index(self.IDX2)
        self.create_index(self.IDX3)
        ilo1 = IndexList(self.client)
        ilo2 = IndexList(self.client, search_pattern=pattern)
        assert ilo1.indices == [self.IDX1, self.IDX2, self.IDX3]
        assert ilo2.indices == [self.IDX1, self.IDX2]
