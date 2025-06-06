"""Test integrations"""

# pylint: disable=C0115, C0116, invalid-name
import os
import warnings
import pytest
from elasticsearch8 import Elasticsearch
from elasticsearch8.exceptions import ElasticsearchWarning, NotFoundError
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
        self.write_config(
            self.args['actionfile'], testvars.filter_by_alias.format('testalias', False)
        )
        self.create_index('my_index')
        self.create_index('dummy')
        self.client.indices.put_alias(index='dummy', name=alias)
        self.invoke_runner()
        assert 1 == len(get_indices(self.client))

    def test_filter_by_array_of_aliases(self):
        alias = 'testalias'
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(
            self.args['actionfile'],
            testvars.filter_by_alias.format(' [ testalias, foo ]', False),
        )
        self.create_index('my_index')
        self.create_index('dummy')
        self.client.indices.put_alias(index='dummy', name=alias)
        self.invoke_runner()
        assert 2 == len(get_indices(self.client))

    def test_filter_by_alias_bad_aliases(self):
        alias = 'testalias'
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(
            self.args['actionfile'],
            testvars.filter_by_alias.format('{"this":"isadict"}', False),
        )
        self.create_index('my_index')
        self.create_index('dummy')
        self.client.indices.put_alias(index='dummy', name=alias)
        self.invoke_runner()
        assert isinstance(self.result.exception, ConfigurationError)
        assert 2 == len(get_indices(self.client))

    def test_filter_closed(self):
        idx1 = 'dummy'
        idx2 = 'my_index'
        ptrn = f'{idx1}*,{idx2}*'
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(
            self.args['actionfile'], testvars.filter_closed.format("True")
        )
        self.create_index(idx1)
        self.create_index(idx2)  # This one will be closed, ergo not deleted
        self.client.indices.close(index=idx2)
        self.invoke_runner()
        assert 1 == len(get_indices(self.client))
        result = self.client.indices.get(index=ptrn, expand_wildcards='open,closed')
        assert idx2 == list(dict(result).keys())[0]

    def test_field_stats_skips_empty_index(self):
        delete_field_stats = (
            '---\n'
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
        """
        Check to ensure that index_stats are being collected if one index is missing
        """
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
        # ElasticsearchWarning: the default value for the wait_for_active_shards
        # parameter will change from '0' to 'index-setting' in version 8;
        # specify 'wait_for_active_shards=index-setting' to adopt the future default
        # behaviour, or 'wait_for_active_shards=0' to preserve today's behaviour
        warnings.filterwarnings("ignore", category=ElasticsearchWarning)
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
        """
        If index_info is missing an index, test to ensure it is populated with
        the zero value
        """
        key = 'docs'
        self.create_index(self.IDX1)
        ilo = IndexList(self.client)
        ilo.population_check(self.IDX1, key)
        assert ilo.index_info[self.IDX1][key] == 0

    def test_population_check_missing_key(self):
        """
        If index_info is missing a key, test to ensure it is populated with a
        zero value
        """
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
        """
        Check if get_index_stats can be skipped if already populated

        THIS IS LITERALLY FOR CODE COVERAGE, so a ``continue`` line in the
        function is tested.
        """
        key = 'docs'
        self.create_index(self.IDX1)
        ilo = IndexList(self.client)
        ilo.get_index_stats()
        ilo.get_index_stats()  # index_info is already populated and it will skip
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


def cleanup(client, idx):
    """Cleanup indices"""
    try:
        client.indices.delete(index=idx)
    except NotFoundError:
        pass


@pytest.mark.parametrize(
    'idx, pattern, hidden, response',
    [
        ('my_index', 'my_*', True, ['my_index']),
        ('my_index', 'my_*', False, []),
        ('my_index', '*_index', True, ['my_index']),
        ('my_index', '*_index', False, []),
        ('.my_index', '.my_*', True, ['.my_index']),
        # ('.my_index', '.my_*', False, []),  # This is a weird behavior or a bug.
        # Check in test__bug__include_hidden_dot_prefix_index
        ('.my_index', '*_index', True, ['.my_index']),
        ('.my_index', '*_index', False, []),
    ],
)
def test_include_hidden_index(idx, pattern, hidden, response):
    """Test that a hidden index is included when include_hidden is True"""
    host = os.environ.get('TEST_ES_SERVER', 'http://127.0.0.1:9200')
    client = Elasticsearch(hosts=host, request_timeout=300)
    warnings.filterwarnings("ignore", category=ElasticsearchWarning)
    cleanup(client, idx)
    client.indices.create(index=idx)
    client.indices.put_settings(index=idx, body={'index': {'hidden': True}})
    ilo = IndexList(client, search_pattern=pattern, include_hidden=hidden)
    assert ilo.indices == response
    # Manual teardown because hidden indices are not returned by get_indices
    del ilo
    cleanup(client, idx)


@pytest.mark.parametrize(
    'idx, pattern, hidden, response',
    [
        ('.my_index', '.my_*', False, ['.my_index']),
        ('.your_index', '.your_*', False, ['.your_index']),
    ],
)
def test__bug__include_hidden_dot_prefix_index(idx, pattern, hidden, response):
    """
    Test that a hidden index is included when include_hidden is True when the
    multi-target search pattern starts with a leading dot and includes a wildcard,
    e.g. '.my_*' or '.your_*'

    This is a bug, and the test is to confirm that behavior until it's fixed.

    https://github.com/elastic/elasticsearch/issues/124167
    """
    host = os.environ.get('TEST_ES_SERVER', 'http://127.0.0.1:9200')
    client = Elasticsearch(hosts=host, request_timeout=300)
    warnings.filterwarnings("ignore", category=ElasticsearchWarning)
    cleanup(client, idx)
    client.indices.create(index=idx)
    client.indices.put_settings(index=idx, body={'index': {'hidden': True}})
    hidden_test = client.indices.get_settings(
        index=idx, filter_path=f'\\{idx}.settings.index.hidden', expand_wildcards='all'
    )
    # We're confirming that our index that starts with a leading dot is hidden
    assert hidden_test[idx]['settings']['index']['hidden'] == 'true'
    ilo = IndexList(client, search_pattern=pattern, include_hidden=hidden)
    # And now we assert that the index is still included in the list
    # WHICH SHOULD NOT HAPPEN -- THIS IS A BUG
    assert ilo.indices == response
    # Manual teardown because hidden indices are not returned by get_indices
    del ilo
    cleanup(client, idx)
