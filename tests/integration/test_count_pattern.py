"""Test count pattern"""

# pylint: disable=C0115, C0116, invalid-name
import os
from . import CuratorTestCase
from . import testvars

HOST = os.environ.get('TEST_ES_SERVER', 'http://127.0.0.1:9200')

TIEREDROUTING = {'allocation': {'include': {'_tier_preference': 'data_content'}}}

DELETE_COUNT_PATTERN = (
    '---\n'
    'actions:\n'
    '  1:\n'
    '    description: "Delete indices as filtered"\n'
    '    action: delete_indices\n'
    '    options:\n'
    '      continue_if_exception: False\n'
    '      disable_action: False\n'
    '    filters:\n'
    '      - filtertype: count\n'
    '        pattern: {0}\n'
    '        use_age: {1}\n'
    '        source: {2}\n'
    '        timestring: {3}\n'
    '        reverse: {4}\n'
    '        count: {5}\n'
)


class TestCLICountPattern(CuratorTestCase):
    def test_match_proper_indices(self):
        for i in range(1, 4):
            self.create_index(f'a-{i}')
        for i in range(4, 7):
            self.create_index(f'b-{i}')
        for i in range(5, 9):
            self.create_index(f'c-{i}')
        self.create_index('not_a_match')
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(
            self.args['actionfile'],
            DELETE_COUNT_PATTERN.format(
                r'^(a|b|c)-\d$', 'false', 'name', '\'%Y.%m.%d\'', 'true', 1
            ),
        )
        self.invoke_runner()
        indices = sorted(list(self.client.indices.get(index='*')))
        assert ['a-3', 'b-6', 'c-8', 'not_a_match'] == indices

    def test_match_proper_indices_by_age(self):
        self.create_index('a-2017.10.01')
        self.create_index('a-2017.10.02')
        self.create_index('a-2017.10.03')
        self.create_index('b-2017.09.01')
        self.create_index('b-2017.09.02')
        self.create_index('b-2017.09.03')
        self.create_index('not_a_match')
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(
            self.args['actionfile'],
            DELETE_COUNT_PATTERN.format(
                r'^(a|b)-\d{4}\.\d{2}\.\d{2}$',
                'true',
                'name',
                '\'%Y.%m.%d\'',
                'true',
                1,
            ),
        )
        self.invoke_runner()
        indices = sorted(list(self.client.indices.get(index='*')))
        assert ['a-2017.10.03', 'b-2017.09.03', 'not_a_match'] == indices

    def test_count_indices_by_age_same_age(self):
        key = 'tag'
        value = 'value'
        alloc = 'include'
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(
            self.args['actionfile'],
            testvars.allocation_count_test.format(key, value, alloc, False),
        )
        idx1, idx2 = ('c-2017.10.03', 'd-2017.10.03')
        idxlist = [
            'a-2017.10.01',
            'a-2017.10.02',
            'a-2017.10.03',
            'b-2017.10.01',
            'b-2017.10.02',
            'b-2017.10.03',
            'c-2017.10.01',
            'c-2017.10.02',
            'd-2017.10.01',
            'd-2017.10.02',
        ]
        for idx in idxlist:
            self.create_index(idx)
        self.create_index(idx1)
        self.create_index(idx2)
        self.invoke_runner()
        response = self.client.indices.get_settings(index='*')
        for idx in (idx1, idx2):
            assert (
                value
                == response[idx]['settings']['index']['routing']['allocation'][alloc][
                    key
                ]
            )
        for idx in idxlist:
            assert TIEREDROUTING == response[idx]['settings']['index']['routing']
