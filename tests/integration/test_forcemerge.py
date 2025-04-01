"""Test forcemerge functionality"""

# pylint: disable=C0115, C0116, invalid-name, protected-access
import os
from time import sleep
from curator import IndexList
from . import CuratorTestCase
from . import testvars

HOST = os.environ.get('TEST_ES_SERVER', 'http://127.0.0.1:9200')


class TestActionFileforceMerge(CuratorTestCase):
    def test_merge(self):
        count = 1
        idx = 'my_index'
        self.create_index(idx)
        self.add_docs(idx)
        ilo1 = IndexList(self.client)
        ilo1.get_segment_counts()
        assert 3 == ilo1.index_info[idx]['segments']
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(
            self.args['actionfile'], testvars.forcemerge_test.format(count, 0.9)
        )
        self.invoke_runner()
        ilo2 = IndexList(self.client)
        # This stupid block is only because it can finish testing before
        # the segments have _reported_ as fully merged. This is forcing
        # 3 checks before giving up and reporting the result.
        for _ in range(0, 3):
            self.client.indices.refresh(index=idx)
            ilo2.get_segment_counts()
            if ilo2.index_info[idx]['segments'] == count:
                break
            sleep(1)
        assert count == ilo2.index_info[idx]['segments']

    def test_extra_option(self):
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(
            self.args['actionfile'], testvars.bad_option_proto_test.format('forcemerge')
        )
        self.invoke_runner()
        assert 1 == self.result.exit_code


class TestCLIforceMerge(CuratorTestCase):
    COUNT = 1
    IDX = 'my_index'
    ILO = None
    CLI_ARGS = None

    def setup(self):
        self.COUNT = 1
        self.IDX = 'my_index'
        self.create_index(self.IDX)
        self.add_docs(self.IDX)
        self.ILO = IndexList(self.client)
        self.ILO.get_segment_counts()
        assert 3 == self.ILO.index_info[self.IDX]['segments']
        self.CLI_ARGS = self.get_runner_args()
        self.CLI_ARGS += [
            '--config',
            self.args['configfile'],
            'forcemerge',
            '--max_num_segments',
            str(self.COUNT),
            '--delay',
            '0.9',
            '--filter_list',
            '{"filtertype":"pattern","kind":"prefix","value":"my"}',
        ]

    def wait_for_merge(self):
        # This stupid block is only for the benefit of testing
        # It apparently can finish testing before the segments have _reported_
        # as fully merged.
        # This is forcing 3 checks before giving up and reporting the result.
        for _ in range(0, 3):
            self.client.indices.refresh(index=self.IDX)
            self.ILO.get_segment_counts()
            if self.ILO.index_info[self.IDX]['segments'] == self.COUNT:
                break
            sleep(1)

    def test_merge(self):
        self.setup()
        assert 0 == self.run_subprocess(
            self.CLI_ARGS, logname='TestCLIforceMerge.test_merge'
        )
        self.wait_for_merge()
        assert self.COUNT == self.ILO.index_info[self.IDX]['segments']
        self.ILO = None
        self.CLI_ARGS = None

    def test_empty_list1(self):
        """Test with an empty list and ignore_empty_list unset"""
        self.setup()
        self.client.indices.forcemerge(index=self.IDX, max_num_segments=1)
        self.wait_for_merge()
        assert 1 == self.run_subprocess(
            self.CLI_ARGS, logname='TestCLIforceMerge.test_empty_list1'
        )
        self.ILO = None
        self.CLI_ARGS = None

    def test_empty_list2(self):
        """Test with an empty list and ignore_empty_list = True"""
        self.setup()
        self.client.indices.forcemerge(index=self.IDX, max_num_segments=1)
        self.wait_for_merge()
        self.CLI_ARGS = self.get_runner_args()
        self.CLI_ARGS += [
            '--config',
            self.args['configfile'],
            'forcemerge',
            '--max_num_segments',
            str(self.COUNT),
            '--delay',
            '0.9',
            '--ignore_empty_list',
            '--filter_list',
            '{"filtertype":"pattern","kind":"prefix","value":"my"}',
        ]
        assert 0 == self.run_subprocess(
            self.CLI_ARGS, logname='TestCLIforceMerge.test_empty_list2'
        )
        self.ILO = None
        self.CLI_ARGS = None
