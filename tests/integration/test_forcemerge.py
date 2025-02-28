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
    def test_merge(self):
        count = 1
        idx = 'my_index'
        self.create_index(idx)
        self.add_docs(idx)
        ilo1 = IndexList(self.client)
        ilo1.get_segment_counts()
        assert 3 == ilo1.index_info[idx]['segments']
        args = self.get_runner_args()
        args += [
            '--config',
            self.args['configfile'],
            'forcemerge',
            '--max_num_segments',
            str(count),
            '--delay',
            '0.9',
            '--filter_list',
            '{"filtertype":"pattern","kind":"prefix","value":"my"}',
        ]
        assert 0 == self.run_subprocess(args, logname='TestCLIforceMerge.test_merge')
        ilo2 = IndexList(self.client)
        # This stupid block is only for the benefit of testing
        # It apparently can finish testing before the segments have _reported_
        # as fully merged.
        # This is forcing 3 checks before giving up and reporting the result.
        for _ in range(0, 3):
            self.client.indices.refresh(index=idx)
            ilo2.get_segment_counts()
            if ilo2.index_info[idx]['segments'] == count:
                break
            sleep(1)
        assert count == ilo2.index_info[idx]['segments']
