"""
Test cold2frozen functionality

This cannot work with the open-source release, so any testing must be manual
"""

# pylint: disable=missing-function-docstring, missing-class-docstring, line-too-long
# import os
# from time import sleep
# from curator import IndexList
# from curator.actions import Cold2Frozen, Snapshot
# from curator.exceptions import (
#     CuratorException, FailedExecution, SearchableSnapshotException)
# from curator.helpers.getters import get_snapshot
# from . import CuratorTestCase
# from . import testvars

# HOST = os.environ.get('TEST_ES_SERVER', 'http://127.0.0.1:9200')
# '      repository: {0}\n'
# '      - filtertype: {1}\n'
# '        source: {2}\n'
# '        direction: {3}\n'
# '        timestring: {4}\n'
# '        unit: {5}\n'
# '        unit_count: {6}\n'
# '        epoch: {7}\n')

# class TestActionFileCold2Frozen(CuratorTestCase):
#     SNAP = 'cold2frozen'
#     ORIG = 'myindex-000001'
#     ALIAS = 'myindex'
#     AL1 = [{'add': {'index': ORIG, 'alias': ALIAS}}]
#     AL2 = [
#         {'remove': {'index': ORIG, 'alias': ALIAS}},
#         {'add': {'index': f'restored-{ORIG}', 'alias': ALIAS}}
#     ]
#     COLD = {'routing': {'allocation': {'include': {'_tier_preference': 'data_cold'}}}}
#     def test_cold2frozen1(self):
#         ### Create snapshots to delete and verify them
#         self.create_repository()
#         self.add_docs(self.ORIG)
#         # Assign an alias
#         self.client.indices.update_aliases(actions=self.AL1)
#         # Forcemerge the index down
#         self.client.indices.forcemerge(index=self.ORIG, max_num_segments=1)
#         # Get an indexlist object
#         ilo = IndexList(self.client)
#         # Snapshot the single index in it
#         snp = Snapshot(
#             ilo, repository=self.args['repository'], name=self.SNAP,
#             wait_interval=0.5
#         )
#         snp.do_action()
#         # Verify the snapshot
#         snapshot = get_snapshot(self.client, self.args['repository'], self.SNAP)
#         assert 1 == len(snapshot['snapshots'])
#         sleep(2.0)
#         # Mount the index in the snapshot in the cold tier
#         self.client.searchable_snapshots.mount(
#             repository=self.args['repository'], snapshot=self.SNAP, index=self.ORIG,
#             renamed_index=f'restored-{self.ORIG}', index_settings=self.COLD,
#             storage='full_copy')
#         # Update the new index to have the aliases of the original one
#         self.client.indices.update_aliases(actions=self.AL2)
#         # Delete the original index
#         self.client.indices.delete(index=self.ORIG)
#         # Get a new indexlist object
#         ilo = IndexList(self.client)
#         # Verify it only has one index in it
#         assert 1 == len(ilo.indices)
#         # ...and that the index is our restored- cold-tier mount
#         settings = self.client.indices.get(
#             index=f'restored-{self.ORIG}')[f'restored-{self.ORIG}']
#         # ...and that it has our alias
#         assert {self.ALIAS: {}} == settings['aliases']
#         # ...and that it has the appropriate settings
#         snapchk = settings['settings']['index']['store']['snapshot']
#         assert self.SNAP == snapchk['snapshot_name']
#         assert self.ORIG == snapchk['index_name']
#         assert self.args['repository'] == snapchk['repository_name']
