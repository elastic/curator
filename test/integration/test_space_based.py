from datetime import datetime, timedelta

import curator

from . import CuratorTestCase

class TestSpaceBasedDeletion(CuratorTestCase):
    # Testing https://github.com/elasticsearch/curator/issues/254
    def test_curator_will_not_match_all_when_no_indices_match(self):
        self.create_indices(10)
        curator.delete(self.client, disk_space=0.0000001, prefix='foo-', timestring='%Y.%m.%d')
        mtd = self.client.cluster.state(index=self.args['prefix'] + '*', metric='metadata')
        self.assertEquals(10, len(mtd['metadata']['indices'].keys()))

