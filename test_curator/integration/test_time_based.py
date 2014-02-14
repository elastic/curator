from . import CuratorTestCase

class TestTimeBasedDeletion(CuratorTestCase):
    def setUp(self):
        super(TestTimeBasedDeletion, self).setUp()
        self.create_day_indices(10)

    def test_curator_will_properly_delete_indices(self):
        self.run_curator(delete_older=3)
        mtd = self.client.cluster.state(index=self.args['prefix'] + '*', metric='metadata')
        self.assertEquals(4, len(mtd['metadata']['indices'].keys()))
