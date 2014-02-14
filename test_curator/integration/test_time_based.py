from . import CuratorTestCase

class TestTimeBasedDeletion(CuratorTestCase):
    def test_curator_will_properly_delete_indices(self):
        self.create_indices(10)
        self.run_curator(delete_older=3)
        mtd = self.client.cluster.state(index=self.args['prefix'] + '*', metric='metadata')
        self.assertEquals(4, len(mtd['metadata']['indices'].keys()))

    def test_curator_will_properly_delete_hourly_indices(self):
        self.create_indices(10, 'hours')
        self.run_curator(delete_older=3, time_unit='hours')
        mtd = self.client.cluster.state(index=self.args['prefix'] + '*', metric='metadata')
        self.assertEquals(4, len(mtd['metadata']['indices'].keys()))

