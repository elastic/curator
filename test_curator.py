from unittest import TestCase, main
from mock import Mock

import curator


class TestUtils(TestCase):
    def test_can_bloom(self):
        client = Mock()

        client.info.return_value = {"version": {"number": "1.0.0"}}
        self.assertTrue(curator.can_bloom(client))

        client.info.return_value = {"version": {"number": "0.90.10"}}
        self.assertTrue(curator.can_bloom(client))

        client.info.return_value = {"version": {"number": "0.90.8"}}
        self.assertFalse(curator.can_bloom(client))

if __name__ == '__main__':
    main()
