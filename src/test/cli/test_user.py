import unittest
from mock import patch

from flotilla.cli.user import get_updates, configure_user

ENVIRONMENT = 'develop'
REGIONS = ('us-east-1', 'us-west-2')
USERNAME = 'pwagner'
KEYS = ['ssh-rsa']


class TestUser(unittest.TestCase):
    def test_get_updates_noop(self):
        updates = get_updates((), None)
        self.assertEquals(updates, {})

    def test_get_updates(self):
        updates = get_updates(KEYS, False)
        self.assertEquals(len(updates), 2)
        self.assertEqual(updates['ssh_keys'], KEYS)
        self.assertEqual(updates['active'], False)

    @patch('flotilla.cli.user.DynamoDbTables')
    @patch('boto.dynamodb2.connect_to_region')
    def test_configure_user(self, dynamo, tables):
        configure_user(ENVIRONMENT, REGIONS, USERNAME, KEYS, None)
        self.assertEquals(dynamo.call_count, len(REGIONS))

    @patch('boto.dynamodb2.connect_to_region')
    def test_configure_user_noop(self, dynamo):
        configure_user(ENVIRONMENT, REGIONS, USERNAME, (), None)
        self.assertEquals(dynamo.call_count, 0)
