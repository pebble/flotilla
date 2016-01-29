import unittest
from mock import patch, MagicMock
from flotilla.cli.region import configure_region, get_updates
from flotilla.client.db import FlotillaClientDynamo

ENVIRONMENT = 'develop'
REGIONS = ('us-east-1', 'us-west-2')

INSTANCE_TYPE = 't2.nano'
COREOS_CHANNEL = 'alpha'
COREOS_VERSION = 'current'
ADMINS = ['pwagner']


class TestRegion(unittest.TestCase):
    def test_get_updates_noop(self):
        updates = get_updates(None, None, None, ())
        self.assertEquals(len(updates), 0)

    def test_get_updates_basic(self):
        updates = get_updates(INSTANCE_TYPE, COREOS_CHANNEL, COREOS_VERSION,
                              ADMINS)
        self.assertEquals(len(updates), 4)

    @patch('flotilla.cli.region.FlotillaClientDynamo')
    @patch('flotilla.cli.region.DynamoDbTables')
    @patch('boto.dynamodb2.connect_to_region')
    def test_configure_region(self, dynamo, tables, db):
        configure_region(ENVIRONMENT, REGIONS, INSTANCE_TYPE, COREOS_CHANNEL,
                         COREOS_VERSION, ADMINS)

        self.assertEquals(dynamo.call_count, len(REGIONS))

    @patch('flotilla.cli.region.FlotillaClientDynamo')
    @patch('flotilla.cli.region.DynamoDbTables')
    @patch('boto.dynamodb2.connect_to_region')
    def test_configure_region_invalid_admin(self, dynamo, tables, db):
        mock_db = MagicMock(spec=FlotillaClientDynamo)
        mock_db.check_users.return_value = ADMINS
        db.return_value = mock_db

        configure_region(ENVIRONMENT, REGIONS, INSTANCE_TYPE, COREOS_CHANNEL,
                         COREOS_VERSION, ADMINS)

        self.assertEquals(dynamo.call_count, len(REGIONS))
        self.assertEqual(db.configure_region.call_count, 0)

    @patch('boto.dynamodb2.connect_to_region')
    def test_configure_region_no_name(self, dynamo):
        configure_region(ENVIRONMENT, (), INSTANCE_TYPE, COREOS_CHANNEL,
                         COREOS_VERSION, ADMINS)
        self.assertEquals(dynamo.call_count, 0)

    @patch('boto.dynamodb2.connect_to_region')
    def test_configure_region_no_updates(self, dynamo):
        configure_region(ENVIRONMENT, REGIONS, None, None, None, ())
        self.assertEquals(dynamo.call_count, 0)
