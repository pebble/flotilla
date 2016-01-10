import unittest
from mock import patch
from flotilla.cli.region import configure_region, get_updates

ENVIRONMENT = 'develop'
REGIONS = ('us-east-1', 'us-west-2')

INSTANCE_TYPE = 't2.nano'
COREOS_CHANNEL = 'alpha'
COREOS_VERSION = 'current'


class TestRegion(unittest.TestCase):
    def test_get_updates_noop(self):
        updates = get_updates(None, None, None)
        self.assertEquals(len(updates), 0)

    def test_get_updates_basic(self):
        updates = get_updates(INSTANCE_TYPE, COREOS_CHANNEL, COREOS_VERSION)
        self.assertEquals(len(updates), 3)

    @patch('flotilla.cli.region.DynamoDbTables')
    @patch('boto.dynamodb2.connect_to_region')
    def test_configure_region(self, dynamo, tables):
        configure_region(ENVIRONMENT, REGIONS, {})

        self.assertEquals(dynamo.call_count, len(REGIONS))
