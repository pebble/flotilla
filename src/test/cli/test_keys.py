import unittest
from mock import MagicMock, patch

from flotilla.ssh.db import FlotillaSshDynamo
from flotilla.cli.keys import get_keys

ENVIRONMENT = 'test'
REGION = 'us-east-1'
SERVICE = 'testapp'


class TestKeys(unittest.TestCase):
    def setUp(self):
        self.db = MagicMock(spec=FlotillaSshDynamo)

    @patch('flotilla.cli.keys.FlotillaSshDynamo')
    @patch('flotilla.cli.keys.DynamoDbTables')
    @patch('boto.dynamodb2.connect_to_region')
    def test_get_keys_service(self, dynamo, tables, db):
        db.return_value = self.db
        get_keys(ENVIRONMENT, REGION, SERVICE, False)
        self.db.get_service_admins.assert_called_with(SERVICE)

    @patch('flotilla.cli.keys.FlotillaSshDynamo')
    @patch('flotilla.cli.keys.DynamoDbTables')
    @patch('boto.dynamodb2.connect_to_region')
    def test_get_keys_gateway(self, dynamo, tables, db):
        db.return_value = self.db
        get_keys(ENVIRONMENT, REGION, None, True)

        self.db.get_bastion_users.assert_called_with()

    @patch('flotilla.cli.keys.FlotillaSshDynamo')
    @patch('flotilla.cli.keys.DynamoDbTables')
    @patch('boto.dynamodb2.connect_to_region')
    def test_get_keys_scheduler(self, dynamo, tables, db):
        db.return_value = self.db
        get_keys(ENVIRONMENT, REGION, None, False)

        self.db.get_region_admins.assert_called_with()
