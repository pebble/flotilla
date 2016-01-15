import unittest
from mock import MagicMock, ANY

from boto.dynamodb2.table import Table
from boto.dynamodb2.exceptions import ItemNotFound

from flotilla.ssh.db import FlotillaSshDynamo

REGION = 'us-east-1'
SERVICE = 'testapp'

REGION_ADMINS = ('user1', 'user2')
SERVICE_ADMINS = ('user2', 'user3')


class TestFlotillaSshDynamo(unittest.TestCase):
    def setUp(self):
        self.regions = MagicMock(spec=Table)
        self.services = MagicMock(spec=Table)
        self.users = MagicMock(spec=Table)

        self.regions.get_item.return_value = {
            'region_name': REGION,
            'admins': REGION_ADMINS
        }
        service = {'service_name': SERVICE, 'admins': SERVICE_ADMINS}
        self.services.get_item.return_value = service
        self.services.scan.return_value = [service]

        self.db = FlotillaSshDynamo(self.regions, self.services, self.users,
                                    REGION)

    def test_get_region_admins_found(self):
        users = self.db.get_region_admins()

        self.assertEqual(users, set(REGION_ADMINS))
        self.regions.get_item.assert_called_with(region_name=REGION)

    def test_get_region_admins_not_found(self):
        self.regions.get_item.side_effect = ItemNotFound()

        users = self.db.get_region_admins()

        self.assertEqual(len(users), 0)

    def test_get_service_admins_found(self):
        users = self.db.get_service_admins(SERVICE)

        self.assertEqual(users, set(REGION_ADMINS + SERVICE_ADMINS))

        self.services.get_item.assert_called_with(service_name=SERVICE,
                                                  attributes=ANY)

    def test_get_service_admins_not_found(self):
        self.services.get_item.side_effect = ItemNotFound()

        users = self.db.get_service_admins(SERVICE)

        self.assertEqual(users, set(REGION_ADMINS))

    def test_get_gateway_users(self):
        users = self.db.get_gateway_users()

        self.assertEqual(users, set(REGION_ADMINS + SERVICE_ADMINS))

    def test_get_keys(self):
        self.users.batch_get.return_value = [
            {
                'username': REGION_ADMINS[0],
                'ssh_keys': ['foo', 'bar']
            },
            {
                'username': REGION_ADMINS[1],
                'ssh_keys': ['foz', 'baz']
            }
        ]

        keys = self.db.get_keys(REGION_ADMINS)
        self.assertEqual(keys, ['foo', 'bar', 'foz', 'baz'])
