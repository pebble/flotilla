import unittest
from mock import MagicMock, ANY
from flotilla.client.db import FlotillaClientDynamo
from flotilla.model import FlotillaServiceRevision, FlotillaDockerService
from boto.dynamodb2.exceptions import ItemNotFound
from boto.dynamodb2.table import Table
from boto.dynamodb2.items import Item
from boto.kms.layer1 import KMSConnection

SERVICE_NAME = 'foo'
USERNAME = 'pwagner'


class TestFlotillaClientDynamo(unittest.TestCase):
    def setUp(self):
        self.revision = FlotillaServiceRevision(units=[
            FlotillaDockerService('redis.service', 'redis:latest', environment={
                'FOO': 'bar'
            })
        ])
        self.rev_hash = self.revision.revision_hash

        self.assignments = MagicMock(spec=Table)
        self.regions = MagicMock(spec=Table)
        self.revisions = MagicMock(spec=Table)
        self.services = MagicMock(spec=Table)
        self.units = MagicMock(spec=Table)
        self.users = MagicMock(spec=Table)
        self.revision_item = MagicMock(spec=Item)
        self.revisions.has_item.return_value = False
        self.revisions.get_item.return_value = self.revision_item

        self.service_item = MagicMock(spec=Item)
        self.service_data = {
            self.rev_hash: 1
        }
        self.service_item.__getitem__.side_effect = \
            self.service_data.__getitem__
        self.service_item.__contains__.side_effect = \
            self.service_data.__contains__
        self.service_item.get.side_effect = \
            self.service_data.get
        self.service_item.keys.side_effect = self.service_data.keys
        self.services.get_item.return_value = self.service_item
        self.kms = MagicMock(spec=KMSConnection)
        self.db = FlotillaClientDynamo(self.assignments,
                                       self.regions,
                                       self.revisions,
                                       self.services,
                                       self.units,
                                       self.users,
                                       self.kms)

    def test_add_revision(self):
        self.db.add_revision(SERVICE_NAME, self.revision)

        self.units.batch_write.assert_called_with()
        self.revisions.new_item.assert_called_with(ANY)

    def test_add_revision_existing(self):
        self.revisions.has_item.return_value = True

        self.db.add_revision(SERVICE_NAME, self.revision)

        self.revisions.new_item.assert_not_called()

    def test_add_revision_missing_unit(self):
        self.units.has_item.return_value = False

        self.db.add_revision(SERVICE_NAME, self.revision)

        self.units.batch_write.assert_called_with()
        self.units.new_item.assert_called_with(ANY)

    def test_add_revision_missing_service(self):
        self.services.get_item.side_effect = ItemNotFound()

        self.db.add_revision(SERVICE_NAME, self.revision)

        self.services.new_item.assert_called_with(SERVICE_NAME)

    def test_del_revision(self):
        self.db.del_revision(SERVICE_NAME, self.rev_hash)
        self.service_item.partial_save.assert_called_with()
        self.revision_item.delete.assert_called_with()

    def test_del_revision_not_on_service(self):
        del self.service_data[self.rev_hash]
        self.db.del_revision(SERVICE_NAME, self.rev_hash)
        self.service_item.partial_save.assert_not_called()

    def test_del_revision_service_missing(self):
        self.services.get_item.side_effect = ItemNotFound()
        self.db.del_revision(SERVICE_NAME, self.rev_hash)
        self.service_item.partial_save.assert_not_called()

    def test_del_revision_revision_missing(self):
        self.revisions.get_item.side_effect = ItemNotFound()
        self.db.del_revision(SERVICE_NAME, self.rev_hash)
        self.revisions.delete.assert_not_called()

    def test_set_revision_weight(self):
        self.db.set_revision_weight(SERVICE_NAME, self.rev_hash, 2)
        self.service_item.partial_save.assert_called_with()

    def test_set_revision_weight_service_missing(self):
        self.services.get_item.side_effect = ItemNotFound()
        self.db.set_revision_weight(SERVICE_NAME, self.rev_hash, 2)
        self.service_item.partial_save.assert_not_called()

    def test_get_revisions(self):
        self.revisions.batch_get.return_value = [
            {'rev_hash': self.rev_hash, 'label': 'test',
             'units': ['000', '001']}]
        self.units.batch_get.return_value = [
            {'name': 'test', 'unit_file': '', 'environment': '',
             'unit_hash': '000'},
            {'name': 'test', 'unit_file': '', 'environment': '',
             'unit_hash': '001'}
        ]
        revisions = self.db.get_revisions(SERVICE_NAME)

        self.assertEqual(1, len(revisions))
        test_rev = revisions[0]
        self.assertEqual('test', test_rev.label)
        self.assertEqual(2, len(test_rev.units))

    def test_get_revisions_not_found(self):
        self.services.get_item.side_effect = ItemNotFound()
        revisions = self.db.get_revisions(SERVICE_NAME)
        self.assertEqual(0, len(revisions))

    def test_configure_region_create(self):
        self.regions.get_item.side_effect = ItemNotFound()
        self.db.configure_region('us-east-1', {'az1': 'us-east-1a'})
        self.regions.new_item.assert_called_with('us-east-1')

    def test_configure_region_exists(self):
        existing_region = MagicMock(spec=Item)
        self.regions.get_item.return_value = existing_region
        self.db.configure_region('us-east-1', {'az1': 'us-east-1a'})
        existing_region.save.assert_called_with()

    def test_configure_service_create(self):
        self.services.get_item.side_effect = ItemNotFound()

        self.db.configure_service(SERVICE_NAME, {'key': 'value'})

        self.services.new_item.assert_called_with(SERVICE_NAME)

    def test_configure_service_exists(self):
        existing_service = MagicMock(spec=Item)
        self.services.get_item.return_value = existing_service

        self.db.configure_service(SERVICE_NAME, {'key': 'value'})

        existing_service.save.assert_called_with()

    def test_configure_user_create(self):
        self.users.get_item.side_effect = ItemNotFound()

        self.db.configure_user(USERNAME, {'key': 'value'})

        self.users.new_item.assert_called_with(USERNAME)

    def test_configure_user_exists(self):
        existing_user = MagicMock(spec=Item)
        self.users.get_item.return_value = existing_user

        self.db.configure_user(USERNAME, {'key': 'value'})

        existing_user.save.assert_called_with()

    def test_set_global(self):
        self.db.set_global(self.revision)

        self.units.batch_write.assert_called_with()
        self.revisions.new_item.assert_called_with(ANY)
        self.assignments.put_item.assert_called_with(ANY, overwrite=True)

    def test_encrypt_environment(self):
        self.kms.generate_data_key.return_value = {
            'Plaintext': '0000000000000000',
            'CiphertextBlob': 'topsecret'
        }

        unit = {}
        self.db._encrypt_environment('key-12345', {}, unit)
        self.assertEqual(unit['environment_key'], 'topsecret'.encode('base64'))
        self.assertTrue('environment_iv' in unit)
        self.assertTrue('environment_data' in unit)

    def check_users(self):
        usernames = ['found', 'missing']
        self.users.batch_get.return_value = [
            {'username': 'found'}
        ]

        missing_users = self.db.check_users(usernames)
        self.assertEquals(missing_users, ['missing'])

    def test_store_revision_encryption(self):
        self.units.has_item.return_value = False
        self.db._encrypt_environment = MagicMock(return_value=('blob', 'key'))

        self.db._store_revision(self.revision, 'key')

        self.db._encrypt_environment.assert_called_with('key', ANY, ANY)
