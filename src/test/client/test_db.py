import unittest
from mock import MagicMock, ANY
from flotilla.client.db import FlotillaClientDynamo
from flotilla.model import FlotillaServiceRevision, FlotillaDockerService
from boto.dynamodb2.exceptions import ItemNotFound
from boto.dynamodb2.table import Table
from boto.dynamodb2.items import Item
import logging

logging.getLogger('boto').setLevel(logging.CRITICAL)

SERVICE_NAME = 'foo'


class TestFlotillaClientDynamo(unittest.TestCase):
    def setUp(self):
        self.revision = FlotillaServiceRevision(units=[
            FlotillaDockerService('redis.service', 'redis:latest', environment={
                'FOO': 'bar'
            })
        ])
        self.rev_hash = self.revision.revision_hash

        self.units = MagicMock(spec=Table)
        self.revisions = MagicMock(spec=Table)
        self.revision_item = MagicMock(spec=Item)
        self.revisions.has_item.return_value = False
        self.revisions.get_item.return_value = self.revision_item

        self.services = MagicMock(spec=Table)
        self.service_item = MagicMock(spec=Item)
        self.service_data = {}
        self.service_data[self.rev_hash] = 1
        self.service_item.__getitem__.side_effect = \
            self.service_data.__getitem__
        self.service_item.__contains__.side_effect = \
            self.service_data.__contains__
        self.services.get_item.return_value = self.service_item
        self.db = FlotillaClientDynamo(self.units,
                                       self.revisions,
                                       self.services)

    def test_add_revision(self):
        self.db.add_revision(SERVICE_NAME, self.revision)

        self.units.batch_write.assert_called_with()
        self.revisions.new_item.assert_called_with(ANY)

    def test_add_revision_existing(self):
        self.revisions.has_item.return_value = True

        self.db.add_revision(SERVICE_NAME, self.revision)
        self.revisions.new_item.assert_not_called()

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
        self.db.get_revisions(SERVICE_NAME)
