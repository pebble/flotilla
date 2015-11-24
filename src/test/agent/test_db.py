import unittest
from mock import MagicMock, ANY
from boto.dynamodb2.exceptions import ItemNotFound
from boto.dynamodb2.table import Table
from flotilla.agent.db import FlotillaAgentDynamo

ASSIGNED = '123456'


class TestFlotillaAgentDynamo(unittest.TestCase):
    def setUp(self):
        self.status = MagicMock(spec=Table)
        self.assignments = MagicMock(spec=Table)
        self.revisions = MagicMock(spec=Table)
        self.units = MagicMock(spec=Table)

        self.instance_id = 'i-123456'
        self.service = 'mock-service'

        self.db = FlotillaAgentDynamo(self.instance_id, self.service,
                                      self.status, self.assignments,
                                      self.revisions, self.units)

        self.assignments.get_item.return_value = {'assignment': ASSIGNED}
        self.revisions.get_item.return_value = {'units': ['1', '2', '3']}
        self.units.batch_get.return_value = [
            {'name': '1', 'unit_file': '', 'environment': ''},
            {'name': '2', 'unit_file': '', 'environment': ''},
            {'name': '3', 'unit_file': '', 'environment': ''}
        ]

    def test_store_status(self):
        self.db.store_status({})

        self.status.put_item.assert_called_with(data=ANY, overwrite=True)
        for other_table in [self.assignments, self.revisions, self.units]:
            other_table.put_item.assert_not_called()

    def test_get_assignments(self):
        assignment = self.db.get_assignment()

        self.assertEqual(ASSIGNED, assignment)

    def test_get_assignments_none(self):
        self.assignments.get_item.side_effect = ItemNotFound()
        assignment = self.db.get_assignment()
        self.assertEqual(None, assignment)

    def test_get_units(self):
        units = self.db.get_units()
        self.assertEqual(3, len(units))
        self.revisions.get_item.assert_called_with(rev_hash=ASSIGNED)
        self.units.batch_get.assert_called_with(keys=[
            {'unit_hash': '1'},
            {'unit_hash': '2'},
            {'unit_hash': '3'}
        ])
