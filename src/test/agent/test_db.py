import unittest
from mock import MagicMock, ANY
from boto.dynamodb2.exceptions import ItemNotFound
from boto.dynamodb2.table import Table
from flotilla.agent.db import FlotillaAgentDynamo

ASSIGNED = 'e697b6b7cef7faba1bc7cbd20e0d247fdb46f96231cdef8897de0b6e19468c76'


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

        self.assignments.get_item.side_effect = [{'assignment': ASSIGNED},
                                                 ItemNotFound()]
        self.revisions.get_item.return_value = {'units': ['1', '2', '3'],
                                                'label': 'test'}
        self.units.batch_get.return_value = [
            {'name': '1', 'unit_file': '', 'environment': '',
             'unit_hash': '6b86b273ff34fce19d6b804eff5a3f5747ada4eaa22f1d49c01e52ddb7875b4b'},
            {'name': '2', 'unit_file': '', 'environment': '',
             'unit_hash': 'd4735e3a265e16eee03f59718b9b5d03019c07d8b6c51f90da3a666eec13ab35'},
            {'name': '3', 'unit_file': '', 'environment': '',
             'unit_hash': '4e07408562bedb8b60ce05c1decfe3ad16b72230967de01f640b7e4729b49fce'}
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

    def test_get_units_global(self):
        self.assignments.get_item.side_effect = [{'assignment': ASSIGNED},
                                                 {'assignment': ASSIGNED}]

        units = self.db.get_units()

        self.assertEqual(6, len(units))

    def test_load_revision_unit_mismatch(self):
        self.units.batch_get.return_value = [
            {'name': '1', 'unit_file': 'pwned', 'environment': '',
             'unit_hash': '6b86b273ff34fce19d6b804eff5a3f5747ada4eaa22f1d49c01e52ddb7875b4b'},
            {'name': '2', 'unit_file': '', 'environment': '',
             'unit_hash': 'd4735e3a265e16eee03f59718b9b5d03019c07d8b6c51f90da3a666eec13ab35'},
            {'name': '3', 'unit_file': '', 'environment': '',
             'unit_hash': '4e07408562bedb8b60ce05c1decfe3ad16b72230967de01f640b7e4729b49fce'}
        ]

        units = self.db._load_revision_units(ASSIGNED)

        self.assertEqual(3, len(units))
