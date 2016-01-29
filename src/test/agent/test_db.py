import unittest
from mock import MagicMock, ANY
from boto.kms.layer1 import KMSConnection
from boto.dynamodb2.exceptions import ItemNotFound
from boto.dynamodb2.table import Table
from flotilla.agent.db import FlotillaAgentDynamo

ASSIGNED = 'e697b6b7cef7faba1bc7cbd20e0d247fdb46f96231cdef8897de0b6e19468c76'

UNIT_1_HASH = '6b86b273ff34fce19d6b804eff5a3f5747ada4eaa22f1d49c01e52ddb7875b4b'
UNIT_2_HASH = 'd4735e3a265e16eee03f59718b9b5d03019c07d8b6c51f90da3a666eec13ab35'
UNIT_3_HASH = '4e07408562bedb8b60ce05c1decfe3ad16b72230967de01f640b7e4729b49fce'


class TestFlotillaAgentDynamo(unittest.TestCase):
    def setUp(self):
        self.status = MagicMock(spec=Table)
        self.assignments = MagicMock(spec=Table)
        self.revisions = MagicMock(spec=Table)
        self.units = MagicMock(spec=Table)
        self.kms = MagicMock(spec=KMSConnection)

        self.instance_id = 'i-123456'
        self.service = 'mock-service'

        self.db = FlotillaAgentDynamo(self.instance_id, self.service,
                                      self.status, self.assignments,
                                      self.revisions, self.units, self.kms)

        self.assignments.batch_get.return_value = [{'assignment': ASSIGNED}]

        self.revision = {
            'units': [UNIT_1_HASH, UNIT_2_HASH, UNIT_3_HASH],
            'rev_hash': ASSIGNED,
            'label': 'test'
        }
        self.revisions.batch_get.return_value = [self.revision]
        self.units.batch_get.return_value = [
            {'name': '1', 'unit_file': '', 'environment': '',
             'unit_hash': UNIT_1_HASH},
            {'name': '2', 'unit_file': '', 'environment': '',
             'unit_hash': UNIT_2_HASH},
            {'name': '3', 'unit_file': '', 'environment': '',
             'unit_hash': UNIT_3_HASH}
        ]

    def test_store_status(self):
        self.db.store_status({})

        self.status.put_item.assert_called_with(data=ANY, overwrite=True)
        for other_table in [self.assignments, self.revisions, self.units]:
            other_table.put_item.assert_not_called()

    def test_get_assignments(self):
        assignment = self.db.get_assignments()

        self.assertEqual([ASSIGNED], assignment)

        self.assignments.batch_get.assert_called_with([
            {'instance_id': self.instance_id},
            {'instance_id': 'global_7'}

        ])

    def test_get_assignments_global(self):
        self.assignments.batch_get.return_value = [
            {'assignment': ASSIGNED},
            {'assignment': ASSIGNED[::-1]}
        ]

        assignment = self.db.get_assignments()

        self.assertEqual(2, len(assignment))

    def test_get_units(self):
        units = self.db.get_units([ASSIGNED])
        self.assertEqual(3, len(units))

        self.revisions.batch_get.assert_called_with([{'rev_hash': ASSIGNED}])
        self.units.batch_get.assert_called_with([
            {'unit_hash': UNIT_3_HASH},
            {'unit_hash': UNIT_1_HASH},
            {'unit_hash': UNIT_2_HASH}
        ])

    def test_get_units_hash_mismatch(self):
        self.units.batch_get.return_value = [
            {'name': '1', 'unit_file': 'pwned', 'environment': '',
             'unit_hash': UNIT_1_HASH},
            {'name': '2', 'unit_file': '', 'environment': '',
             'unit_hash': UNIT_2_HASH},
            {'name': '3', 'unit_file': '', 'environment': '',
             'unit_hash': UNIT_3_HASH}
        ]

        units = self.db.get_units([ASSIGNED])

        self.assertEquals(3, len(units))

    def test_get_units_decrypt(self):
        self.revision['units'] = [
            '2e96c29527f87d9d6a1dbab735590a23132abea196a785f607ec52d1c1a4c730']
        self.units.batch_get.return_value = [{
            'name': '1',
            'unit_file': '',
            'unit_hash': '2e96c29527f87d9d6a1dbab735590a23132abea196a785f607ec52d1c1a4c730',
            'environment_key': 'kms-ciphertext'.encode('base64'),
            'environment_iv': 'MDAwMDAwMDAwMDAwMDAwMA==',
            'environment_data': 'guZyiyEsGQ6e8HIOMRQsdeXMl+6k2ywTfZi+MojMrAg='
        }]
        self.kms.decrypt.return_value = {
            'Plaintext': '0000000000000000'
        }

        units = self.db.get_units([ASSIGNED])

        self.kms.decrypt.assert_called_with('kms-ciphertext')
        decrypted_env = units[0].environment
        self.assertEqual(len(decrypted_env), 2)
        self.assertEqual(decrypted_env['foo'], 'bar')
        self.assertEqual(decrypted_env['typesafe'], True)
