import unittest
from mock import MagicMock, ANY
import time
from flotilla.scheduler.db import FlotillaSchedulerDynamo, INSTANCE_EXPIRY
from boto.dynamodb2.table import Table, BatchTable

SERVICE = 'test'
INSTANCE_ID = 'i-123456'
REVISION = 'rev1'


class TestFlotillaSchedulerDynamo(unittest.TestCase):
    def setUp(self):
        self.assignments = MagicMock(spec=Table)
        self.assignments._dynamizer = MagicMock()
        self.regions = MagicMock(spec=Table)
        self.services = MagicMock(spec=Table)
        self.stacks = MagicMock(spec=Table)
        self.status = MagicMock(spec=Table)

        self.db = FlotillaSchedulerDynamo(self.assignments, self.regions,
                                          self.services, self.stacks,
                                          self.status)

    def test_get_revision_weights_empty(self):
        weights = self.db.get_revision_weights()
        self.assertEqual(0, len(weights))

    def test_get_revision_weights(self):
        self.services.scan.return_value = [
            {
                'service_name': SERVICE,
                'regions': ['us-east-1'],
                REVISION: 1,
                'rev2': 2
            },
            {
                'service_name': 'test2',
                'rev3': 1,
                'rev4': 2
            }
        ]

        weights = self.db.get_revision_weights()
        self.assertEqual(2, len(weights))

        service_test = weights[SERVICE]
        self.assertEqual(1, service_test[REVISION])
        self.assertEqual(2, service_test['rev2'])

        service_test = weights['test2']
        self.assertEqual(1, service_test['rev3'])
        self.assertEqual(2, service_test['rev4'])

    def test_set_assignment(self):
        self.db.set_assignment(SERVICE, INSTANCE_ID, REVISION)
        self.assignments.put_item.assert_called_with(data=ANY, overwrite=True)

    def test_set_assignments(self):
        mock_batch = MagicMock(spec=BatchTable)
        self.assignments.batch_write.return_value = mock_batch

        self.db.set_assignments([
            {'instance_id': INSTANCE_ID},
            {'instance_id': INSTANCE_ID}
        ])
        mock_batch.put_item.call_count = 2

    def test_get_instance_assignments_empty(self):
        assignments = self.db.get_instance_assignments(SERVICE)
        self.assertEqual(0, len(assignments))

    def test_get_instance_assignments_assigned(self):
        self.status.query_2.return_value = [{
            'instance_id': INSTANCE_ID,
            'status_time': time.time()
        }]
        self.assignments.batch_get.return_value = [{
            'instance_id': INSTANCE_ID,
            'assignment': REVISION
        }]

        assignments = self.db.get_instance_assignments(SERVICE)
        self.assertEqual(1, len(assignments[REVISION]))

    def test_get_instance_assignments_unassigned(self):
        self.status.query_2.return_value = [{
            'instance_id': INSTANCE_ID,
            'status_time': time.time()
        }]
        assignments = self.db.get_instance_assignments(SERVICE)
        self.assertEqual(1, len(assignments[None]))

    def test_get_instance_assignments_garbage_collection(self):
        self.status.query_2.return_value = [{
            'instance_id': INSTANCE_ID,
            'status_time': time.time() - (INSTANCE_EXPIRY + 1)
        }]

        assignments = self.db.get_instance_assignments(SERVICE)

        self.assertEqual(0, len(assignments))
        self.status.batch_write.assert_called_with()
        self.assignments.batch_write.assert_called_with()

    def test_get_stacks_empty(self):
        stacks = self.db.get_stacks()
        self.assertEqual(0, len(stacks))

    def test_get_stacks(self):
        self.stacks.scan.return_value = [{'service_name': 'fred'}]
        stacks = self.db.get_stacks()
        self.assertEqual(1, len(stacks))
        self.assertEquals('fred', stacks[0]['service_name'])

    def test_set_stacks(self):
        self.db.set_stacks([{'stack_arn': 'foo'}])
        self.stacks.batch_write.assert_called_with()

    def test_get_region_params_empty(self):
        region_params = self.db.get_region_params(['us-east-1'])
        self.assertEqual(len(region_params), 1)
        self.assertEqual(region_params['us-east-1'], {})

    def test_get_region_params(self):
        self.regions.batch_get.return_value = [
            {'region_name': 'us-east-1', 'az1': 'us-east-1e'}
        ]
        region_params = self.db.get_region_params(['us-east-1', 'us-west-2'])

        self.assertEqual(len(region_params), 2)
        self.assertEqual(region_params['us-east-1'],
                         {'region_name': 'us-east-1', 'az1': 'us-east-1e'})
        self.assertEqual(region_params['us-west-2'], {})
