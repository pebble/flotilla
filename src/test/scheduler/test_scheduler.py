import unittest
from mock import MagicMock, ANY
from collections import defaultdict
from boto.dynamodb2.items import Item
from flotilla.db import DynamoDbLocks
from flotilla.scheduler.scheduler import FlotillaScheduler
from flotilla.scheduler.db import FlotillaSchedulerDynamo

SERVICE = 'test'
REVISION = 'rev1'
REVISION2 = 'rev2'


class TestFlotillaScheduler(unittest.TestCase):
    def setUp(self):
        self.db = MagicMock(spec=FlotillaSchedulerDynamo)
        self.db.get_instance_assignments.return_value = defaultdict(list)
        self.locks = MagicMock(spec=DynamoDbLocks)
        self.scheduler = FlotillaScheduler(self.db, self.locks)
        self.scheduler.active = True

    def test_loop_not_active(self):
        self.scheduler.active = False
        self.scheduler.loop()
        self.db.get_all_revision_weights.assert_not_called()
        self.db.get_instance_assignments.assert_not_called()

    def test_loop_no_services(self):
        self.scheduler.loop()
        self.db.get_instance_assignments.assert_not_called()

    def test_loop_service_without_revisions(self):
        self.db.get_all_revision_weights.return_value = {
            SERVICE: {}
        }
        self.scheduler.loop()

    def test_loop_assignments_no_instances(self):
        self.db.get_all_revision_weights.return_value = {SERVICE: {REVISION: 1}}

        self.scheduler.loop()

        self.db.set_assignments.assert_not_called()

    def test_loop_assignments(self):
        self.db.get_all_revision_weights.return_value = {SERVICE: {REVISION: 1}}
        assignment = MagicMock(spec=Item)
        self.db.get_instance_assignments.return_value[None].append(assignment)

        self.scheduler.loop()

        self.db.set_assignments.assert_called_with(ANY)

    def test_loop_assignments_reassign(self):
        self.db.get_all_revision_weights.return_value = {SERVICE: {REVISION: 1}}
        assignment = MagicMock(spec=Item)
        self.db.get_instance_assignments.return_value[None]
        self.db.get_instance_assignments.return_value[REVISION2].append(
            assignment)

        self.scheduler.loop()

        self.db.set_assignments.assert_called_with(ANY)

    def test_loop_assignments_reassign_partial(self):
        self.db.get_all_revision_weights.return_value = {
            SERVICE: {REVISION: 1, REVISION2: 1}}
        assignment = MagicMock(spec=Item)
        self.db.get_instance_assignments.return_value[None]
        for i in range(2):
            self.db.get_instance_assignments.return_value[REVISION2].append(
                assignment)

        self.scheduler.loop()

        self.db.set_assignments.assert_called_with(ANY)

    def test_instance_targets(self):
        targets = self.scheduler._instance_targets({REVISION: 1}, 1)
        self.assertEqual(1, len(targets))
        self.assertEqual(1, targets[REVISION])

    def test_instance_targets_rounding(self):
        targets = self.scheduler._instance_targets({REVISION: 1, REVISION2: 1},
                                                   3)
        self.assertEqual(2, len(targets))
        self.assertEqual(1, targets[REVISION])
        self.assertEqual(2, targets[REVISION2])

    def test_lock_acquire(self):
        self.scheduler.active = False
        self.locks.try_lock.return_value = True
        self.scheduler.lock()
        self.assertTrue(self.scheduler.active)

    def test_lock_release(self):
        self.locks.try_lock.return_value = False
        self.scheduler.lock()
        self.assertFalse(self.scheduler.active)
