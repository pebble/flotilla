import unittest
from mock import MagicMock
from flotilla.db import DynamoDbLocks
from flotilla.scheduler.scheduler import FlotillaScheduler
from flotilla.scheduler.db import FlotillaSchedulerDynamo

SERVICE = 'test'


class TestFlotillaScheduler(unittest.TestCase):
    def setUp(self):
        self.db = MagicMock(spec=FlotillaSchedulerDynamo)
        self.locks = MagicMock(spec=DynamoDbLocks)
        self.scheduler = FlotillaScheduler(self.db, self.locks)
        self.scheduler.active = True

    def test_loop_not_active(self):
        self.scheduler.active = False
        self.scheduler.loop()
        self.db.get_revision_weights.assert_not_called()
        self.db.get_instance_assignments.assert_not_called()

    def test_loop_no_services(self):
        self.scheduler.loop()
        self.db.get_instance_assignments.assert_not_called()

    def test_loop_service_without_revisions(self):
        self.db.get_revision_weights.return_value = {
            SERVICE: []
        }
        self.scheduler.loop()

        self.db.get_instance_assignments.assert_not_called()

    def test_lock_acquire(self):
        self.locks.try_lock.return_value = True
        self.scheduler.lock()
        self.assertTrue(self.scheduler.active)

    def test_lock_release(self):
        self.locks.try_lock.return_value = False
        self.scheduler.lock()
        self.assertFalse(self.scheduler.active)
