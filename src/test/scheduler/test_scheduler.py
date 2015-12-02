import unittest
from mock import MagicMock
from flotilla.scheduler.scheduler import FlotillaScheduler
from flotilla.scheduler.db import FlotillaSchedulerDynamo

SERVICE = 'test'


class TestFlotillaScheduler(unittest.TestCase):
    def setUp(self):
        self.db = MagicMock(spec=FlotillaSchedulerDynamo)
        self.scheduler = FlotillaScheduler(self.db)
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
