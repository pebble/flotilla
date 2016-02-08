import unittest
from mock import MagicMock, ANY
from flotilla.agent import *
from flotilla.db import DynamoDbLocks

SERVICE = 'mock-service'
ASSIGNED_REVISION = '00000000000000000000000000000000'


class TestFlotillaAgent(unittest.TestCase):
    def setUp(self):
        self.db = MagicMock(spec=FlotillaAgentDynamo)
        self.db.get_assignments.return_value = [ASSIGNED_REVISION]
        self.locks = MagicMock(spec=DynamoDbLocks)
        self.systemd = MagicMock(spec=SystemdUnits)
        self.systemd.get_unit_status.return_value = {}
        self.elb = MagicMock(spec=LoadBalancer)
        self.messaging = MagicMock(spec=FlotillaAgentMessaging)
        self.agent = FlotillaAgent(SERVICE, self.db, self.locks, self.systemd,
                                   self.messaging, self.elb)

    def test_health(self):
        self.agent.health()
        self.db.store_status.assert_called_with(ANY)
        self.messaging.reschedule.assert_called_with()

    def test_health_second(self):
        self.agent.health()
        self.agent.health()
        self.assertEquals(self.messaging.reschedule.call_count, 1)

    def test_assignment_noop(self):
        self.db.get_assignments.return_value = []

        self.agent.assignment()

        self.systemd.set_units.assert_not_called()

    def test_assignment_change(self):
        self.agent.assignment()

        self.elb.unregister.assert_called_with()
        self.elb.register.assert_called_with()
        self.db.get_units.assert_called_with([ASSIGNED_REVISION])
        self.locks.try_lock.assert_called_with('mock-service-deploy')
        self.locks.release_lock.assert_called_with('mock-service-deploy')
        self.systemd.set_units.assert_called_with(ANY)
        self.messaging.service_failure.assert_not_called()

    def test_assignment_failure(self):
        self.elb.register.return_value = False
        self.agent.assignment()
        self.messaging.service_failure.assert_called_with(ASSIGNED_REVISION)

    def test_assignment_elb_safe(self):
        self.agent = FlotillaAgent(SERVICE, self.db, self.locks, self.systemd,
                                   self.messaging, None)

        self.agent.assignment()

        self.elb.unregister.assert_not_called()
        self.elb.register.assert_not_called()
