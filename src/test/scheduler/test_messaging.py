import unittest
from mock import MagicMock
import json

from flotilla.scheduler.doctor import ServiceDoctor
from flotilla.scheduler.messaging import FlotillaSchedulerMessaging, \
    MESSAGE_RESCHEDULE, MESSAGE_SERVICE_FAILURE

from flotilla.scheduler.scheduler import FlotillaScheduler

SERVICE = 'test'


class TestFlotillaSchedulerMessaging(unittest.TestCase):
    def setUp(self):
        self.queue = MagicMock()
        self.message = MagicMock()
        self.queue.receive_messages.return_value = [self.message]
        self.scheduler = MagicMock(spec=FlotillaScheduler)
        self.doctor = MagicMock(spec=ServiceDoctor)

        self.messaging = FlotillaSchedulerMessaging(self.queue, self.scheduler,
                                                    self.doctor)

    def test_receive_empty(self):
        self.queue.receive_messages.return_value = []

        self.messaging.receive()

        self.message.delete.assert_not_called()

    def test_receive_invalid(self):
        self.message.body = 'not_json'

        self.messaging.receive()

        self.message.delete.assert_called_with()

    def test_receive_service_reschedule(self):
        self.message.body = json.dumps({
            'type': MESSAGE_RESCHEDULE,
            'service': SERVICE
        })

        self.messaging.receive()

        self.scheduler.schedule_service.assert_called_with(SERVICE)
        self.message.delete.assert_called_with()

    def test_receive_service_did_not_start(self):
        self.message.body = json.dumps({
            'type': MESSAGE_SERVICE_FAILURE,
            'service': SERVICE,
            'revision': 'abcdef',
            'instance': 'i-123456',
        })

        self.messaging.receive()

        self.scheduler.schedule_service.assert_not_called()
        self.doctor.failed_revision(SERVICE, 'abcdef', 'i-123456')
        self.message.delete.assert_called_with()

    def test_receive_unknown_type(self):
        self.message.body = json.dumps({
            'type': 'NotImplemented'
        })

        self.messaging.receive()

        self.scheduler.schedule_service.assert_not_called()
        self.message.delete.assert_called_with()
