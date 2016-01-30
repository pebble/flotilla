import unittest
from mock import MagicMock
import json

from boto.sqs.message import Message
from boto.sqs.queue import Queue

from flotilla.scheduler.doctor import ServiceDoctor
from flotilla.scheduler.messaging import FlotillaSchedulerMessaging
from flotilla.scheduler.scheduler import FlotillaScheduler

SERVICE = 'test'


class TestFlotillaSchedulerMessaging(unittest.TestCase):
    def setUp(self):
        self.queue = MagicMock(spec=Queue)
        self.message = MagicMock(spec=Message)
        self.queue.get_messages.return_value = [self.message]
        self.scheduler = MagicMock(spec=FlotillaScheduler)
        self.doctor = MagicMock(spec=ServiceDoctor)

        self.messaging = FlotillaSchedulerMessaging(self.queue, self.scheduler,
                                                    self.doctor)

    def test_receive_empty(self):
        self.queue.get_messages.return_value = []

        self.messaging.receive()

        self.message.delete.assert_not_called()

    def test_receive_invalid(self):
        self.message.get_body.return_value = 'not_json'

        self.messaging.receive()

        self.message.delete.assert_called_with()

    def test_receive_service_reschedule(self):
        self.message.get_body.return_value = json.dumps({
            'type': 'ServiceReschedule',
            'service': SERVICE
        })

        self.messaging.receive()

        self.scheduler.schedule_service.assert_called_with(SERVICE)
        self.message.delete.assert_called_with()

    def test_receive_service_did_not_start(self):
        self.message.get_body.return_value = json.dumps({
            'type': 'ServiceDidNotStart',
            'service': SERVICE,
            'revision': 'abcdef',
            'instance': 'i-123456',
        })

        self.messaging.receive()

        self.scheduler.schedule_service.assert_not_called()
        self.doctor.failed_revision(SERVICE, 'abcdef', 'i-123456')
        self.message.delete.assert_called_with()

    def test_receive_unknown_type(self):
        self.message.get_body.return_value = json.dumps({
            'type': 'NotImplemented'
        })

        self.messaging.receive()

        self.scheduler.schedule_service.assert_not_called()
        self.message.delete.assert_called_with()
