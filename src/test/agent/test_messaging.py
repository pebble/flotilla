import unittest
from mock import MagicMock
import json

from flotilla.agent.messaging import FlotillaAgentMessaging, \
    MESSAGE_DEPLOY_LOCK_RELEASED

SERVICE = 'testapp'
INSTANCE_ID = 'i-123456'
REVISION = '0000000000000000'


class TestFlotillaAgentMessaging(unittest.TestCase):
    def setUp(self):
        self.scheduler_q = MagicMock()
        self.service_q = MagicMock()
        self.message = MagicMock()
        self.service_q.receive_messages.return_value = [self.message]

        self.messaging = FlotillaAgentMessaging(SERVICE, INSTANCE_ID,
                                                self.scheduler_q,
                                                self.service_q)

    def test_reschedule(self):
        self.messaging.reschedule()
        body = '{"type": "ServiceReschedule", "service": "testapp"}'
        self.scheduler_q.send_message.assert_called_with(MessageBody=body)

    def test_service_failure(self):
        self.messaging.service_failure(REVISION)
        body = '{"instance": "i-123456", "type": "ServiceDidNotStart", ' \
               '"service": "testapp", "revision": "0000000000000000"}'
        self.scheduler_q.send_message.assert_called_with(MessageBody=body)

    def test_deploy_lock_released(self):
        self.messaging.deploy_lock_released()
        body = '{"type": "DeployLockReleased"}'
        self.service_q.send_message.assert_called_with(MessageBody=body)

    def test_receive_invalid(self):
        self.message.body = 'not_json'
        self.messaging.receive()
        self.message.delete.assert_called_with()

    def test_receive_deploy_lock(self):
        self.message.body = json.dumps({
            'type': MESSAGE_DEPLOY_LOCK_RELEASED
        })
        self.messaging.receive()
        self.message.delete.assert_called_with()

    def test_receive_unknown_type(self):
        self.message.body = json.dumps({
            'type': 'NotImplemented'
        })
        self.messaging.receive()
        self.message.delete.assert_called_with()
