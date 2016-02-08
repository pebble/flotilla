import json
import logging

logger = logging.getLogger('flotilla')

from flotilla.scheduler.messaging import MESSAGE_RESCHEDULE, \
    MESSAGE_SERVICE_FAILURE

MESSAGE_DEPLOY_LOCK_RELEASED = 'DeployLockReleased'


class FlotillaAgentMessaging(object):
    def __init__(self, service, instance_id, scheduler_q, service_q):
        self._service = service
        self._instance_id = instance_id
        self._scheduler_q = scheduler_q
        self._service_q = service_q

    def reschedule(self):
        message = json.dumps({
            'type': MESSAGE_RESCHEDULE,
            'service': self._service
        })
        self._scheduler_q.send_message(MessageBody=message)

    def service_failure(self, target_rev):
        message = json.dumps({
            'type': MESSAGE_SERVICE_FAILURE,
            'service': self._service,
            'revision': target_rev,
            'instance': self._instance_id
        })
        self._scheduler_q.send_message(MessageBody=message)

    def deploy_lock_released(self):
        message = json.dumps({
            'type': MESSAGE_DEPLOY_LOCK_RELEASED
        })

        self._service_q.send_message(MessageBody=message)

    def receive(self):
        for msg in self._service_q.receive_messages(WaitTimeSeconds=20):
            try:
                payload = json.loads(msg.body)
                msg_type = payload['type']
            except:
                logger.warn('Invalid message')
                msg.delete()
                continue

            if msg_type == MESSAGE_DEPLOY_LOCK_RELEASED:
                # FIXME: do stuff here
                pass
            else:
                logger.warn('Unknown message: %s', msg_type)
            msg.delete()
