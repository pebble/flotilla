import json
import logging

logger = logging.getLogger('flotilla')


class FlotillaSchedulerMessaging(object):
    def __init__(self, messages_q, scheduler, doctor):
        self._q = messages_q
        self._scheduler = scheduler
        self._doctor = doctor

    def receive(self):
        for msg in self._q.receive_messages(WaitTimeSeconds=20):
            try:
                payload = json.loads(msg.body)
                msg_type = payload['type']
            except:
                logger.warn('Invalid message')
                msg.delete()
                continue

            if msg_type == 'ServiceReschedule':
                service = payload['service']
                logger.debug('Service reschedule: %s', service)
                self._scheduler.schedule_service(service)
            elif msg_type == 'ServiceDidNotStart':
                service = payload['service']
                rev = payload['revision']
                instance = payload['instance']
                self._doctor.failed_revision(service, rev, instance)
            else:
                logger.warn('Unknown message: %s', msg_type)
            msg.delete()
