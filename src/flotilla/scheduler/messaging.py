import json
import logging

logger = logging.getLogger('flotilla')


class FlotillaSchedulerMessaging(object):
    def __init__(self, messages_q, scheduler):
        self._q = messages_q
        self._scheduler = scheduler

    def receive(self):
        for msg in self._q.get_messages(wait_time_seconds=20):
            try:
                payload = json.loads(msg.get_body())
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
                instance = payload['instance']
                rev = payload['revision']
                logger.info('Failed revision on %s: %s', instance, rev)
                # TODO: are there other instances running the same rev?
                # if yes, terminate _this_ instance because it's broken
                # if no, flag revision as broken and drop from scheduling
            else:
                logger.warn('Unknown message: %s', msg_type)
            msg.delete()
