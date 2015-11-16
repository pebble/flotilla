import logging
import time
from boto.exception import BotoServerError

logger = logging.getLogger('flotilla')


class LoadBalancer(object):
    def __init__(self, instance_id, elb_name, elb):
        self._id = instance_id
        self._elb_name = elb_name
        self._elb = elb

    def unregister(self):
        if not self._elb:
            return

        logger.debug('Unregistering from %s.', self._elb_name)
        try:
            self._elb.deregister_instances(self._elb_name, [self._id])
        except BotoServerError as e:
            if e.error_code == 'InvalidInstance':
                logger.warn('Not a member of %s.', self._elb_name)
                return
            else:
                raise e

        self._wait_for_state('OutOfService')
        logger.debug('Unregistered from %s.', self._elb_name)

    def register(self):
        if not self._elb:
            return True

        logger.debug('Registering to %s.', self._elb_name)
        self._elb.register_instances(self._elb_name, [self._id])
        reg_state = self._wait_for_state('InService')
        logger.debug('Registered from %s.', self._elb_name)
        return reg_state

    def _wait_for_state(self, state, timeout=60):
        start = time.time()
        while True:
            states = \
                self._elb.describe_instance_health(self._elb_name, [self._id])
            instance_state = states[0]
            if instance_state.state == state:
                return True

            if (time.time() - start) > timeout:
                return False

            time.sleep(0.5)
