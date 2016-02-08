import logging
import time
from boto.exception import BotoServerError

logger = logging.getLogger('flotilla')


class LoadBalancer(object):
    """Interacts with ELB.

    Required permissions:
    - elasticloadbalancing:DeregisterInstancesFromLoadBalancer
    - elasticloadbalancing:RegisterInstancesWithLoadBalancer

    Note: these should be scoped to a specific ELB!
    """

    def __init__(self, instance_id, elb_name, elb, backoff=0.5):
        self._id = instance_id
        self._elb_name = elb_name
        self._elb = elb
        self._backoff = backoff

    def unregister(self, timeout=60):
        """Unregister and wait for connection draining."""
        if not self._elb:
            logger.debug('Unregistering no-op (no ELB)')
            return True

        logger.debug('Unregistering from %s.', self._elb_name)
        try:
            self._elb.deregister_instances(self._elb_name, [self._id])
        except BotoServerError as e:
            if e.error_code == 'InvalidInstance':
                logger.warn('Not a member of %s.', self._elb_name)
                return True
            raise e

        state = self._wait_for_state('OutOfService', timeout)
        logger.debug('Unregistered from %s.', self._elb_name)
        return state

    def register(self, timeout=120):
        """Register and wait for health checks
        :param timeout Max time to wait
        :return Boolean registration state.
        """
        if not self._elb:
            logger.debug('Registering no-op (no ELB)')
            return True

        logger.debug('Registering to %s.', self._elb_name)
        self._elb.register_instances(self._elb_name, [self._id])
        state = self._wait_for_state('InService', timeout)
        logger.debug('Registered to %s - %s.', self._elb_name, state)
        return state

    def _wait_for_state(self, state, timeout):
        start = time.time()
        while True:
            states = \
                self._elb.describe_instance_health(self._elb_name, [self._id])
            instance_state = states[0]
            if instance_state.state == state:
                return True

            if (time.time() - start) > timeout:
                return False

            time.sleep(self._backoff)
