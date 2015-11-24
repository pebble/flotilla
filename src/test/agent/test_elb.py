import unittest
from mock import MagicMock
from boto.exception import BotoServerError
from boto.ec2.elb import ELBConnection
from flotilla.agent.elb import LoadBalancer

INSTANCE_ID = 'i-123456'
ELB_NAME = 'elb-123456'


class TestLoadBalancer(unittest.TestCase):
    def setUp(self):
        self.elb = MagicMock(spec=ELBConnection)
        self._in_service()

        self.lb = LoadBalancer(INSTANCE_ID, ELB_NAME, self.elb, backoff=0.00001)

    def test_register(self):
        registered = self.lb.register()
        self.assertTrue(registered)

    def test_register_timeout(self):
        self._out_of_service()
        registered = self.lb.register(timeout=0.001)
        self.assertFalse(registered)

    def test_register_noop(self):
        self.lb = LoadBalancer(INSTANCE_ID, ELB_NAME, None)
        registered = self.lb.register()
        self.assertTrue(registered)

    def test_unregister(self):
        self._out_of_service()
        registered = self.lb.unregister()
        self.assertTrue(registered)

    def test_unregister_timeout(self):
        registered = self.lb.unregister(timeout=0.001)
        self.assertFalse(registered)

    def test_unregister_not_found(self):
        self.elb.deregister_instances.side_effect = BotoServerError(
            'InvalidInstance', 'Invalid instance',
            body='<Code>InvalidInstance</Code>')
        registered = self.lb.unregister(timeout=0.001)
        self.assertTrue(registered)

    def test_unregister_exception(self):
        self.elb.deregister_instances.side_effect = BotoServerError(
            'Explosion', 'Kaboom')
        self.assertRaises(BotoServerError, self.lb.unregister)

    def test_unregister_noop(self):
        self.lb = LoadBalancer(INSTANCE_ID, ELB_NAME, None)
        registered = self.lb.unregister()
        self.assertTrue(registered)

    def _out_of_service(self):
        self.elb.describe_instance_health.return_value = [
            MagicMock(state='OutOfService')
        ]

    def _in_service(self):
        self.elb.describe_instance_health.return_value = [
            MagicMock(state='InService')
        ]
