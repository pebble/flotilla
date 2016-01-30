import unittest
from mock import MagicMock, ANY

import time

from flotilla.scheduler.db import FlotillaSchedulerDynamo
from flotilla.scheduler.doctor import ServiceDoctor, SERVICE_EXPIRY

SERVICE = 'testapp'
REV = '0000000000000000'
INSTANCE = 'i-123456'


class TestServiceDoctor(unittest.TestCase):
    def setUp(self):
        self.db = MagicMock(spec=FlotillaSchedulerDynamo)
        self.elb = MagicMock()
        self.service = {
            'service_name': SERVICE,
            REV: 1,
            'cf_outputs': {
                'Elb': 'test-elb',
                'InstanceSg': 'sg-123456'
            }
        }
        self.db.get_service.return_value = self.service

        self.doctor = ServiceDoctor(self.db, self.elb)

    def test_failed_revision_missing(self):
        self.db.get_service.return_value = None
        self.doctor.failed_revision(SERVICE, REV, INSTANCE)
        self.db.set_services.assert_not_called()

    def test_failed_revision_not_running(self):
        # No instances are running this rev
        self.doctor.failed_revision(SERVICE, REV, INSTANCE)
        self.elb.describe_instance_health.assert_not_called()
        self.assertEqual(self.service[REV], -1)
        self.db.set_services.assert_called_with(ANY)

    def test_failed_revision_not_healthy(self):
        # Instances running, but ELB doesn't have them registered
        self._mock_service_status()
        self.doctor.failed_revision(SERVICE, REV, INSTANCE)
        self.assertEqual(self.service[REV], -1)
        self.db.set_services.assert_called_with(ANY)

    def test_failed_revision_healthy(self):
        self._mock_service_status()
        self.elb.describe_instance_health.return_value = {
            'InstanceStates': [
                {'InstanceId': INSTANCE,
                 'State': 'InService'}
            ]
        }
        self.doctor.failed_revision(SERVICE, REV, INSTANCE)
        self.assertEqual(self.service[REV], 1)
        self.db.set_services.assert_not_called()

    def test_healthy_instances_no_outputs(self):
        del self.service['cf_outputs']
        healthy = self.doctor._healthy_instances(self.service, [INSTANCE])
        self.assertEquals(healthy, set())

    def test_healthy_instances_no_elb(self):
        del self.service['cf_outputs']['Elb']
        healthy = self.doctor._healthy_instances(self.service, [INSTANCE])
        self.assertEquals(healthy, set())

    def _mock_service_status(self):
        self.db.get_service_status.return_value = {
            'i-654321': {
                'service-foo': {
                    'active_enter_time': time.time() - SERVICE_EXPIRY,
                    'sub_state': 'running'
                }
            }
        }.items()
