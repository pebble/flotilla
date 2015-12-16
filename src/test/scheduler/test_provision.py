import unittest
from mock import MagicMock, ANY
from flotilla.scheduler.cloudformation import FlotillaCloudFormation
from flotilla.scheduler.coreos import CoreOsAmiIndex
from flotilla.scheduler.db import FlotillaSchedulerDynamo
from flotilla.scheduler.provisioner import FlotillaProvisioner
from flotilla.scheduler.scheduler import FlotillaScheduler

ENVIRONMENT = 'test'
DOMAIN = 'test.com'
REGION = 'us-east-1'
SERVICE = 'testapp'


class TestFlotillaProvisioner(unittest.TestCase):
    def setUp(self):
        self.scheduler = MagicMock(spec=FlotillaScheduler)
        self.scheduler.active = True
        self.db = MagicMock(spec=FlotillaSchedulerDynamo)
        self.cloudformation = MagicMock(spec=FlotillaCloudFormation)
        self.coreos = MagicMock(spec=CoreOsAmiIndex)
        self.provisioner = FlotillaProvisioner(ENVIRONMENT, DOMAIN,
                                               self.scheduler, self.db,
                                               self.cloudformation, self.coreos)

    def test_provision_not_active(self):
        self.scheduler.active = False

        self.provisioner.provision()

        self.db.services.assert_not_called()

    def test_provision_no_regions(self):
        self.db.services.return_value = [
            {'service_name': SERVICE}
        ]

        self.provisioner.provision()

        # No other DB calls are made:
        self.db.get_stacks.assert_not_called()
        self.db.set_stacks.assert_not_called()

    def test_provision_no_region_vpc(self):
        self.mock_service()

        self.provisioner.provision()

        # VPC is created, service is not:
        self.cloudformation.vpc.assert_called_with(REGION, ANY)
        self.cloudformation.service.assert_not_called()
        self.db.set_stacks.assert_called_with(ANY)

    def test_provision_with_region_vpc(self):
        self.mock_service()
        self.db.get_stacks.return_value = [
            {'region': REGION,
             'outputs': {'VpcId': 'vpc-123456'}}
        ]
        self.provisioner._complete = MagicMock()
        self.provisioner._complete.side_effect = [True, False]

        self.provisioner.provision()

        self.cloudformation.vpc.assert_not_called()
        self.cloudformation.service.assert_called_with(REGION, SERVICE, ANY)
        self.db.set_stacks.assert_called_with(ANY)

    def test_provision_existing(self):
        self.mock_service()
        self.db.get_stacks.return_value = [
            {'region': REGION,
             'outputs': {'VpcId': 'vpc-123456'}},
            {'region': REGION,
             'service': SERVICE,
             'outputs': {'foo': 'bar'}}
        ]
        self.provisioner._complete = MagicMock(return_value=True)

        self.provisioner.provision()

        self.cloudformation.vpc.assert_not_called()
        self.cloudformation.service.assert_not_called()
        self.db.set_stacks.assert_not_called()

    def test_provision_delete(self):
        self.db.services.return_value = [
            {'service_name': 'newservice',
             'regions': [REGION]}
        ]
        self.db.get_stacks.return_value = [
            {'region': REGION,
             'service': SERVICE,
             'outputs': {'foo': 'bar'}}
        ]

        self.provisioner.provision()
        # FIXME: test when delete implemented

    def test_complete_missing(self):
        self.assertFalse(self.provisioner._complete(None, 'foo'))

    def test_complete_hash_mismatch(self):
        stack = {'stack_hash': 'bar'}
        self.assertFalse(self.provisioner._complete(stack, 'foo'))

    def test_complete_no_outputs(self):
        stack = {'stack_hash': 'foo'}
        self.assertFalse(self.provisioner._complete(stack, 'foo'))

    def test_complete(self):
        stack = {'stack_hash': 'foo', 'outputs': {'foo': 'bar'}}
        self.assertTrue(self.provisioner._complete(stack, 'foo'))

    def test_stack_params_generate_dns(self):
        service = {
            'service_name': SERVICE
        }

        stack_params = self.provisioner._stack_params(REGION, service, {})
        print stack_params
        self.assertEqual(stack_params['VirtualHostDomain'], DOMAIN + '.')
        self.assertEqual(stack_params['VirtualHost'], 'testapp-test.test.com')

    def test_stack_params_dns_parameter(self):
        service = {
            'service_name': SERVICE,
            'dns_name': 'testapp.test.com'
        }

        stack_params = self.provisioner._stack_params(REGION, service, {})
        self.assertEqual(stack_params['VirtualHostDomain'], DOMAIN + '.')
        self.assertEqual(stack_params['VirtualHost'], 'testapp.test.com')

    def mock_service(self):
        self.db.services.return_value = [
            {'service_name': SERVICE,
             'regions': [REGION]}
        ]
