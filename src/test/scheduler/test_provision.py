import unittest
from mock import MagicMock, ANY
from flotilla.scheduler.cloudformation import FlotillaCloudFormation
from flotilla.scheduler.db import FlotillaSchedulerDynamo
from flotilla.scheduler.provisioner import FlotillaProvisioner
from flotilla.scheduler.scheduler import FlotillaScheduler

ENVIRONMENT = 'test'
REGION = 'us-east-1'
SERVICE = 'testapp'


class TestFlotillaProvisioner(unittest.TestCase):
    def setUp(self):
        self.scheduler = MagicMock(spec=FlotillaScheduler)
        self.scheduler.active = True
        self.db = MagicMock(spec=FlotillaSchedulerDynamo)
        self.cloudformation = MagicMock(spec=FlotillaCloudFormation)
        self.provisioner = FlotillaProvisioner(ENVIRONMENT, REGION,
                                               self.scheduler, self.db,
                                               self.cloudformation)

    def test_provision_not_active(self):
        self.scheduler.active = False

        self.provisioner.provision()

        self.db.services.assert_not_called()

    def test_provision_no_regions(self):
        self.db.services.return_value = [
            {'service_name': SERVICE, 'provision': False}
        ]

        self.provisioner.provision()

        # No other DB calls are made:
        self.db.get_stacks.assert_not_called()
        self.db.set_stacks.assert_not_called()

    def test_provision_no_region_vpc(self):
        self.mock_service()
        self.cloudformation.vpc.return_value = {}

        self.provisioner.provision()

        # VPC is created, service is not:
        self.cloudformation.vpc.assert_called_with(ANY, None)
        self.cloudformation.service.assert_not_called()
        self.db.set_stacks.assert_called_with(ANY)

    def test_provision_with_region_vpc(self):
        self.mock_service()
        vpc_outputs = {'VpcId': 'vpc-123456'}
        self.db.get_stacks.return_value = [
            {'region': REGION,
             'outputs': vpc_outputs}
        ]
        self.cloudformation._complete = MagicMock(side_effect=[True, False])

        self.provisioner.provision()

        self.cloudformation.vpc.assert_called_with(ANY, ANY)
        self.cloudformation.service.assert_called_with(ANY, self.service,
                                                       ANY, ANY)
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
        self.cloudformation.vpc.return_value = None
        self.cloudformation.service.return_value = None

        self.provisioner.provision()

        self.cloudformation.vpc.assert_called_with(ANY, ANY)
        self.cloudformation.service.assert_called_with(ANY, ANY, ANY, ANY)
        self.db.set_stacks.assert_called_with([])

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

    def mock_service(self):
        self.service = {'service_name': SERVICE, 'provision': 1}
        self.db.services.return_value = [self.service]
