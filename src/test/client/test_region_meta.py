import unittest
from mock import MagicMock, patch, call, ANY
from boto.dynamodb2.layer1 import DynamoDBConnection
from boto.vpc import VPC, VPCConnection
from boto.exception import BotoServerError

from flotilla.client.region_meta import RegionMetadata

ENVIRONMENT = 'test'
REGION = 'us-east-1'
REGION_OTHER = 'us-west-2'
SCHEDULER = 't2.nano'
CHANNEL = 'stable'
VERSION = 'current'


class TestRegionMetadata(unittest.TestCase):
    def setUp(self):
        self.region_meta = RegionMetadata(ENVIRONMENT)

    @patch('boto.vpc.connect_to_region')
    def test_create_region_item(self, mock_connect):
        message = 'Value (us-east-1-zzz) for parameter availabilityZone is ' \
                  'invalid. Subnets can currently only be created in the ' \
                  'following availability zones: us-east-1c, us-east-1a, ' \
                  'us-east-1d, us-east-1e.'
        self.mock_subnet_error(mock_connect, message)

        region_item = self.region_meta._create_region_item(REGION)
        self.assertEqual(region_item['az1'], 'us-east-1c')
        self.assertEqual(region_item['az2'], 'us-east-1a')
        self.assertEqual(region_item['az3'], 'us-east-1d')
        self.assertEqual(region_item['region_name'], REGION)

    @patch('boto.vpc.connect_to_region')
    def test_create_region_item_wrap(self, mock_connect):
        message = 'Value (us-east-1-zzz) for parameter availabilityZone is ' \
                  'invalid. Subnets can currently only be created in the ' \
                  'following availability zones: us-east-1c, us-east-1a. '
        self.mock_subnet_error(mock_connect, message)

        region_item = self.region_meta._create_region_item(REGION)
        self.assertEqual(region_item['az1'], 'us-east-1c')
        self.assertEqual(region_item['az2'], 'us-east-1a')
        self.assertEqual(region_item['az3'], 'us-east-1c')

    @patch('boto.vpc.connect_to_region')
    def test_create_region_item_exception(self, mock_connect):
        vpc = MagicMock(spec=VPCConnection)
        mock_connect.return_value = vpc
        vpc.get_all_vpcs.side_effect = BotoServerError(400, 'Kaboom')

        self.assertRaises(BotoServerError, self.region_meta._create_region_item,
                          REGION)

    @patch('boto.dynamodb2.connect_to_region')
    def test_store_regions(self, mock_connect):
        dynamo = MagicMock(spec=DynamoDBConnection)
        mock_connect.return_value = dynamo
        self.region_meta._create_region_item = lambda x: {'region_name': x}

        region_params = self.region_meta.store_regions((REGION, REGION_OTHER),
                                                       True, SCHEDULER,
                                                       CHANNEL, VERSION)

        dynamo.batch_write_item.assert_called_with(ANY)
        region_param = region_params[REGION]
        self.assertEqual(region_param['scheduler_coreos_channel'], CHANNEL)
        self.assertEqual(region_param['scheduler_coreos_version'], VERSION)
        self.assertEqual(region_param['scheduler_instance_type'], SCHEDULER)

    def mock_subnet_error(self, mock_connect, message):
        vpc = MagicMock(spec=VPCConnection)
        mock_connect.return_value = vpc
        mock_vpc = MagicMock(spec=VPC)
        mock_vpc.id = 'vpc-123456'
        vpc.get_all_vpcs.return_value = [mock_vpc]
        message = '<Message>%s</Message>' % message
        vpc.create_subnet.side_effect = BotoServerError(400, 'Bad Request',
                                                        message)
