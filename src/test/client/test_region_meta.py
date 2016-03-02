import unittest
from mock import MagicMock, patch, call, ANY
from boto.dynamodb2.layer1 import DynamoDBConnection
from botocore.exceptions import ClientError

from flotilla.client.region_meta import RegionMetadata

ENVIRONMENT = 'test'
REGION = 'us-east-1'
REGION_OTHER = 'us-west-2'
SCHEDULER = 't2.nano'
CHANNEL = 'stable'
VERSION = 'current'
CONTAINER = 'pebbletech/flotilla'


class TestRegionMetadata(unittest.TestCase):
    def setUp(self):
        self.region_meta = RegionMetadata(ENVIRONMENT)

    @patch('boto3.client')
    def test_region_params(self, mock_connect):
        message = 'Value (us-east-1-zzz) for parameter availabilityZone is ' \
                  'invalid. Subnets can currently only be created in the ' \
                  'following availability zones: us-east-1c, us-east-1a, ' \
                  'us-east-1d, us-east-1e.'
        self.mock_subnet_error(mock_connect, message)

        region_item = self.region_meta._region_params(REGION)
        self.assertEqual(region_item['az1'], 'us-east-1a')
        self.assertEqual(region_item['az2'], 'us-east-1c')
        self.assertEqual(region_item['az3'], 'us-east-1d')

    @patch('boto3.client')
    def test_region_params_wrap(self, mock_connect):
        message = 'Value (us-east-1-zzz) for parameter availabilityZone is ' \
                  'invalid. Subnets can currently only be created in the ' \
                  'following availability zones: us-east-1c, us-east-1a. '
        self.mock_subnet_error(mock_connect, message)

        region_item = self.region_meta._region_params(REGION)
        self.assertEqual(region_item['az1'], 'us-east-1a')
        self.assertEqual(region_item['az2'], 'us-east-1c')
        self.assertEqual(region_item['az3'], 'us-east-1a')

    @patch('boto3.client')
    def test__region_params_exception(self, mock_connect):
        vpc = MagicMock()
        mock_connect.return_value = vpc
        vpc.describe_vpcs.side_effect = ClientError({'Error': {}}, '')

        self.assertRaises(ClientError, self.region_meta._region_params, REGION)

    @patch('boto3.resource')
    def test_store_regions(self, mock_connect):
        dynamo = MagicMock()
        mock_connect.return_value = dynamo
        self.region_meta._region_params = MagicMock(return_value={})

        self.region_meta.store_regions((REGION, REGION_OTHER),
                                       False, SCHEDULER,
                                       CHANNEL, VERSION,
                                       CONTAINER)
        self.assertEquals(mock_connect.call_count, 2)

    @patch('boto3.resource')
    def test_store_regions_per_region(self, mock_connect):
        dynamo = MagicMock()
        mock_connect.return_value = dynamo
        self.region_meta._region_params = MagicMock(return_value={})

        self.region_meta.store_regions((REGION, REGION_OTHER),
                                       True, SCHEDULER,
                                       CHANNEL, VERSION, CONTAINER)
        self.assertEquals(mock_connect.call_count, 2)

    def mock_subnet_error(self, mock_connect, message):
        vpc = MagicMock()
        mock_connect.return_value = vpc
        mock_vpc = {'VpcId': 'vpc-123456'}
        vpc.describe_vpcs.return_value = {'Vpcs': [mock_vpc]}

        client_error = ClientError({'Error': {'Message': message}}, '')
        vpc.create_subnet.side_effect = client_error
