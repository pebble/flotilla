import unittest
from mock import MagicMock, patch, ANY
from boto.exception import BotoServerError
from boto.cloudformation.connection import CloudFormationConnection
from boto.cloudformation.stack import Stack
from flotilla.scheduler.cloudformation import FlotillaCloudFormation, \
    CAPABILITIES
from flotilla.scheduler.coreos import CoreOsAmiIndex

ENVIRONMENT = 'test'
DOMAIN = 'test.com'
NAME = 'service'
REGION_NAME = 'us-east-1'
REGION = {'region_name': REGION_NAME}
REGIONS = (REGION_NAME, 'us-west-2')
STACK_ARN = 'stack_arn'
TEMPLATE = '{}'
SERVICE_STACK = 'flotilla-test-worker-service'


class TestFlotillaCloudFormation(unittest.TestCase):
    def setUp(self):
        self.cloudformation = MagicMock(spec=CloudFormationConnection)
        self.stack = MagicMock(spec=Stack)
        self.stack.stack_status = 'CREATE_COMPLETE'
        self.cloudformation.describe_stacks.return_value = [self.stack]
        self.coreos = MagicMock(spec=CoreOsAmiIndex)
        self.service = {
            'service_name': NAME
        }
        self.cf = FlotillaCloudFormation(ENVIRONMENT, DOMAIN, self.coreos,
                                         backoff=0.001)

    @patch('boto.cloudformation.connect_to_region')
    def test_client_cache(self, mock_connect):
        mock_connect.return_value = self.cloudformation
        self.assertEqual(0, len(self.cf._clients))

        self.cf._client(REGION_NAME)

        self.assertEqual(1, len(self.cf._clients))
        self.assertEqual(self.cloudformation, self.cf._clients[REGION_NAME])

    def test_service_hash(self):
        service_hash = self.cf._service_hash(self.service, {'foo': 'bar'})
        self.assertIsNotNone(service_hash)

    def test_service_hash_string_fields(self):
        hash_base = self.cf._service_hash(self.service, {'foo': 'bar'})

        self.service['instance_type'] = 't2.micro'
        hash_with_string = self.cf._service_hash(self.service, {'foo': 'bar'})

        self.assertNotEqual(hash_base, hash_with_string)

    def test_service_hash_iterable_fields(self):
        hash_base = self.cf._service_hash(self.service, {'foo': 'bar'})

        self.service['regions'] = ['us-east-1', 'us-west-2']
        hash_with_list = self.cf._service_hash(self.service, {'foo': 'bar'})
        self.assertNotEqual(hash_base, hash_with_list)

        self.service['regions'] = ['us-west-2', 'us-east-1']
        hash_with_list_order = self.cf._service_hash(self.service,
                                                     {'foo': 'bar'})
        self.assertNotEqual(hash_base, hash_with_list_order)
        self.assertEqual(hash_with_list, hash_with_list_order)

    def test_stack_does_not_exists(self):
        self.mock_client()
        not_found = BotoServerError(400, 'Not Found')
        self.cloudformation.describe_stacks.side_effect = not_found
        self.cloudformation.create_stack.return_value = STACK_ARN

        stack = self.cf._stack(REGION_NAME, NAME, TEMPLATE, {})

        self.cloudformation.create_stack. \
            assert_called_with(NAME,
                               capabilities=CAPABILITIES,
                               template_body=TEMPLATE,
                               parameters=ANY)
        self.cloudformation.update_stack.assert_not_called()
        self.assertEqual(stack.stack_id, STACK_ARN)

    def test_stack_existing_in_progress(self):
        self.mock_client()
        self.stack.stack_status = 'CREATE_IN_PROGRESS'

        stack = self.cf._stack(REGION_NAME, NAME, '{}', {})

        self.assertEqual(stack.stack_status, 'CREATE_IN_PROGRESS')
        self.cloudformation.create_stack.assert_not_called()
        self.cloudformation.update_stack.assert_not_called()

    def test_stack_existing_update(self):
        self.mock_client()
        self.cloudformation.update_stack.return_value = STACK_ARN

        stack = self.cf._stack(REGION_NAME, NAME, TEMPLATE, {})

        self.cloudformation.create_stack.assert_not_called()
        self.cloudformation.update_stack. \
            assert_called_with(NAME,
                               capabilities=CAPABILITIES,
                               template_body=TEMPLATE,
                               parameters=ANY)
        self.assertEqual(stack.stack_id, STACK_ARN)

    def test_stack_existing_update_exception(self):
        self.mock_client()
        unknown_error = BotoServerError(400, 'Unknown error')
        self.cloudformation.update_stack.side_effect = unknown_error

        self.assertRaises(BotoServerError, self.cf._stack, REGION_NAME, NAME,
                          TEMPLATE, {})

    def test_stack_existing_update_noop(self):
        self.mock_client()
        no_updates = BotoServerError(400, 'Unknown error',
                                     body='<Message>No updates are to be'
                                          ' performed.</Message>')
        self.cloudformation.update_stack.side_effect = no_updates

        stack = self.cf._stack(REGION_NAME, NAME, TEMPLATE, {})

        self.assertEqual(stack, self.stack)

    def test_service(self):
        self.cf._stack = MagicMock()

        self.cf.service(REGION, self.service, {}, None)

        self.cf._stack.assert_called_with(REGION_NAME, SERVICE_STACK, ANY, ANY)

    def test_service_complete(self):
        self.cf._complete = MagicMock(return_value=True)
        service = self.cf.service(REGION, self.service, {}, None)
        self.assertIsNone(service)

    def test_service_public_ports(self):
        self.cf._stack = MagicMock()
        self.service['public_ports'] = {'9200': 'HTTP'}

        self.cf.service(REGION, self.service, {}, None)

        self.cf._stack.assert_called_with(REGION_NAME, SERVICE_STACK, ANY, ANY)

    def test_service_private_ports(self):
        self.cf._stack = MagicMock()
        self.service['private_ports'] = {'9300': ['TCP']}

        self.cf.service(REGION, self.service, {}, None)

        self.cf._stack.assert_called_with(REGION_NAME, SERVICE_STACK, ANY, ANY)

    def test_service_params_generate_dns(self):
        stack_params = self.cf._service_params(REGION, self.service, {})
        self.assertEqual(stack_params['VirtualHostDomain'], DOMAIN + '.')
        self.assertEqual(stack_params['VirtualHost'], 'service-test.test.com')

    def test_service_params_dns_parameter(self):
        self.service['dns_name'] = 'testapp.test.com'

        stack_params = self.cf._service_params(REGION, self.service, {})
        self.assertEqual(stack_params['VirtualHostDomain'], DOMAIN + '.')
        self.assertEqual(stack_params['VirtualHost'], 'testapp.test.com')

    def test_service_params_custom_container(self):
        region = REGION.copy()
        region['flotilla_container'] = 'pwagner/flotilla'
        stack_params = self.cf._service_params(region, self.service, {})
        self.assertEqual(stack_params['FlotillaContainer'], 'pwagner/flotilla')

    def test_vpc(self):
        self.cf._stack = MagicMock()

        self.cf.vpc({'region_name': REGION_NAME}, None)

        self.cf._stack.assert_called_with(REGION_NAME, 'flotilla-test-vpc',
                                          self.cf._template('vpc'), ANY)

    def test_vpc_complete(self):
        self.cf._complete = MagicMock(return_value=True)
        vpc = self.cf.vpc({'region_name': REGION_NAME}, None)
        self.assertIsNone(vpc)

    def test_vpc_params_empty(self):
        params = self.cf._vpc_params({'region_name': REGION_NAME})
        self.assertEqual(params['Az1'], 'us-east-1a')
        self.assertEqual(params['Az2'], 'us-east-1b')
        self.assertEqual(params['Az3'], 'us-east-1c')
        self.assertTrue('FlotillaContainer' not in params)

    def test_vpc_params_container(self):
        params = self.cf._vpc_params({'region_name': REGION_NAME,
                                      'flotilla_container': 'pwagner/flotilla'})
        self.assertEqual(params['FlotillaContainer'], 'pwagner/flotilla')

    def test_vpc_params_nat_per_az(self):
        params = self.cf._vpc_params({'region_name': REGION_NAME,
                                      'nat_per_az': 'true'})
        self.assertEqual(params['NatPerAz'], 'true')

    def test_vpc_params_nat_per_az(self):
        params = self.cf._vpc_params({'region_name': REGION_NAME,
                                      'nat_per_az': 'true'})
        self.assertEqual(params['NatPerAz'], 'true')

    def test_vpc_params_nat_per_az_invalid(self):
        params = self.cf._vpc_params({'region_name': REGION_NAME,
                                      'nat_per_az': 'meow'})
        self.assertEqual(params['NatPerAz'], 'false')

    def test_tables_done(self):
        self.mock_client()
        self.cf._stack = MagicMock(return_value=self.stack)

        self.cf.tables(REGIONS)

        self.assertEqual(self.cf._stack.call_count, len(REGIONS))
        self.cloudformation.describe_stacks.assert_not_called()

    def test_tables_wait(self):
        self.mock_client()
        self.cf._stack = MagicMock()

        self.cf.tables(REGIONS)

        self.assertEqual(self.cloudformation.describe_stacks.call_count,
                         len(REGIONS))

    def test_scheduler_for_regions(self):
        template = self.cf._scheduler_for_regions(('ap-northeast-1',))
        self.assertTrue(template.find('ap-northeast-1') != 1)
        self.assertTrue(template.find('us-east-1') != 1)

    def test_schedulers_every(self):
        self.mock_client()
        self.cf._stack = MagicMock()
        regions = {
            REGION_NAME: {
                'scheduler': True,
                'scheduler_instance_type': 't2.nano',
                'scheduler_coreos_channel': 'stable',
                'scheduler_coreos_version': 'current',
                'az1': 'us-east-1a',
                'az2': 'us-east-1b',
                'az3': 'us-east-1c',
                'flotilla_container': 'pwagner/flotilla'
            }
        }

        self.cf.schedulers(regions)

        self.cf._stack.assert_called_with(REGION_NAME,
                                          'flotilla-test-scheduler',
                                          self.cf._template('scheduler'), ANY)

    def test_schedulers_light(self):
        self.mock_client()
        self.cf._stack = MagicMock()
        self.cf._scheduler_for_regions = MagicMock()
        regions = {
            REGION_NAME: {
                'scheduler': True,
                'scheduler_instance_type': 't2.nano',
                'scheduler_coreos_channel': 'stable',
                'scheduler_coreos_version': 'current',
                'az1': 'us-east-1a',
                'az2': 'us-east-1b',
                'az3': 'us-east-1c',
            },
            'us-west-2': {}
        }

        self.cf.schedulers(regions)

        self.cf._scheduler_for_regions.assert_called_with(ANY)
        self.cf._stack.assert_called_with(REGION_NAME,
                                          'flotilla-test-scheduler',
                                          ANY, ANY)

    def test_complete_missing(self):
        self.assertFalse(self.cf._complete(None, 'foo'))

    def test_complete_hash_mismatch(self):
        stack = {'stack_hash': 'bar'}
        self.assertFalse(self.cf._complete(stack, 'foo'))

    def test_complete_no_outputs(self):
        stack = {'stack_hash': 'foo'}
        self.assertFalse(self.cf._complete(stack, 'foo'))

    def test_complete(self):
        stack = {'stack_hash': 'foo', 'outputs': {'foo': 'bar'}}
        self.assertTrue(self.cf._complete(stack, 'foo'))

    def mock_client(self):
        self.cf._client = MagicMock(return_value=self.cloudformation)
