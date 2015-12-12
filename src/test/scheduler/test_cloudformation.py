import unittest
from mock import MagicMock, patch, ANY
from boto.exception import BotoServerError
from boto.cloudformation.connection import CloudFormationConnection
from boto.cloudformation.stack import Stack
from flotilla.scheduler.cloudformation import FlotillaCloudFormation

ENVIRONMENT = 'test'
NAME = 'service'
REGION = 'us-east-1'
STACK_ARN = 'stack_arn'
TEMPLATE = '{}'


class TestFlotillaCloudFormation(unittest.TestCase):
    def setUp(self):
        self.cloudformation = MagicMock(spec=CloudFormationConnection)
        self.stack = MagicMock(spec=Stack)
        self.stack.stack_status = 'CREATE_COMPLETE'
        self.cloudformation.describe_stacks.return_value = [self.stack]
        self.cf = FlotillaCloudFormation(ENVIRONMENT)

    @patch('boto.cloudformation.connect_to_region')
    def test_client_cache(self, mock_connect):
        mock_connect.return_value = self.cloudformation
        self.assertEqual(0, len(self.cf._clients))

        self.cf._client(REGION)

        self.assertEqual(1, len(self.cf._clients))
        self.assertEqual(self.cloudformation, self.cf._clients[REGION])

    def test_service_hash(self):
        service_hash = self.cf.service_hash({'foo': 'bar'})
        self.assertIsNotNone(service_hash)

    def test_vpc_hash(self):
        vpc_hash = self.cf.vpc_hash({'foo': 'bar'})
        self.assertIsNotNone(vpc_hash)

    def test_stack_does_not_exists(self):
        self.mock_client()
        not_found = BotoServerError(400, 'Not Found')
        self.cloudformation.describe_stacks.side_effect = not_found
        self.cloudformation.create_stack.return_value = STACK_ARN

        stack = self.cf._stack(REGION, NAME, TEMPLATE, {})

        self.cloudformation.create_stack. \
            assert_called_with(NAME,
                               capabilities=['CAPABILITY_IAM'],
                               template_body=TEMPLATE,
                               parameters=ANY)
        self.cloudformation.update_stack.assert_not_called()
        self.assertEqual(stack.stack_id, STACK_ARN)

    def test_stack_existing_in_progress(self):
        self.mock_client()
        self.stack.stack_status = 'CREATE_IN_PROGRESS'

        stack = self.cf._stack(REGION, NAME, '{}', {})

        self.assertEqual(stack.stack_status, 'CREATE_IN_PROGRESS')
        self.cloudformation.create_stack.assert_not_called()
        self.cloudformation.update_stack.assert_not_called()

    def test_stack_existing_update(self):
        self.mock_client()
        self.cloudformation.update_stack.return_value = STACK_ARN

        stack = self.cf._stack(REGION, NAME, TEMPLATE, {})

        self.cloudformation.create_stack.assert_not_called()
        self.cloudformation.update_stack. \
            assert_called_with(NAME,
                               capabilities=['CAPABILITY_IAM'],
                               template_body=TEMPLATE,
                               parameters=ANY)
        self.assertEqual(stack.stack_id, STACK_ARN)

    def test_stack_existing_update_exception(self):
        self.mock_client()
        unknown_error = BotoServerError(400, 'Unknown error')
        self.cloudformation.update_stack.side_effect = unknown_error

        self.assertRaises(BotoServerError, self.cf._stack, REGION, NAME,
                          TEMPLATE, {})

    def test_stack_existing_update_noop(self):
        self.mock_client()
        no_updates = BotoServerError(400, 'Unknown error',
                                     body='<Message>No updates are to be'
                                          ' performed.</Message>')
        self.cloudformation.update_stack.side_effect = no_updates

        stack = self.cf._stack(REGION, NAME, TEMPLATE, {})

        self.assertEqual(stack, self.stack)

    def test_service(self):
        self.cf._stack = MagicMock()

        self.cf.service(REGION, NAME, {})

        self.cf._stack.assert_called_with(REGION, 'flotilla-test-service',
                                          self.cf._service_elb, {})

    def test_vpc(self):
        self.cf._stack = MagicMock()

        self.cf.vpc(REGION, {})

        self.cf._stack.assert_called_with(REGION, 'flotilla-test-vpc',
                                          self.cf._vpc, {})

    def mock_client(self):
        self.cf._client = MagicMock(return_value=self.cloudformation)
