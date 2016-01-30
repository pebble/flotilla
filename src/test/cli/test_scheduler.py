import unittest
from mock import patch, MagicMock

from botocore.exceptions import ClientError

from flotilla.cli.scheduler import start_scheduler, QUEUE_NOT_FOUND

REGIONS = ['us-east-1']
ENVIRONMENT = 'develop'
DOMAIN = 'test.com'


class TestScheduler(unittest.TestCase):
    @patch('flotilla.cli.scheduler.get_instance_id')
    @patch('flotilla.cli.scheduler.DynamoDbTables')
    @patch('flotilla.cli.scheduler.RepeatingFunc')
    @patch('boto.dynamodb2.connect_to_region')
    @patch('boto3.resource')
    def test_start_scheduler(self, sqs, dynamo, repeat, tables,
                             get_instance_id):
        get_instance_id.return_value = 'i-123456'

        start_scheduler(ENVIRONMENT, DOMAIN, REGIONS, 0.1, 0.1, 0.1)

        self.assertEquals(4, repeat.call_count)

    @patch('flotilla.cli.scheduler.get_instance_id')
    @patch('flotilla.cli.scheduler.DynamoDbTables')
    @patch('flotilla.cli.scheduler.RepeatingFunc')
    @patch('boto.dynamodb2.connect_to_region')
    @patch('boto3.resource')
    def test_start_scheduler_multiregion(self, sqs, dynamo, repeat, tables,
                                         get_instance_id):
        get_instance_id.return_value = 'i-123456'

        start_scheduler(ENVIRONMENT, DOMAIN, ['us-east-1', 'us-west-2'], 0.1,
                        0.1, 0.1)

        self.assertEquals(8, repeat.call_count)

    @patch('flotilla.cli.scheduler.get_instance_id')
    @patch('flotilla.cli.scheduler.DynamoDbTables')
    @patch('flotilla.cli.scheduler.RepeatingFunc')
    @patch('boto.dynamodb2.connect_to_region')
    @patch('boto3.resource')
    def test_start_scheduler_without_messaging(self, sqs, dynamo, repeat,
                                               tables, get_instance_id):
        mock_sqs = MagicMock()
        client_error = ClientError({'Error': {'Code': QUEUE_NOT_FOUND}}, '')
        mock_sqs.get_queue_by_name.side_effect = client_error
        sqs.return_value = mock_sqs

        start_scheduler(ENVIRONMENT, DOMAIN, REGIONS, 0.1, 0.1, 0.1)

        self.assertEquals(3, repeat.call_count)
