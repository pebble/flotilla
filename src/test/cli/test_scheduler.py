import unittest
from mock import patch

from flotilla.cli.scheduler import start_scheduler

REGIONS = ['us-east-1']
ENVIRONMENT = 'develop'
DOMAIN = 'test.com'


class TestScheduler(unittest.TestCase):
    @patch('flotilla.cli.scheduler.get_instance_id')
    @patch('flotilla.cli.scheduler.DynamoDbTables')
    @patch('flotilla.cli.scheduler.RepeatingFunc')
    @patch('boto.dynamodb2.connect_to_region')
    def test_start_scheduler(self, dynamo, repeat, tables, get_instance_id):
        get_instance_id.return_value = 'i-123456'

        start_scheduler(ENVIRONMENT, DOMAIN, REGIONS, 0.1, 0.1, 0.1)

        self.assertEquals(3, repeat.call_count)
