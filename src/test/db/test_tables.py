import unittest
from mock import MagicMock, ANY
from boto.exception import BotoServerError
from boto.dynamodb2.layer1 import DynamoDBConnection
from flotilla.db.tables import DynamoDbTables


class TestDynamoDbTables(unittest.TestCase):
    def setUp(self):
        self.dynamo = MagicMock(spec=DynamoDBConnection)
        self.dynamo.describe_table.return_value = {
            'Table': {
                'TableStatus': 'ACTIVE',
                'ProvisionedThroughput': {
                    'ReadCapacityUnits': 1,
                    'WriteCapacityUnits': 1
                }
            }
        }
        self.tables = DynamoDbTables(self.dynamo, backoff=0.001)

    def test_setup_existing(self):
        self.tables.setup(['revisions'])
        self.assertNotEqual(self.tables.revisions, None)
        self.dynamo.create_table.assert_not_called()

    def test_setup_create(self):
        self.dynamo.describe_table.side_effect = [
            BotoServerError(400, 'Not Found',
                            '<Code>ResourceNotFoundException</Code>'),
            self.dynamo.describe_table.return_value
        ]

        self.tables.setup(['revisions'])

        self.assertNotEqual(self.tables.revisions, None)
        self.dynamo.create_table.assert_called_with(
                table_name='flotilla-revisions',
                attribute_definitions=ANY,
                key_schema=ANY,
                provisioned_throughput=ANY
        )

    def test_setup_create_wait(self):
        self.dynamo.describe_table.side_effect = [
            BotoServerError(400, 'Not Found',
                            '<Code>ResourceNotFoundException</Code>'),
            {
                'Table': {
                    'TableStatus': 'CREATING',
                    'ProvisionedThroughput': {
                        'ReadCapacityUnits': 1,
                        'WriteCapacityUnits': 1
                    }
                }
            },
            self.dynamo.describe_table.return_value
        ]

        self.tables.setup(['revisions'])
        self.assertEqual(self.dynamo.describe_table.call_count, 3)

    def test_setup_create_error(self):
        self.dynamo.describe_table.side_effect = [
            BotoServerError(400, 'Kaboom')
        ]
        self.assertRaises(Exception, self.tables.setup, ['revisions'])

    def test_setup_unknown_table(self):
        self.tables.setup(['meow'])

        self.dynamo.describe_table.assert_not_called()
        self.dynamo.create_table.assert_not_called()

    def test_setup_environment(self):
        self.tables = DynamoDbTables(self.dynamo, environment='test')
        self.tables.setup(['revisions'])
        self.assertEqual(self.tables.revisions.table_name,
                         'flotilla-test-revisions')
