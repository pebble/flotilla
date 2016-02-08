import unittest
from mock import patch, MagicMock

from flotilla.cli.agent import start_agent

ENVIRONMENT = 'test'
SERVICE = 'test-app'
REGION = 'us-east-1'
ELB = 'elb-1234'


class TestAgent(unittest.TestCase):
    @patch('flotilla.cli.agent.get_queue')
    @patch('flotilla.cli.agent.get_instance_id')
    @patch('flotilla.cli.agent.DynamoDbTables')
    @patch('flotilla.cli.agent.Manager')
    @patch('flotilla.cli.agent.RepeatingFunc')
    @patch('boto.ec2.elb.connect_to_region')
    @patch('boto.dynamodb2.connect_to_region')
    @patch('boto.kms.connect_to_region')
    @patch('boto3.resource')
    def test_start_agent_no_elb(self, resource, kms, dynamo, elb, repeat,
                                manager, tables, get_instance_id, get_queue):
        get_queue.return_value = None
        get_instance_id.return_value = 'i-123456'

        start_agent(ENVIRONMENT, SERVICE, REGION, None, 0.1, 0.1)

        dynamo.assert_called_with(REGION)
        kms.assert_called_with(REGION)
        elb.assert_not_called()

        self.assertEquals(2, repeat.call_count)

    @patch('flotilla.cli.agent.get_instance_id')
    @patch('flotilla.cli.agent.DynamoDbTables')
    @patch('flotilla.cli.agent.FlotillaAgentDynamo')
    @patch('flotilla.cli.agent.Manager')
    @patch('flotilla.cli.agent.RepeatingFunc')
    @patch('boto.ec2.elb.connect_to_region')
    @patch('boto.dynamodb2.connect_to_region')
    @patch('boto.kms.connect_to_region')
    @patch('boto3.resource')
    def test_start_agent_elb(self, resource, kms, dynamo, elb, repeat, manager,
                             agent_db, tables, get_instance_id):
        get_instance_id.return_value = 'i-123456'

        start_agent(ENVIRONMENT, SERVICE, REGION, ELB, 0.1, 0.1)

        elb.assert_called_with(REGION)

    @patch('flotilla.cli.agent.get_queue')
    @patch('flotilla.cli.agent.get_instance_id')
    @patch('flotilla.cli.agent.DynamoDbTables')
    @patch('flotilla.cli.agent.FlotillaAgentDynamo')
    @patch('flotilla.cli.agent.Manager')
    @patch('flotilla.cli.agent.RepeatingFunc')
    @patch('boto.ec2.elb.connect_to_region')
    @patch('boto.dynamodb2.connect_to_region')
    @patch('boto.kms.connect_to_region')
    @patch('boto3.resource')
    def test_start_agent_messaging(self, resource, kms, dynamo, elb, repeat,
                                   manager, agent_db, tables, get_instance_id,
                                   get_queue):
        get_instance_id.return_value = 'i-123456'
        get_queue.return_value = MagicMock()

        start_agent(ENVIRONMENT, SERVICE, REGION, ELB, 0.1, 0.1)

        self.assertEquals(3, repeat.call_count)
