import unittest
from mock import patch

from flotilla.cli.agent import start_agent

ENVIRONMENT = 'test'
SERVICE = 'test-app'
REGION = 'us-east-1'
ELB = 'elb-1234'


class TestAgent(unittest.TestCase):
    @patch('flotilla.cli.agent.get_instance_id')
    @patch('flotilla.cli.agent.DynamoDbTables')
    @patch('flotilla.cli.agent.FlotillaAgentDynamo')
    @patch('flotilla.cli.agent.Manager')
    @patch('flotilla.cli.agent.RepeatingFunc')
    @patch('boto.ec2.elb.connect_to_region')
    @patch('boto.dynamodb2.connect_to_region')
    @patch('boto.kms.connect_to_region')
    def test_start_agent_no_elb(self, kms, dynamo, elb, repeat, manager,
                                client_db, tables, get_instance_id):
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
    def test_start_agent_elb(self, kms, dynamo, elb, repeat, manager,
                             client_db, tables, get_instance_id):
        get_instance_id.return_value = 'i-123456'

        start_agent(ENVIRONMENT, SERVICE, REGION, ELB, 0.1, 0.1)

        elb.assert_called_with(REGION)
