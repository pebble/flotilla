import unittest
from mock import patch

from flotilla.cli.service import configure_service, get_updates

ENVIRONMENT = 'develop'
REGIONS = ('us-east-1', 'us-west-2')
SERVICE = 'test'

ELB_SCHEME = 'internal'
DNS = 'test.test.com'
HEALTH_CHECK = 'HTTP:9200/'
INSTANCE_TYPE = 't2.nano'
NO_PROVISION = True


class TestService(unittest.TestCase):
    def test_get_updates_noop(self):
        updates = get_updates(None, None, None, None, None, (), ())
        self.assertEquals(len(updates), 0)

    def test_get_updates_basic(self):
        updates = get_updates(ELB_SCHEME, DNS, HEALTH_CHECK, INSTANCE_TYPE,
                              NO_PROVISION, None, None)
        self.assertEquals(len(updates), 5)

    def test_get_updates_public_ports(self):
        updates = get_updates(None, None, None, None, None,
                              ('80-http', '9200-http'), None)
        self.assertEquals(len(updates), 1)
        ports = updates['public_ports']
        self.assertEquals(len(ports), 2)
        self.assertEquals(ports[80], 'HTTP')
        self.assertEquals(ports[9200], 'HTTP')

    def test_get_updates_public_ports_invalid(self):
        updates = get_updates(None, None, None, None, None, ('swag',), None)
        self.assertEquals(len(updates), 0)

    def test_get_updates_private_ports(self):
        updates = get_updates(None, None, None, None, None, None, ('9300-tcp',))
        self.assertEquals(len(updates), 1)
        ports = updates['private_ports']
        self.assertEquals(len(ports), 1)
        self.assertEquals(ports[9300], ['TCP'])

    def test_get_updates_private_ports_invalid(self):
        updates = get_updates(None, None, None, None, None, None, ('swag',))
        self.assertEquals(len(updates), 0)

    @patch('flotilla.cli.service.DynamoDbTables')
    @patch('boto.dynamodb2.connect_to_region')
    @patch('boto.kms.connect_to_region')
    def test_configure_service(self, kms, dynamo, tables):
        configure_service(ENVIRONMENT, REGIONS, SERVICE, {})
        self.assertEquals(kms.call_count, len(REGIONS))
        self.assertEquals(dynamo.call_count, len(REGIONS))
