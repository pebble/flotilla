import unittest
from mock import MagicMock, patch, ANY

from flotilla.client.region_meta import RegionMetadata
from flotilla.scheduler.cloudformation import FlotillaCloudFormation
from flotilla.cli.init import bootstrap

REGIONS = ['us-east-1']
ENVIRONMENT = 'develop'
DOMAIN = 'test.com'
AVAILABLE = False
CONTAINER = 'pwagner/flotilla'
INSTANCE_TYPE = 't2.nano'
COREOS_CHANNEL = 'stable'
COREOS_VERSION = 'current'


class TestInit(unittest.TestCase):
    @patch('flotilla.cli.init.FlotillaCloudFormation')
    @patch('flotilla.cli.init.RegionMetadata')
    def test_bootstrap(self, region, cloudformation):
        mock_region = MagicMock(spec=RegionMetadata)
        mock_cf = MagicMock(spec=FlotillaCloudFormation)
        region.return_value = mock_region
        cloudformation.return_value = mock_cf

        bootstrap(REGIONS, ENVIRONMENT, DOMAIN, INSTANCE_TYPE, COREOS_CHANNEL,
                  COREOS_VERSION, AVAILABLE, CONTAINER)

        region.assert_called_with(ENVIRONMENT)
        mock_region.store_regions.assert_called_with(ANY,
                                                     AVAILABLE,
                                                     INSTANCE_TYPE,
                                                     COREOS_CHANNEL,
                                                     COREOS_VERSION,
                                                     CONTAINER)

        cloudformation.assert_called_with(ENVIRONMENT, DOMAIN, ANY)
        mock_cf.tables.assert_called_with(REGIONS)
