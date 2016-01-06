import unittest
from mock import MagicMock, patch, ANY

from flotilla.client.region_meta import RegionMetadata
from flotilla.scheduler.cloudformation import FlotillaCloudFormation
from flotilla.cli.init import bootstrap

REGIONS = ['us-east-1']
ENVIRONMENT = 'develop'
DOMAIN = 'test.com'


class TestInit(unittest.TestCase):
    @patch('flotilla.cli.init.FlotillaCloudFormation')
    @patch('flotilla.cli.init.RegionMetadata')
    def test_bootstrap(self, region, cloudformation):
        mock_region = MagicMock(spec=RegionMetadata)
        mock_cf = MagicMock(spec=FlotillaCloudFormation)
        region.return_value = mock_region
        cloudformation.return_value = mock_cf

        bootstrap(REGIONS, ENVIRONMENT, DOMAIN, 't2.nano', 'stable',
                  'current', False)

        region.assert_called_with(ENVIRONMENT)
        mock_region.store_regions.assert_called_with(ANY, ANY, ANY, ANY, ANY)

        cloudformation.assert_called_with(ENVIRONMENT, DOMAIN, ANY)
        mock_cf.tables.assert_called_with(REGIONS)
