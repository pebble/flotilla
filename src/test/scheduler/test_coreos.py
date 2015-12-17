import unittest
from urllib2 import HTTPError

from flotilla.scheduler.coreos import CoreOsAmiIndex
from mock import MagicMock, patch

CHANNEL = 'stable'
VERSION = '835.9.0'
REGION = 'us-east-1'


@patch('urllib2.urlopen')
class TestCoreOsAmiIndex(unittest.TestCase):
    def setUp(self):
        self.coreos = CoreOsAmiIndex()

    def test_get_ami(self, mock_open):
        self._mock_amis(mock_open)

        ami = self.coreos.get_ami(CHANNEL, VERSION, REGION)
        self.assertEqual('ami-123456', ami)

    def test_get_ami_not_found(self, mock_open):
        mock_open.side_effect = HTTPError('url', 404, 'Not Found', {}, None)

        ami = self.coreos.get_ami(CHANNEL, VERSION, REGION)
        self.assertIsNone(ami)

    def test_get_ami_cache(self, mock_open):
        self._mock_amis(mock_open)

        ami = self.coreos.get_ami(CHANNEL, VERSION, REGION)
        self.assertEqual('ami-123456', ami)

        ami = self.coreos.get_ami(CHANNEL, VERSION, REGION)
        self.assertEqual('ami-123456', ami)
        self.assertEquals(1, mock_open.call_count)

    @staticmethod
    def _mock_amis(mock_open):
        ami_list = '{"amis":[{"name":"us-east-1","hvm":"ami-123456"}]}'
        mock_response = MagicMock()
        mock_response.read.return_value = ami_list
        mock_open.return_value = mock_response
