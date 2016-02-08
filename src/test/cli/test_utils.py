import unittest
from mock import MagicMock

from flotilla.cli.utils import get_queue, QUEUE_NOT_FOUND
from botocore.exceptions import ClientError

QUEUE_NAME = 'test-queue'


class TestUtils(unittest.TestCase):
    def setUp(self):
        self.sqs = MagicMock()

    def test_get_queue_found(self):
        queue = get_queue(self.sqs, QUEUE_NAME)
        self.assertIsNotNone(queue)

    def test_get_queue_not_found(self):
        client_error = ClientError({'Error': {'Code': QUEUE_NOT_FOUND}}, '')
        self.sqs.get_queue_by_name.side_effect = client_error
        queue = get_queue(self.sqs, QUEUE_NAME)
        self.assertIsNone(queue)

    def test_get_queue_error(self):
        client_error = ClientError({'Error': {}}, '')
        self.sqs.get_queue_by_name.side_effect = client_error
        self.assertRaises(ClientError, get_queue, self.sqs, QUEUE_NAME)
