import time
import unittest
from boto.dynamodb2.exceptions import ConditionalCheckFailedException, \
    ItemNotFound
from boto.dynamodb2.items import Item
from boto.dynamodb2.table import Table
from flotilla.db.lock import DynamoDbLocks
from mock import MagicMock, ANY

INSTANCE_ID = 'i-123456'
OTHER_OWNER = 'i-654321'
LOCK_NAME = 'test-lock'
CONDITIONAL_EXCEPTION = ConditionalCheckFailedException(400, 'Kaboom')


class TestDynamoDbLocks(unittest.TestCase):
    def setUp(self):
        self.lock_table = MagicMock(spec=Table)
        self.lock_data = {
            'owner': INSTANCE_ID,
            'acquire_time': time.time()
        }
        self.lock_item = MagicMock(spec=Item)
        self.lock_item.__getitem__.side_effect = self.lock_data.__getitem__
        self.lock_item.__setitem__.side_effect = self.lock_data.__setitem__
        self.lock_table.get_item.return_value = self.lock_item

        self.locks = DynamoDbLocks(INSTANCE_ID, self.lock_table)

    def test_release_lock_found(self):
        self.locks.release_lock(LOCK_NAME)
        self.lock_item.delete.assert_called_with()

    def test_release_lock_not_found(self):
        self.lock_table.get_item.side_effect = ItemNotFound()
        self.locks.release_lock(LOCK_NAME)

    def test_release_not_owner(self):
        self.lock_data['owner'] = OTHER_OWNER
        self.locks.release_lock(LOCK_NAME)
        self.lock_item.delete.assert_not_called()

    def test_try_lock_no_owner(self):
        self.lock_table.get_item.side_effect = ItemNotFound()
        locked = self.locks.try_lock(LOCK_NAME)
        self.assertTrue(locked)
        self.lock_table.put_item.assert_called_with(ANY)

    def test_try_lock_no_owner_conflict(self):
        self.lock_table.get_item.side_effect = ItemNotFound()

        self.lock_table.put_item.side_effect = CONDITIONAL_EXCEPTION
        locked = self.locks.try_lock(LOCK_NAME)
        self.assertFalse(locked)

    def test_try_lock_already_held(self):
        locked = self.locks.try_lock(LOCK_NAME)
        self.assertTrue(locked)
        self.lock_item.save.assert_not_called()

    def test_try_lock_already_held_refresh(self):
        locked = self.locks.try_lock(LOCK_NAME, refresh=True)
        self.assertTrue(locked)
        self.lock_item.save.assert_called_with()

    def test_try_lock_held_by_other(self):
        self.lock_data['owner'] = OTHER_OWNER
        locked = self.locks.try_lock(LOCK_NAME)
        self.assertFalse(locked)

    def test_try_lock_held_by_other_timeout(self):
        self.lock_data['owner'] = OTHER_OWNER
        self.lock_data['acquire_time'] = time.time() - 10

        locked = self.locks.try_lock(LOCK_NAME, ttl=5)
        self.assertTrue(locked)

    def test_try_lock_held_by_other_timeout_conflict(self):
        self.lock_data['owner'] = OTHER_OWNER
        self.lock_data['acquire_time'] = time.time() - 10
        self.lock_item.save.side_effect = CONDITIONAL_EXCEPTION

        locked = self.locks.try_lock(LOCK_NAME, ttl=5)
        self.assertFalse(locked)

    def test_get_owner_found(self):
        owner, acquire_time = self.locks.get_owner(LOCK_NAME)
        self.assertEquals(owner, INSTANCE_ID)

    def test_get_owner_not_found(self):
        self.lock_table.get_item.side_effect = ItemNotFound()
        owner, acquire_time = self.locks.get_owner(LOCK_NAME)
        self.assertEquals(owner, None)
