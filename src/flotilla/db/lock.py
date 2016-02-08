import logging

logger = logging.getLogger('flotilla')

import time
from boto.dynamodb2.exceptions import ConditionalCheckFailedException, \
    ItemNotFound


class DynamoDbLocks(object):
    def __init__(self, instance_id, lock_table):
        self._id = instance_id
        self._locks = lock_table

    def try_lock(self, name, ttl=60, refresh=False):
        acquire_time = time.time()

        try:
            lock_item = self._locks.get_item(lock_name=name, consistent=True)
        except ItemNotFound:
            logger.debug('Lock %s not found, creating.', name)
            try:
                self._locks.put_item({
                    'lock_name': name,
                    'acquire_time': acquire_time,
                    'owner': self._id,
                })
                return True
            except Exception as e:
                logger.exception(e)
                return False

        # Lock found, check ttl:
        acquired_time = float(lock_item['acquire_time'])
        if (acquire_time - acquired_time) > ttl:
            logger.debug('Lock %s has expired, attempting to acquire.', name)
            lock_item['owner'] = self._id
            lock_item['acquire_time'] = acquire_time
            try:
                lock_item.save()
                logger.debug('Acquired expired lock %s.', name)
                return True
            except ConditionalCheckFailedException:
                logger.debug('Did not acquire expired lock %s.', name)
                return False

        owner = lock_item['owner']
        if owner == self._id:
            logger.debug('Lock %s is held by me (since %s).', name,
                         acquired_time)
            if refresh:
                lock_item['acquire_time'] = acquire_time
                lock_item.save()

            return True
        else:
            logger.debug('Lock %s is held by %s (since %s).', name, owner,
                         acquired_time)
            return False

    def release_lock(self, name):
        try:
            logger.debug('Looking up lock %s to release', name)
            lock_item = self._locks.get_item(lock_name=name, consistent=True)
            owner = lock_item['owner']
            if owner == self._id:
                logger.debug('Lock belongs to me, releasing')
                lock_item.delete()
            else:
                logger.debug('Lock belongs to %s, unable to release', owner)
        except ItemNotFound:
            logger.debug('Lock %s not found to release', name)

    def get_owner(self, name):
        try:
            lock_item = self._locks.get_item(lock_name=name, consistent=True)
            return lock_item['owner'], float(lock_item['acquire_time'])
        except ItemNotFound:
            return None, None
