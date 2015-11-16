import json
import logging
import time
from boto.dynamodb2.fields import HashKey, RangeKey
from boto.dynamodb2.table import Table
from boto.dynamodb2.exceptions import *
from flotilla import FlotillaUnit

logger = logging.getLogger('git-deploy')


class DynamoDbFlotillaStorage(object):
    def __init__(self, instance_id, dynamo):
        self._id = instance_id
        self._dynamo = dynamo

        self._services = self._table('flotilla-services', [
            HashKey('service_name')
        ], 1, 1)
        self._revisions = self._table('flotilla-service-revisions', [
            HashKey('label')
        ], 1, 1)
        self._units = self._table('flotilla-units', [
            HashKey('unit_hash')
        ], 1, 1)
        self._assignments = self._table('flotilla-service-assignments', [
            HashKey('service_name'),
            RangeKey('instance_id'),
        ], 1, 1)
        self._status = self._table('flotilla-status', [
            HashKey('instance_id')
        ], 1, 1)

        self._locks = self._table('flotilla-locks', [
            HashKey('lock_name')
        ], 1, 1)

    def _table(self, name, schema, read, write):
        table = Table(name, connection=self._dynamo)
        try:
            table.describe()
            return table
        except Exception as e:
            if e.error_code != 'ResourceNotFoundException':
                raise e
            return Table.create(name, schema=schema, throughput={
                'read': read,
                'write': write
            }, connection=self._dynamo)

    def add_revision(self, service, revision):
        # Store units:
        with self._units.batch_write() as batch:
            for unit in revision.units:
                unit_hash = unit.unit_hash
                if self._units.has_item(unit_hash=unit_hash):
                    logger.debug('Unit %s exists.', unit_hash)
                    continue

                logger.debug('Adding unit %s.', unit_hash)
                unit_item = self._units.new_item(unit_hash)
                unit_item['name'] = unit.name
                unit_item['unit_file'] = unit.unit_file
                if unit.environment:
                    # TODO: KMS encrypt
                    unit_item['environment'] = unit.environment
                batch.put_item(data=unit_item)

        # Link units to revision + label:
        label = revision.label
        try:
            rev_item = self._revisions.get_item(label=label)
        except ItemNotFound:
            rev_item = self._revisions.new_item(label)
        rev_item['units'] = [unit.unit_hash for unit in revision.units]
        rev_item.save()

        # Link revision to service + weight:
        try:
            rev_item = self._services.get_item(service_name=service)
        except ItemNotFound:
            rev_item = self._services.new_item(service)
        rev_item['__rev__%s' % label] = revision.weight
        rev_item.save()

    def del_revision(self, service, label):
        try:
            rev_item = self._services.get_item(service_name=service)
        except ItemNotFound:
            return
        item_key = '__rev__%s' % label
        if item_key in rev_item:
            del rev_item[item_key]
            rev_item.save()

    def set_revision_weight(self, service, label, weight):
        try:
            rev_item = self._services.get_item(service_name=service)
        except ItemNotFound:
            return
        del rev_item['__rev__%s' % label]
        rev_item.partial_save()

    def get_revision_weights(self):
        # TODO: use segments+total_segments to shard schedulers by service
        services = {}
        for service_item in self._services.scan():
            rev_weights = {}
            for key, value in service_item.items():
                if key.startswith('__rev__'):
                    rev_label = key[7:]
                    rev_weights[rev_label] = int(value)
            name = service_item['service_name']
            services[name] = rev_weights
        return services

    def heartbeat(self, service):
        logger.debug('Storing heartbeat for %s as %s', service, self._id)
        now = time.time()
        try:
            assignment_item = self._assignments.get_item(service_name=service,
                                                         instance_id=self._id)
            assignment_item['heartbeat'] = now
            assignment_item.partial_save()
        except ItemNotFound:
            self._assignments.put_item(data={
                'service_name': service,
                'instance_id': self._id,
                'heartbeat': now
            })
        logger.debug('Stored heartbeat for %s as %s', service, self._id)

    def get_units(self, service):
        units = []

        assignment_item = self._assignments.get_item(service_name=service,
                                                     instance_id=self._id)
        assigned_revision = assignment_item['assignment']
        if assigned_revision:
            revision_item = self._revisions.get_item(label=assigned_revision)
            unit_items = self._units.batch_get(keys=[
                {'unit_hash': unit for unit in revision_item['units']}
            ])
            for unit_item in unit_items:
                units.append(FlotillaUnit(unit_item['name'],
                                          unit_item['unit_file'],
                                          unit_item['environment']))

                # Select global services
                # Select services that are assigned to the instance
        return units

    def store_status(self, unit_status):
        logger.debug('Storing status as %s.', self._id)
        data = {name: json.dumps(status)
                for name, status in unit_status.items()}
        data['instance_id'] = self._id
        data['status_time'] = time.time()
        item = self._status.put_item(data=data, overwrite=True)
        logger.debug('Stored status as %s.', self._id)

    def get_assignments(self, service):
        """Get instances and assigned revisions for a service.
        :param service:  Service name.
        :return: Dict of instance id to assigned revision.
        """
        return {a['instance_id']: a
                for a in self._assignments.query_2(service_name__eq=service)}

    def get_assignment(self, service):
        try:
            assignment = self._assignments.get_item(service_name=service,
                                                    instance_id=self._id)
            return assignment['assignment']
        except ItemNotFound:
            pass
        return None

    def set_assignment(self, service, machine, assignment):
        self._assignments.put_item(data={
            'service_name': service,
            'instance_id': machine,
            'assignment': assignment
        }, overwrite=True)

    def set_assignments(self, assignments):
        """Store assignments in a batch.
        :param assignments: Assignments to store.
        """
        with self._assignments.batch_write() as batch:
            for assignment in assignments:
                batch.put_item(assignment)

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
                lock_item['acquire_time'] = acquire_time;
                lock_item.save()

            return True
        else:
            logger.debug('Lock %s is held by %s (since %s).', name, owner,
                         acquired_time)
            return False

    def release_lock(self, name):
        try:
            lock_item = self._locks.get_item(lock_name=name, consistent=True)
            if lock_item['owner'] == self._id:
                lock_item.delete()
        except Exception as e:
            logger.exception(e)
