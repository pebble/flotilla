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
            HashKey('rev_hash')
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
        # TODO: block until tables complete?

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

