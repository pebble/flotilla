import logging
import time
from boto.dynamodb2.table import Table
from boto.dynamodb2.fields import HashKey, RangeKey

logger = logging.getLogger('flotilla')

SCHEMAS = {
    'assignments': [HashKey('instance_id')],
    'locks': [HashKey('lock_name')],
    'revisions': [HashKey('rev_hash')],
    'services': [HashKey('service_name')],
    'stacks': [HashKey('stack_arn')],
    'status': [HashKey('service'), RangeKey('instance_id')],
    'units': [HashKey('unit_hash')]
}


class DynamoDbTables(object):
    def __init__(self, dynamo, environment=None):
        self._dynamo = dynamo
        if environment:
            self._prefix = 'flotilla-{0}-'.format(environment)
        else:
            self._prefix = 'flotilla-'
        self.assignments = None
        self.locks = None
        self.revisions = None
        self.services = None
        self.stacks = None
        self.status = None
        self.units = None

    def setup(self, tables):
        tables = [t for t in tables if t in SCHEMAS]
        for table_name in tables:
            full_name = self._prefix + table_name
            table = self._table(full_name, SCHEMAS[table_name], 1, 1)
            setattr(self, table_name, table)

        for table_name in tables:
            table = getattr(self, table_name)
            table_status = table.describe()['Table']['TableStatus']
            while table_status != 'ACTIVE':
                time.sleep(0.5)
                table_status = table.describe()['Table']['TableStatus']

    def _table(self, name, schema, read, write):
        table = Table(name, connection=self._dynamo)
        try:
            table.describe()
            return table
        except Exception as e:
            if e.error_code != 'ResourceNotFoundException':
                raise e
            logger.debug('Creating table %s', name)
            table = Table.create(name, schema=schema, throughput={
                'read': read,
                'write': write
            }, connection=self._dynamo)
            return table
