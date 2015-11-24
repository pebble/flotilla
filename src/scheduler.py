#!/usr/bin/env python

import boto.dynamodb2
from main import get_instance_id, setup_logging
from flotilla.db import DynamoDbTables
from flotilla.scheduler import FlotillaSchedulerDynamo, FlotillaScheduler

if __name__ == '__main__':
    setup_logging()

    dynamo = boto.dynamodb2.connect_to_region('us-east-1')

    tables = DynamoDbTables(dynamo)
    tables.setup(['assignments', 'locks', 'services', 'status'])

    db = FlotillaSchedulerDynamo(tables.assignments, tables.services,
                                 tables.status)

    scheduler = FlotillaScheduler(db)
    scheduler.active = True

    scheduler.loop()

    # print db.get_instances('test')
