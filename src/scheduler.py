#!/usr/bin/env python

import boto.dynamodb2
from main import get_instance_id, setup_logging
from flotilla.db import DynamoDbTables, DynamoDbLocks
from flotilla.scheduler import FlotillaSchedulerDynamo, FlotillaScheduler
from flotilla.thread import RepeatingFunc

if __name__ == '__main__':
    setup_logging()

    # Identity:
    instance_id = get_instance_id()

    # AWS services:
    dynamo = boto.dynamodb2.connect_to_region('us-east-1')

    # DynamoDB:
    tables = DynamoDbTables(dynamo)
    tables.setup(['assignments', 'locks', 'services', 'status'])
    db = FlotillaSchedulerDynamo(tables.assignments, tables.services,
                                 tables.status)
    locks = DynamoDbLocks(instance_id, tables.locks)

    # Assemble into scheduler:
    scheduler = FlotillaScheduler(db, locks, lock_ttl=45)

    # Start loops:
    funcs = [
        RepeatingFunc('scheduler', scheduler.loop, 15),
        RepeatingFunc('scheduler-lock', scheduler.lock, 15),
    ]
    map(RepeatingFunc.start, funcs)
