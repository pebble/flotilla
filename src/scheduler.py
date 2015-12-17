#!/usr/bin/env python

import os
import boto.dynamodb2
from main import get_instance_id, setup_logging
from flotilla.db import DynamoDbTables, DynamoDbLocks
from flotilla.scheduler import FlotillaCloudFormation, FlotillaSchedulerDynamo, \
    FlotillaScheduler, CoreOsAmiIndex, FlotillaProvisioner
from flotilla.thread import RepeatingFunc

if __name__ == '__main__':
    setup_logging()

    # Identity:
    instance_id = get_instance_id()
    environment = os.environ.get('FLOTILLA_ENV')
    domain = os.environ.get('FLOTILLA_DOMAIN')

    # AWS services:
    db_region = os.environ.get('FLOTILLA_REGION', 'us-east-1')
    dynamo = boto.dynamodb2.connect_to_region(db_region)

    # DynamoDB:
    tables = DynamoDbTables(dynamo, environment=environment)
    tables.setup(['assignments', 'locks', 'regions', 'services', 'stacks',
                  'status'])
    db = FlotillaSchedulerDynamo(tables.assignments, tables.regions,
                                 tables.services, tables.stacks, tables.status)
    locks = DynamoDbLocks(instance_id, tables.locks)

    coreos = CoreOsAmiIndex()
    cloudformation = FlotillaCloudFormation(environment, domain, coreos)

    # Assemble into scheduler:
    scheduler = FlotillaScheduler(db, locks, lock_ttl=45)
    provisioner = FlotillaProvisioner(environment, scheduler, db,
                                      cloudformation)

    # Start loops:
    funcs = [
        RepeatingFunc('scheduler-lock', scheduler.lock, 15),
        RepeatingFunc('scheduler', scheduler.loop, 15),
        RepeatingFunc('provisioner', provisioner.provision, 15)
    ]
    map(RepeatingFunc.start, funcs)
