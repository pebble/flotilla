#!/usr/bin/env python

import os
import boto.ec2.elb
from systemd.manager import Manager
from main import get_instance_id, setup_logging
from flotilla.agent import FlotillaAgent, FlotillaAgentDynamo, LoadBalancer, \
    SystemdUnits
from flotilla.db import DynamoDbTables, DynamoDbLocks
from flotilla.thread import RepeatingFunc


def get_elb(local_id):
    elb_name = os.environ.get('FLOTILLA_LB')
    if elb_name:
        elb_region = os.environ.get('FLOTILLA_LB_REGION', 'us-west-2')
        elb = boto.ec2.elb.connect_to_region(elb_region)
        return LoadBalancer(local_id, elb_name, elb)
    else:
        return None


if __name__ == '__main__':
    setup_logging()

    # Identity:
    instance_id = get_instance_id()
    service = os.environ['FLOTILLA_SERVICE']
    environment = os.environ.get('FLOTILLA_ENV')

    # Systemd:
    manager = Manager()
    systemd = SystemdUnits(manager)

    # AWS services:
    lb = get_elb(instance_id)
    db_region = os.environ.get('FLOTILLA_REGION', 'us-east-1')
    dynamo = boto.dynamodb2.connect_to_region(db_region)

    # DynamoDB:
    tables = DynamoDbTables(dynamo, environment=environment)
    tables.setup(['status', 'assignments', 'revisions', 'units', 'locks'])
    db = FlotillaAgentDynamo(instance_id, service, tables.status,
                             tables.assignments, tables.revisions,
                             tables.units)
    locks = DynamoDbLocks(instance_id, tables.locks)

    # Assemble into agent:
    agent = FlotillaAgent(service, db, locks, systemd, lb)

    # Start loops:
    funcs = [
        RepeatingFunc('health', agent.health, 15),
        RepeatingFunc('assignment', agent.assignment, 15),
    ]
    map(RepeatingFunc.start, funcs)
