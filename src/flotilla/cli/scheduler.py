import click
import logging
import boto.dynamodb2
import boto.sqs

from main import get_instance_id, setup_logging, REGIONS, DEFAULT_REGIONS, \
    DEFAULT_ENVIRONMENT
from flotilla.db import DynamoDbTables, DynamoDbLocks
from flotilla.scheduler import *
from flotilla.thread import RepeatingFunc

logger = logging.getLogger('flotilla')


@click.group()
def scheduler_cmd():  # pragma: no cover
    pass


@scheduler_cmd.command(help='Start as scheduler.')
@click.option('--environment', type=click.STRING, envvar='FLOTILLA_ENV',
              default=DEFAULT_ENVIRONMENT, help='Environment name.')
@click.option('--domain', envvar='FLOTILLA_DOMAIN', help='Domain name.')
@click.option('--region', multiple=True, type=click.Choice(REGIONS),
              envvar='FLOTILLA_REGION', default=DEFAULT_REGIONS,
              help='Regions (multiple allowed).')
@click.option('--lock-interval', type=click.INT,
              envvar='FLOTILLA_LOCK_INTERVAL', default=15,
              help='Frequency of lock check (seconds).')
@click.option('--loop-interval', type=click.INT,
              envvar='FLOTILLA_LOOP_INTERVAL', default=15,
              help='Frequency of scheduler loop (seconds).')
@click.option('--provision-interval', type=click.INT,
              envvar='FLOTILLA_PROVISION_INTERVAL', default=15,
              help='Frequency of provision loop (seconds).')
def scheduler(environment, domain, region, lock_interval, loop_interval,
              provision_interval):  # pragma: no cover
    setup_logging()
    start_scheduler(environment, domain, region, lock_interval, loop_interval,
                    provision_interval)


def start_scheduler(environment, domain, regions, lock_interval, loop_interval,
                    provision_interval):
    instance_id = get_instance_id()

    coreos = CoreOsAmiIndex()
    cloudformation = FlotillaCloudFormation(environment, domain, coreos)

    funcs = []
    for region in regions:
        # DynamoDB:
        dynamo = boto.dynamodb2.connect_to_region(region)
        tables = DynamoDbTables(dynamo, environment=environment)
        tables.setup(['assignments', 'locks', 'regions', 'services', 'stacks',
                      'status'])
        db = FlotillaSchedulerDynamo(tables.assignments, tables.regions,
                                     tables.services, tables.stacks,
                                     tables.status)
        locks = DynamoDbLocks(instance_id, tables.locks)

        sqs = boto.sqs.connect_to_region(region)
        message_q = sqs.get_queue(
                'flotilla-%s-scheduler' % environment)

        # Assemble into scheduler:
        schedule = FlotillaScheduler(db, locks, lock_ttl=lock_interval * 3)
        provisioner = FlotillaProvisioner(environment, region, schedule, db,
                                          cloudformation)

        funcs += [
            RepeatingFunc('scheduler-lock-%s' % region, schedule.lock,
                          lock_interval),
            RepeatingFunc('scheduler-%s' % region, schedule.loop,
                          loop_interval),
            RepeatingFunc('provisioner-%s' % region, provisioner.provision,
                          provision_interval)
        ]

        if message_q:
            messaging = FlotillaSchedulerMessaging(message_q, schedule)
            funcs.append(RepeatingFunc('scheduler-message-%s' % region,
                                       messaging.receive, 20.1))

    # Start loops:
    map(RepeatingFunc.start, funcs)
