import click
import logging
import boto.dynamodb2

from main import get_instance_id, setup_logging, REGIONS, DEFAULT_REGIONS, \
    DEFAULT_ENVIRONMENT
from flotilla.db import DynamoDbTables, DynamoDbLocks
from flotilla.scheduler import FlotillaCloudFormation, FlotillaSchedulerDynamo, \
    FlotillaScheduler, CoreOsAmiIndex, FlotillaProvisioner
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
              help='Frequency of health writes (seconds).')
@click.option('--loop-interval', type=click.INT,
              envvar='FLOTILLA_LOOP_INTERVAL', default=15,
              help='Frequency of assignment reads (seconds).')
@click.option('--provision-interval', type=click.INT,
              envvar='FLOTILLA_PROVISION_INTERVAL', default=15,
              help='Frequency of assignment reads (seconds).')
def scheduler(environment, domain, region, lock_interval, loop_interval,
              provision_interval):  # pragma: no cover
    setup_logging()
    start_scheduler(environment, domain, region, lock_interval, loop_interval,
                    provision_interval)


def start_scheduler(environment, domain, regions, lock_interval, loop_interval,
                    provision_interval):
    instance_id = get_instance_id()

    # AWS services:
    db_region = regions[0]
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
    schedule = FlotillaScheduler(db, locks, lock_ttl=45)
    provisioner = FlotillaProvisioner(environment, schedule, db,
                                      cloudformation)

    # Start loops:
    funcs = [
        RepeatingFunc('scheduler-lock', schedule.lock, lock_interval),
        RepeatingFunc('scheduler', schedule.loop, loop_interval),
        RepeatingFunc('provisioner', provisioner.provision, provision_interval)
    ]
    map(RepeatingFunc.start, funcs)
