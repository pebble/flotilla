import click
import logging
import boto.dynamodb2

from main import setup_logging
from flotilla.cli.options import *
from flotilla.db import DynamoDbTables
from flotilla.client import FlotillaClientDynamo

logger = logging.getLogger('flotilla')


@click.group()
def region_cmd():  # pragma: no cover
    pass


@region_cmd.command(help='Configure region.')
@click.option('--environment', type=click.STRING, envvar='FLOTILLA_ENV',
              default=DEFAULT_ENVIRONMENT, help='Environment name.')
@click.option('--region', '-r', multiple=True, type=click.Choice(REGIONS),
              envvar='FLOTILLA_REGION',
              help='Regions (multiple allowed).')
@click.option('--nat-instance-type', type=click.Choice(INSTANCE_TYPES),
              help='NAT instance type.')
@click.option('--nat-coreos-channel', type=click.Choice(COREOS_CHANNELS),
              envvar='FLOTILLA_NAT_CHANNEL',
              help='NAT instance CoreOS channel.')
@click.option('--nat-coreos-version', type=click.STRING,
              envvar='FLOTILLA_NAT_VERSION',
              help='NAT instance CoreOS version.')
def region(environment, region, nat_instance_type, nat_coreos_channel,
           nat_coreos_version):  # pragma: no cover
    setup_logging()

    if not region:
        logger.warn('Must specify region(s) to update.')
        return

    updates = get_updates(nat_instance_type, nat_coreos_channel,
                          nat_coreos_version)

    if not updates:
        logger.warn('No updates to do!')
        return

    configure_region(environment, region, updates)
    logger.info('Regions %s updated.', ', '.join(region))


def get_updates(instance_type, coreos_channel, coreos_version):
    updates = {}
    if instance_type:
        updates['nat_instance_type'] = instance_type
    if coreos_channel:
        updates['nat_coreos_channel'] = coreos_channel
    if coreos_version:
        updates['nat_coreos_version'] = coreos_version
    return updates


def configure_region(environment, regions, updates):
    for aws_region in regions:
        dynamo = boto.dynamodb2.connect_to_region(aws_region)

        tables = DynamoDbTables(dynamo, environment=environment)
        tables.setup(['regions'])

        db = FlotillaClientDynamo(None, tables.regions, None, None, None, None)
        db.configure_region(aws_region, updates)
