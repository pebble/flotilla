import click
import logging
import boto.dynamodb2

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
@click.option('--bastion-instance-type', type=click.Choice(INSTANCE_TYPES),
              help='Bastion instance type.')
@click.option('--bastion-coreos-channel', type=click.Choice(COREOS_CHANNELS),
              help='Bastion instance CoreOS channel.')
@click.option('--bastion-coreos-version', type=click.STRING,
              help='Bastion instance CoreOS version.')
@click.option('--admin', type=click.STRING, multiple=True,
              help='Administrative user(s).')
def region(environment, region, bastion_instance_type, bastion_coreos_channel,
           bastion_coreos_version, admin):  # pragma: no cover
    configure_region(environment, region, bastion_instance_type,
                     bastion_coreos_channel, bastion_coreos_version, admin)


def get_updates(instance_type, coreos_channel, coreos_version, admins):
    updates = {}
    if instance_type:
        updates['bastion_instance_type'] = instance_type
    if coreos_channel:
        updates['bastion_coreos_channel'] = coreos_channel
    if coreos_version:
        updates['bastion_coreos_version'] = coreos_version
    if len(admins) > 0:
        updates['admins'] = list(admins)
    return updates


def configure_region(environment, regions, bastion_instance_type,
                     bastion_coreos_channel, bastion_coreos_version, admins):
    if not regions:
        logger.warn('Must specify region(s) to update.')
        return

    updates = get_updates(bastion_instance_type, bastion_coreos_channel,
                          bastion_coreos_version, admins)

    if not updates:
        logger.warn('No updates to do!')
        return

    for aws_region in regions:
        dynamo = boto.dynamodb2.connect_to_region(aws_region)

        tables = DynamoDbTables(dynamo, environment=environment)
        tables.setup(['regions', 'users'])

        db = FlotillaClientDynamo(None, tables.regions, None, None, None,
                                  tables.users, None)

        admins = updates.get('admins')
        if admins:
            missing_users = db.check_users(admins)
            if len(missing_users) > 0:
                logger.error('User(s): %s do not exist in %s.',
                             ', '.join(missing_users), region)
                continue
        db.configure_region(aws_region, updates)
    logger.info('Region(s): %s updated.', ', '.join(regions))
