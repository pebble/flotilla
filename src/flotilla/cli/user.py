import click
import logging
import boto.dynamodb2

from flotilla.cli.options import *
from flotilla.db import DynamoDbTables
from flotilla.client import FlotillaClientDynamo

logger = logging.getLogger('flotilla')


@click.group()
def user_cmd():  # pragma: no cover
    pass


@user_cmd.command(help='Configure a user.')
@click.option('--environment', type=click.STRING, envvar='FLOTILLA_ENV',
              default=DEFAULT_ENVIRONMENT, help='Environment name.')
@click.option('--region', '-r', multiple=True, type=click.Choice(REGIONS),
              envvar='FLOTILLA_REGION', default=DEFAULT_REGIONS,
              help='Regions (multiple allowed).')
@click.option('--name', type=click.STRING, help='User name.')
@click.option('--ssh-key', type=click.STRING, multiple=True,
              help='User SSH key(s).')
@click.option('--active/--inactive', default=None,
              help='Disable automatic provisioning.')
def user(environment, region, name, ssh_key, active):  # pragma: no cover
    configure_user(environment, region, name, ssh_key, active)


def get_updates(ssh_keys, active):
    updates = {}
    if len(ssh_keys) > 0:
        updates['ssh_keys'] = list(ssh_keys)
    if active is not None:
        updates['active'] = active
    return updates


def configure_user(environment, regions, name, ssh_keys, active):
    updates = get_updates(ssh_keys, active)
    if not updates:
        logger.warn('No updates to do!')
        return

    for region in regions:
        dynamo = boto.dynamodb2.connect_to_region(region)
        tables = DynamoDbTables(dynamo, environment=environment)
        tables.setup(['users'])
        db = FlotillaClientDynamo(None, None, None, None, None, tables.users,
                                  None)

        db.configure_user(name, updates)
    logger.info('User: %s updated in region(s): %s updated.', name,
                ', '.join(regions))
