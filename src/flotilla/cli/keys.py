import click
from flotilla.cli.options import *
from flotilla.db import DynamoDbTables
from flotilla.ssh import FlotillaSshDynamo

import boto.dynamodb2


@click.group()
def keys_cmd():  # pragma: no cover
    pass


@keys_cmd.command(
        help='Get authorized SSH keys, intended as AuthorizedKeysCommand.')
@click.option('--environment', type=click.STRING, envvar='FLOTILLA_ENV',
              default=DEFAULT_ENVIRONMENT, help='Environment name.')
@click.option('--region', '-r', type=click.Choice(REGIONS),
              envvar='FLOTILLA_REGION', default=DEFAULT_REGIONS[0],
              help='Regions (multiple allowed).')
@click.option('--service', type=click.STRING, help='Service name')
@click.option('--bastion', is_flag=True, default=False,
              help='Bastion instance flag.')
def keys(environment, region, service, bastion):  # pragma: no cover
    get_keys(environment, region, service, bastion)


def get_keys(environment, region, service, bastion):
    dynamo = boto.dynamodb2.connect_to_region(region)
    tables = DynamoDbTables(dynamo, environment=environment)
    tables.setup(['regions', 'services', 'users'])
    db = FlotillaSshDynamo(tables.regions, tables.services, tables.users,
                           region)
    if service is not None:
        users = db.get_service_admins(service)
    elif bastion:
        users = db.get_bastion_users()
    else:
        users = db.get_region_admins()

    ssh_keys = db.get_keys(users)
    print '\n'.join(ssh_keys)
