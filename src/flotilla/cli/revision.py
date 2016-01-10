import click
import os.path
import sys
import tarfile
from io import BytesIO
import boto.dynamodb2
import boto.kms

from flotilla.model import FlotillaUnit, FlotillaDockerService, \
    FlotillaServiceRevision
from flotilla.cli.options import *
from flotilla.db.tables import DynamoDbTables
from flotilla.client.db import FlotillaClientDynamo


@click.group()
def revision_cmd():  # pragma: no cover
    pass


@revision_cmd.command(help='Add a revision.')
@click.option('--environment', type=click.STRING, envvar='FLOTILLA_ENV',
              default=DEFAULT_ENVIRONMENT, help='Environment name.')
@click.option('--region', '-r', multiple=True, type=click.Choice(REGIONS),
              envvar='FLOTILLA_REGION', default=DEFAULT_REGIONS,
              help='Regions (multiple allowed).')
@click.option('--name', type=click.STRING, help='Service name.')
@click.option('--label', type=click.STRING, help='Revision label.')
def revision(environment, region, name, label):  # pragma: no cover
    add_revision(environment, region, name, label, sys.stdin)


def add_revision(environment, regions, service_name, label, stream_in):
    files = files_from_tar(stream_in)
    units = get_units(files)

    service_revision = FlotillaServiceRevision(label, units=units)

    for region in regions:
        kms = boto.kms.connect_to_region(region)

        dynamo = boto.dynamodb2.connect_to_region(region)
        tables = DynamoDbTables(dynamo, environment=environment)

        tables.setup(['revisions', 'services', 'units'])
        db = FlotillaClientDynamo(None, None, tables.revisions, tables.services,
                                  tables.units, kms)

        db.add_revision(service_name, service_revision)


def files_from_tar(tar_in):
    tar_contents = {}
    if tar_in.isatty():
        return tar_contents

    try:
        tar_bytes = BytesIO(tar_in.read())
        with tarfile.open(mode='r', fileobj=tar_bytes) as tar:
            for member in tar.getmembers():
                tar_contents[member.name] = tar.extractfile(member).read()
    except:
        pass

    return tar_contents


def parse_env(contents):
    env = {}
    for line in contents.split('\n'):
        if line.startswith('#'):
            continue

        equals_pos = line.find('=')
        if equals_pos < 0:
            continue
        env[line[:equals_pos]] = line[equals_pos + 1:]

    return env


def get_units(files):
    services = {}
    environments = {}

    for path, contents in files.items():
        filename = os.path.basename(path)
        name, extension = os.path.splitext(filename)

        if extension == '.service' and contents:
            services[name] = contents
        elif extension == '.env':
            environments[name] = parse_env(contents)

    units = []
    for name, service in services.items():
        env = environments.get(name, {})
        unit = FlotillaUnit('%s.service' % name, service, env)
        units.append(unit)

    for name, env in environments.items():
        if name in services:
            continue

        image = env.get('DOCKER_IMAGE')
        if not image:
            continue
        del env['DOCKER_IMAGE']

        ports = {}
        for key, value in env.items():
            try:
                if key.startswith('DOCKER_PORT_'):
                    ports[int(key[12:])] = int(value)
                    del env[key]
            except:
                continue

        logdriver = env.get('DOCKER_LOG_DRIVER')
        if logdriver:
            del env['DOCKER_LOG_DRIVER']

        unit = FlotillaDockerService('%s.service' % name, image, ports=ports,
                                     environment=env, logdriver=logdriver)
        units.append(unit)

    return units
