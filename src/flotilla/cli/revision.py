import click
import os.path
import sys
import tarfile
import json
from time import time, sleep
import logging
from io import BytesIO
import boto.dynamodb2
import boto.kms
import boto3

from flotilla.model import FlotillaUnit, FlotillaDockerService, \
    FlotillaServiceRevision
from flotilla.cli.options import *
from flotilla.cli.service import parse_private_ports, parse_public_ports
from flotilla.db.tables import DynamoDbTables
from flotilla.client.db import FlotillaClientDynamo
from flotilla.scheduler.db import FlotillaSchedulerDynamo
from flotilla.scheduler.doctor import ServiceDoctor

logger = logging.getLogger('flotilla')


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
@click.option('--highlander', type=click.INT, default=0,
              help='Timeout to wait for healthy service, if healthy other revisions are removed. (seconds)')
@click.option('--env-var', type=click.STRING, multiple=True,
              help='Environment variable overrides.')
def revision(environment, region, name, label, highlander,
             env_var):  # pragma: no cover
    add_revision(environment, region, name, label, env_var, highlander,
                 sys.stdin)


SERVICE_UPDATE_KEYS = (
    'ELB_SCHEME',
    'DNS_NAME',
    'HEALTH_CHECK',
    'INSTANCE_TYPE',
    'INSTANCE_MIN',
    'INSTANCE_MAX',
    'KMS_KEY',
    'COREOS_CHANNEL',
    'COREOS_VERSION'
)


def add_revision(environment, regions, service_name, label, env_vars,
                 highlander, stream_in):
    # Extract services and files from input:
    files = files_from_tar(stream_in)
    services, environments = get_services_environments(files, environment,
                                                       env_vars)

    # Extract metadata from environments:
    service_updates = extract_service_updates(environments.values())
    env_regions = extract_regions(environments.values())
    regions = set(regions) | env_regions

    # Build a ServiceRevision with services+enviroment that are left:
    units = get_units(services, environments)
    service_revision = FlotillaServiceRevision(label, units=units)

    # Add revision and perform updates in each region:
    dynamo_cache = {}
    for region in regions:
        kms = boto.kms.connect_to_region(region)

        dynamo = boto.dynamodb2.connect_to_region(region)
        tables = DynamoDbTables(dynamo, environment=environment)
        dynamo_cache[region] = tables

        tables.setup(['revisions', 'services', 'units'])
        db = FlotillaClientDynamo(None, None, tables.revisions, tables.services,
                                  tables.units, None, kms)

        db.add_revision(service_name, service_revision)
        if service_updates:
            db.configure_service(service_name, service_updates)

    if highlander > 0:
        wait_for_deployment(dynamo_cache, regions, service_name,
                            service_revision.revision_hash, highlander)


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


def get_services_environments(files, environment=None, env_vars=()):
    services = {}
    environments = {}
    for path, contents in files.items():
        filename = os.path.basename(path)
        name, extension = os.path.splitext(filename)

        if extension == '.service' and contents:
            services[name] = contents
        elif extension == '.env':
            environments[name] = parse_env(contents)
        elif extension == '.json':
            environments[name] = parse_json(contents, environment)
    for env_var in env_vars:
        var_key, var_value = env_var.split('=', 1)
        for environment in environments.values():
            environment[var_key] = var_value
    return services, environments


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


def parse_json(json_str, environment):
    body = json.loads(json_str)
    flotilla_body = body.get('flotilla')
    if not flotilla_body:
        flotilla_body = body

    defaults = flotilla_body.get('defaults', {})
    env = flotilla_body.get(environment, {})
    merged = defaults.copy()
    merged.update(env)
    return merged


def extract_service_updates(environments):
    service_updates = {}
    public_ports = []
    private_ports = []
    for environment in environments:
        for env_key, env_value in environment.items():
            if env_key in SERVICE_UPDATE_KEYS:
                service_updates[env_key.lower()] = env_value
            elif env_key.startswith('PUBLIC_PORT'):
                public_ports.append(env_value)
            elif env_key.startswith('PRIVATE_PORT'):
                private_ports.append(env_value)
            else:
                continue
            del environment[env_key]
    if public_ports:
        parsed_ports = parse_public_ports(public_ports)
        if parsed_ports:
            service_updates['public_ports'] = parsed_ports
    if private_ports:
        parsed_ports = parse_private_ports(private_ports)
        if parsed_ports:
            service_updates['private_ports'] = parsed_ports
    return service_updates


def extract_regions(environments):
    regions = set()
    for environment in environments:
        region_val = environment.get('REGION')
        if region_val:
            del environment['REGION']
            env_regions = (isinstance(region_val, str) and
                           region_val.split(',') or region_val)
            for region in env_regions:
                if region:
                    regions.add(region)
    return regions


def get_units(services, environments):
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


def wait_for_deployment(dynamo_cache, regions, service_name, rev_hash, timeout):
    logger.info('Waiting for %s in %s regions...', rev_hash, len(regions))

    # There can be only one!
    doctor_cache = {}
    for region in regions:
        tables = dynamo_cache[region]
        tables.setup(['status'])
        db = FlotillaSchedulerDynamo(None, None, tables.services, None,
                                     tables.status)
        elb = boto3.client('elb', region)
        doctor = ServiceDoctor(db, elb)
        doctor_cache[region] = doctor

    start_time = time()
    while True:
        all_healthy = True
        try:
            for region, doctor in doctor_cache.items():
                healthy = doctor.is_healthy_revision(service_name, rev_hash)
                if not healthy:
                    all_healthy = False
                    logger.info('Waiting for %s in %s...', rev_hash, region)
                    continue
                logger.info('Region %s has a health %s instance!', region,
                            rev_hash)
                doctor.db.make_only_revision(service_name, rev_hash)
        except ValueError:
            break

        if all_healthy:
            logger.info('All regions have a health %s instance!', rev_hash)
            return True
        if time() - start_time > timeout:
            break
        sleep(5)

    wait_time = time() - start_time
    logger.info('Revision %s not stable after %s seconds.', rev_hash, wait_time)
    for region, doctor in doctor_cache.items():
        service_item = doctor.db.get_service(service_name)
        if rev_hash in service_item and service_item[rev_hash] > 0:
            service_item[rev_hash] *= -1
            doctor.db.set_services([service_item])
