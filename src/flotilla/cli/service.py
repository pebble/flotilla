import click
import logging
import boto.dynamodb2
import boto.kms

from collections import defaultdict

from main import setup_logging, REGIONS, INSTANCE_TYPES, DEFAULT_REGIONS, \
    DEFAULT_ENVIRONMENT
from flotilla.db import DynamoDbTables
from flotilla.client import FlotillaClientDynamo

logger = logging.getLogger('flotilla')

ELB_SCHEMES = ('internal', 'internet-facing')


@click.group()
def service_cmd():  # pragma: no cover
    pass


@service_cmd.command(help='Configure a service.')
@click.option('--environment', type=click.STRING, envvar='FLOTILLA_ENV',
              default=DEFAULT_ENVIRONMENT, help='Environment name.')
@click.option('--region', '-r', multiple=True, type=click.Choice(REGIONS),
              envvar='FLOTILLA_REGION', default=DEFAULT_REGIONS,
              help='Regions (multiple allowed).')
@click.option('--name', type=click.STRING, help='Service name.')
@click.option('--elb-scheme', type=click.Choice(ELB_SCHEMES),
              help='ELB scheme.')
@click.option('--dns-name', type=click.STRING,
              help='Custom DNS entry for service')
@click.option('--health-check', type=click.STRING,
              help='ELB health check target, http://goo.gl/6ue44c .')
@click.option('--instance-type', type=click.Choice(INSTANCE_TYPES),
              help='Worker instance type.')
@click.option('--provision/--no-provision', default=None,
              help='Disable automatic provisioning.')
@click.option('--public-ports', type=click.STRING, multiple=True,
              help='Public ports, exposed by ELB. e.g. 80-http, 6379-tcp', )
@click.option('--private-ports', type=click.STRING, multiple=True,
              help='Private ports, exposed to peers. e.g. 9300-tcp, 9200-tcp')
def service(environment, region, name, elb_scheme, dns_name, health_check,
            instance_type, provision, public_ports,
            private_ports):  # pragma: no cover
    setup_logging()
    updates = get_updates(elb_scheme, dns_name, health_check, instance_type,
                          provision, public_ports, private_ports)

    if not updates:
        logger.warn('No updates to do!')
        return

    configure_service(environment, region, name, updates)
    logger.info('Service %s updated in: %s', name, ', '.join(region))


def get_updates(elb_scheme, dns, health_check, instance_type, provision,
                public_ports, private_ports):
    updates = {}
    if elb_scheme:
        updates['elb_scheme'] = elb_scheme
    if dns:
        updates['dns_name'] = dns
    if health_check:
        updates['health_check'] = health_check
    if instance_type:
        updates['instance_type'] = instance_type
    if provision is not None:
        updates['provision'] = provision
    if public_ports:
        parsed_ports = {}
        for public_port in public_ports:
            try:
                port, proto = public_port.split('-')
                parsed_ports[int(port)] = proto.upper()
            except:
                continue
        if parsed_ports:
            updates['public_ports'] = parsed_ports
    if private_ports:
        parsed_ports = defaultdict(list)
        for private_port in private_ports:
            try:
                port, proto = private_port.split('-')
                parsed_ports[int(port)].append(proto.upper())
            except:
                continue
        if parsed_ports:
            updates['private_ports'] = dict(parsed_ports)
    return updates


def configure_service(environment, regions, service_name, updates):
    for region in regions:
        dynamo = boto.dynamodb2.connect_to_region(region)
        kms = boto.kms.connect_to_region(region)
        tables = DynamoDbTables(dynamo, environment=environment)
        tables.setup(['assignments', 'regions', 'revisions', 'services',
                      'units'])
        db = FlotillaClientDynamo(tables.assignments, tables.regions,
                                  tables.revisions, tables.services,
                                  tables.units, kms)

        db.configure_service(service_name, updates)
