import boto.dynamodb2
import boto.ec2.elb
import boto.kms
import click
import logging

from main import get_instance_id, setup_logging
from flotilla.cli.options import REGIONS
from flotilla.agent import FlotillaAgent, FlotillaAgentDynamo, LoadBalancer, \
    SystemdUnits
from flotilla.db import DynamoDbTables, DynamoDbLocks
from flotilla.thread import RepeatingFunc

try:
    from systemd.manager import Manager
except ImportError:
    import mock

    Manager = mock.MagicMock()

logger = logging.getLogger('flotilla')


@click.group()
def agent_cmd():  # pragma: no cover
    pass


@agent_cmd.command(help='Start as agent.')
@click.option('--service', type=click.STRING, envvar='FLOTILLA_SERVICE',
              help='Service name.')
@click.option('--environment', type=click.STRING, envvar='FLOTILLA_ENV',
              help='Environment name.')
@click.option('--region', type=click.Choice(REGIONS), envvar='FLOTILLA_REGION',
              help='Region.')
@click.option('--elb', type=click.STRING, envvar='FLOTILLA_LB',
              help='ELB name (optional).')
@click.option('--health-interval', type=click.INT,
              envvar='FLOTILLA_HEALTH_INTERVAL', default=15,
              help='Frequency of health writes (seconds).')
@click.option('--assignment-interval', type=click.INT,
              envvar='FLOTILLA_ASSIGNMENT_INTERVAL', default=15,
              help='Frequency of assignment reads (seconds).')
def agent(service, environment, region, elb, health_interval,
          assignment_interval):  # pragma: no cover
    setup_logging()
    start_agent(environment, service, region, elb, health_interval,
                assignment_interval)


def get_elb(local_id, elb_name, elb_region):
    if elb_name:
        elb = boto.ec2.elb.connect_to_region(elb_region)
        return LoadBalancer(local_id, elb_name, elb)
    else:
        return None


def start_agent(environment, service, region, elb_name, health_interval,
                assignment_interval):
    # Identity:
    instance_id = get_instance_id()
    logger.debug('Resolved id: %s', instance_id)

    # Systemd:
    manager = Manager()
    systemd = SystemdUnits(manager)

    # AWS services:
    lb = get_elb(instance_id, elb_name, region)
    dynamo = boto.dynamodb2.connect_to_region(region)
    kms = boto.kms.connect_to_region(region)

    # DynamoDB:
    tables = DynamoDbTables(dynamo, environment=environment)
    tables.setup(['status', 'assignments', 'revisions', 'units', 'locks'])
    db = FlotillaAgentDynamo(instance_id, service, tables.status,
                             tables.assignments, tables.revisions,
                             tables.units, kms)
    locks = DynamoDbLocks(instance_id, tables.locks)

    # Assemble into agent:
    agent = FlotillaAgent(service, db, locks, systemd, lb)

    # Start loops:
    funcs = [
        RepeatingFunc('health', agent.health, health_interval),
        RepeatingFunc('assignment', agent.assignment, assignment_interval),
    ]
    map(RepeatingFunc.start, funcs)
    logger.info('Startup complete.')
