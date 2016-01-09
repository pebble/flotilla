import click
import logging
from more_itertools import unique_everseen
from main import setup_logging, INSTANCE_TYPES, REGIONS, DEFAULT_REGIONS, \
    DEFAULT_ENVIRONMENT

from flotilla.client import RegionMetadata
from flotilla.scheduler import CoreOsAmiIndex, FlotillaCloudFormation

logger = logging.getLogger('flotilla')

CHANNELS = ('stable',
            'beta',
            'alpha')

DEFAULT_INSTANCE_TYPE = 't2.nano'
DEFAULT_CHANNEL = 'stable'
DEFAULT_VERSION = 'current'


@click.group()
def init_cmd():  # pragma: no cover
    pass


@init_cmd.command(help='Bootstrap AWS with scheduler.')
@click.option('--region', '-r', multiple=True, type=click.Choice(REGIONS),
              envvar='FLOTILLA_REGIONS', default=DEFAULT_REGIONS,
              help='Regions (multiple allowed).')
@click.option('--environment', envvar='FLOTILLA_ENV',
              default=DEFAULT_ENVIRONMENT, help='Name of environment.')
@click.option('--domain', envvar='FLOTILLA_DOMAIN', help='Domain name.')
@click.option('--instance-type', type=click.Choice(INSTANCE_TYPES),
              envvar='FLOTILLA_SCHEDULER_TYPE', default=DEFAULT_INSTANCE_TYPE,
              help='Scheduler instance type.')
@click.option('--coreos-channel', type=click.Choice(CHANNELS),
              envvar='FLOTILLA_SCHEDULER_CHANNEL', default=DEFAULT_CHANNEL,
              help='Scheduler CoreOS channel.')
@click.option('--coreos-version', type=click.STRING,
              envvar='FLOTILLA_SCHEDULER_VERSION', default=DEFAULT_VERSION,
              help='Scheduler CoreOS version.')
@click.option('--available', is_flag=True,
              help='Launch scheduler in every region (or just the first).')
def init(region, environment, domain, instance_type, coreos_channel,
         coreos_version, available):  # pragma: no cover
    setup_logging()
    bootstrap(region, environment, domain, instance_type, coreos_channel,
              coreos_version, available)


def bootstrap(region, environment, domain, instance_type, coreos_channel,
              coreos_version, available):
    coreos = CoreOsAmiIndex()
    cloudformation = FlotillaCloudFormation(environment, domain, coreos)
    region_meta = RegionMetadata(environment)
    regions = [r for r in unique_everseen(region)]
    cloudformation.tables(regions)
    region_params = region_meta.store_regions(regions, available, instance_type,
                                              coreos_channel, coreos_version)
    cloudformation.schedulers(region_params)
    logger.info('Bootstrap complete')
