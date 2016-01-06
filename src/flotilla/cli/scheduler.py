import click
import logging

logger = logging.getLogger('flotilla')


@click.group()
def scheduler_cmd():  # pragma: no cover
    pass


@scheduler_cmd.command()
def scheduler():
    """Start as scheduler."""
    logger.info('Start scheduler')
