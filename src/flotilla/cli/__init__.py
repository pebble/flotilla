import click
from .agent import agent_cmd
from .init import init_cmd
from .region import region_cmd
from .scheduler import scheduler_cmd
from .service import service_cmd

cli = click.CommandCollection(sources=(agent_cmd, init_cmd, region_cmd,
                                       scheduler_cmd, service_cmd))
