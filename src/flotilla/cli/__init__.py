import click
from .agent import agent_cmd
from .init import init_cmd
from .scheduler import scheduler_cmd

cli = click.CommandCollection(sources=[agent_cmd, scheduler_cmd, init_cmd])
