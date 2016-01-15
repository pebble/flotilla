import click
from .agent import agent_cmd
from .init import init_cmd
from .keys import keys_cmd
from .region import region_cmd
from .revision import revision_cmd
from .scheduler import scheduler_cmd
from .service import service_cmd
from .user import user_cmd

commands = (agent_cmd, init_cmd, keys_cmd, region_cmd, revision_cmd,
            scheduler_cmd, service_cmd, user_cmd)
cli = click.CommandCollection(sources=commands)
