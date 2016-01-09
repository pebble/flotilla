#!/usr/bin/env python

from main import setup_logging
from flotilla.cli import cli

if __name__ == '__main__':
    setup_logging()
    cli()
