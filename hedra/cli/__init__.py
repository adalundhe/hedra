from .base import CLI
import warnings
warnings.simplefilter("ignore")

import click
from .base import CLI

@click.group(cls=CLI)
def run():
    pass