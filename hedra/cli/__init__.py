
import click
import atexit
import warnings
import uvloop
from multiprocessing import current_process, active_children

from .base import CLI
uvloop.install()
warnings.simplefilter("ignore")

@click.group(cls=CLI)
def run():


    def stop_processes_at_exit():
        child_processes = active_children()
        for child in child_processes:
            child.close()

        process = current_process()
        if process:
            process.close()

        else:
            print('\n')
 
    atexit.register(stop_processes_at_exit)
