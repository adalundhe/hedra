from .base import CLI
import signal
import atexit
import warnings
import uvloop
from multiprocessing import current_process, active_children
uvloop.install()
warnings.simplefilter("ignore")

import click
from .base import CLI

@click.group(cls=CLI)
def run():
    def stop_processes(sig, frame):

        child_processes = active_children()
        for child in child_processes:

            try:
                child.kill()
            except Exception:
                pass

            child.close()

        process = current_process()
        if process:
            process.close()

        else:
            print('\n')

    def stop_processes_at_exit():

        child_processes = active_children()
        for child in child_processes:
            child.close()


        process = current_process()
        if process:
            process.close()

        else:
            print('\n')
 
 

    signal.signal(signal.SIGINT, stop_processes)
    atexit.register(stop_processes_at_exit)
