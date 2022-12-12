import click
import os
from typing import List, Union
from importlib.metadata import version
from art import text2art
from hedra.logging import HedraLogger


class CLI(click.MultiCommand):

    command_files = {
        'ping': 'ping.py',
        'graph': 'graph.py',
        'project': 'project.py',
        'cloud': 'cloud.py',
        'plugin': 'plugin.py'
    }
    logger = HedraLogger()

    def list_commands(self, ctx: click.Context) -> List[str]:
        self.logger.initialize()

        header_text = text2art(f'hedra', font='alligator').strip('\n')
        hedra_version = version('hedra')

        self.logger['console'].sync.info(f'\n{header_text} {hedra_version}\n\n')

        rv = []
        for filename in self.command_files.values():
            rv.append(filename[:-3])
        rv.sort()
        return rv

    def get_command(self, ctx: click.Context, name: str) -> Union[click.Command, None]:
        self.logger.initialize()

        header_text = text2art(f'hedra', font='alligator').strip('\n')
        hedra_version = version('hedra')

        self.logger['console'].sync.info(f'\n{header_text} {hedra_version}\n\n')

        ns = {}
        
        command_file = os.path.join(
            os.path.dirname(__file__),
            self.command_files.get(name, 'ping.py')
        )

        with open(command_file) as f:
            code = compile(f.read(), command_file, 'exec')
            eval(code, ns, ns)

        return ns.get(name)