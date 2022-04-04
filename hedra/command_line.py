import sys
from easy_logger import Logger
from pycli_tools import BaseConfig
from pycli_tools.arguments.bundler import Bundler
from importlib.metadata import version

from hedra.testing import Test

from .config.cli.hedra import hedra_cli
from .docs import DocsManager

class CommandLine(BaseConfig):
    def __init__(self):
        super(CommandLine, self).__init__(cli=hedra_cli, package_version=version('hedra'))
        self.jobs_config = {}
        self.distributed_config = {}
        self.executor_config = {}
        self.reporter_config = {}
        self.actions = {}
        self.as_server = False
        self.runner_mode = 'local'
        self.embedded_stats = False
        self.log_level = 'info'

    def execute_cli(self):

        if "--about" == sys.argv[-1]:
            sys.argv.append("topics")

        self.config_helper.setup()
        self.config_helper.set_cli()
        self.config_helper.parse_cli()

    def copy(self, cli):
        copy_result = super().copy(cli)
        copy_result.executor_config['actions_code_filepath'] = self.executor_config.get('actions_code_filepath')
        
        return copy_result

    def generate_config(self, is_server=False):

        if self.config_helper["about"]:

            docs_arg = self.config_helper["about"]

            config_help_string = self.config_helper.generate_help_string()
            docs_manager = DocsManager(docs_arg, config_help_string=config_help_string)

            docs_manager.print_docs()
            
            exit(0)
        
        self.config_helper.merge('config', 'executor_config', sub_key='executor_config')
        self.config_helper.merge('config', 'distributed_config', sub_key='distributed_config')
        self.config_helper.merge('config', 'jobs_config', sub_key='jobs_config')
        self.config_helper.merge('config', 'reporter_config', sub_key='reporter_config')

        self.embedded_stats = self.config_helper['embedded_stats']

        self.runner_mode = self.config_helper['runner_mode']
        self.log_level = self.config_helper['log_level']

        jobs_config = self.config_helper['jobs_config']
        if jobs_config:
            self.jobs_config.update(jobs_config)

        distributed_config = self.config_helper['distributed_config']
        if distributed_config:
            self.distributed_config.update(distributed_config)
        
        executor_config = self.config_helper['executor_config']
        if executor_config:
            self.executor_config.update(executor_config)

        reporter_config = self.config_helper['reporter_config']
        if reporter_config:
            self.reporter_config = self.config_helper['reporter_config']
            
        self.actions = self.config_helper['actions']

        code_actions = self.config_helper['code_actions']
        if code_actions and len(code_actions) > 0:

            config_map = self.config_helper._assembler.mapped.get('code_actions')
            if config_map.argument:
                bundler = Bundler(options={
                    'class_type': Test,
                    'package_name': config_map.argument.original_arg
                })

                discovered = bundler.discover()

                if len(discovered) > 0:
                    test_config = discovered.pop()()
                    self.executor_config.update(test_config.executor_config)
                    self.reporter_config.update(test_config.reporter_config)
                    self.embedded_stats = test_config.embedded_stats
                    self.log_level = test_config.log_level
                    self.runner_mode = test_config.runner_mode

                self.executor_config['actions_code_filepath'] = config_map.argument.original_arg

            self.actions = code_actions
            self.executor_config['engine_type'] = 'action-set'

        logger = Logger()
        session_logger = logger.generate_logger('hedra')

        if self.as_server is False and len(self.executor_config) == 0:
            session_logger.error('Error: No config JSON or CLI args specified.')
            exit(0)