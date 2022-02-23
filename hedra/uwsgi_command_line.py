from pycli_tools import BaseConfig
from .config.cli.hedra_uwsgi import hedra_uwsgi_cli


class UWSGICommandLine(BaseConfig):

    def __init__(self):
        super(UWSGICommandLine, self).__init__(cli=hedra_uwsgi_cli)
        self.uwsgi_ini_path = None

    def get_uwsgi_config(self):
        self.config_helper.setup()
        self.config_helper.set_cli()
        self.config_helper.parse_cli()
        self.uwsgi_ini_path = self.config_helper['uwsgi_ini_path']
