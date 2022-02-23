import subprocess
from .uwsgi_command_line import UWSGICommandLine
from easy_logger import Logger

uwsgi_config = UWSGICommandLine()
def run_uwsgi():
    uwsgi_config.get_uwsgi_config()

    logger = Logger()
    session_logger = logger.generate_logger('hedra')
    session_logger.info('Initializing server mode...')
    subprocess.call([
        'uwsgi --ini {ini_path}'.format(
            ini_path=uwsgi_config.uwsgi_ini_path
        )
    ], shell=True)