import time
from hedra.tools.helpers import check_event_loop
from statserve.server import Server


class EmbeddedStatserve:

    def __init__(self, config):
        self.config = config
        self.server = Server(
            streams=self.config.distributed_config.get(
                'streams',
                [
                    {
                        'stream_name': 'hedra'
                    }
                ]
            ),
            config=self.config.distributed_config
        )
        self.running = False
        self.init_wait = self.config.distributed_config.get('init_wait', 10)

    def run(self):
        self.running = True
        self.server.start(blocking=False)
        time.sleep(self.init_wait)

    def kill(self):
        self.server.stop()