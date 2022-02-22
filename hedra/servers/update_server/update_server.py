import os
import json
from flask import Flask
from multiprocessing import Process
from .services import UpdateService


class UpdateServer:

    def __init__(self, config=None):
        self.app = Flask(__name__)
        self.running = False
        self.update_service = UpdateService(config)
    
    def get_app(self):
        return self.app

    def run(self, host=None, port=6672):
        self.running = True
        self.app.run(host=host, port=port)

    def run_local(self, host=None, port=6672):
        self.running = True
        self.setup()
        self.app.run(host=host, port=port)

    def setup(self):
        for endpoint_config, endpoint_handler in self.update_service:   
            self.app.route(
                endpoint_config.get('path'),
                methods=endpoint_config.get('methods')
            )(endpoint_handler)
        
