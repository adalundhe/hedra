import os
import json
from flask import Flask
from .services import JobsService


class JobServer:
    
    def __init__(self, config, reporter_config):
        self.app = Flask(__name__)
        self.jobs_service = JobsService(config, reporter_config)

    def get_app(self):
        return self.app

    def run(self, host=None, port=None):
        self.jobs_service.host = host
        self.jobs_service.port = port
        self.app.run(host=host, port=port)

    def setup(self):
        for endpoint_config, endpoint_handler in self.jobs_service:   
            self.app.route(
                endpoint_config.get('path'),
                methods=endpoint_config.get('methods')
            )(endpoint_handler)
