import threading
import psutil
import os
import uuid
import asyncio
from async_tools.functions import awaitable
from prometheus_client.exposition import basic_auth_handler
from prometheus_client import (
    CollectorRegistry,
    start_http_server,
    make_wsgi_app,
    push_to_gateway,
)
from prometheus_client.core import REGISTRY
from wsgiref.simple_server import make_server

class Server:

    def __init__(self, config):

        self.server_port = config.get(
            'prometheus_server_port',
            os.getenv('PROMETHEUS_SERVER_PORT', 9090)
        )

        self.pushgateway_host = config.get(
            'prometheus_pushgateway_host',
            os.getenv('PROMETHEUS_PUSHGATEWAY_HOST', 'localhost')
        )

        self.pushgateway_port = config.get(
            'prometheus_pushgateway_port',
            os.getenv('PROMETHEUS_PUSHGATEWAY_PORT', 9091)
        )

        self.prometheus_workers = config.get(
            'prometheus_workers',
            psutil.cpu_count(logical=False)
        )

        self.pushgateway_address = '{pushgateway_host}:{pushgateway_port}'.format(
            pushgateway_host=self.pushgateway_host,
            pushgateway_port=self.pushgateway_port
        )

        self.server_process = None
        self.type = config.get('prometheus_connection_type', 'http')
        self.server = None
        self.running = False
        self._auth = config.get('auth')
        self._job_name = config.get(
            'job_name',
            str(uuid.uuid4())
        )
        self.registry = CollectorRegistry()
        REGISTRY.register(self.registry)

    async def start(self):
    

        if self.type == 'wsgi':
            await self._serve_wsgi()

        elif self.type == 'http':
            await self._serve_http()

    async def stop(self):
        if self.type == 'wsgi':
            await self.server.shutdown()
        
        await self.server_process
        self.running = False

    async def push(self) -> None:

        if self._auth:
            await awaitable(
                push_to_gateway,
                self.pushgateway_address,
                job=self._job_name,
                registry=self.registry,
                handler=self._generate_auth
            )

        else:
            await awaitable(
                push_to_gateway,
                self.pushgateway_address,
                job=self._job_name,
                registry=self.registry
            )

    async def _serve_wsgi(self):
        app = make_wsgi_app()
        self.server = make_server(self.server_host, self.server_port, app)

        self.server_process = asyncio.create_task(
            awaitable(
                self.server.serve_forever
            )
        )

        self.running = True

    async def _serve_http(self):

        self.server_process = asyncio.create_task(
            awaitable(
                start_http_server,
                self.server_port
            )
        )

        self.running = True

    def _generate_auth(self) -> basic_auth_handler:
        return basic_auth_handler(
            self.pushgateway_address,
            self._auth.get('auth_request_method', 'GET'),
            self._auth.get('auth_request_timeout', 60000),
            {
                'Content-Type': 'application/json'
            },
            self._auth.get('auth_request_data'),
            username=self._auth.get(
                'prometheus_username',
                os.getenv('PROMETHEUS_USERNAME')
            ),
            password=self._auth.get(
                'prometheus_password',
                os.getenv('PROMETHEUS_PASSWORD')
            )
        )

