import asyncio
from typing import Coroutine, Dict, Iterator, List, Union
from hedra.core.engines.client.config import Config
from hedra.core.engines.types.task.runner import MercuryTaskRunner
from hedra.core.engines.types.task.task import Task
from hedra.core.engines.types.common.types import RequestTypes
from hedra.core.engines.types.common import Timeouts
from hedra.core.engines.client.store import ActionsStore
from hedra.core.graphs.hooks.types.hook import Hook
from hedra.logging import HedraLogger
from .base_client import BaseClient


class TaskClient(BaseClient):

    def __init__(self, config: Config) -> None:
        super().__init__()

        self.session = MercuryTaskRunner(
            concurrency=config.batch_size,
            timeouts=Timeouts(
                total_timeout=config.request_timeout
            )
        )
        self.request_type = RequestTypes.HTTP
        self.client_type = self.request_type.capitalize()
        self.next_name = None

        self.logger = HedraLogger()
        self.logger.initialize()

    def call(
        self, 
        task: Coroutine, 
        env: str = None,
        user: str = None,
        tags: List[Dict[str, str]] = []
    ):
        task_action = Task(
            self.next_name,
            task,
            source=env,
            user=user,
            tags=tags
        )

        return task_action, self.session