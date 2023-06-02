import re
import traceback
from typing import (
    Callable, 
    Awaitable, 
    Any, 
    Dict, 
    Union, 
    List, 
    Tuple
)
from hedra.core.engines.client.config import Config
from hedra.core.hooks.types.base.hook_type import HookType
from hedra.core.hooks.types.base.hook import Hook
from hedra.data.connectors.aws_lambda.aws_lambda_connector_config import AWSLambdaConnectorConfig
from hedra.data.connectors.bigtable.bigtable_connector_config import BigTableConnectorConfig
from hedra.data.connectors.connector import Connector


RawResultsSet = Dict[str, Dict[str, Union[int, float, List[Any]]]]


class LoadHook(Hook):

    def __init__(
        self, 
        name: str, 
        shortname: str, 
        call: Callable[..., Awaitable[Any]], 
        *names: Tuple[str, ...],
        loader: Union[
            AWSLambdaConnectorConfig,
            BigTableConnectorConfig,
        ]=None, 
        order: int=1,
        skip: bool=False
    ) -> None:
        super().__init__(
            name, 
            shortname, 
            call, 
            order=order,
            skip=skip,
            hook_type=HookType.LOAD
        )

        self.names = list(set(names))
        self.loader_config = loader
        self.parser_config: Union[Config, None] = None
        self.loader: Union[Connector, None] = Connector(
            self.stage,
            self.loader_config,
            self.parser_config
        )

        self._strip_pattern = re.compile('[^a-z]+')
        self.loaded = False

    async def call(self, **kwargs) -> None:

        condition_result = await self._execute_call(**kwargs)

        if self.skip or self.loaded or condition_result is False:
            return kwargs

        if self.loader.connected is False:
            self.loader.selected.stage = self.stage
            self.loader.selected.parser_config = self.parser_config

            await self.loader.connect()

        hook_args = {
            name: value for name, value in kwargs.items() if name in self.params
        }
        
        load_result: Union[Dict[str, Any], Any] = await self._call(**{
            **hook_args,
            'loader': self.loader
        })
        
        await self.loader.close()

        self.loaded = True

        if isinstance(load_result, dict):
            return {
                **kwargs,
                **load_result
            }

        return {
            **kwargs,
            self.shortname: load_result
        }
    
    def copy(self):
        load_hook = LoadHook(
            self.name,
            self.shortname,
            self._call,
            *self.names,
            loader=self.loader_config,
            order=self.order,
            skip=self.skip
        )

        load_hook.stage = self.stage
        load_hook.parser_config = self.parser_config

        return load_hook