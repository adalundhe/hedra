import functools
from typing import Tuple, Union
from hedra.core.hooks.types.base.hook_type import HookType
from hedra.core.hooks.types.base.registrar import registrar
from hedra.data.connectors.aws_lambda.aws_lambda_connector_config import AWSLambdaConnectorConfig
from hedra.data.connectors.bigtable.bigtable_connector_config import BigTableConnectorConfig
from .validator import LoadHookValidator


@registrar(HookType.LOAD)
def load(
    *names: Tuple[str, ...], 
    loader: Union[
        AWSLambdaConnectorConfig,
        BigTableConnectorConfig,
    ]=None, 
    order: int=1,
    skip: bool=False
):
    
    LoadHookValidator(
        names=names,
        loader=loader,
        order=order,
        skip=skip
    )
    
    def wrapper(func):

        @functools.wraps(func)
        def decorator(*args, **kwargs):
            return func(*args, **kwargs)

        return decorator

    return wrapper