from hedra.data.connectors.aws_lambda.aws_lambda_connector_config import AWSLambdaConnectorConfig
from hedra.data.connectors.bigtable.bigtable_connector_config import BigTableConnectorConfig
from typing import Tuple, Optional, Union


from pydantic import (
    BaseModel, 
    StrictStr, 
    StrictInt,
    StrictBool
)


class LoadHookValidator(BaseModel):
    names: Optional[Tuple[StrictStr, ...]]
    loader: Union[
        AWSLambdaConnectorConfig,
        BigTableConnectorConfig,
    ]
    order: StrictInt
    skip: StrictBool

    class Config:
        arbitrary_types_allowed=True


