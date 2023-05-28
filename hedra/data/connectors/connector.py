from hedra.core.engines.client.config import Config
from typing import Dict, Union, Any, Callable
from .aws_lambda.aws_lambda_connector import AWSLambdaConnector
from .aws_lambda.aws_lambda_connector_config import AWSLambdaConnectorConfig
from .bigtable.bigtable_connector import BigTableConnector
from .bigtable.bigtable_connector_config import BigTableConnectorConfig
from .common.connector_type import ConnectorType

ConnectorConfig = Union[
    AWSLambdaConnectorConfig,
    BigTableConnectorConfig
]


class Connector:

    def __init__(
        self, 
        stage: str,
        connector_config: ConnectorConfig,
        parser_config: Config
    ) -> None:
        self._connectors: Dict[
            ConnectorType,
            Callable[
                [
                    Union[
                        AWSLambdaConnectorConfig,
                        BigTableConnectorConfig
                    ],
                    str,
                    Config
                ],
                Union[
                    AWSLambdaConnector,
                    BigTableConnector
                ]
            ]
        ] = {
            ConnectorType.AWSLambda: lambda config, stage, parser_config: AWSLambdaConnector(
                config,
                stage,
                parser_config
            ),
            ConnectorType.BigTable: lambda config, stage, parser_config: BigTableConnector(
                config,
                stage,
                parser_config
            )
        }   

        self.selected = self._connectors.get(
            connector_config.connector_type
        )(
            connector_config,
            self.stage,
            self.parser_config
        )

        self.stage = stage
        self.parser_config = parser_config
        self.connected = False

    async def connect(self):
        await self.selected.connect()
        self.connected = True

    async def load_actions(
        self,
        options: Dict[str, Any]={}
    ):
        return await self.selected.load_action_data(
            options=options
        )
    
    async def load_data(
        self,
        options: Dict[str, Any]={}
    ):
        return await self.load_data(
            options=options
        )
    
    async def close(self):
        return await self.close()