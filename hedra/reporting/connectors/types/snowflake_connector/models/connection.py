import snowflake.connector
import os
from hedra.reporting.connectors.types.postgres_connector.models import Connection as PostgresConnection
from async_tools.functions import awaitable


class Connection(PostgresConnection):

    def __init__(self, config):
        super(Connection, self).__init__(config)

    async def connect(self) -> None:
        self.connection = snowflake.connector.connect(
            user=self.config.get(
                'snowflake_user', 
                os.getenv('SNOWFLAKE_USER')
            ),
            password=self.config.get(
                'snowflake_password', 
                os.getenv('SNOWFLAKE_PASSWORD')
            ),
            account=self.config.get(
                'snowflake_account', 
                os.getenv('SNOWFLAKE_ACCOUNT')
            ),
            database=self.config.get(
                'snowflake_database', 
                os.getenv('SNOWFLAKE_DATABASE')
            )
        )

        self.cursor = await awaitable(self.connection.cursor)

    
