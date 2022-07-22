import aiopg
import psycopg2
from .types import ConnectionConfig

class Connection:

    def __init__(self, config):
        self.config = ConnectionConfig(config)
        self.pool = None

    async def connect(self) -> None:
        await self.config.parse_connection_config()

    async def execute(self, statement, has_return=False, is_transaction=False) -> list:
        if self.config.has_auth:
            connection = await aiopg.connect(
                host=self.config.postgres_host,
                port=self.config.postgres_port,
                user=self.config.postgres_user,
                password=self.config.postgres_password,
                database=self.config.postgres_db
            )

        else:
            connection = await aiopg.connect(
                host=self.config.postgres_host,
                port=self.config.postgres_port,
                database=self.config.postgres_db
            )

        cursor = await connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        response = []
        error = None
        
        if is_transaction:
            transaction = await cursor.begin()
            try:
                await cursor.execute(statement)
                await transaction.commit()
            except Exception as transaction_exception:
                await transaction.rollback()
                error = transaction_exception

        else:
            await cursor.execute(statement)

        if has_return:
            response =  [dict(record) for record in await cursor.fetchall()]

        await connection.close() 

        if error:
            raise error
            
        return response