import psycopg2
import psycopg2.extras
from .types import ConnectionConfig

class Connection:

    def __init__(self, config):
        self.config = ConnectionConfig(config)
        self.connection = None
        self.cursor = None

    def connect(self) -> None:
        
        self.config.parse_connection_config()

        if self.config.has_auth:
            self.connection = psycopg2.connect(
                host=self.config.postgres_host,
                port=self.config.postgres_port,
                user=self.config.postgres_user,
                password=self.config.postgres_password,
                database=self.config.postgres_db
            )

        else:
            self.connection = psycopg2.connect(
                host=self.config.postgres_host,
                port=self.config.postgres_port,
                database=self.config.postgres_db
            )

        self.cursor = self.connection.cursor(cursor_factory=psycopg2.extras.DictCursor)

    def execute(self, statement, finalize=True) -> None:
        self.cursor.execute(statement)

        if finalize:
            self.connection.commit()

    def commit(self, single=False) -> list:

        self.connection.commit()
        
        if single:
            return self.cursor.fetchone()

        if self.cursor.description:
            column_names = [description[0] for description in self.cursor.description]
            response = [record for record in self.cursor.fetchall()]
            return list(
                map(
                    lambda item: dict(
                        zip(column_names, item)), 
                        response
                    )
                )
        else:
            return []

    def close(self) -> None:
        self.connection.close()