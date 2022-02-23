from __future__ import annotations
import os
import psycopg2
from .models import (
    Connection,
    Statement
)


class PostgresConnector:

    def __init__(self, config):
        self.format = 'postgres'
        self.config = config
        self.connection = Connection(config)
        self.tables = {}
        self._table_types = {}

    def connect(self) -> PostgresConnector:
        self.connection.connect()
        return self
        
    def setup(self, tables, finalize=False) -> PostgresConnector:
        for table_config in tables:
            table = Statement(table_config)

            self._table_types[table.table_statement.table_name] = table.table_statement.field_types

            self.tables[table.table_statement.table_name] = table

            self.connection.execute(
                table.assemble_create_table_statement(),
                finalize=finalize
            )

        return self

    def execute(self, query, finalize=False) -> PostgresConnector:
        table_name = query.get('table')
        field_types = self._table_types.get(table_name, {})

        query = Statement(query, field_types=field_types)

        self.connection.execute(
            query.assemble_query_statement(),
            finalize=finalize
        )
        return self

    def commit(self, single=False) -> list:
        return self.connection.commit(single=single)

    def clear(self, table=None) -> PostgresConnector:
        for table in self.tables.values():
            self.connection.execute(table.assemble_delete_table_statement())
        
        return self

    def close(self) -> PostgresConnector:
        self.connection.close()
        return

