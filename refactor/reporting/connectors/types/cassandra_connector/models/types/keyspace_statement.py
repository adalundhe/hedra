class KeyspaceStatement:

    def __init__(self, query):
        self.keyspace_name = query.get('name')
        self.keyspace_options = query.get('keyspace_options')

    async def assemble_create_statement(self) -> str:
        keyspace_statement = f'CREATE KEYSPACE {self.keyspace_name}'

        if self.keyspace_options:

            replication_options = self.keyspace_options.get('replication_options')
            durable_writes = self.keyspace_options.get('durable_writes')
            
            if replication_options:
                replication_options=str(self.keyspace_options.get('replication_options'))

                keyspace_statement = f'{keyspace_statement} WITH replication = {replication_options}'

            if durable_writes:
                keyspace_statement = f'{keyspace_statement} AND durable_writes = true'

        return f'{keyspace_statement};'

    async def assemble_delete_statement(self) -> str:
        return f'DROP KEYSPACE {self.keyspace_name};'
                