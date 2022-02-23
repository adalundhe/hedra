class Database:

    def __init__(self, connection, config):
        self.database_name = config.get('mongodb_database')
        self.options = config.get('options', {})
        self.client = connection
        self.database = connection[self.database_name]
        self.cursor = []
        self.collections = set()

    async def execute(self, query) -> None:

        self.collections.update(query.collection)

        collection = self.database[query.collection]

        if query.type == 'insert':
            
            if query.single_document_query:
                await collection.insert_one(query.documents.single())
            else:
                await collection.insert_many(query.documents.all())

        elif query.type == 'delete':
            delete_dict = await query.assemble_delete()

            if query.single_document_query:
                await collection.delete_one(delete_dict)    
            else:
                await collection.delete_many(delete_dict)

        else:
            pipeline = await query.assemble_pipeline()
            async for operation in collection.aggregate(pipeline):
                operation['_id'] = str(operation.get('_id'))
                self.cursor.append(operation)
    
    async def commit(self) -> list:
        return list(self.cursor)

    async def clear(self) -> None:

        if self.options.get('remove_collections'):
            for collection_name in self.collections:
                collection = self.database[collection_name]
                await collection.remove()
        else:
            for collection_name in self.collections:
                collection = self.database[collection_name]
                await collection.drop()

        if self.options.get('drop_db_on_clear'):
            await self.database.drop()


