from __future__ import annotations
from .documents import Documents
from .utils.query_tools import (
    assemble_filter,
    assemble_join,
    assemble_sort,
    assemble_limit
)

class Query:

    def __init__(self, query):
        self.query = query.get('query')
        self.pipeline = query.get('pipeline', [])
        self.collection = query.get('collection')
        self.type = query.get('type')
        self.options = query.get('options', {})
        self.documents = Documents(query.get('documents'))

        self.single_document_query = self.options.get(
            'single',
            self.documents.count == 1
        )
    
    async def assemble_pipeline(self) -> list:
        pipeline = []

        if self.query:
            pipeline.append(self.query)

        for option in self.pipeline:
            
            option_type = option.get('type')
            del option['type']

            if option_type == 'join':
                option_dict = assemble_join(option)
                pipeline.append(option_dict)

            elif option_type == 'filter':
                option_dict = assemble_filter(option)
                pipeline.append(option_dict)

            elif option_type == 'sort':
                option_dict = assemble_sort(option)
                pipeline.append(option_dict)

            elif option_type == 'limit':
                option_dict = assemble_limit(option)
                pipeline.append(option_dict)

        return pipeline

    async def assemble_delete(self) -> dict():

        if self.options.get('filter'):
            return assemble_filter(filter_fields=self.options.get('filter'))

        return self.query


    