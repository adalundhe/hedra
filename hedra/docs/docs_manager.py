from .docs_items import (
    PipelineDocs,
    PersonasDocs,
    OptimizerDocs,
    EngineDocs,
    RunnerDocs,
    TestingDocs,
    CliDocs,
    TopicsDocs,
    ResultsDocs,
    ActionsDocs
)


class DocsManager:

    def __init__(self, docs_arg, config_help_string=None) -> None:
        self.docs_arg = docs_arg

        self.docs_items = {
            "pipelines": PipelineDocs(docs_arg),
            "personas": PersonasDocs(docs_arg),
            "optimizers": OptimizerDocs(docs_arg),
            "engines": EngineDocs(docs_arg),
            "runners": RunnerDocs(docs_arg),
            "results": ResultsDocs(docs_arg),
            "testing": TestingDocs(docs_arg),
            "topics": TopicsDocs(docs_arg),
            "actions": ActionsDocs(docs_arg),
            "cli": CliDocs(docs_arg, config_help_string)
        }

        docs_item = None

        for docs_item_name in self.docs_items:
            if docs_item_name in docs_arg:
                docs_item = self.docs_items.get(docs_item_name)

        
        if docs_item is None:
            docs_item = TopicsDocs(docs_arg)


        self.docs_item = docs_item


    def print_docs(self):
        self.docs_item.print_docs()