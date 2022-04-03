from hedra.reporting.handlers import Handler
from .reporting_docs import ReportingDocs


class ResultsDocs:

    def __init__(self, docs_arg) -> None:
        self.docs_arg = docs_arg
        self.reporting_docs = ReportingDocs(docs_arg)


    def print_docs(self):
        
        docs_item = None
        if ":" in self.docs_arg:
            docs_items = self.docs_arg.split(":")
            docs_item = docs_items[1]

        if docs_item in self.reporting_docs.topics:
            self.reporting_docs.print_docs()

        else:
            print(Handler.about())