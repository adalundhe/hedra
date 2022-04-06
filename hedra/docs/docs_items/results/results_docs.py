from turtle import ontimer
from .handler_docs import HandlerDocs
from .events_docs import EventsDocs
from .metrics_docs import MetricDocs
from .reporter_docs import ReporterDocs


class ResultsDocs:

    def __init__(self, docs_arg) -> None:
        self.docs_arg = docs_arg
        self.topics = [
            "events",
            "metrics",
            "reporting"
        ]


    def print_docs(self):

        if ":" in self.docs_arg:
            docs_items = self.docs_arg.split(":")

            docs_item = docs_items[1]

            if docs_item == "events":

                events_docs = EventsDocs(docs_items)
                events_docs.print_docs()

            elif docs_item == "metrics":

                metrics_docs = MetricDocs(docs_items)
                metrics_docs.print_docs()

            elif docs_item == 'reporting':
                reporter_docs = ReporterDocs(docs_items)
                reporter_docs.print_docs()

            else:
                print(HandlerDocs.about())

        else:
            print(HandlerDocs.about())