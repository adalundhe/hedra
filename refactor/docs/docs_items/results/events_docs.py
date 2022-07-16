from hedra.reporting.events.event import Event


class EventsDocs:

    def __init__(self, docs_arg) -> None:
        self.docs_arg = docs_arg

    def print_docs(self):
        print(Event.about())
