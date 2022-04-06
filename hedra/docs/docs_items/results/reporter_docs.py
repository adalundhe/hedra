from hedra.reporting.reporters.reporter import Reporter


class ReporterDocs:

    def __init__(self, docs_arg) -> None:
        self.docs_arg = docs_arg


    def print_docs(self):

        if len(self.docs_arg) > 2:
            reporter_docs = Reporter.reporter_types.get(self.docs_arg[2])

            if reporter_docs is None:
                print(Reporter.about())

            else:
                print(reporter_docs.about())

        
        else:
            print(Reporter.about())
