from hedra.reporting import Reporter


class ReportingDocs:


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

                events_about_item = None

                if len(docs_items) > 2:
                    events_about_item = docs_items[2]

                if events_about_item:
                    print(Reporter.about_event_type(events_about_item))

                else:
                    print(Reporter.about_events())

            elif docs_item == "metrics":

                metrics_about_item = None

                if len(docs_items) > 2:
                    metrics_about_item = docs_items[2]

                if metrics_about_item:
                    print(Reporter.about_metric_type(metrics_about_item))

                else:
                    print(Reporter.about_metrics())

            else:

                reporter_about_item = None

                if len(docs_items) > 2:
                    reporter_about_item = docs_items[2]

                if reporter_about_item:
                    print(Reporter.about_reporter(reporter_about_item))

                else:
                    print(Reporter.about())