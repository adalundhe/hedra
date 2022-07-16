from hedra.reporting.metrics.metric import Metric


class MetricDocs:

    def __init__(self, docs_arg) -> None:
        self.docs_arg = docs_arg


    def print_docs(self):

        if len(self.docs_arg) > 2:
            metric_docs = Metric.metric_types.get(self.docs_arg[2])

            if metric_docs is None:
                print(Metric.about())

            else:
                print(metric_docs.about())

        
        else:
            print(Metric.about())
