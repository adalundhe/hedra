class Response:

    def __init__(self, response_data, metric=None):
        self.response_data = response_data     
        self.cleaned_data = {}
        self.metric = metric

    async def format_range_response(self):

        await self._clean_metric_metadata()
        
        for result in self.response_data:

            cleaned_values = []
            metric_name = result.get('metric').get('__name__')

            for result_pair in result.get('values'):
                cleaned_values = [*cleaned_values, *result_pair[1:]]

            self.cleaned_data[metric_name]['values'] = {
                'range': cleaned_values
            }

    async def format_value_response(self):

        await self._clean_metric_metadata()

        for result in self.response_data:

            metric_name = result.get('metric').get('__name__')
            self.cleaned_data[metric_name]['values'] = {
                'last': result.get('value')[1:].pop()
            }

    async def format_aggregation_response(self):
        self.cleaned_data[self.metric.name] = {
            'name': self.metric.name,
            'values': self.response_data
        }

    async def _clean_metric_metadata(self):

        for result in self.response_data:

            metric = dict(result.get('metric'))
            metric_name = metric.get('__name__')

            del metric['__name__']

            self.cleaned_data[metric_name] = {
                'context': {
                    **metric
                }
            } 