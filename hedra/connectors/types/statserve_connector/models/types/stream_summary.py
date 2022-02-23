from .field_stats_summary import FieldStatsSummary


class StreamSummary:

    def __init__(self, metrics_summary):
        self.stream_fields = []

        field_stats = metrics_summary.get('fieldStats')
        for field in field_stats:
            self.stream_fields.append(
                FieldStatsSummary(field_stats.get(field))
            )

    def to_dict_list(self):
        dict_list = []

        for field_summary in self.stream_fields:
            dict_list.extend(field_summary.to_dict_list())

        return dict_list
