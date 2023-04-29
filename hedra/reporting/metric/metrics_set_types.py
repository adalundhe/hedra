from pydantic import (
    BaseModel,
    StrictFloat
)


class TotalMetrics(BaseModel):
    total_median: StrictFloat
    total_mean: StrictFloat
    total_variance: StrictFloat
    total_stdev: StrictFloat
    total_minimum: StrictFloat
    total_maximum: StrictFloat
    total_quantile_10th: StrictFloat
    total_quantile_20th: StrictFloat
    total_quantile_30th: StrictFloat
    total_quantile_40th: StrictFloat
    total_quantile_50th: StrictFloat
    total_quantile_60th: StrictFloat
    total_quantile_70th: StrictFloat
    total_quantile_80th: StrictFloat
    total_quantile_90th: StrictFloat
    total_quantile_95th: StrictFloat
    total_quantile_99th: StrictFloat


class WaitingMetrics(TotalMetrics):
    waiting_median: StrictFloat
    waiting_mean: StrictFloat
    waiting_variance: StrictFloat
    waiting_stdev: StrictFloat
    waiting_minimum: StrictFloat
    waiting_maximum: StrictFloat
    waiting_quantile_10th: StrictFloat
    waiting_quantile_20th: StrictFloat
    waiting_quantile_30th: StrictFloat
    waiting_quantile_40th: StrictFloat
    waiting_quantile_50th: StrictFloat
    waiting_quantile_60th: StrictFloat
    waiting_quantile_70th: StrictFloat
    waiting_quantile_80th: StrictFloat
    waiting_quantile_90th: StrictFloat
    waiting_quantile_95th: StrictFloat
    waiting_quantile_99th: StrictFloat


class ConnectingMetrics(WaitingMetrics):
    connecting_median: StrictFloat
    connecting_mean: StrictFloat
    connecting_variance: StrictFloat
    connecting_stdev: StrictFloat
    connecting_minimum: StrictFloat
    connecting_maximum: StrictFloat
    connecting_quantile_10th: StrictFloat
    connecting_quantile_20th: StrictFloat
    connecting_quantile_30th: StrictFloat
    connecting_quantile_40th: StrictFloat
    connecting_quantile_50th: StrictFloat
    connecting_quantile_60th: StrictFloat
    connecting_quantile_70th: StrictFloat
    connecting_quantile_80th: StrictFloat
    connecting_quantile_90th: StrictFloat
    connecting_quantile_95th: StrictFloat
    connecting_quantile_99th: StrictFloat


class WritingMetrics(ConnectingMetrics):
    writing_median: StrictFloat
    writing_mean: StrictFloat
    writing_variance: StrictFloat
    writing_stdev: StrictFloat
    writing_minimum: StrictFloat
    writing_maximum: StrictFloat
    writing_quantile_10th: StrictFloat
    writing_quantile_20th: StrictFloat
    writing_quantile_30th: StrictFloat
    writing_quantile_40th: StrictFloat
    writing_quantile_50th: StrictFloat
    writing_quantile_60th: StrictFloat
    writing_quantile_70th: StrictFloat
    writing_quantile_80th: StrictFloat
    writing_quantile_90th: StrictFloat
    writing_quantile_95th: StrictFloat
    writing_quantile_99th: StrictFloat


class ReadingMetrics(WritingMetrics):
    reading_median: StrictFloat
    reading_mean: StrictFloat
    reading_variance: StrictFloat
    reading_stdev: StrictFloat
    reading_minimum: StrictFloat
    reading_maximum: StrictFloat
    reading_quantile_10th: StrictFloat
    reading_quantile_20th: StrictFloat
    reading_quantile_30th: StrictFloat
    reading_quantile_40th: StrictFloat
    reading_quantile_50th: StrictFloat
    reading_quantile_60th: StrictFloat
    reading_quantile_70th: StrictFloat
    reading_quantile_80th: StrictFloat
    reading_quantile_90th: StrictFloat
    reading_quantile_95th: StrictFloat
    reading_quantile_99th: StrictFloat


class GroupMetricsSet(ReadingMetrics):
    pass