from decimal import Decimal
from pydantic import (
    BaseModel,
    StrictFloat,
    StrictStr,
    StrictInt
)
from hedra.core.personas.streaming.stream_analytics import StreamAnalytics
from typing import Union, List, Dict


class CommonMetrics(BaseModel):
    total: StrictInt
    succeeded: StrictInt
    failed: StrictInt
    aps: StrictFloat


class TotalMetrics(BaseModel):
    total_med: StrictFloat
    total_μ: StrictFloat
    total_var: StrictFloat
    total_std: StrictFloat
    total_min: StrictFloat
    total_max: StrictFloat
    total_q_10: StrictFloat
    total_q_20: StrictFloat
    total_q_30: StrictFloat
    total_q_40: StrictFloat
    total_q_50: StrictFloat
    total_q_60: StrictFloat
    total_q_70: StrictFloat
    total_q_80: StrictFloat
    total_q_90: StrictFloat
    total_q_95: StrictFloat
    total_q_99: StrictFloat


class WaitingMetrics(TotalMetrics):
    waiting_med: StrictFloat
    waiting_μ: StrictFloat
    waiting_var: StrictFloat
    waiting_std: StrictFloat
    waiting_min: StrictFloat
    waiting_max: StrictFloat
    waiting_q_10: StrictFloat
    waiting_q_20: StrictFloat
    waiting_q_30: StrictFloat
    waiting_q_40: StrictFloat
    waiting_q_50: StrictFloat
    waiting_q_60: StrictFloat
    waiting_q_70: StrictFloat
    waiting_q_80: StrictFloat
    waiting_q_90: StrictFloat
    waiting_q_95: StrictFloat
    waiting_q_99: StrictFloat


class ConnectingMetrics(WaitingMetrics):
    connecting_med: StrictFloat
    connecting_μ: StrictFloat
    connecting_var: StrictFloat
    connecting_std: StrictFloat
    connecting_min: StrictFloat
    connecting_max: StrictFloat
    connecting_q_10: StrictFloat
    connecting_q_20: StrictFloat
    connecting_q_30: StrictFloat
    connecting_q_40: StrictFloat
    connecting_q_50: StrictFloat
    connecting_q_60: StrictFloat
    connecting_q_70: StrictFloat
    connecting_q_80: StrictFloat
    connecting_q_90: StrictFloat
    connecting_q_95: StrictFloat
    connecting_q_99: StrictFloat


class WritingMetrics(ConnectingMetrics):
    writing_med: StrictFloat
    writing_μ: StrictFloat
    writing_var: StrictFloat
    writing_std: StrictFloat
    writing_min: StrictFloat
    writing_max: StrictFloat
    writing_q_10: StrictFloat
    writing_q_20: StrictFloat
    writing_q_30: StrictFloat
    writing_q_40: StrictFloat
    writing_q_50: StrictFloat
    writing_q_60: StrictFloat
    writing_q_70: StrictFloat
    writing_q_80: StrictFloat
    writing_q_90: StrictFloat
    writing_q_95: StrictFloat
    writing_q_99: StrictFloat


class ReadingMetrics(WritingMetrics):
    reading_med: StrictFloat
    reading_μ: StrictFloat
    reading_var: StrictFloat
    reading_std: StrictFloat
    reading_min: StrictFloat
    reading_max: StrictFloat
    reading_q_10: StrictFloat
    reading_q_20: StrictFloat
    reading_q_30: StrictFloat
    reading_q_40: StrictFloat
    reading_q_50: StrictFloat
    reading_q_60: StrictFloat
    reading_q_70: StrictFloat
    reading_q_80: StrictFloat
    reading_q_90: StrictFloat
    reading_q_95: StrictFloat
    reading_q_99: StrictFloat


class GroupMetricsSet(ReadingMetrics):

    def get_group(self, group: str) -> Dict[str, Union[int, float]]:
        return {
            key: value for key, value in self.dict().items() if group in key
        }


    @property
    def total(self) -> Dict[str, Union[int, float]]:
        return {
            key: value for key, value in self.dict().items() if 'total' in key
        }


    @property
    def waiting(self) -> Dict[str, Union[int, float]]:
        return {
            key: value for key, value in self.dict().items() if 'waiting' in key
        }

    @property
    def connecting(self) -> Dict[str, Union[int, float]]:
        return {
            key: value for key, value in self.dict().items() if 'connecting' in key
        }
    
    @property
    def reading(self) -> Dict[str, Union[int, float]]:
        return {
            key: value for key, value in self.dict().items() if 'reading' in key
        }
    
    @property
    def writing(self) -> Dict[str, Union[int, float]]:
        return {
            key: value for key, value in self.dict().items() if 'writing' in key
        }
        

class StageMetrics(BaseModel):
    name: StrictStr
    persona: StrictStr
    batch_size: StrictInt
    total: StrictInt
    succeeded: StrictInt
    failed: StrictInt
    aps: StrictFloat
    time: StrictFloat
    μ_sec: Union[StrictFloat, Decimal]
    μ_waiting: Union[StrictFloat, Decimal]
    μ_connecting: Union[StrictFloat, Decimal]
    μ_reading: Union[StrictFloat, Decimal]
    μ_writing: Union[StrictFloat, Decimal]
    streamed_completion_rates: Union[List[float], None]
    streamed_completed: Union[List[float], None]
    streamed_succeeded: Union[List[float], None]
    streamed_failed: Union[List[float], None]
    streamed_batch_timings: Union[List[float], None]