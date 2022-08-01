
import time
from numpy import float32, float64, int16, int32, int64
from typing import Any
from datetime import datetime


class AWSTimestreamErrorRecord:

    def __init__(self, record_name: str, record_stage: str, error_message: str, count: int, session_uuid: str) -> None:


        self.record_type = 'error_metric'
        self.record_name = record_name

        self.time = str(int(round(time.time() * 1000)))
        self.time_unit = 'MILLISECONDS'
        self.dimensions = [
            {
                "Name": "type",
                "Value": 'error_metric'
            },
            {
                "Name": "name",
                "Value": record_name
            },
            {
                "Name": "stage",
                "Value": record_stage
            },
            {
                "Name": "error_message",
                "Value": error_message
            },
            {
                "Name": "session_uuid",
                "Value": session_uuid
            }
        ]
        self.measure_name = f'{record_name}_errors_{session_uuid}'
        self.measure_value = str(count)
        self.measure_value_type = 'BIGINT'

    def to_dict(self):
        return  {
            "Time": self.time,
            "TimeUnit": self.time_unit,
            "Dimensions": self.dimensions,
            "MeasureName": self.measure_name,
            "MeasureValue": self.measure_value,
            "MeasureValueType": self.measure_value_type
        }
