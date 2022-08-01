
import time
from numpy import float32, float64, int16, int32, int64
from typing import Any
from datetime import datetime


class AWSTimestreamRecord:

    def __init__(self, record_type: str, record_name: str, record_stage: str, group_name: str, field_name: str, value: Any, session_uuid: str) -> None:

        measure_value_type = None

        if isinstance(value, (float, float32, float64)):
            measure_value_type = "DOUBLE"

        elif isinstance(value, str):
            measure_value_type = "VARCHAR"
        
        elif isinstance(value, bool):
            measure_value_type = "BOOLEAN"

        elif isinstance(value, (int, int16, int32, int64)):
            measure_value_type = "BIGINT"

        elif isinstance(value, datetime):
            value = int(value.timestamp())
            measure_value_type = "TIMESTAMP"

        self.record_type = record_type
        self.record_name = record_name
        self.field = field_name

        self.time = str(int(round(time.time() * 1000)))
        self.time_unit = 'MILLISECONDS'
        self.dimensions = [
            {
                "Name": "type",
                "Value": record_type
            },
            {
                "Name": "source",
                "Value": record_name
            },
            {
                "Name": "stage",
                "Value": record_stage
            },
            {
                "Name": "group",
                "Value": group_name
            },
            {
                "Name": "field_name", 
                "Value": field_name
            },
            {
                "Name": "session_uuid",
                "Value": session_uuid
            }
        ]
        self.measure_name = f'{record_name}_{field_name}_{session_uuid}'
        self.measure_value = str(value)
        self.measure_value_type = measure_value_type

    def to_dict(self):
        return  {
            "Time": self.time,
            "TimeUnit": self.time_unit,
            "Dimensions": self.dimensions,
            "MeasureName": self.measure_name,
            "MeasureValue": self.measure_value,
            "MeasureValueType": self.measure_value_type
        }
