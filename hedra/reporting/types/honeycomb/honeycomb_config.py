from pydantic import BaseModel
from hedra.reporting.types.common.types import ReporterTypes


class HoneycombConfig(BaseModel):
    api_key: str
    dataset: str
    reporter_type: ReporterTypes=ReporterTypes.Honeycomb