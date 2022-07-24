from typing import Dict

from hedra.reporting.types.common.types import ReporterTypes


class SnowflakeConfig:
    username: str=None
    password: str=None
    account_id: str=None
    private_key: str=None
    warehouse: str=None
    database: str=None
    schema: str=None
    events_table: str=None
    metrics_table: str=None
    custom_fields: Dict[str, str]={}
    reporter_type: ReporterTypes=ReporterTypes.Snowflake