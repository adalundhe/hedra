import os
from hedra.core.graphs.stages import (
    Submit
)

from hedra.reporting.types import (
    SnowflakeConfig
)


class SubmitSnowflakeResultsStage(Submit):
    config=SnowflakeConfig(
        username=os.getenv('SNOWFLAKE_USERNAME', ''),
        password=os.getenv('SNOWFLAKE_PASSWORD', ''),
        organization_id=os.getenv('SNOWFLAKE_ORGANIZATION', ''),
        account_id=os.getenv('SNOWFLAKE_ACCOUNT_ID', ''),
        private_key=os.getenv('SNOWFLAKE_PRIVATE_KEY', ''),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE', ''),
        database='results',
        database_schema='results',
        events_table='events',
        metrics_table='metrics'
    )