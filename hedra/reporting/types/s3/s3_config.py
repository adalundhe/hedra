from hedra.reporting.types.common.types import ReporterTypes


class S3Config:
    aws_access_key_id: str=None
    aws_secret_access_key: str=None
    region_name: str=None
    reporter_type: ReporterTypes=ReporterTypes.S3