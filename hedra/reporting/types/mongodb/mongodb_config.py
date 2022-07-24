from hedra.reporting.types.common.types import ReporterTypes


class MongoDBConfig:
    host: str=None
    username: str=None
    password: str=None
    database: str=None
    events_collection: str=None
    metrics_collection: str=None
    reporter_type: ReporterTypes=ReporterTypes.MongoDB