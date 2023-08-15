import uuid
from datetime import datetime
from hedra.core.engines.types.common.types import RequestTypes


try:
    from cassandra.cqlengine import columns
    from cassandra.cqlengine.models import Model
    has_connector = True

except Exception:
    columns = object
    Model = None
    has_connector = False


class CassandraWebsocketResultSchema:

    def __init__(self, table_name: str) -> None:
        self.results_columns = {
            'id': columns.UUID(primary_key=True, default=uuid.uuid4),
            'name': columns.Text(min_length=1, index=True),
            'method': columns.Text(min_length=1),
            'url': columns.Text(min_length=1),
            'headers': columns.Text(),
            'data': columns.Text(),
            'error': columns.Text(),
            'status': columns.Integer(),
            'reason': columns.Text(),
            'params': columns.Map(
                columns.Text(),
                columns.Text()
            ),
            'wait_start': columns.Float(),
            'start': columns.Float(),
            'connect_end': columns.Float(),
            'write_end': columns.Float(),
            'complete': columns.Float(),
            'checks': columns.List(
                columns.Text()
            ),
            'user': columns.Text(),
            'tags': columns.List(
                columns.Map(
                    columns.Text(),
                    columns.Text()
                )
            ),
            'created_at': columns.DateTime(default=datetime.now)
        }

        self.results_table = type(
            f'{table_name}_websocket',
            (Model, ), 
            self.results_columns
        )

        self.type = RequestTypes.WEBSOCKET
