import asyncio
import functools
import os
import signal
import psutil
import uuid
from typing import List, Dict, Any, Union, Callable
from concurrent.futures import ThreadPoolExecutor
from hedra.core.hooks.types.action.hook import ActionHook
from hedra.data.parsers.parser import Parser
from hedra.logging import HedraLogger
from .cassandra_connector_config import CassandraConnectorConfig
from .cassandra_load_validator import CassandraLoadValidator


from cassandra.cqlengine import columns
from cassandra.cqlengine import connection
from datetime import datetime
from cassandra.cqlengine.management import sync_table
from cassandra.cqlengine.models import Model
from cassandra.cqlengine.query import ModelQuerySet
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import dict_factory
has_connector = True


# try:
#     from cassandra.cqlengine import columns
#     from cassandra.cqlengine import connection
#     from datetime import datetime
#     from cassandra.cqlengine.management import sync_table
#     from cassandra.cqlengine.models import Model
#     from cassandra.cluster import Cluster
#     from cassandra.auth import PlainTextAuthProvider
#     has_connector = True

# except Exception:
#     Cluster = None
#     PlainTextAuthProvider = None
#     has_connector = False


def handle_loop_stop(
    signame, 
    executor: ThreadPoolExecutor, 
    loop: asyncio.AbstractEventLoop
): 
    try:
        executor.shutdown(wait=False, cancel_futures=True) 
        loop.stop()
    except Exception:
        pass


class Cassandra:

    def __init__(self, config: CassandraConnectorConfig) -> None:
        self.cluster = None
        self.session = None

        self.hosts = config.hosts
        self.port = config.port or 9042

        self.username = config.username
        self.password = config.password
        self.keyspace = config.keyspace
        
        self.table_name = config.table_name
        
        self.replication_strategy = config.replication_strategy
        self.replication = config.replication       
        self.ssl = config.ssl

        self.session_uuid = str(uuid.uuid4())
        self.metadata_string: str = None
        self.logger = HedraLogger()
        self.logger.initialize()

        self._table: Union[Model, None] = None

        self._executor = ThreadPoolExecutor(max_workers=psutil.cpu_count(logical=False))
        self._loop = asyncio.get_event_loop()

        self.parser = Parser()

        self._primary_column_types = {
            'ascii': columns.Ascii,
            'bigint': columns.BigInt,
            'blob': columns.Blob,
            'boolean': columns.Boolean,
            'date': columns.Date,
            'datetime': columns.DateTime,
            'decimal': columns.Decimal,
            'duration': columns.Duration,
            'uuid': columns.UUID,
            'integer': columns.Integer,
            'ipaddr': columns.Inet,
            'float': columns.Float,
            'map': columns.Map,
            'smallint': columns.SmallInt,
            'text': columns.Text,
            'time': columns.Time,
            'time_uuid': columns.TimeUUID,
            'tinyint': columns.TinyInt,
            'varint': columns.VarInt,
        }

        self._fields: Dict[str, Dict[str, Any]] = {}
        self._columns_factory: Dict[
            str,
            Callable[
                [Dict[str, Any]],
                columns.Column
            ]
        ] = {
            'ascii': lambda column_config: columns.Ascii(
                min_length=column_config.get('min_length', 1),
                **column_config,
            ),
            'bigint': lambda column_config: columns.BigInt(**column_config),
            'blob': lambda column_config: columns.Blob(**column_config),
            'boolean': lambda column_config: columns.Boolean(**column_config),
            'date': lambda column_config: columns.Date(**column_config),
            'datetime': lambda column_config: columns.DateTime(**column_config),
            'decimal': lambda column_config: columns.Decimal(**column_config),
            'duration': lambda column_config: columns.Duration(**column_config),
            'uuid': lambda column_config: columns.UUID(
                primary_key=column_config.get('primary_key', True),
                default=uuid.uuid4,
                **column_config
            ),
            'integer': lambda column_config: columns.Integer(**column_config),
            'ipaddr': lambda column_config: columns.Inet(**column_config),
            'list': lambda column_config: columns.List(
                value_type=self._primary_column_types.get(
                    column_config.get('value_type')
                ),
                **column_config
            ),
            'map': lambda column_config: columns.Map(
                key_type=self._primary_column_types.get(
                    column_config.get('value_type')
                ),
                value_type=self._primary_column_types.get(
                    column_config.get('value_type')
                ),
                **column_config
            ),
            'float': lambda column_config: columns.Float(**column_config),
            'set': lambda column_config: columns.Set(
                value_type=self._primary_column_types.get(
                    column_config.get('value_type')
                ),
                **column_config
            ),
            'smallint': lambda column_config: columns.SmallInt(**column_config),
            'text': lambda column_config: columns.Text(
                min_length=column_config.get('min_length', 1),
                **column_config,
            ),
            'time': lambda column_config: columns.Time(**column_config),
            'time_uuid': lambda column_config: columns.TimeUUID(
                primary_key=column_config.get('primary_key', True),
                default=uuid.uuid1,
                **column_config
            ),
            'tinyint':  lambda column_config: columns.TinyInt(**column_config),
            'tuple': lambda column_config: columns.Tuple(
                **column_config
            ),
            'varint':  lambda column_config: columns.VarInt(**column_config),
        }

    async def connect(self):

        for signame in ('SIGINT', 'SIGTERM', 'SIG_IGN'):
            self._loop.add_signal_handler(
                getattr(signal, signame),
                lambda signame=signame: handle_loop_stop(
                    signame,
                    self._executor,
                    self._loop
                )
            )

        host_port_combinations = ', '.join([
            f'{host}:{self.port}' for host in self.hosts
        ])

        await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Opening amd authorizing connection to Cassandra Cluster at - {host_port_combinations}')
        await self.logger.filesystem.aio['hedra.core'].debug(f'{self.metadata_string} - Opening session - {self.session_uuid}')

        auth = None
        if self.username and self.password:
            auth = PlainTextAuthProvider(self.username, self.password)
        

        self.cluster = Cluster(
            self.hosts,
            port=self.port,
            auth_provider=auth,
            ssl_context=self.ssl
        )

        self.session = await self._loop.run_in_executor(
            None,
            self.cluster.connect
        )

        self.session.row_factory = dict_factory

        await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Connected to Cassandra Cluster at - {host_port_combinations}')

        if self.keyspace is None:
            self.keyspace = 'hedra'

        await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Creating Keyspace - {self.keyspace}')

        keyspace_options = f"'class' : '{self.replication_strategy}', 'replication_factor' : {self.replication}"
        keyspace_query = f"CREATE KEYSPACE IF NOT EXISTS {self.keyspace} WITH REPLICATION = " + "{" + keyspace_options  + "};"

        await self._loop.run_in_executor(
            None,
            self.session.execute,
            keyspace_query
        )

        await self._loop.run_in_executor(
            None,
            self.session.set_keyspace,
            self.keyspace
        )
        if os.getenv('CQLENG_ALLOW_SCHEMA_MANAGEMENT') is None:
            os.environ['CQLENG_ALLOW_SCHEMA_MANAGEMENT'] = '1'

        await self._loop.run_in_executor(
            self._executor,
            functools.partial(
                connection.setup,
                self.hosts, 
                self.keyspace, 
                protocol_version=3
            )
        )

        await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Created Keyspace - {self.keyspace}')

    async def load_data(
        self,
        options: Dict[str, Any]={}
    ) -> List[Dict[str, Any]]:
        
        cassandra_load_request = CassandraLoadValidator(**options)
        self._fields.update(**{
        })

        if self._table is None:
            self._table = type(
                self.table_name.capitalize()
                (Model, ),
                self._fields
            )

        if cassandra_load_request.filters:
            self._table.filter()

            data_rows: ModelQuerySet = await self._loop.run_in_executor(
                self._executor,
                functools.partial(
                    self._table.filter,
                    **cassandra_load_request.filters
                )
            )

        else:
            data_rows: ModelQuerySet = await self._loop.run_in_executor(
                self._executor,
                self._table.all
            )

        if cassandra_load_request.limit:
            data_rows = await self._loop.run_in_executor(
                self._executor,
                functools.partial(
                    data_rows.limit,
                    cassandra_load_request.limit
                )
            )



        return [
            row for row in data_rows
        ]

    async def load_actions(
        self,
        options: Dict[str, Any]={}
    ) -> List[ActionHook]:
        actions = await self.load_data()

        return await asyncio.gather(*[
            self.parser.parse(
                action_data,
                self.stage,
                self.parser_config,
                options
            ) for action_data in actions
        ])
    
    async def close(self):
        self._executor.shutdown()
    