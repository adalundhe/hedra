import asyncio
import functools
import os
import json
import signal
import psutil
import uuid
from typing import (
    List, 
    Dict, 
    Any, 
    Union, 
    Callable,
    Coroutine
)
from concurrent.futures import ThreadPoolExecutor
from hedra.core.engines.client.config import Config
from hedra.core.engines.types.common.results_set import ResultsSet
from hedra.core.hooks.types.action.hook import ActionHook
from hedra.data.connectors.common.connector_type import ConnectorType
from hedra.data.connectors.common.execute_stage_summary_validator import ExecuteStageSummaryValidator
from hedra.data.parsers.parser import Parser
from hedra.logging import HedraLogger
from .cassandra_connector_config import CassandraConnectorConfig
from .cassandra_load_validator import CassandraLoadValidator

def noop_factory():
    pass


try:
    from cassandra.cqlengine import columns
    from cassandra.cqlengine import connection
    from cassandra.query import dict_factory
    from cassandra.cqlengine.query import ModelQuerySet
    from cassandra.cqlengine.models import Model
    from cassandra.cluster import Cluster
    from cassandra.auth import PlainTextAuthProvider
    has_connector = True

except ImportError:
    columns = object
    connection = object
    dict_factory=noop_factory
    ModelQuerySet = object
    Model = object
    Cluster = object
    PlainTextAuthProvider = object
    has_connector = False


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


class CassandraConnector:
    connector_type = ConnectorType.Cassandra

    def __init__(
        self, 
        config: CassandraConnectorConfig,
        stage: str,
        parser_config: Config,
    ) -> None:
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
        self.stage = stage
        self.parser_config = parser_config

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

        self._fields: Dict[str, columns.Column] = {}
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

    async def load_execute_stage_summary(
        self,
        options: Dict[str, Any]={}
    ) -> Coroutine[Any, Any, ExecuteStageSummaryValidator]:
        execute_stage_summary = await self.load_data(
            options=options
        )
        
        return ExecuteStageSummaryValidator(**execute_stage_summary)

    async def load_data(
        self,
        options: Dict[str, Any]={}
    ) -> Coroutine[Any, Any, List[Dict[str, Any]]]:
        
        cassandra_load_request = CassandraLoadValidator(**options)

        if self._table is None:
            
            self._fields.update(**{
                field_name: self._columns_factory.get(
                    field_config.field_type
                )(
                    field_config.options
                ) for field_name, field_config in cassandra_load_request.fields.items()
            })

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
    ) -> Coroutine[Any, Any, List[ActionHook]]:
        
        self._fields = {
            'id': columns.UUID(primary_key=True, default=uuid.uuid4),
            'engine': columns.Text(min_length=1, index=True),
            'name': columns.Text(min_length=1, index=True),
            'result': columns.Blob()
        }

        results = await self.load_data()        

        return await asyncio.gather(*[
            self.parser.parse_action(
                {
                    'name': result_data.get('name'),
                    **json.loads(
                        result_data.get('result', {})
                    )
                },
                self.stage,
                self.parser_config,
                options
            ) for result_data in results
        ])
    
    async def load_results(
        self,
        options: Dict[str, Any]={}
    ) -> Coroutine[Any, Any, ResultsSet]:
        
        self._fields = {
            'id': columns.UUID(primary_key=True, default=uuid.uuid4),
            'engine': columns.Text(min_length=1, index=True),
            'name': columns.Text(min_length=1, index=True),
            'action': columns.Blob()
        }

        results = await self.load_data()

        return ResultsSet({
            'stage_results': await asyncio.gather(*[
                self.parser.parse_result(
                    results_data,
                    self.stage,
                    self.parser_config,
                    options
                ) for results_data in results
            ])
        })
    
    async def close(self):

        await self._loop.run_in_executor(
            self._executor,
            self.cluster.shutdown
        )

        self._executor.shutdown(cancel_futures=True)
    