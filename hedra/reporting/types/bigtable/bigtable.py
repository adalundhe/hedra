import asyncio
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import List, Dict

import psutil
from hedra.logging import HedraLogger
from hedra.reporting.processed_result.types.base_processed_result import BaseProcessedResult
from hedra.reporting.experiment.experiments_collection import ExperimentMetricsCollectionSet
from hedra.reporting.metric.stage_streams_set import StageStreamsSet
from hedra.reporting.metric import (
    MetricsSet
)
from .bigtable_config import BigTableConfig

try:
    from google.cloud import bigtable
    from google.auth import load_credentials_from_file
    has_connector = True

except Exception:
    bigtable = None
    Credentials = None
    has_connector = False


class BigTable:

    def __init__(self, config: BigTableConfig) -> None:

        self.service_account_json_path = config.service_account_json_path

        self.instance_id = config.instance_id
        self.events_table_id = config.events_table
        self.metrics_table_id = config.metrics_table
        self.streams_table_id = config.streams_table

        self.experiments_table_id = config.experiments_table
        self.variants_table_id = f'{config.experiments_table}_variants'
        self.mutations_table_id = f'{config.experiments_table}_mutations'

        self.shared_metrics_table_id = f'{self.metrics_table_id}_metrics'
        self.custom_metrics_table_ids = {}
        self.errors_table_id = f'{self.metrics_table_id}_errors'

        self._executor = ThreadPoolExecutor(max_workers=psutil.cpu_count(logical=False))

        self._events_column_family_id = f'{self.events_table_id}_columns'
        self._metrics_column_family_id = f'{self.metrics_table_id}_columns'
        self._streams_column_family_id = f'{self.streams_table_id}_columns'

        self._experiments_column_family_id = f'{self.experiments_table_id}_columns'
        self._variants_column_family_id = f'{self.experiments_table_id}_variants_columns'
        self._mutations_column_family_id = f'{self.experiments_table_id}_mutations_columns'

        self._shared_metrics_column_family_id = f'{self.metrics_table_id}_shared_columns'
        self._custom_metrics_column_family_id = f'{self.metrics_table_id}_custom_columns'
        self._errors_column_family_id = f'{self.metrics_table_id}_errors_columns'

        self.instance = None

        self.session_uuid = str(uuid.uuid4())
        self.metadata_string: str = None
        self.logger = HedraLogger()
        self.logger.initialize()

        self._events_table = None
        self._stage_metrics_table = None
        self._streams_table = None

        self._experiments_table = None
        self._variants_table = None
        self._mutations_table = None

        self._custom_metrics_tables = {}
        self._metrics_table = None
        self._errors_table = None

        self._experiments_table_columns = None
        self._variants_table_columns = None
        self._mutations_table_columns = None

        self._events_table_columns = None
        self._stage_metrics_table_columns = None
        self._streams_table_columns = None

        self._custom_metrics_table_columns = {}
        self._metrics_table_columns = None
        self._errors_table_columns = None

        self.credentials = None
        self.client = None
        self._loop = asyncio.get_event_loop()

    async def connect(self):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Opening amd authorizing connection to Google Cloud - Loading account config from - {self.service_account_json_path}')
        await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Opening session - {self.session_uuid}')

        credentials, project_id = load_credentials_from_file(self.service_account_json_path)
        self.client = bigtable.Client(
            project=project_id,
            credentials=credentials,
            admin=True
        )
        self.instance = self.client.instance(self.instance_id)

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Opened connection to Google Cloud - Created Client Instance - ID:{self.instance_id}')
        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Opened connection to Google Cloud - Loaded account config from - {self.service_account_json_path}')

    async def submit_streams(self, stream_metrics: Dict[str, StageStreamsSet]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Streams to Table {self.streams_table_id}')
        self._streams_table = self.instance.table(self.streams_table_id)
        
        try:

            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Creating Streams Column Family - {self._streams_column_family_id} - if not exists')
            await self._loop.run_in_executor(
                self._executor,
                self._streams_table.create
            )

            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Created Streams Column Family - {self._streams_column_family_id} - if not exists')

        except Exception:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Skipping creation of Streams Column Family - {self._streams_column_family_id} - if not exists')

        self._streams_table_columns = self._streams_table.column_family(
            self._streams_column_family_id
        )

        try:

            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Creating Streams Column for Column Family - {self._streams_column_family_id} - if not exists')
            await self._loop.run_in_executor(
                self._executor,
                self._streams_table_columns.create
            )

            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Created Streams Column for Column Family - {self._streams_column_family_id} - if not exists')

        except Exception:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Skipping creation of Streams Column for Column Family - {self._streams_column_family_id} - if not exists')

        rows = []
        for stage_name, stream  in stream_metrics.items():
            for group_name, group in stream.grouped.items():
                stream_record = {
                    'name': f'{stage_name}_stream',
                    'stage': stage_name,
                    'group': group_name,
                    **group
                }

                row_key = f'{stage_name}_stream_{str(uuid.uuid4())}'
                row = self._variants_table.direct_row(row_key)

                for field, value in stream_record.items():
                    if not isinstance(value, bytes):
                        value = f'{value}'.encode()

                    row.set_cell(
                        self._variants_column_family_id,
                        field,
                        value,
                        timestamp=datetime.now()
                    )

                rows.append(row)

        await self._loop.run_in_executor(
            self._executor,
            self._variants_table.mutate_rows,
            rows
        )

    async def submit_experiments(self, experiment_metrics: ExperimentMetricsCollectionSet):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Experiments to Table {self.experiments_table_id}')
        self._experiments_table = self.instance.table(self.experiments_table_id)
        
        try:

            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Creating Experiments Column Family - {self._experiments_column_family_id} - if not exists')
            await self._loop.run_in_executor(
                self._executor,
                self._experiments_table.create
            )

            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Created Experiments Column Family - {self._experiments_column_family_id} - if not exists')

        except Exception:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Skipping creation of Experiments Column Family - {self._experiments_column_family_id} - if not exists')

        self._experiments_table_columns = self._experiments_table.column_family(
            self._experiments_column_family_id
        )

        try:

            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Creating Experiments Column for Column Family - {self._experiments_column_family_id} - if not exists')
            await self._loop.run_in_executor(
                self._executor,
                self._experiments_table_columns.create
            )

            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Created Experiments Column for Column Family - {self._experiments_column_family_id} - if not exists')

        except Exception:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Skipping creation of Experiments Column for Column Family - {self._experiments_column_family_id} - if not exists')

        rows = []
        for experiment in experiment_metrics.experiment_summaries:

            row_key = f'{experiment.experiment_name}_{str(uuid.uuid4())}'
            row = self._experiments_table.direct_row(row_key)

            for field, value in experiment.record.items():
                if not isinstance(value, bytes):
                    value = f'{value}'.encode()

                row.set_cell(
                    self._experiments_column_family_id,
                    field,
                    value,
                    timestamp=datetime.now()
                )

            rows.append(row)

        await self._loop.run_in_executor(
            self._executor,
            self._experiments_table.mutate_rows,
            rows
        )

    async def submit_variants(self, experiment_metrics: ExperimentMetricsCollectionSet):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Variants to Table {self.variants_table_id}')
        self._variants_table = self.instance.table(self.variants_table_id)
        
        try:

            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Creating Variants Column Family - {self._variants_column_family_id} - if not exists')
            await self._loop.run_in_executor(
                self._executor,
                self._variants_table.create
            )

            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Created Variants Column Family - {self._variants_column_family_id} - if not exists')

        except Exception:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Skipping creation of Variants Column Family - {self._variants_column_family_id} - if not exists')

        self._variants_table_columns = self._variants_table.column_family(
            self._variants_column_family_id
        )

        try:

            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Creating Variants Column for Column Family - {self._variants_column_family_id} - if not exists')
            await self._loop.run_in_executor(
                self._executor,
                self._variants_table_columns.create
            )

            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Created Variants Column for Column Family - {self._variants_column_family_id} - if not exists')

        except Exception:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Skipping creation of Variants Column for Column Family - {self._variants_column_family_id} - if not exists')

        rows = []
        for variant in experiment_metrics.variant_summaries:

            row_key = f'{variant.variant_name}_{str(uuid.uuid4())}'
            row = self._variants_table.direct_row(row_key)

            for field, value in variant.record:
                if not isinstance(value, bytes):
                    value = f'{value}'.encode()

                row.set_cell(
                    self._variants_column_family_id,
                    field,
                    value,
                    timestamp=datetime.now()
                )

            rows.append(row)

        await self._loop.run_in_executor(
            self._executor,
            self._variants_table.mutate_rows,
            rows
        )

    async def submit_mutations(self, experiment_metrics: ExperimentMetricsCollectionSet):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Mutations to Table {self.mutations_table_id}')
        self._mutations_table = self.instance.table(self.mutations_table_id)
        
        try:

            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Creating Mutations Column Family - {self._mutations_column_family_id} - if not exists')
            await self._loop.run_in_executor(
                self._executor,
                self._mutations_table.create
            )

            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Created Mutations Column Family - {self._mutations_column_family_id} - if not exists')

        except Exception:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Skipping creation of Mutations Column Family - {self._mutations_column_family_id} - if not exists')

        self._mutations_table_columns = self._mutations_table.column_family(
            self._mutations_column_family_id
        )

        try:

            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Creating Mutations Column for Column Family - {self._mutations_column_family_id} - if not exists')
            await self._loop.run_in_executor(
                self._executor,
                self._mutations_table_columns.create
            )

            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Created Mutations Column for Column Family - {self._mutations_column_family_id} - if not exists')

        except Exception:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Skipping creation of Mutations Column for Column Family - {self._mutations_column_family_id} - if not exists')

        rows = []
        for mutation in experiment_metrics.mutation_summaries:

            row_key = f'{mutation.mutation_name}_{str(uuid.uuid4())}'
            row = self._mutations_table.direct_row(row_key)

            for field, value in mutation.record:
                if not isinstance(value, bytes):
                    value = f'{value}'.encode()

                row.set_cell(
                    self._mutations_column_family_id,
                    field,
                    value,
                    timestamp=datetime.now()
                )

            rows.append(row)

        await self._loop.run_in_executor(
            self._executor,
            self._mutations_table.mutate_rows,
            rows
        )

    async def submit_events(self, events: List[BaseProcessedResult]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Events to Table {self.events_table_id}')
        self._events_table = self.instance.table(self.events_table_id)
        
        try:

            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Creating Events Column Family - {self._events_column_family_id} - if not exists')
            await self._loop.run_in_executor(
                self._executor,
                self._events_table.create
            )

            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Created Events Column Family - {self._events_column_family_id} - if not exists')

        except Exception:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Skipping creation of Events Column Family - {self._events_column_family_id} - if not exists')

        self._events_table_columns = self._events_table.column_family(
            self._events_column_family_id
        )

        try:

            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Creating Events Column for Column Family - {self._events_column_family_id} - if not exists')
            await self._loop.run_in_executor(
                self._executor,
                self._events_table_columns.create
            )

            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Created Events Column for Column Family - {self._events_column_family_id} - if not exists')

        except Exception:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Skipping creation of Events Column for Column Family - {self._events_column_family_id} - if not exists')

        rows = []
        for event in events:
            row_key = f'{event.name}_{str(uuid.uuid4())}'
            row = self._events_table.direct_row(row_key)

            for field, value in event.record.items():
                if not isinstance(value, bytes):
                    value = f'{value}'.encode()

                row.set_cell(
                    self._events_column_family_id,
                    field,
                    value,
                    timestamp=datetime.now()
                )

            rows.append(row)

        await self._loop.run_in_executor(
            self._executor,
            self._events_table.mutate_rows,
            rows
        )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Events to Table {self.events_table_id}')

    async def submit_common(self, metrics_sets: List[MetricsSet]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Shared Metrics to Table {self.shared_metrics_table_id}')

        stage_metrics_table = self.instance.table(self.shared_metrics_table_id)
            
        try:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Creating Shared Metrics Column Family - {self._shared_metrics_column_family_id} - if not exists')
            await self._loop.run_in_executor(
                self._executor,
                stage_metrics_table.create
            )

            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Created Shared Metrics Column Family - {self._shared_metrics_column_family_id} - if not exists')

        except Exception:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Skipping creation of Shared Metrics Column Family - {self._shared_metrics_column_family_id} - if not exists')

        self._metrics_table_columns = stage_metrics_table.column_family(
            self._shared_metrics_column_family_id
        )

        
        try:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Creating Shared Metrics Column for Column Family - {self._shared_metrics_column_family_id} - if not exists')
            await self._loop.run_in_executor(
                self._executor,
                self._metrics_table_columns.create
            )

            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Created Shared Metrics Column for Column Family - {self._shared_metrics_column_family_id} - if not exists')

        except Exception:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Skipping creation of Shared Metrics Column for Column Family - {self._shared_metrics_column_family_id} - if not exists')


        rows = []
        for metrics_set in metrics_sets:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Shared Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}')

            row_key = f'{self.shared_metrics_table_id}_{str(uuid.uuid4())}'
            row = stage_metrics_table.direct_row(row_key)

            stage_metrics_record = {
                'name': metrics_set.name,
                'stage': metrics_set.stage,
                'group': 'common',
                **metrics_set.common_stats
            }

            for field, value in stage_metrics_record.items():
                if not isinstance(value, bytes):
                    value = f'{value}'.encode()

                row.set_cell(
                    self._metrics_column_family_id,
                    field,
                    value,
                    timestamp=datetime.now()
                )

            rows.append(row)

        await self._loop.run_in_executor(
            self._executor,
            stage_metrics_table.mutate_rows,
            rows
        )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Shared Metrics to Table {self.shared_metrics_table_id}')
    
    async def submit_metrics(self, metrics: List[MetricsSet]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Metrics to Table {self.metrics_table_id}')

        metrics_table = self.instance.table(self.metrics_table_id)
            
        try:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Creating Metrics Column Family - {self._metrics_column_family_id} - if not exists')
            await self._loop.run_in_executor(
                self._executor,
                metrics_table.create
            )

            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Created Metrics Column Family - {self._metrics_column_family_id} - if not exists')

        except Exception:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Skipping creation of Metrics Column Family - {self._metrics_column_family_id} - if not exists')

        self._metrics_table_columns = metrics_table.column_family(
            self._metrics_column_family_id
        )
        
        try:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Creating Metrics Column for Column Family - {self._metrics_column_family_id} - if not exists')
            await self._loop.run_in_executor(
                self._executor,
                self._metrics_table_columns.create
            )

            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Created Metrics Column for Column Family - {self._metrics_column_family_id} - if not exists')

        except Exception:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Skipping creation of Metrics Column for Column Family - {self._metrics_column_family_id} - if not exists')
        
        rows = []
        for metrics_set in metrics:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}')

            for group_name, group in metrics_set.groups.items():
                await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Metrics Group - {group_name}:{group.metrics_group_id}')

                row_key = f'{self.metrics_table_id}_{str(uuid.uuid4())}'
                row = metrics_table.direct_row(row_key)

                record = {
                    'group': group_name,
                    **group.record
                }

                for field, value in record.items():
                    if not isinstance(value, bytes):
                        value = f'{value}'.encode()

                    row.set_cell(
                        self._metrics_column_family_id,
                        field,
                        value,
                        timestamp=datetime.now()
                    )

                rows.append(row)

        await self._loop.run_in_executor(
            self._executor,
            metrics_table.mutate_rows,
            rows
        )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Metrics to Table {self.metrics_table_id}')

    async def submit_custom(self, metrics_sets: List[MetricsSet]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Custom Metrics to Table {self.metrics_table_id}')

        await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Custom Metrics Group - Custom')

        custom_metrics_table = self.instance.table(self.custom_metrics_table_id)

        try:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Creating Custom Metrics Column Family - {self._custom_metrics_column_family_id} - if not exists')
            await self._loop.run_in_executor(
                self._executor,
                custom_metrics_table.create
            )

            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Created Custom Metrics Column Family - {self._custom_metrics_column_family_id} - if not exists')
        
        except Exception:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Skipping creation of Custom Metrics Column Family - {self._custom_metrics_column_family_id} - if not exists')

        custom_metrics_table_columns = custom_metrics_table.column_family(
            self.custom_metrics_table_id
        )

        try:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Creating Custom Metrics Column for Column Family - {self._custom_metrics_column_family_id} - if not exists')
            await self._loop.run_in_executor(
                self._executor,
                custom_metrics_table_columns.create
            )

            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Created Custom Metrics Column for Column Family - {self._custom_metrics_column_family_id} - if not exists')

        except Exception:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Skipping creation of Custom Metrics Column for Column Family - {self._custom_metrics_column_family_id} - if not exists')

        rows = []
        for metrics_set in metrics_sets:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Custom Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}')

            row_key = f'custom_{str(uuid.uuid4())}'
            custom_metrics_table_row = custom_metrics_table.direct_row(row_key)

            custom_metrics_row_data = {
                'name': metrics_set.name,
                'stage': metrics_set.stage,
                'group': 'custom',
                **{
                    metric.metric_shortname: metric.metric_value for metric in metrics_set.custom_metrics.values()
                }
            }

            for field, value in custom_metrics_row_data.items():
                custom_metrics_table_row.set_cell(
                    field,
                    f'{value}'.encode()
                )
                
                rows.append(custom_metrics_table_row)

        await self._loop.run_in_executor(
            self._executor,
            custom_metrics_table.mutate_rows,
            rows
        )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Custom Metrics to Table {self.metrics_table_id}')

    async def submit_errors(self, metrics_sets: List[MetricsSet]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Error Metrics to Table {self.errors_table_id}')

        errors_table = self.instance.table(self.errors_table_id)
            
        try:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Creating Error Metrics Column Family - {self._errors_column_family_id} - if not exists')
            await self._loop.run_in_executor(
                self._executor,
                errors_table.create
            )

            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Created Error Metrics Column Family - {self._errors_column_family_id} - if not exists')

        except Exception:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Skipping creation of Error Metrics Column Family - {self._errors_column_family_id} - if not exists')

        self._errors_table = errors_table

        self._errors_table_columns = errors_table.column_family(
            self._errors_column_family_id
        )

        try:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Creating Error Column for Column Family - {self._errors_column_family_id} - if not exists')
            await self._loop.run_in_executor(
                self._executor,
                self._metrics_table_columns.create
            )

            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Created Error Column for Column Family - {self._errors_column_family_id} - if not exists')

        except Exception:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Skipping creation of Error Column for Column Family - {self._errors_column_family_id} - if not exists')
        
        rows = []
        for metrics_set in metrics_sets:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Error Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}')
            
            for error in metrics_set.errors:

                row_key = f'{self.errors_table_id}_{str(uuid.uuid4())}'
                errors_row = errors_table.direct_row(row_key)

                error_message = error.get('message')
                error_count = error.get('count')

                errors_row.set_cell(
                    self._metrics_column_family_id,
                    'error_type',
                    f'{error_message}'.encode()
                )

                errors_row.set_cell(
                    self._metrics_column_family_id,
                    'error_count',
                    f'{error_count}'.encode()
                )

                rows.append(errors_row)
                
        await self._loop.run_in_executor(
            self._executor,
            errors_table.mutate_rows,
            rows
        )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Error Metrics to Table {self.errors_table_id}')

    async def close(self):
        await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Closing session - {self.session_uuid}')
        await self._loop.run_in_executor(
            self._executor,
            self.client.close
        )


        await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Session Closed - {self.session_uuid}')
        