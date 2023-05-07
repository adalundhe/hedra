import uvloop
import asyncio
import os
import inspect
import json
import sys
import importlib
import ntpath
import signal
from multiprocessing import current_process, active_children
from pathlib import Path
from hedra.core.graphs.stages.base.stage import Stage
from hedra.core.graphs import Graph
from hedra.core.graphs.status import GraphStatus
from hedra.versioning.flags.types.base.active import active_flags
from hedra.versioning.flags.types.base.flag_type import FlagTypes
from hedra.logging import (
    HedraLogger,
    LoggerTypes,
    logging_manager
)
from hedra.logging.table.summary_table import SummaryTable
from hedra.logging.table.table_types import GraphExecutionResults
from typing import Dict, Union


uvloop.install()

def run_graph(
    path: str, 
    cpus: int, 
    skip: str,
    show_summaries: str,
    hide_summaries: str,
    log_level: str, 
    logfiles_directory: str,
    bypass_connection_validation: bool,
    connection_validation_retries: int,
    enable_latest: bool,
):

    if enable_latest:
        active_flags[FlagTypes.UNSTABLE_FEATURE] = True

    if logfiles_directory is None:
        logfiles_directory = os.getcwd()

    logging_manager.disable(
        LoggerTypes.DISTRIBUTED,
        LoggerTypes.DISTRIBUTED_FILESYSTEM
    )

    logging_manager.update_log_level(log_level)
    logging_manager.logfiles_directory = logfiles_directory

    if os.path.exists(logfiles_directory) is False:
        os.mkdir(logfiles_directory)

    elif os.path.isdir(logfiles_directory) is False:
        os.remove(logfiles_directory)
        os.mkdir(logfiles_directory)

    logger = HedraLogger()
    logger.initialize()

    graph_name = path
    if os.path.isfile(graph_name):
        graph_name = Path(graph_name).stem

    logger['console'].sync.info(f'Loading graph - {graph_name.capitalize()}\n')

    hedra_config_filepath = os.path.join(
        os.getcwd(),
        '.hedra.json'
    )

    hedra_config = {}
    if os.path.exists(hedra_config_filepath):
        with open(hedra_config_filepath, 'r') as hedra_config_file:
            hedra_config = json.load(hedra_config_file)

    hedra_graphs = hedra_config.get('graphs', {})
    hedra_core_config = hedra_config.get('core', {
        'connection_validation_retries': 3
    })

    hedra_core_config['bypass_connection_validation'] = bypass_connection_validation

    if connection_validation_retries:
        hedra_core_config['connection_validation_retries'] = connection_validation_retries
    
    if path in hedra_graphs:
        path = hedra_graphs.get(path)
    
    package_dir = Path(path).resolve().parent
    package_dir_path = str(package_dir)
    package_dir_module = package_dir_path.split('/')[-1]
    
    package = ntpath.basename(path)
    package_slug = package.split('.')[0]
    spec = importlib.util.spec_from_file_location(f'{package_dir_module}.{package_slug}', path)
    
    if path not in sys.path:
        sys.path.append(str(package_dir.parent))

    module = importlib.util.module_from_spec(spec)

    sys.modules[module.__name__] = module

    spec.loader.exec_module(module)
    
    direct_decendants = list({cls.__name__: cls for cls in Stage.__subclasses__()}.values())

    discovered = {}
    for name, stage_candidate in inspect.getmembers(module):
        if inspect.isclass(
            stage_candidate
        ) and issubclass(
            stage_candidate, 
            Stage
        ) and stage_candidate not in direct_decendants:
            discovered[name] = stage_candidate

    if hedra_graphs.get(graph_name) is None:
        hedra_graphs[graph_name] = module.__file__

    graph_skipped_stages = skip.split(',')
    hedra_config['logging'] = {
        'logfiles_directory': logfiles_directory,
        'log_level': log_level
    }    
    
    with open(hedra_config_filepath, 'w') as hedra_config_file:
        hedra_config['graphs'] = hedra_graphs
        json.dump(hedra_config, hedra_config_file, indent=4)   

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    logger.filesystem.sync.create_logfile('hedra.core.log')
    logger.filesystem.create_filelogger('hedra.core.log')

    graph = Graph(
        graph_name,
        list(discovered.values()),
        config={
            **hedra_core_config,
            'graph_path': path,
            'graph_module': module.__name__,
            'graph_skipped_stages': graph_skipped_stages
        },
        cpus=cpus
    )

    def handle_loop_stop(signame):
        try:
            graph.cleanup()
            child_processes = active_children()
            for child in child_processes:
                child.kill()
                
            process = current_process()
            if process:
                try:
                    process.kill()
                
                except Exception:
                    pass

        except BrokenPipeError:
            logger.console.sync.critical('\n\nAborted.\n')   

        except RuntimeError:
            logger.console.sync.critical('\n\nAborted.\n')

        if len(child_processes) < 1:
            logger.console.sync.critical('\n\nAborted.\n')  
            os._exit(1) 
  
    graph.assemble()
    for signame in ('SIGINT', 'SIGTERM'):
        loop.add_signal_handler(
            getattr(signal, signame),
            lambda signame=signame: handle_loop_stop(signame)
        )


    graph_execution_results: Union[GraphExecutionResults, None] = None

    try:
        graph_execution_results = loop.run_until_complete(graph.run())
        
    except BrokenPipeError:
        pass

    except RuntimeError:
        pass
    
    exit_code = 0

    if graph.status == GraphStatus.FAILED:
        logger.filesystem.sync['hedra.core'].info(f'{graph.metadata_string} - Failed - {graph.logger.spinner.display.total_timer.elapsed_message}\n')
        logger.console.sync.info(f'\nGraph - {graph_name.capitalize()} - failed. {graph.logger.spinner.display.total_timer.elapsed_message}\n')
        exit_code = 1

    elif graph.status == GraphStatus.CANCELLED:
        logger.console.sync.critical('\nAborted.\n') 
        exit_code = 1  

    else:

        if graph_execution_results:
            enabled_summaries = show_summaries.split(',')
            disabled_summaries = hide_summaries.split(',')

            summaries_visibility_config: Dict[str, bool] = {}

            for enabled_summary in enabled_summaries:
                summaries_visibility_config[enabled_summary] = True

            for disabled_summary in disabled_summaries:
                summaries_visibility_config[disabled_summary] = False
                
            summary_table = SummaryTable(
                graph_execution_results,
                summaries_visibility_config=summaries_visibility_config
            )
            
            summary_table.generate_tables()
            summary_table.show_tables()
            
        logger.filesystem.sync['hedra.core'].info(f'{graph.metadata_string} - Completed - {graph.logger.spinner.display.total_timer.elapsed_message}\n')
        logger.console.sync.info(f'\nGraph - {graph_name.capitalize()} - completed! {graph.logger.spinner.display.total_timer.elapsed_message}\n')
    
    if graph.status == GraphStatus.FAILED or graph.status == GraphStatus.COMPLETE:
        child_processes = active_children()
        for child in child_processes:
            child.kill()
            
        process = current_process()
        if process:
            try:
                process.kill()
            
            except Exception:
                pass
            
    graph.cleanup()
    os._exit(exit_code)
