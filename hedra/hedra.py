from art import text2art
from hedra.runners.bootstrap_services import bootstrap_server
from .command_line import CommandLine
from easy_logger import Logger
from .runners import (
    ParallelLocalWorker,
    LocalWorker,
    EmbeddedStatserve,
    LeaderJobServer,
    DistributedWorkerServer,
    BootstrapServer
)
from .servers import (
    UpdateServer,
    JobServer
)
from async_tools.functions import check_event_loop
from importlib.metadata import version




def run():
    header_text = text2art(f'hedra', font='alligator').strip('\n')
    hedra_version = version('hedra')
    
    command_line = CommandLine()
    logger = Logger()
    command_line.execute_cli()
    command_line.generate_config()


    updates_server = UpdateServer(config=command_line)
    embedded_server = EmbeddedStatserve(command_line)

    logger.setup(command_line.log_level)
    session_logger = logger.generate_logger('hedra')


    check_event_loop(session_logger)

    session_logger.info(f'\n{header_text} {hedra_version}\n\n')

    if command_line.embedded_stats:
        embedded_server.run()
        session_logger.info('\n')

    if command_line.runner_mode == 'ephemeral-leader':
        leader = LeaderJobServer(command_line)
        leader.register()   
        leader.run()
        leader.kill()

    elif command_line.runner_mode == 'ephemeral-worker':
        worker = DistributedWorkerServer(command_line)
        worker.register()
        worker.run()
        worker.kill()

    elif command_line.runner_mode == 'parallel':
        parallel_worker = ParallelLocalWorker(command_line)
        parallel_worker.run()
        parallel_worker.kill()
    
    elif command_line.runner_mode == 'update-server':
        updates_server.run_local()

    elif command_line.runner_mode == 'job-leader':
        leader_job_server = LeaderJobServer(command_line)
        leader_job_server.register()   
        leader_job_server.run()

    elif command_line.runner_mode == 'job-worker':
        job_worker = DistributedWorkerServer(command_line)
        job_worker.register()
        job_worker.run()

    elif command_line.runner_mode == 'job-server':
        job_server = JobServer(command_line)
        job_server.setup()
        job_server.run()
        
    elif command_line.runner_mode == 'bootstrap-server':
        bootstrap_server = BootstrapServer(command_line)
        bootstrap_server.start()
        bootstrap_server.run()

    else:
        worker = LocalWorker(command_line)
        worker.run()
        worker.calculate_results()
        worker.kill()

    if embedded_server.running:
        embedded_server.kill()
