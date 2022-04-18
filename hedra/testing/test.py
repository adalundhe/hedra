import psutil


class Test:
    log_level='info'
    runner_mode='local'
    embedded_stats=False
    engine_type='http'
    persona_type='simple'
    total_time='00:01:00'
    batch_size=1000
    batch_time=1
    batch_count=0
    batch_interval=0
    batch_interval_range=None
    warmup=0
    optimize=0
    optimize_iter_duration=10
    optimizer_type='shgo'
    gradient=0.1
    pool_size=psutil.cpu_count(logical=False)
    no_run_visuals=False
    session_url=None
    request_timeout=60
    reporter_type='statstream'
    reporter_config={
        "save_to_file": True
    }
    options={
        
    }

    def __init__(self) -> None:
        self.executor_config = {
            'engine_type': self.engine_type,
            'batch_interval_range': self.batch_interval_range,
            'session_url': self.session_url,
            'persona_type': self.persona_type,
            'total_time': self.total_time,
            'batch_interval': self.batch_interval,
            'batch_size': self.batch_size,
            'batch_count':self.batch_count,
            'batch_time': self.batch_time,
            'gradient': self.gradient,
            'request_timeout': self.request_timeout,
            'warmup': self.warmup,
            'optimize': self.optimize,
            'optimize_iter_duration': self.optimize_iter_duration,
            'optimizer_type': self.optimizer_type,
            'pool_size': self.pool_size,
            'no_run_visuals': self.no_run_visuals
        }

        self.reporter_config = {
            'reporter_type': self.reporter_type,
            **self.reporter_config
        }
        
    @classmethod
    def about(cls):
        return '''
        Test

        The Test class provides a base class to allow you to write test config as code. Like the 
        ActionSet, you must write a class that inherits the Test class, the specify test configuration 
        as class attributes. For example:

        class MyTest(Test):
            runner_mode='parallel'
            engine='fast-http'
            total_time='00:05:00'
            persona_type='sequence'
            batch_size=5000
            batch_time=10
            warmup=60
            optimize=20
            optimize_iter_duration=10
            optimizer_type='shgo'
            no_run_visuals=True
            pool_size=8

        Specifies that:

            - We want to use the Parallel runner.

            - We want to run a test for 5 minutes using the FastHttp engine and Sequence persona.
        
            - We want batch sizes of 5000 actions per batch with a batch time limit of 10 seconds.

            - We want to execute a warmup of 60 seconds

            - We want to optimize for 20 iterations, 10 seconds per iteration, using the SHGO optimizer .

            - We want to limit UI/graphics to reduce CPU overhead.

            - We want a pool of 8 parallel processes.

        Currently the Test class supports the following config options:

            ### Run Config ###
            - log_level (default: 'info')
            - runner_mode (default: 'local')

            ### Execution Config ###
            - embedded_stats (default: False)
            - engine_type (default: 'http')
            - persona_type (default: 'simple')
            - total_time (default: '00:01:00')
            - batch_size (default: 1000)
            - batch_time (default: 10)
            - batch_count (default: 0)
            - batch_interval (default: 0)
            - batch_interval_range (default: None)
            - warmup (default: 0)
            - optimize (default: 0)
            - optimize_iter_duration (default: 10)
            - optimizer_type (default: 'shgo')
            - gradient (default: 0.1)
            - pool_size (default: <N_PHYSICAL_CPUS> + 2)
            - no_run_visuals (default: false)
            - session_url: (default: None)
            - request_timeout: (default: None)

            ### Reporter Config ###
            - update_connector_type (default: 'statserve')
            - fetch_connector_type (default: 'statserve')
            - submit_connector_type (default: 'statserve')
            - update_config (default: {"stream_config": { "stream_name": "hedra", "fields": {}})
            - fetch_config (default: {"stream_config": { "stream_name": "hedra", "fields": {}})
            - submit_config (default: {"stream_config": { "save_to_file": True, "stream_name": "hedra", "fields": {}})

        We also recommend using the Test class with the:

            @use(<Test>)

        hook to minimize boilerplate code to setup Engine sessions.

        '''