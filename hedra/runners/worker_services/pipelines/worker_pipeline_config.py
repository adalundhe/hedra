class WorkerPipelineConfig:
    def __init__(self):
        self.executor_config = {}
        self.distributed_config = {}
        self.reporter_config = {}
        self.actions = {}
        self.as_server = False
        self.runner_mode = 'local'
        self.embedded_stats = False
        self.log_level = 'info'