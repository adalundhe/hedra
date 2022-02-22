from hedra.command_line import CommandLine


class ConfigStore:

    def __init__(self):
        self.configs = {}
        self.reporter_configs = {}

    def add_config(self, config_json):
        job_name = config_json.get('job_name')
        self._add_primary_config(job_name, config_json)

        return job_name

    def _add_primary_config(self, job_name, config_json):
        command_line = CommandLine()

        command_line.executor_config = config_json.get('executor_config')
        command_line.actions = config_json.get('actions')
        command_line.reporter_config = config_json.get('reporter_config', {})

        self.configs[job_name] = command_line

    def get(self, job_name):
        config = self.configs.get(job_name)
        reporter_config = self.reporter_configs.get(job_name)
        return config, reporter_config

    def delete_config(self, job):

        if self.configs.get(job):
            del self.configs[job]

        if self.reporter_configs.get(job):
            del self.reporter_configs[job]
