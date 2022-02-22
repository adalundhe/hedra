from zebra_automate_logging import Logger
from hedra.execution.personas.utils import parse_time


class BaseStage:

    def __init__(self, config, persona) -> None:

        logger = Logger()
        self.session_logger = logger.generate_logger()

        self._is_parallel = config.runner_mode.find('parallel') > -1
        self._no_run_visuals = config.executor_config.get('no_run_visuals', False)
        self.total_time = parse_time(
            config.executor_config.get('total_time', 60)
        )

        self.execute_stage = True
        self.selected_persona = persona
        self.config = config.executor_config
        self.actions = self.selected_persona.actions
        self.order = 0
        self.name = None
