import statistics
from easy_logger import Logger
from hedra.core.pipelines.stages.types.stage_states import StageStates
from hedra.core.pipelines.stages.types.stage_types import StageTypes
from hedra.results import results_types
from .stage import Stage


class Analyze(Stage):
    stage_type=StageTypes.ANALYZE
    is_parallel = False
    handler = None

    def __init__(self) -> None:
        super().__init__()
        logger = Logger()
        self.session_logger = logger.generate_logger('hedra')
        self.raw_results = {}
        
    async def run(self):
        self.state.ANALYZING
        summaries = {}

        for stage_name, stage_results in self.raw_results.items():

            # stage = stages.get(stage_name)
            # if stage.state == StageStates.EXECUTED:
            #     stage.state = StageStates.ANALYZING
            
    
            results =  {}
            stage_total = 0
            for stage_result in stage_results:
                result = results_types.get(stage_result.type)(stage_result)

                if results.get(result.name) is None:
                    results[result.name] = [result]
                
                else:
                    results[result.name].append(result)
            
            grouped_stats = {}
            
            for results_group_name, results_group in results.items():

                timings = [result.time for result in results_group]
        
                quantiles = {
                    (idx + 1)*10: quantile for idx, quantile in enumerate(
                        statistics.quantiles(timings, n=10)
                    )
                }

                group_total = len(results_group)
                grouped_stats[results_group_name] = {
                    'total': group_total,
                    'success': len([result for result in results_group if result.error is None]),
                    'failure': len([result for result in results_group if result.error]),
                    'median': statistics.median(timings),
                    'mean': statistics.mean(timings),
                    'variance': statistics.variance(timings),
                    'stdev': statistics.stdev(timings),
                    'minimum': min(timings),
                    'maximum': max(timings),
                    'quantiles': quantiles
                }

                stage_total += group_total

            summaries[stage_name] = {
                'total': stage_total,
                **grouped_stats
            }
        
        self.state = StageStates.ANALYZED
        return summaries
            