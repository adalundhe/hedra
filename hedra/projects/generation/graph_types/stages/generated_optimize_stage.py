from hedra.core.graphs.stages import Optimize


class OptimizeStage(Optimize):
    optimize_iterations=10
    algorithm='shg'
    optimize_params={
        'batch_size': (0.5, 2)
    }