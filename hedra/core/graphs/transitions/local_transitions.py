from hedra.core.graphs.stages.types.stage_types import StageTypes
from .common.transtition_metadata import TransitionMetadata


local_transitions = {

        # State: Idle
        (StageTypes.IDLE, StageTypes.IDLE): TransitionMetadata(allow_multiple_edges=True, is_valid=True),
        (StageTypes.IDLE, StageTypes.SETUP):  TransitionMetadata(allow_multiple_edges=True, is_valid=True),
        (StageTypes.IDLE, StageTypes.OPTIMIZE):  TransitionMetadata(allow_multiple_edges=True, is_valid=False),
        (StageTypes.IDLE, StageTypes.EXECUTE):   TransitionMetadata(allow_multiple_edges=True, is_valid=False),
        (StageTypes.IDLE, StageTypes.ANALYZE):  TransitionMetadata(allow_multiple_edges=True, is_valid=False),
        (StageTypes.IDLE, StageTypes.SUBMIT):  TransitionMetadata(allow_multiple_edges=True, is_valid=False),
        (StageTypes.IDLE, StageTypes.COMPLETE):  TransitionMetadata(allow_multiple_edges=True, is_valid=False),
        (StageTypes.IDLE, StageTypes.ERROR):  TransitionMetadata(allow_multiple_edges=True, is_valid=False),

        # State: Setup
        (StageTypes.SETUP, StageTypes.SETUP):  TransitionMetadata(allow_multiple_edges=False, is_valid=False),
        (StageTypes.SETUP, StageTypes.IDLE):  TransitionMetadata(allow_multiple_edges=False, is_valid=False),
        (StageTypes.SETUP, StageTypes.OPTIMIZE):  TransitionMetadata(allow_multiple_edges=False, is_valid=True),
        (StageTypes.SETUP, StageTypes.EXECUTE):  TransitionMetadata(allow_multiple_edges=False, is_valid=True),
        (StageTypes.SETUP, StageTypes.ANALYZE):  TransitionMetadata(allow_multiple_edges=False, is_valid=False),
        (StageTypes.SETUP, StageTypes.SUBMIT):  TransitionMetadata(allow_multiple_edges=False, is_valid=False),
        (StageTypes.SETUP, StageTypes.COMPLETE):  TransitionMetadata(allow_multiple_edges=False, is_valid=False),
        (StageTypes.SETUP, StageTypes.ERROR):  TransitionMetadata(allow_multiple_edges=False, is_valid=False),

        # State: Optimize
        (StageTypes.OPTIMIZE, StageTypes.OPTIMIZE): TransitionMetadata(allow_multiple_edges=False, is_valid=False),
        (StageTypes.OPTIMIZE, StageTypes.IDLE): TransitionMetadata(allow_multiple_edges=False, is_valid=False),
        (StageTypes.OPTIMIZE, StageTypes.SETUP): TransitionMetadata(allow_multiple_edges=False, is_valid=False),
        (StageTypes.OPTIMIZE, StageTypes.EXECUTE): TransitionMetadata(allow_multiple_edges=False, is_valid=True),
        (StageTypes.OPTIMIZE, StageTypes.ANALYZE): TransitionMetadata(allow_multiple_edges=False, is_valid=False),
        (StageTypes.OPTIMIZE, StageTypes.COMPLETE): TransitionMetadata(allow_multiple_edges=False, is_valid=False),
        (StageTypes.OPTIMIZE, StageTypes.SUBMIT): TransitionMetadata(allow_multiple_edges=False, is_valid=False),
        (StageTypes.OPTIMIZE, StageTypes.ERROR): TransitionMetadata(allow_multiple_edges=False, is_valid=False),

        # State: Execute
        (StageTypes.EXECUTE, StageTypes.EXECUTE): TransitionMetadata(allow_multiple_edges=False, is_valid=True),
        (StageTypes.EXECUTE, StageTypes.IDLE): TransitionMetadata(allow_multiple_edges=False, is_valid=False),
        (StageTypes.EXECUTE, StageTypes.SETUP): TransitionMetadata(allow_multiple_edges=False, is_valid=True),
        (StageTypes.EXECUTE, StageTypes.OPTIMIZE): TransitionMetadata(allow_multiple_edges=False, is_valid=True),
        (StageTypes.EXECUTE, StageTypes.ANALYZE): TransitionMetadata(allow_multiple_edges=False, is_valid=True),
        (StageTypes.EXECUTE, StageTypes.SUBMIT): TransitionMetadata(allow_multiple_edges=False, is_valid=False),
        (StageTypes.EXECUTE, StageTypes.COMPLETE): TransitionMetadata(allow_multiple_edges=False, is_valid=False),
        (StageTypes.EXECUTE, StageTypes.ERROR): TransitionMetadata(allow_multiple_edges=False, is_valid=False),

        # State: Analyze
        (StageTypes.ANALYZE, StageTypes.ANALYZE): TransitionMetadata(allow_multiple_edges=False, is_valid=False),
        (StageTypes.ANALYZE, StageTypes.IDLE): TransitionMetadata(allow_multiple_edges=False, is_valid=False),
        (StageTypes.ANALYZE, StageTypes.SETUP): TransitionMetadata(allow_multiple_edges=False, is_valid=False),
        (StageTypes.ANALYZE, StageTypes.OPTIMIZE): TransitionMetadata(allow_multiple_edges=False, is_valid=False),
        (StageTypes.ANALYZE, StageTypes.EXECUTE): TransitionMetadata(allow_multiple_edges=False, is_valid=False),
        (StageTypes.ANALYZE, StageTypes.SUBMIT): TransitionMetadata(allow_multiple_edges=False, is_valid=True),
        (StageTypes.ANALYZE, StageTypes.COMPLETE): TransitionMetadata(allow_multiple_edges=False, is_valid=False),
        (StageTypes.ANALYZE, StageTypes.ERROR): TransitionMetadata(allow_multiple_edges=False, is_valid=False),

        # State: Submit
        (StageTypes.SUBMIT, StageTypes.SUBMIT): TransitionMetadata(allow_multiple_edges=False, is_valid=True),
        (StageTypes.SUBMIT, StageTypes.IDLE): TransitionMetadata(allow_multiple_edges=False, is_valid=False),
        (StageTypes.SUBMIT, StageTypes.SETUP): TransitionMetadata(allow_multiple_edges=False, is_valid=True),
        (StageTypes.SUBMIT, StageTypes.OPTIMIZE): TransitionMetadata(allow_multiple_edges=False, is_valid=True),
        (StageTypes.SUBMIT, StageTypes.EXECUTE): TransitionMetadata(allow_multiple_edges=False, is_valid=True),
        (StageTypes.SUBMIT, StageTypes.ANALYZE): TransitionMetadata(allow_multiple_edges=False, is_valid=False),
        (StageTypes.SUBMIT, StageTypes.COMPLETE): TransitionMetadata(allow_multiple_edges=False, is_valid=True),
        (StageTypes.SUBMIT, StageTypes.ERROR): TransitionMetadata(allow_multiple_edges=False, is_valid=False),

        # State: Complete
        (StageTypes.COMPLETE, StageTypes.COMPLETE): TransitionMetadata(allow_multiple_edges=False, is_valid=True),
        (StageTypes.COMPLETE, StageTypes.IDLE): TransitionMetadata(allow_multiple_edges=False, is_valid=True),
        (StageTypes.COMPLETE, StageTypes.SETUP): TransitionMetadata(allow_multiple_edges=False, is_valid=True),
        (StageTypes.COMPLETE, StageTypes.OPTIMIZE): TransitionMetadata(allow_multiple_edges=False, is_valid=True),
        (StageTypes.COMPLETE, StageTypes.EXECUTE): TransitionMetadata(allow_multiple_edges=False, is_valid=True),
        (StageTypes.COMPLETE, StageTypes.ANALYZE): TransitionMetadata(allow_multiple_edges=False, is_valid=True),
        (StageTypes.COMPLETE, StageTypes.SUBMIT): TransitionMetadata(allow_multiple_edges=False, is_valid=True),
        (StageTypes.COMPLETE, StageTypes.ERROR): TransitionMetadata(allow_multiple_edges=False, is_valid=False),

        # State: Error
        (StageTypes.ERROR, StageTypes.ERROR): TransitionMetadata(allow_multiple_edges=False, is_valid=False),
        (StageTypes.ERROR, StageTypes.IDLE): TransitionMetadata(allow_multiple_edges=False, is_valid=False),
        (StageTypes.ERROR, StageTypes.SETUP): TransitionMetadata(allow_multiple_edges=False, is_valid=False),
        (StageTypes.ERROR, StageTypes.OPTIMIZE): TransitionMetadata(allow_multiple_edges=False, is_valid=False),
        (StageTypes.ERROR, StageTypes.EXECUTE): TransitionMetadata(allow_multiple_edges=False, is_valid=False),
        (StageTypes.ERROR, StageTypes.ANALYZE): TransitionMetadata(allow_multiple_edges=False, is_valid=False),
        (StageTypes.ERROR, StageTypes.SUBMIT): TransitionMetadata(allow_multiple_edges=False, is_valid=False),
        (StageTypes.ERROR, StageTypes.COMPLETE): TransitionMetadata(allow_multiple_edges=False, is_valid=False)
    }