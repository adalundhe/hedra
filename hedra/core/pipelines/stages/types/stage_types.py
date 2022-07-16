from enum import Enum


class StageTypes(Enum):
    IDLE='Idle'
    ANALYZE='Analyze'
    CHECKPOINT='Checkpoint'
    EXECUTE='Execute'
    OPTIMIZE='Optimize'
    SETUP='Setup'
    TEARDOWN='Teardown'
    COMPLETE='Complete'
    ERROR='Error'

