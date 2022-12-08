from enum import Enum


class StageTypes(Enum):
    IDLE='Idle'
    ANALYZE='Analyze'
    CHECKPOINT='Checkpoint'
    EXECUTE='Execute'
    OPTIMIZE='Optimize'
    SETUP='Setup'
    VALIDATE='Validate'
    TEARDOWN='Teardown'
    COMPLETE='Complete'
    SUBMIT='Submit'
    ERROR='Error'
    WAIT='Wait'

