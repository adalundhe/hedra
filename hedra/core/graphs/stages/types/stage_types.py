from enum import Enum


class StageTypes(Enum):
    IDLE='Idle'
    ANALYZE='Analyze'
    EXECUTE='Execute'
    OPTIMIZE='Optimize'
    SETUP='Setup'
    COMPLETE='Complete'
    SUBMIT='Submit'
    ERROR='Error'

