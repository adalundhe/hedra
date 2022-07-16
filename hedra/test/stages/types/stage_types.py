from enum import Enum


class StageTypes(Enum):
    ANALYIZE='Analyze'
    CHECKPOINT='Checkpoint'
    EXECUTE='Execute'
    OPTIMIZE='Optimize'
    SETUP='Setup'
    TEARDOWN='Teardown'

