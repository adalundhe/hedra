from enum import Enum


class NodeState(Enum):
    FOLLOWER='FOLLOWER'
    CANDIDATE='CANDIDATE'
    LEADER='LEADER'