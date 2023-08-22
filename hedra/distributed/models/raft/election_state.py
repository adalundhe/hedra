from enum import Enum


class ElectionState(Enum):
    ACTIVE='ACTIVE'
    CONFIRMED='CONFIRMED'
    PENDING='PENDING'
    READY='READY'