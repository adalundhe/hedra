from enum import Enum


class ElectionState(Enum):
    ACCEPTED='ACCEPTED'
    ACTIVE='ACTIVE'
    CONFIRMED='CONFIRMED'
    PENDING='PENDING'
    READY='READY'
    REJECTED='REJECTED'