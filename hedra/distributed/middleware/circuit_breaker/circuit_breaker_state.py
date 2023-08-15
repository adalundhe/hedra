from enum import Enum


class CircuitBreakerState(Enum):
    CLOSED='CLOSED'
    HALF_OPEN='HALF_OPEN'
    OPEN='OPEN'