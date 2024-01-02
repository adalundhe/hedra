from enum import Enum


class ResolvedArgType(Enum):
    URL='URL'
    QUERY='QUERY'
    HEADERS='HEADERS'
    PARAMS='PARAMS'
    AUTH='AUTH'
    DATA='DATA'
    OPTIONS='OPTIONS'