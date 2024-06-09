from enum import Enum


class ResolvedArgType(Enum):
    URL='URL'
    QUERY='QUERY'
    HEADERS='HEADERS'
    METHOD='METHOD'
    PARAMS='PARAMS'
    AUTH='AUTH'
    DATA='DATA'
    OPTIONS='OPTIONS'