
from enum import Enum


class PluginHooks(Enum):
    ON_CONNECT='ON_CONNECT'
    ON_EXECUTE='ON_EXECUTE'
    ON_CLOSE='ON_CLOSE'