from typing import Any
from .flag_type import FlagTypes


class Flag:

    def __init__(
            self,
            feature_name: str,
            feature: Any,
            flag_type: FlagTypes,
            exception: Exception,
            enabled: bool=False
        ) -> None:
        self.name = feature_name
        self.feature = feature
        self.type = flag_type
        self.exception = exception
        self.enabled = enabled