from typing import Any
from hedra.versioning.flags.types.base.feature import Flag
from hedra.versioning.flags.types.base.flag_type import FlagTypes
from hedra.versioning.flags.exceptions.unsafe_not_enabled import UnsafeNotEnabledException


class UnsafeFeature(Flag):

    def __init__(
            self,
            feature_name: str,
            feature: Any
        ) -> None:
        super().__init__(
            feature_name,
            feature,
            FlagTypes.UNSAFE_FEATURE,
            UnsafeNotEnabledException
        )
        