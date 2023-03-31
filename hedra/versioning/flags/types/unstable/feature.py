from typing import Any
from hedra.versioning.flags.types.base.feature import Flag
from hedra.versioning.flags.types.base.flag_type import FlagTypes
from hedra.versioning.flags.exceptions.latest_not_enabled import LatestNotEnabledException


class UnstableFeature(Flag):

    def __init__(
            self,
            feature_name: str,
            feature: Any,
        ) -> None:
        super().__init__(
            feature_name,
            feature,
            FlagTypes.UNSTABLE_FEATURE,
            LatestNotEnabledException,
        )
        