from typing import Optional
from hedra.versioning.flags.types.unstable.flag import unstable


@unstable
class Variant:

    def __init__(
        self,
        stage_name: str,
        weight: Optional[float] = None
    ) -> None:
        self.stage_name = stage_name
        self.weight = weight