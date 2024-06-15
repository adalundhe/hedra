from pydantic import BaseModel, StrictFloat, StrictInt, StrictStr


class FrameLocatorCommand(BaseModel):
    selector: StrictStr
    timeout: StrictInt | StrictFloat