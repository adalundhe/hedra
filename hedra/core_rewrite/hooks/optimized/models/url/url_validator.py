from pydantic import (
    AnyUrl,
    BaseModel,
    IPvAnyAddress,
)


class URLValidator(BaseModel):
    value: AnyUrl | IPvAnyAddress
