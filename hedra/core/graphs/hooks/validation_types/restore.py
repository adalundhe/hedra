from pydantic import BaseModel, Field, StrictStr


class RestoreHookValidator(BaseModel):
    key: StrictStr=Field(..., min_length=1)
    restore_filepath: StrictStr=Field(..., min_length=1)


class RestoreValidator:

    def __init__(__pydantic_self__, key: str, restore_filepath: str) -> None:
        RestoreHookValidator(
            key=key,
            restore_filepath=restore_filepath
        )