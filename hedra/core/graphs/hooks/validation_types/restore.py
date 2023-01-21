from pydantic import BaseModel, Field


class RestoreValidator(BaseModel):
    key: str=Field(..., min_length=1)
    restore_filepath: str=Field(..., min_length=1)

    def __init__(__pydantic_self__, key: str, restore_filepath: str) -> None:
        super().__init__(
            key=key,
            restore_filepath=restore_filepath
        )